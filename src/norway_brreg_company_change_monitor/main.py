"""Apify edge: input validation, Dataset/KVS, PPE billing and run health."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from apify import Actor
from pydantic import ValidationError

from common.delivery import begin_delivery, complete_delivery, delivery_already_started
from common.input_fields import split_unknown_input
from common.monitoring import RunMetrics
from common.monitoring.health import operational_summary
from norway_brreg_company_change_monitor.billing import (
    PricingContractError,
    affordable_targets,
)
from norway_brreg_company_change_monitor.lock import MonitorBusyError, MonitorLock
from norway_brreg_company_change_monitor.models import (
    ACTOR_NAME,
    EVENT_COMPANY_MONITORED,
    SOURCE_NAME,
    ActorInput,
    CompanyQuery,
    InvalidCompany,
    OutputStatus,
    validate_organization,
)
from norway_brreg_company_change_monitor.runtime import configure_actor_logging
from norway_brreg_company_change_monitor.service import (
    MonitorPassFailed,
    RunResult,
    TargetOutcome,
    budget_skipped_outcome,
    build_run_result,
    collect_outcomes,
    failed_pass_result,
    invalid_organization_record,
    run_summary,
)
from norway_brreg_company_change_monitor.source import BrregClient
from norway_brreg_company_change_monitor.state import MonitorStateStore

SUMMARY_KEY = "RUN_SUMMARY"


async def main() -> None:
    async with Actor:
        logger = configure_actor_logging()
        if await delivery_already_started(Actor):
            return
        try:
            raw_input, ignored_fields = split_unknown_input(
                await Actor.get_input() or {}, ActorInput
            )
            if ignored_fields:
                Actor.log.warning("Ignoring unknown input fields: %s", ", ".join(ignored_fields))
            actor_input = ActorInput.model_validate(raw_input)
        except (ValidationError, ValueError) as exc:
            await Actor.fail(status_message=f"Invalid input: {_validation_message(exc)}")
            return

        try:
            async with await MonitorLock.open(actor_input.monitor_key):
                failure = await _run(actor_input, logger)
        except MonitorBusyError as exc:
            await Actor.fail(status_message=str(exc))
            return
        if failure:
            await Actor.fail(status_message=failure)


async def _run(actor_input: ActorInput, logger: logging.Logger) -> str | None:
    metrics = RunMetrics(actor=ACTOR_NAME, source=SOURCE_NAME)
    started_at = datetime.now(UTC)
    observed_at = started_at.isoformat(timespec="seconds").replace("+00:00", "Z")

    valid, invalid, duplicates = _prepare(actor_input)
    metrics.increment("watchlist_size", len(actor_input.organization_numbers))
    metrics.increment("invalid_targets", len(invalid))
    metrics.increment("duplicate_targets", duplicates)

    charging_manager = Actor.get_charging_manager()
    pay_per_event = charging_manager.get_pricing_info().is_pay_per_event
    try:
        chargeable = affordable_targets(charging_manager)
    except PricingContractError as exc:
        return f"Pricing is not the contract this Actor implements: {exc}"

    state_store = await MonitorStateStore.open(metrics=metrics, logger=logger)
    previous_states = await state_store.load(actor_input.monitor_key)
    previous_cursor = state_store.cursor_update_id
    if await _recover_delivery(
        state_store, previous_states, previous_cursor, actor_input.monitor_key, observed_at
    ):
        return None

    selected = valid if chargeable is None else valid[:chargeable]
    skipped = valid[len(selected) :]
    outcomes: dict[str, TargetOutcome] = {}
    cutoff_id: int | None = None
    counters: dict[str, int] = {}
    pass_failure: MonitorPassFailed | None = None
    source_requests = 0
    source_retries = 0

    async with BrregClient(logger=logger, metrics=metrics) as client:
        try:
            if selected:
                outcomes, cutoff_id, counters = await collect_outcomes(
                    selected,
                    client=client,
                    previous_states=previous_states,
                    previous_cursor=previous_cursor,
                )
        except MonitorPassFailed as exc:
            pass_failure = exc
        source_requests = client.request_count
        source_retries = client.retry_count

    if pass_failure is not None:
        result = failed_pass_result(
            valid,
            failure=pass_failure,
            previous_states=previous_states,
            previous_cursor=previous_cursor,
            actor_input=actor_input,
            observed_at=observed_at,
        )
    else:
        outcomes.update({query.organization_number: budget_skipped_outcome() for query in skipped})
        result = build_run_result(
            valid,
            outcomes=outcomes,
            previous_states=previous_states,
            previous_cursor=previous_cursor,
            cutoff_id=cutoff_id,
            actor_input=actor_input,
            observed_at=observed_at,
            counters=counters,
            budget_skipped=len(skipped),
        )

    if pass_failure is None and result.state_updates:
        if not state_store.check_capacity(
            actor_input.monitor_key,
            result.state_updates,
            previous_states=previous_states,
            cursor_update_id=result.cursor_after,
            updated_at=observed_at,
            pending_delivery=_delivery_receipt(result, invalid, actor_input, observed_at, {}),
        ):
            return (
                "The monitor state or a delivery row exceeds its storage limit. "
                "No company-check charges or output writes were made; previous state is preserved. "
                "Split this watchlist across monitor keys."
            )

    # Resurrecting this same run may never repeat ambiguous Dataset writes or charges.
    await begin_delivery(Actor)
    await _push_rows(
        result,
        invalid,
        metrics,
        actor_input=actor_input,
        observed_at=observed_at,
        pay_per_event=pay_per_event,
        previous_states=previous_states,
    )

    state_written = pass_failure is None and bool(result.state_updates)
    summary = _build_summary(
        actor_input,
        result,
        invalid,
        metrics,
        logger,
        duplicates=duplicates,
        source_requests=source_requests,
        source_retries=source_retries,
        started_at=started_at,
        state_written=state_written,
    )
    if pass_failure is not None:
        # Nothing is written: the stored cursor and every stored snapshot are the
        # last state this monitor could actually verify.
        await Actor.set_value(SUMMARY_KEY, summary)
        await complete_delivery(Actor)
        return str(pass_failure)

    # Delivery chunks are written before their manifest and cursor are committed
    # together. Recovery sees either the complete new receipt or the old state.
    pending: dict[str, Any] | None = None
    if state_written:
        pending = _delivery_receipt(result, invalid, actor_input, observed_at, summary)
        if not await state_store.save(
            actor_input.monitor_key,
            result.state_updates,
            cursor_update_id=result.cursor_after,
            updated_at=observed_at,
            pending_delivery=pending,
        ):
            raise RuntimeError("Monitor state could not be saved; the previous state is preserved.")
    await Actor.set_value(SUMMARY_KEY, summary)

    if result.protocol_changed:
        await Actor.set_status_message(
            "At least one BRREG record did not match the implemented contract; the affected "
            "organizations are reported as unverified."
        )
    elif result.disagreements:
        await Actor.set_status_message(
            "The official patch and the current record disagreed for at least one organization; "
            "those snapshots were preserved rather than advanced. See RUN_SUMMARY."
        )
    elif any(item.status is OutputStatus.SKIPPED for item in result.results):
        await Actor.set_status_message(
            "Charge limit reached; the update cursor was not advanced. See SKIPPED rows."
        )
    await complete_delivery(Actor)
    if pending is not None:
        if not await state_store.save(
            actor_input.monitor_key,
            result.state_updates,
            cursor_update_id=result.cursor_after,
            updated_at=observed_at,
            pending_delivery={**pending, "phase": "COMPLETE"}
            if Actor.configuration.actor_run_id is not None
            else None,
        ):
            raise RuntimeError("Could not acknowledge durable output; recovery retained.")
    return None


async def _recover_delivery(
    store: MonitorStateStore,
    targets: dict[str, dict[str, Any]],
    cursor: int | None,
    monitor_key: str,
    observed_at: str,
) -> bool:
    """Replay unacknowledged output once per recovery run, without charging.

    The monitor lock excludes concurrent writers. COMPLETE acknowledges durable
    Dataset, summary and state writes, not the platform's later run status.
    """
    pending = store.pending_delivery
    if pending is None or pending["phase"] == "COMPLETE":
        return False

    summary = {
        **pending["summary"],
        "recovery_only": True,
        "recovered_from_run_id": pending["run_id"],
        "charged_targets": 0,
        "source_requests": 0,
        "source_retries": 0,
        "update_chunks": 0,
        "update_pages": 0,
        "billable_targets": 0,
        "run_metrics": {},
        "duration_ms": 0,
        "operational": {
            **pending["summary"]["operational"],
            "sourceState": "SOURCE_NOT_ATTEMPTED",
            "recoveryOnly": True,
        },
    }
    await begin_delivery(Actor)
    if not await store.save(
        monitor_key,
        targets,
        cursor_update_id=cursor,
        updated_at=observed_at,
        pending_delivery={
            **pending,
            "run_id": Actor.configuration.actor_run_id,
            "summary": summary,
        },
    ):
        raise RuntimeError("Could not persist recovery ownership; previous delivery retained.")
    for row in pending["rows"]:
        await Actor.push_data(row)
    await Actor.set_value(SUMMARY_KEY, summary)
    await Actor.set_status_message(
        "Recovered saved observations without new company checks or company-check charges. "
        "Run again for a fresh BRREG check."
    )
    await complete_delivery(Actor)
    if not await store.save(
        monitor_key,
        targets,
        cursor_update_id=cursor,
        updated_at=observed_at,
        pending_delivery={
            **pending,
            "run_id": Actor.configuration.actor_run_id,
            "summary": summary,
            "phase": "COMPLETE",
        }
        if Actor.configuration.actor_run_id is not None
        else None,
    ):
        raise RuntimeError("Could not acknowledge recovered output; delivery retained.")
    return True


def _delivery_receipt(
    result: RunResult,
    invalid: list[InvalidCompany],
    actor_input: ActorInput,
    observed_at: str,
    summary: dict[str, Any],
) -> dict[str, Any]:
    rows = [(item.query.index, item.record) for item in result.results if item.emit]
    rows.extend(
        (
            item.index,
            invalid_organization_record(
                item, observed_at=observed_at, monitor_key=actor_input.monitor_key
            ),
        )
        for item in invalid
    )
    return {
        "version": 1,
        "phase": "PENDING",
        "run_id": Actor.configuration.actor_run_id,
        "rows": [row for _, row in sorted(rows, key=lambda entry: entry[0])],
        "summary": summary,
    }


def _prepare(actor_input: ActorInput) -> tuple[list[CompanyQuery], list[InvalidCompany], int]:
    """Validate each entry in isolation and drop repeats of the same organization.

    One malformed entry must not deny the rest their answer - and BRREG rejects a
    whole filter chunk containing one - and the same company listed twice is one
    company: monitored once, charged once, one stored snapshot.
    """
    valid: list[CompanyQuery] = []
    invalid: list[InvalidCompany] = []
    seen: set[str] = set()
    duplicates = 0
    for index, raw in enumerate(actor_input.organization_numbers):
        prepared = validate_organization(raw, index=index)
        if isinstance(prepared, InvalidCompany):
            invalid.append(prepared)
        elif prepared.organization_number in seen:
            duplicates += 1
        else:
            seen.add(prepared.organization_number)
            valid.append(prepared)
    return valid, invalid, duplicates


async def _push_rows(
    result: RunResult,
    invalid: list[InvalidCompany],
    metrics: RunMetrics,
    *,
    actor_input: ActorInput,
    observed_at: str,
    pay_per_event: bool,
    previous_states: dict[str, dict[str, Any]],
) -> None:
    """Emit rows in input order: a caller's entry N is their organization N."""
    invalid_rows = {
        item.index: invalid_organization_record(
            item, observed_at=observed_at, monitor_key=actor_input.monitor_key
        )
        for item in invalid
    }
    emitted = {item.query.index: position for position, item in enumerate(result.results)}
    for index in sorted(set(invalid_rows) | set(emitted)):
        position = emitted.get(index)
        if position is None:
            row = invalid_rows[index]
            await Actor.push_data(row)
            metrics.record_outcome(str(row["status"]))
            metrics.record_error(str(row["error"]["code"]))
            metrics.increment("dataset_write_count")
            continue
        item = result.results[position]
        if item.billable and pay_per_event:
            # Charge before publishing, never after: a refused charge must leave
            # a SKIPPED row rather than an unpaid registry record the caller can
            # already read. A completed monitoring pass over one organization is
            # the product, so an unchanged company that changesOnly does not
            # write is still charged - once.
            charge = await Actor.charge(EVENT_COMPANY_MONITORED, count=1)
            metrics.increment("company_charged_count", charge.charged_count)
            if charge.charged_count != 1:
                _skip_target(
                    result,
                    position,
                    previous_states,
                    actor_input=actor_input,
                    observed_at=observed_at,
                )
                item = result.results[position]
        metrics.record_outcome(str(item.status))
        if item.record.get("error"):
            metrics.record_error(str(item.record["error"]["code"]))
        for change_type in item.record["change_types"]:
            metrics.increment(f"change_{change_type}")
        if not item.emit:
            metrics.increment("suppressed_records")
            continue
        await Actor.push_data(item.record)
        metrics.increment("dataset_write_count")


def _skip_target(
    result: RunResult,
    position: int,
    previous_states: dict[str, dict[str, Any]],
    *,
    actor_input: ActorInput,
    observed_at: str,
) -> None:
    """Replace an uncharged observation with a SKIPPED row and restore its state.

    The row is rebuilt from scratch rather than patched, so an organization the
    caller was not charged for never leaves with its registry data, its change
    list or its fingerprint attached. The cursor is pinned back too: an interval
    nobody paid to have covered must be read again next run.
    """
    item = result.results[position]
    result.results[position] = build_run_result(
        [item.query],
        outcomes={item.query.organization_number: budget_skipped_outcome()},
        previous_states={},
        previous_cursor=None,
        cutoff_id=None,
        actor_input=actor_input,
        observed_at=observed_at,
    ).results[0]
    stored = previous_states.get(item.query.organization_number)
    if stored is not None:
        result.state_updates[item.query.organization_number] = stored
    else:
        result.state_updates.pop(item.query.organization_number, None)
    result.cursor_advanced = False
    result.cursor_after = result.cursor_before


def _build_summary(
    actor_input: ActorInput,
    result: RunResult,
    invalid: list[InvalidCompany],
    metrics: RunMetrics,
    logger: logging.Logger,
    *,
    duplicates: int,
    source_requests: int,
    source_retries: int,
    started_at: datetime,
    state_written: bool,
) -> dict[str, Any]:
    metrics.finish()
    summary = run_summary(
        actor_input=actor_input,
        result=result,
        invalid=invalid,
        submitted=len(actor_input.organization_numbers),
        duplicates=duplicates,
        source_requests=source_requests,
        source_retries=source_retries,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        state_written=state_written,
    )
    summary["charged_targets"] = metrics.counters["company_charged_count"]
    summary["run_metrics"] = metrics.summary()
    summary["operational"] = operational_summary(
        # Units of work undertaken, so a benign duplicate in the watchlist does
        # not make a healthy run look partial.
        requested=summary["valid_targets"],
        completed=summary["valid_targets"]
        - summary["failed_targets"]
        - summary["partial_targets"]
        - summary["skipped_targets"],
        failed=summary["failed_targets"],
        invalid=summary["invalid_targets"],
        ambiguous=summary["partial_targets"],
        source_state=_source_state(summary),
        budget_limited=summary["budget_limit_reached"],
        output_writes=summary["emitted_records"],
        duration_seconds=summary["duration_ms"] / 1000,
        partial_source=bool(summary["source_protocol_changed"]),
    )
    logger.info("run summary %s", _loggable(summary))
    return summary


def _source_state(summary: dict[str, Any]) -> str:
    if summary["pass_failure"]:
        return "SOURCE_FAILED"
    if summary["source_requests"] == 0:
        return "SOURCE_NOT_ATTEMPTED"
    return "SOURCE_SUCCESS"


def _loggable(summary: dict[str, Any]) -> dict[str, Any]:
    """Counts only. Watched organization numbers and the monitor key stay out."""
    return {
        key: value
        for key, value in summary.items()
        if key not in {"run_metrics", "monitor_key", "operational"}
    }


def _validation_message(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first.get("loc", ())) or "input"
        return f"{location}: {first.get('msg', 'invalid value')}"
    return str(exc)
