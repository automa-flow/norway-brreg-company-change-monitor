"""Orchestration: a watchlist in, one row per watched organization out.

The whole pipeline is plain data and one injected source client, so baselining,
the cursor barrier, change detection and billing eligibility are all testable
without the Apify platform. The platform edge - Dataset, KVS, charging - lives in
``main``.

The shape of a run:

1. capture the newest update id as the run's start cursor;
2. baseline every target that has no stored snapshot, by entity lookup;
3. capture the newest update id again as the run's cutoff;
4. walk the update stream over ``[stored cursor + 1, cutoff]`` for targets that
   already had a snapshot, and over ``[start + 1, cutoff]`` for targets that were
   just baselined - the barrier that closes the race against the baseline;
5. re-fetch the current record only for organizations that actually moved;
6. diff, emit, and only then advance the cursor to the cutoff.

Nothing in steps 4-6 is partial: if any chunk or page of the interval fails, the
pass is abandoned with the stored cursor and every stored snapshot untouched,
because a cursor advanced over an interval that was not fully read would lose
those changes permanently.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from norway_brreg_company_change_monitor.models import (
    CONCLUSIVE_STATUSES,
    MAX_ORGNR_PER_CHUNK,
    SCHEMA_VERSION,
    SOURCE_NAME,
    ActorInput,
    BaselineMode,
    CompanyQuery,
    InvalidCompany,
    Mode,
    OutputStatus,
    RecordType,
    entity_source_url,
    error_record,
    update_source_id,
)
from norway_brreg_company_change_monitor.monitor import (
    CHANGE_OTHER,
    PARTIAL_MESSAGE,
    PATCH_PATH_FIELDS,
    ChangeDecision,
    detect_changes,
    patch_change_types,
    redact_patch,
    source_change_type,
)
from norway_brreg_company_change_monitor.normalize import (
    EntityValidationError,
    Snapshot,
    absent,
    normalize_entity,
    removed,
)
from norway_brreg_company_change_monitor.source import (
    BrregClient,
    BrregSourceError,
    EntityResult,
    EntityState,
    UpdateEvent,
    UpdateInterval,
    split_chunks,
)

#: A change row carries the official patch as evidence, not as an archive. One
#: BRREG event has at most a few dozen operations; this bounds a pathological one
#: rather than trimming a normal one.
MAX_PATCH_OPERATIONS = 200

BUDGET_MESSAGE = (
    "The run charge limit cannot fund monitoring this organization. It was not checked, its "
    "stored snapshot was not touched, and the update cursor was not advanced."
)
NOT_ATTEMPTED_MESSAGE = "This organization was never requested from the register."
INTERVAL_FAILED_MESSAGE = (
    "The BRREG update interval for this run could not be read completely, so no organization "
    "can be reported as unchanged. The stored cursor and every stored snapshot are preserved; "
    "run again to retry the same interval."
)
RECONCILE_FAILED_MESSAGE = (
    "BRREG reported an update for a watched organization but its current record could not be "
    "fetched. Advancing the cursor would lose that change permanently, so the whole pass was "
    "abandoned with the stored cursor and every stored snapshot preserved."
)


class MonitorPassFailed(Exception):
    """The interval could not be fully covered. Nothing is advanced, nothing is charged."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class TargetOutcome:
    """What this run established about one organization, and nothing more."""

    snapshot: Snapshot | None = None
    events: tuple[UpdateEvent, ...] = ()
    error: dict[str, Any] | None = None
    #: True when the snapshot came from a fresh entity fetch in this run.
    refetched: bool = False


@dataclass(frozen=True, slots=True)
class TargetResult:
    query: CompanyQuery
    record: dict[str, Any]
    status: OutputStatus
    billable: bool
    emit: bool


@dataclass(slots=True)
class RunResult:
    results: list[TargetResult] = field(default_factory=list)
    state_updates: dict[str, dict[str, Any]] = field(default_factory=dict)
    cursor_before: int | None = None
    cursor_after: int | None = None
    cursor_advanced: bool = False
    baseline_targets: int = 0
    update_chunks: int = 0
    update_pages: int = 0
    source_events: int = 0
    changed_targets: int = 0
    refetched_targets: int = 0
    disagreements: int = 0
    protocol_changed: bool = False
    failure: str | None = None


def budget_skipped_outcome() -> TargetOutcome:
    return TargetOutcome(
        error=error_record("BUDGET", "BUDGET_LIMIT", BUDGET_MESSAGE, retryable=False)
    )


async def collect_outcomes(
    queries: list[CompanyQuery],
    *,
    client: BrregClient,
    previous_states: dict[str, dict[str, Any]],
    previous_cursor: int | None,
) -> tuple[dict[str, TargetOutcome], int, dict[str, int]]:
    """Run the baseline, the barrier and the recurring interval. Returns the cutoff.

    Raises :class:`MonitorPassFailed` when the interval could not be fully read,
    because there is no safe partial answer to give.
    """
    counters = {"chunks": 0, "pages": 0, "events": 0, "baselines": 0, "refetched": 0}
    try:
        start_id = await client.latest_update_id()
    except BrregSourceError as exc:
        raise MonitorPassFailed(exc.code, f"{INTERVAL_FAILED_MESSAGE} ({exc})") from exc
    highest_acknowledged = max(
        [
            previous_cursor or 0,
            *(state.get("last_event_id", 0) for state in previous_states.values()),
        ]
    )
    if start_id < highest_acknowledged:
        raise MonitorPassFailed(
            "CURSOR_WENT_BACKWARD",
            f"The newest BRREG update id ({start_id}) is below the last acknowledged id "
            f"({highest_acknowledged}). The stream is not the one this monitor was following, so "
            "nothing is reported and nothing is advanced.",
        )

    # A stored snapshot without a stored cursor cannot be compared safely: the
    # interval it would be compared over is unknown. Re-baseline it instead.
    known_numbers = {
        query.organization_number
        for query in queries
        if previous_cursor is not None and query.organization_number in previous_states
    }
    known = [query for query in queries if query.organization_number in known_numbers]
    fresh = [query for query in queries if query.organization_number not in known_numbers]

    outcomes: dict[str, TargetOutcome] = {}
    if fresh:
        counters["baselines"] = len(fresh)
        entities = await client.fetch_entities([q.organization_number for q in fresh])
        for query in fresh:
            outcomes[query.organization_number] = _entity_outcome(
                entities[query.organization_number]
            )

    cutoff_id = await _cutoff(client, start_id) if fresh else start_id

    intervals: list[tuple[tuple[str, ...], int]] = []
    if known:
        assert previous_cursor is not None
        intervals += [
            (chunk, previous_cursor + 1)
            for chunk in split_chunks([q.organization_number for q in known], MAX_ORGNR_PER_CHUNK)
        ]
    if fresh:
        # The barrier: anything that moved while the snapshots above were being
        # fetched. Without it a change landing mid-baseline would sit behind the
        # advanced cursor forever.
        intervals += [
            (chunk, start_id + 1)
            for chunk in split_chunks([q.organization_number for q in fresh], MAX_ORGNR_PER_CHUNK)
        ]
    counters["chunks"] = len(intervals)

    completed: list[UpdateInterval] = list(
        await asyncio.gather(
            *(
                client.fetch_interval(chunk, from_update_id=start, to_update_id=cutoff_id)
                for chunk, start in intervals
            )
        )
    )
    counters["pages"] = sum(interval.pages for interval in completed)
    failed = next((interval for interval in completed if not interval.succeeded), None)
    if failed is not None:
        detail = (failed.error or {}).get("message", "")
        raise MonitorPassFailed("INTERVAL_INCOMPLETE", f"{INTERVAL_FAILED_MESSAGE} ({detail})")

    events: dict[str, list[UpdateEvent]] = {}
    for interval in completed:
        for event in interval.events:
            # A pinned cursor can replay successful peers of an unverified target.
            # Their snapshots already account for these events, including patches
            # for fields outside the normalized snapshot.
            acknowledged = previous_states.get(event.organization_number, {}).get(
                "last_event_id", -1
            )
            if event.organization_number in known_numbers and event.update_id <= acknowledged:
                continue
            events.setdefault(event.organization_number, []).append(event)
    for series in events.values():
        series.sort(key=lambda event: event.update_id)
    counters["events"] = sum(len(series) for series in events.values())

    moved = [query for query in queries if query.organization_number in events]
    counters["refetched"] = len(moved)
    if moved:
        entities = await client.fetch_entities([q.organization_number for q in moved])
        for query in moved:
            number = query.organization_number
            result = entities[number]
            if not result.succeeded and number in known_numbers:
                # Rule: never advance a cursor past a change whose new state we
                # could not read. One unreadable record fails the whole pass.
                raise MonitorPassFailed(
                    "RECONCILE_FAILED",
                    f"{RECONCILE_FAILED_MESSAGE} ({(result.error or {}).get('message', '')})",
                )
            outcomes[number] = _entity_outcome(result, refetched=True)

    for number, series in events.items():
        outcome = outcomes.get(number)
        if outcome is not None:
            outcomes[number] = TargetOutcome(
                snapshot=outcome.snapshot,
                events=tuple(series),
                error=outcome.error,
                refetched=outcome.refetched,
            )
    for query in known:
        outcomes.setdefault(query.organization_number, TargetOutcome())
    return outcomes, cutoff_id, counters


async def _cutoff(client: BrregClient, start_id: int) -> int:
    try:
        cutoff_id = await client.latest_update_id()
    except BrregSourceError as exc:
        raise MonitorPassFailed(exc.code, f"{INTERVAL_FAILED_MESSAGE} ({exc})") from exc
    if cutoff_id < start_id:
        raise MonitorPassFailed(
            "CURSOR_WENT_BACKWARD",
            f"The newest BRREG update id fell from {start_id} to {cutoff_id} during this run.",
        )
    return cutoff_id


def _entity_outcome(result: EntityResult, *, refetched: bool = False) -> TargetOutcome:
    if not result.succeeded:
        return TargetOutcome(error=result.error, refetched=refetched)
    try:
        snapshot = _snapshot_for(result)
    except EntityValidationError as exc:
        return TargetOutcome(
            error=error_record("SOURCE", "MALFORMED_ENTITY", str(exc), retryable=False),
            refetched=refetched,
        )
    return TargetOutcome(snapshot=snapshot, refetched=refetched)


def _snapshot_for(result: EntityResult) -> Snapshot:
    if result.state == EntityState.ABSENT:
        return absent(result.organization_number)
    if result.state == EntityState.REMOVED:
        return removed(result.organization_number, result.payload)
    return normalize_entity(result.payload, organization_number=result.organization_number)


def build_run_result(
    queries: list[CompanyQuery],
    *,
    outcomes: dict[str, TargetOutcome],
    previous_states: dict[str, dict[str, Any]],
    previous_cursor: int | None,
    cutoff_id: int | None,
    actor_input: ActorInput,
    observed_at: str,
    counters: dict[str, int] | None = None,
    budget_skipped: int = 0,
) -> RunResult:
    """Turn per-organization outcomes into one row per watched organization."""
    counts = counters or {}
    result = RunResult(
        cursor_before=previous_cursor,
        cursor_after=previous_cursor,
        baseline_targets=counts.get("baselines", 0),
        update_chunks=counts.get("chunks", 0),
        update_pages=counts.get("pages", 0),
        source_events=counts.get("events", 0),
        refetched_targets=counts.get("refetched", 0),
    )
    for query in queries:
        outcome = outcomes.get(query.organization_number) or TargetOutcome(
            error=error_record("SOURCE", "NOT_ATTEMPTED", NOT_ATTEMPTED_MESSAGE, retryable=True)
        )
        previous = previous_states.get(query.organization_number)
        row = _build_row(
            query,
            outcome=outcome,
            previous=previous if previous_cursor is not None else None,
            actor_input=actor_input,
            observed_at=observed_at,
        )
        if row.record["change_types"]:
            result.changed_targets += 1
        if row.status is OutputStatus.PARTIAL:
            result.disagreements += 1
        if outcome.error is not None and outcome.error.get("code") == "MALFORMED_ENTITY":
            result.protocol_changed = True
        result.results.append(
            TargetResult(
                query=query,
                record=row.record,
                status=row.status,
                billable=row.status in CONCLUSIVE_STATUSES,
                emit=row.emit,
            )
        )
        state = row.state if row.state is not None else previous
        # A 410 purges the cached record; the tombstone that replaces it holds
        # identity and the removal date only, so the organization stays monitored
        # without a re-baseline and without a cached copy of a removed entity.
        if state is not None:
            result.state_updates[query.organization_number] = state

    # Unpaid or unverified known organizations still need this interval. A failed
    # fresh lookup has no snapshot and is automatically baselined again next run.
    unresolved_known = any(
        item.status not in CONCLUSIVE_STATUSES
        and previous_cursor is not None
        and item.query.organization_number in previous_states
        for item in result.results
    )
    result.cursor_advanced = cutoff_id is not None and not budget_skipped and not unresolved_known
    if result.cursor_advanced:
        result.cursor_after = cutoff_id
    return result


@dataclass(frozen=True, slots=True)
class _Row:
    record: dict[str, Any]
    status: OutputStatus
    emit: bool
    state: dict[str, Any] | None


def _build_row(
    query: CompanyQuery,
    *,
    outcome: TargetOutcome,
    previous: dict[str, Any] | None,
    actor_input: ActorInput,
    observed_at: str,
) -> _Row:
    events = outcome.events
    patch = [operation for event in events for operation in event.changes]
    latest = events[-1] if events else None
    # The last event in the interval is the one that produced the state the
    # re-fetched record shows; every type seen is kept in source_change_types.
    source_type = source_change_type(latest.source_change_type) if latest is not None else None

    if outcome.snapshot is None and outcome.error is not None:
        code = outcome.error.get("code")
        status = OutputStatus.SKIPPED if code == "BUDGET_LIMIT" else OutputStatus.SOURCE_FAILED
        return _Row(
            record=_record(
                query,
                snapshot=None,
                status=status,
                record_type=RecordType.ERROR,
                change_types=(),
                changes={},
                events=events,
                source_type=source_type,
                patch=patch,
                actor_input=actor_input,
                observed_at=observed_at,
                error=outcome.error,
            ),
            status=status,
            # A failure the user cannot see is a failure they will act on wrongly.
            emit=True,
            state=None,
        )

    if outcome.snapshot is None:
        # A known organization the interval reported nothing about. Its stored
        # snapshot is still the register's last word, so it is monitored, not
        # re-fetched: this is the whole point of the update stream.
        if previous is None:
            return _Row(
                record=_record(
                    query,
                    snapshot=None,
                    status=OutputStatus.SOURCE_FAILED,
                    record_type=RecordType.ERROR,
                    change_types=(),
                    changes={},
                    events=events,
                    source_type=source_type,
                    patch=patch,
                    actor_input=actor_input,
                    observed_at=observed_at,
                    error=error_record(
                        "SOURCE", "NOT_ATTEMPTED", NOT_ATTEMPTED_MESSAGE, retryable=True
                    ),
                ),
                status=OutputStatus.SOURCE_FAILED,
                emit=True,
                state=None,
            )
        return _unchanged_row(
            query, previous=previous, actor_input=actor_input, observed_at=observed_at
        )

    snapshot = outcome.snapshot
    decision = (
        detect_changes(previous, snapshot.to_state(), patch=patch)
        if previous is not None
        else ChangeDecision((), {})
    )
    # Only fresh source events reach this point. Do not silently discard an
    # official change merely because its fields have no normalized counterpart.
    # The empty before/after map remains truthful for this evidence-only type.
    if previous is not None and any(op["path"] not in PATCH_PATH_FIELDS for op in patch):
        decision = ChangeDecision(
            (*decision.change_types, CHANGE_OTHER), decision.changes, decision.disagreement
        )
    return _observed_row(
        query,
        snapshot=snapshot,
        decision=decision,
        previous=previous,
        events=events,
        source_type=source_type,
        patch=patch,
        actor_input=actor_input,
        observed_at=observed_at,
    )


def _unchanged_row(
    query: CompanyQuery,
    *,
    previous: dict[str, Any],
    actor_input: ActorInput,
    observed_at: str,
) -> _Row:
    found = bool(previous.get("found"))
    status = OutputStatus.SUCCESS if found else OutputStatus.NOT_FOUND
    record_type = RecordType.SNAPSHOT if found else RecordType.NOT_FOUND
    return _Row(
        record=_record(
            query,
            snapshot=None,
            status=status,
            record_type=record_type,
            change_types=(),
            changes={},
            events=(),
            source_type=None,
            patch=[],
            actor_input=actor_input,
            observed_at=observed_at,
            error=None,
            current=_current_from_state(previous),
        ),
        status=status,
        emit=actor_input.mode is Mode.SNAPSHOT_AND_CHANGES,
        state=previous,
    )


def _observed_row(
    query: CompanyQuery,
    *,
    snapshot: Snapshot,
    decision: ChangeDecision,
    previous: dict[str, Any] | None,
    events: tuple[UpdateEvent, ...],
    source_type: str | None,
    patch: list[dict[str, Any]],
    actor_input: ActorInput,
    observed_at: str,
) -> _Row:
    partial = bool(decision.disagreement)
    if partial:
        status = OutputStatus.PARTIAL
    elif snapshot.found or snapshot.deleted:
        status = OutputStatus.SUCCESS
    else:
        status = OutputStatus.NOT_FOUND

    if not snapshot.found and snapshot.deleted:
        record_type = RecordType.REMOVED
        emit = True
    elif decision.change_types:
        record_type = RecordType.CHANGE
        emit = True
    elif previous is None:
        record_type = RecordType.BASELINE if snapshot.found else RecordType.NOT_FOUND
        emit = actor_input.baseline_mode is BaselineMode.EMIT_SNAPSHOT
    else:
        record_type = RecordType.SNAPSHOT if snapshot.found else RecordType.NOT_FOUND
        emit = actor_input.mode is Mode.SNAPSHOT_AND_CHANGES

    return _Row(
        record=_record(
            query,
            snapshot=snapshot,
            status=status,
            record_type=record_type,
            change_types=decision.change_types,
            changes=dict(decision.changes),
            events=events,
            source_type=source_type,
            patch=patch,
            actor_input=actor_input,
            observed_at=observed_at,
            error=(
                error_record(
                    "SOURCE_DISAGREEMENT", "PATCH_MISMATCH", PARTIAL_MESSAGE, retryable=True
                )
                | {"fields": list(decision.disagreement)}
                if partial
                else None
            ),
        ),
        status=status,
        emit=emit or partial,
        # A record the Actor cannot explain must not become the baseline the next
        # comparison trusts.
        state=None
        if partial
        else {
            **snapshot.to_state(),
            **({"last_event_id": events[-1].update_id} if events else {}),
        },
    )


def _current_from_state(state: dict[str, Any]) -> dict[str, Any]:
    """Republish a stored snapshot without re-fetching it."""
    number = state["organization_number"]
    return {
        **{
            key: value
            for key, value in state.items()
            if key not in {"fingerprint", "last_event_id"}
        },
        "source_url": entity_source_url(number),
        "schema_version": SCHEMA_VERSION,
        "fingerprint": state["fingerprint"],
    }


def _record(
    query: CompanyQuery,
    *,
    snapshot: Snapshot | None,
    status: OutputStatus,
    record_type: RecordType,
    change_types: tuple[str, ...],
    changes: dict[str, Any],
    events: tuple[UpdateEvent, ...],
    source_type: str | None,
    patch: list[dict[str, Any]],
    actor_input: ActorInput,
    observed_at: str,
    error: dict[str, Any] | None,
    current: dict[str, Any] | None = None,
) -> dict[str, Any]:
    latest = events[-1] if events else None
    body = current if current is not None else (snapshot.to_output() if snapshot else None)
    return {
        "record_type": str(record_type),
        "status": str(status),
        "monitor_key": actor_input.monitor_key,
        "organization_number": query.organization_number,
        "submitted_organization_number": query.submitted,
        "company_name": (body or {}).get("name"),
        "event_id": latest.update_id if latest else None,
        "event_ids": [event.update_id for event in events],
        "event_published_at": latest.published_at if latest else None,
        "source_change_type": source_type,
        "source_change_types": sorted({source_change_type(e.source_change_type) for e in events}),
        "change_types": list(change_types),
        "changes": changes,
        "patch_change_types": list(patch_change_types(patch)),
        "source_patch": redact_patch(patch[:MAX_PATCH_OPERATIONS])
        if actor_input.include_source_patch
        else [],
        "source_patch_truncated": actor_input.include_source_patch
        and len(patch) > MAX_PATCH_OPERATIONS,
        "current": body,
        "source": SOURCE_NAME,
        "source_id": update_source_id(latest.update_id)
        if latest
        else f"brreg-entity:{query.organization_number}",
        "source_url": entity_source_url(query.organization_number),
        "scraped_at": observed_at,
        "schema_version": SCHEMA_VERSION,
        "fingerprint": (body or {}).get("fingerprint"),
        "error": error,
    }


def invalid_organization_record(
    item: InvalidCompany, *, observed_at: str, monitor_key: str
) -> dict[str, Any]:
    """A rejected entry still gets a row, so nothing silently disappears."""
    return {
        "record_type": str(RecordType.ERROR),
        "status": str(OutputStatus.INVALID_INPUT),
        "monitor_key": monitor_key,
        "organization_number": None,
        "submitted_organization_number": item.submitted,
        "company_name": None,
        "event_id": None,
        "event_ids": [],
        "event_published_at": None,
        "source_change_type": None,
        "source_change_types": [],
        "change_types": [],
        "changes": {},
        "patch_change_types": [],
        "source_patch": [],
        "source_patch_truncated": False,
        "current": None,
        "source": SOURCE_NAME,
        "source_id": f"invalid:{item.index + 1}",
        "source_url": None,
        "scraped_at": observed_at,
        "schema_version": SCHEMA_VERSION,
        "fingerprint": None,
        "error": error_record("INPUT", item.code, item.message, retryable=False),
    }


def failed_pass_result(
    queries: list[CompanyQuery],
    *,
    failure: MonitorPassFailed,
    previous_states: dict[str, dict[str, Any]],
    previous_cursor: int | None,
    actor_input: ActorInput,
    observed_at: str,
) -> RunResult:
    """Every valid target reports the same unverified interval. Nothing advances."""
    error = error_record("SOURCE", failure.code, str(failure), retryable=True)
    result = build_run_result(
        queries,
        outcomes={query.organization_number: TargetOutcome(error=error) for query in queries},
        previous_states=previous_states,
        previous_cursor=previous_cursor,
        cutoff_id=None,
        actor_input=actor_input,
        observed_at=observed_at,
    )
    result.failure = failure.code
    return result


def run_summary(
    *,
    actor_input: ActorInput,
    result: RunResult,
    invalid: list[InvalidCompany],
    submitted: int,
    duplicates: int,
    source_requests: int,
    source_retries: int,
    started_at: datetime,
    finished_at: datetime,
    state_written: bool,
) -> dict[str, Any]:
    """The compact structured summary a scheduled run is judged by."""
    statuses = [item.status for item in result.results]
    change_counts: dict[str, int] = {}
    for item in result.results:
        for change_type in item.record["change_types"]:
            change_counts[change_type] = change_counts.get(change_type, 0) + 1
    return {
        "watchlist_size": submitted,
        "valid_targets": len(result.results),
        "invalid_targets": len(invalid),
        "duplicate_targets": duplicates,
        "baseline_targets": result.baseline_targets,
        "update_chunks": result.update_chunks,
        "update_pages": result.update_pages,
        "source_requests": source_requests,
        "source_retries": source_retries,
        "source_events": result.source_events,
        "refetched_targets": result.refetched_targets,
        "changed_targets": result.changed_targets,
        "not_found_targets": statuses.count(OutputStatus.NOT_FOUND),
        "removed_targets": _count_type(result, RecordType.REMOVED),
        "failed_targets": statuses.count(OutputStatus.SOURCE_FAILED),
        "partial_targets": statuses.count(OutputStatus.PARTIAL),
        "skipped_targets": statuses.count(OutputStatus.SKIPPED),
        "budget_limit_reached": OutputStatus.SKIPPED in statuses,
        "baseline_rows": _count_type(result, RecordType.BASELINE),
        "change_rows": _count_type(result, RecordType.CHANGE),
        "snapshot_rows": _count_type(result, RecordType.SNAPSHOT),
        "not_found_rows": _count_type(result, RecordType.NOT_FOUND),
        "unchanged": sum(
            1
            for item in result.results
            if item.status in CONCLUSIVE_STATUSES and not item.record["change_types"]
        ),
        "emitted_records": sum(1 for item in result.results if item.emit) + len(invalid),
        "change_types": change_counts,
        "source_disagreements": result.disagreements,
        "cursor_before": result.cursor_before,
        "cursor_after": result.cursor_after,
        "cursor_advanced": result.cursor_advanced,
        "state_updates": len(result.state_updates),
        "state_updated": state_written,
        "billable_targets": sum(1 for item in result.results if item.billable),
        "source_protocol_changed": result.protocol_changed,
        "pass_failure": result.failure,
        "monitor_key": actor_input.monitor_key,
        "mode": str(actor_input.mode),
        "baseline_mode": str(actor_input.baseline_mode),
        "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
    }


def _count_type(result: RunResult, record_type: RecordType) -> int:
    return sum(1 for item in result.results if item.record["record_type"] == str(record_type))
