"""Compact comparison state in the Apify KVS. Not a history database.

One record per monitor key holds the update cursor and the last *successful*
snapshot of each watched organization. Full observations already live in the
Dataset, run by run, so nothing here accumulates history - it only has to make
the next diff possible and to survive a run in which BRREG could not be reached.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any

from common.monitoring import RunMetrics
from norway_brreg_company_change_monitor.models import STATE_VERSION
from norway_brreg_company_change_monitor.monitor import redact_patch

DEFAULT_STATE_STORE = "norway-brreg-company-change-monitor-state"
#: Lets a private staging twin keep its own state store. Under limited
#: permissions a named store belongs to the Actor that created it.
STATE_STORE_ENV = "NORWAY_BRREG_STATE_STORE"
KEY_PREFIX = "BRREG_MONITOR_STATE_V1_"
#: Leave headroom below the platform limit for each independently written record.
MAX_STATE_BYTES = 8 * 1024 * 1024
MAX_DELIVERY_BYTES = 1024 * 1024
SUMMARY_RESERVE_BYTES = 64 * 1024


def _encoded(value: Any) -> bytes:
    # Includes indentation and ASCII escaping, conservatively accounting for
    # JSON serialization at the SDK edge as well as our own data.
    return json.dumps(value, indent=2, sort_keys=True).encode("utf-8")


def _delivery_chunks(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    chunks: list[list[dict[str, Any]]] = []
    chunk: list[dict[str, Any]] = []
    size = 2
    for row in rows:
        encoded = _encoded(row)
        # A dict is indented two extra spaces when nested in a JSON array.
        row_size = len(encoded) + 2 * (encoded.count(b"\n") + 1) + 2
        if row_size + 2 > MAX_DELIVERY_BYTES:
            raise ValueError("One delivery row exceeds the bounded KVS chunk size.")
        if size + row_size > MAX_DELIVERY_BYTES:
            chunks.append(chunk)
            chunk, size = [], 2
        chunk.append(row)
        size += row_size
    if chunk:
        chunks.append(chunk)
    return chunks


def configured_state_store() -> str:
    name = os.environ.get(STATE_STORE_ENV, DEFAULT_STATE_STORE).strip()
    if not name:
        raise ValueError(f"{STATE_STORE_ENV} must not be empty")
    return name


def state_key(monitor_key: str) -> str:
    digest = hashlib.sha256(monitor_key.encode("utf-8")).hexdigest()[:16]
    return f"{KEY_PREFIX}{digest}"


class MonitorStateStore:
    def __init__(self, kvs: Any, *, metrics: RunMetrics, logger: logging.Logger) -> None:
        self._kvs = kvs
        self._metrics = metrics
        self._logger = logger
        self.pending_delivery: dict[str, Any] | None = None
        self.cursor_update_id: int | None = None
        self.cursor_updated_at: str | None = None
        self._delivery_keys: set[str] = set()
        self._retired_keys: set[str] = set()

    @classmethod
    async def open(
        cls,
        *,
        metrics: RunMetrics,
        logger: logging.Logger,
        name: str | None = None,
    ) -> MonitorStateStore:
        from apify import Actor  # noqa: PLC0415 - state stays testable without SDK setup

        kvs = await Actor.open_key_value_store(name=name or configured_state_store())
        return cls(kvs, metrics=metrics, logger=logger)

    async def load(self, monitor_key: str) -> dict[str, dict[str, Any]]:
        """Return the stored per-organization snapshots, or an empty baseline."""
        self._metrics.increment("kvs_read_count")
        value = await self._kvs.get_value(state_key(monitor_key))
        self.pending_delivery = None
        self.cursor_update_id = None
        self.cursor_updated_at = None
        self._delivery_keys = set()
        self._retired_keys = set()
        if value is None:
            return {}
        if not _valid_envelope(value, monitor_key=monitor_key):
            # An unreadable or foreign record is treated as "no baseline", which
            # produces a fresh baseline run. It is never partially trusted, and
            # in particular its cursor is never reused.
            self._logger.warning(
                "Ignoring incompatible monitor state for this monitor key; "
                "this run re-establishes the baseline."
            )
            self._metrics.increment("invalid_state_count")
            return {}
        self._retired_keys = self._chunk_keys(value.get("retiredDeliveryChunks", []), monitor_key)
        pending = value.get("pendingDelivery")
        if pending is not None:
            if (
                not isinstance(pending, dict)
                or pending.get("version") not in (1, 2)
                or pending.get("phase") not in ("PENDING", "COMPLETE")
                or not isinstance(pending.get("summary"), dict)
                or "run_id" not in pending
                or (pending["run_id"] is not None and not isinstance(pending["run_id"], str))
            ):
                raise RuntimeError("Unusable pending delivery; preserve the state for recovery.")
            if pending["version"] == 2:
                self._delivery_keys = self._chunk_keys(pending.get("rowChunks"), monitor_key)
                rows = []
                # Completed output needs no replay. Keep only references for
                # retirement; a quiet check should not download old output.
                for key in pending["rowChunks"] if pending["phase"] == "PENDING" else []:
                    self._metrics.increment("kvs_read_count")
                    chunk = await self._kvs.get_value(key)
                    if (
                        not isinstance(chunk, list)
                        or not all(isinstance(row, dict) for row in chunk)
                        or self._chunk_key(monitor_key, chunk) != key
                    ):
                        raise RuntimeError(
                            "Unusable pending delivery chunk; preserve recovery state."
                        )
                    rows.extend(chunk)
                if pending["phase"] == "PENDING" and len(rows) != pending.get("rowCount"):
                    raise RuntimeError("Incomplete pending delivery; preserve recovery state.")
            else:
                legacy_rows = pending.get("rows")
                if not isinstance(legacy_rows, list):
                    raise RuntimeError("Unusable pending delivery rows; preserve recovery state.")
                rows = legacy_rows
            if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                raise RuntimeError("Unusable pending delivery rows; preserve recovery state.")
            # Legacy receipts may predate patch minimization. Recovery must apply
            # the same privacy contract as a newly collected observation.
            rows = [
                {**row, "source_patch": redact_patch(row["source_patch"])}
                if isinstance(row.get("source_patch"), list)
                else row
                for row in rows
            ]
            self.pending_delivery = {
                key: item for key, item in pending.items() if key not in {"rowChunks", "rowCount"}
            } | {"version": 1, "rows": rows}
        cursor = value.get("cursorUpdateId")
        if cursor is not None and (
            isinstance(cursor, bool) or not isinstance(cursor, int) or cursor < 0
        ):
            raise RuntimeError(
                "The stored BRREG cursor is not a non-negative integer. Preserve this state and "
                "use a new monitor key rather than risking a skipped update interval."
            )
        self.cursor_update_id = cursor
        self.cursor_updated_at = value.get("cursorUpdatedAt")
        targets = value.get("targets")
        if not isinstance(targets, dict):
            return {}
        for snapshot in targets.values():
            if isinstance(snapshot, dict) and "last_event_id" in snapshot:
                event_id = snapshot["last_event_id"]
                if isinstance(event_id, bool) or not isinstance(event_id, int) or event_id < 0:
                    raise RuntimeError(
                        "Invalid stored event acknowledgement; preserve monitor state."
                    )
        return {
            number: snapshot
            for number, snapshot in targets.items()
            if isinstance(number, str) and _valid_snapshot(snapshot, number=number)
        }

    async def save(
        self,
        monitor_key: str,
        targets: dict[str, dict[str, Any]],
        *,
        cursor_update_id: int | None,
        updated_at: str,
        pending_delivery: dict[str, Any] | None = None,
    ) -> bool:
        """Persist the whole comparison state. Returns False if it was refused."""
        try:
            record, chunks = self._prepare_save(
                monitor_key, targets, cursor_update_id, updated_at, pending_delivery
            )
        except ValueError as exc:
            self._logger.error("Refusing oversized delivery: %s", exc)
            self._metrics.increment("state_write_refused_count")
            return False
        if not self._fits(record):
            return False
        # Immutable chunks are durable before their pointer is committed. A
        # failed write cannot damage a previous receipt or advance its cursor.
        for key, rows in chunks.items():
            if key not in self._delivery_keys:
                await self._kvs.set_value(key, rows)
                self._metrics.increment("kvs_write_count")
        await self._kvs.set_value(state_key(monitor_key), record)
        self._retired_keys = set(record["retiredDeliveryChunks"])
        self._delivery_keys = set(chunks)
        self.pending_delivery = pending_delivery
        self.cursor_update_id = cursor_update_id
        self.cursor_updated_at = updated_at
        self._metrics.increment("kvs_write_count")
        # The committed manifest retains deletion work until a later save
        # acknowledges it. Cleanup may be retried after an interrupted request.
        for key in sorted(self._retired_keys):
            await self._kvs.set_value(key, None)
            self._retired_keys.remove(key)
            self._metrics.increment("kvs_write_count")
        return True

    def check_capacity(
        self,
        monitor_key: str,
        targets: dict[str, dict[str, Any]],
        *,
        previous_states: dict[str, dict[str, Any]],
        cursor_update_id: int | None,
        updated_at: str,
        pending_delivery: dict[str, Any],
    ) -> bool:
        """Preflight before billing, including state restored by a refused charge."""
        largest = {
            number: max(
                (snapshot, previous_states.get(number, {})), key=lambda item: len(_encoded(item))
            )
            for number, snapshot in targets.items()
        }
        try:
            record, _ = self._prepare_save(
                monitor_key, largest, cursor_update_id, updated_at, pending_delivery
            )
        except ValueError:
            self._metrics.increment("state_write_refused_count")
            return False
        return self._fits(record, reserve=SUMMARY_RESERVE_BYTES)

    def _prepare_save(
        self,
        monitor_key: str,
        targets: dict[str, dict[str, Any]],
        cursor: int | None,
        updated_at: str,
        pending: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
        chunks = {}
        if pending is not None:
            groups = _delivery_chunks(pending["rows"])
            if len(groups) > 1:
                chunks = {self._chunk_key(monitor_key, rows): rows for rows in groups}
                pending = {key: value for key, value in pending.items() if key != "rows"} | {
                    "version": 2,
                    "rowChunks": [self._chunk_key(monitor_key, rows) for rows in groups],
                    "rowCount": len(pending["rows"]),
                }
        record = {
            "schemaVersion": STATE_VERSION,
            "monitorKey": monitor_key,
            "cursorUpdateId": cursor,
            "cursorUpdatedAt": updated_at,
            "updatedAt": updated_at,
            "targets": targets,
            "pendingDelivery": pending,
            "retiredDeliveryChunks": sorted(
                (self._delivery_keys | self._retired_keys) - set(chunks)
            ),
        }
        return record, chunks

    def _fits(self, record: dict[str, Any], *, reserve: int = 0) -> bool:
        encoded = len(_encoded(record)) + reserve
        if encoded > MAX_STATE_BYTES:
            # Writing a truncated record would silently drop snapshots and then
            # manufacture change events for the dropped organizations next run.
            self._logger.error(
                "Refusing to write %d bytes of monitor state (limit %d). The previous "
                "state is preserved; split this watchlist across monitor keys.",
                encoded,
                MAX_STATE_BYTES,
            )
            self._metrics.increment("state_write_refused_count")
            return False
        return True

    @staticmethod
    def _chunk_key(monitor_key: str, rows: list[dict[str, Any]]) -> str:
        digest = hashlib.sha256(_encoded(rows)).hexdigest()
        return f"{state_key(monitor_key)}_DELIVERY_{digest}"

    @staticmethod
    def _chunk_keys(keys: Any, monitor_key: str) -> set[str]:
        prefix = f"{state_key(monitor_key)}_DELIVERY_"
        if not isinstance(keys, list) or not all(
            isinstance(key, str)
            and key.startswith(prefix)
            and len(key.removeprefix(prefix)) == 64
            and all(character in "0123456789abcdef" for character in key.removeprefix(prefix))
            for key in keys
        ):
            raise RuntimeError(
                "Unusable pending delivery chunk references; preserve monitor state."
            )
        return set(keys)


def _valid_envelope(value: Any, *, monitor_key: str) -> bool:
    return (
        isinstance(value, dict)
        and value.get("schemaVersion") == STATE_VERSION
        and value.get("monitorKey") == monitor_key
    )


def _valid_snapshot(value: Any, *, number: str) -> bool:
    if not isinstance(value, dict):
        return False
    fingerprint = value.get("fingerprint")
    return (
        value.get("organization_number") == number
        and isinstance(value.get("found"), bool)
        and isinstance(fingerprint, str)
        and len(fingerprint) == 64
    )
