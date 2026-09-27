from __future__ import annotations

import logging
from typing import Any

import pytest

from common.monitoring import RunMetrics
from norway_brreg_company_change_monitor.models import STATE_VERSION
from norway_brreg_company_change_monitor.normalize import normalize_entity
from norway_brreg_company_change_monitor.state import (
    MAX_STATE_BYTES,
    MonitorStateStore,
    state_key,
)

EQUINOR = "923609016"


class FakeKvs:
    def __init__(self, values: dict[str, Any] | None = None) -> None:
        self.values = values or {}
        self.writes = 0

    async def get_value(self, key: str) -> Any:
        return self.values.get(key)

    async def set_value(self, key: str, value: Any) -> None:
        self.writes += 1
        self.values[key] = value


def store(values: dict[str, Any] | None = None) -> MonitorStateStore:
    return MonitorStateStore(
        FakeKvs(values), metrics=RunMetrics(actor="test"), logger=logging.getLogger("test")
    )


def snapshot(load_json) -> dict[str, Any]:
    return normalize_entity(
        load_json("entity_current.json"), organization_number=EQUINOR
    ).to_state()


def envelope(targets: dict[str, Any], *, cursor: int | None = 500, monitor_key="suppliers-no"):
    return {
        "schemaVersion": STATE_VERSION,
        "monitorKey": monitor_key,
        "cursorUpdateId": cursor,
        "cursorUpdatedAt": "2026-09-08T09:00:00Z",
        "updatedAt": "2026-09-08T09:00:00Z",
        "targets": targets,
        "pendingDelivery": None,
    }


def test_the_key_is_namespaced_per_monitor_and_stable():
    assert state_key("default") == state_key("default")
    assert state_key("default") != state_key("suppliers-no")
    assert state_key("default").startswith("BRREG_MONITOR_STATE_V1_")


async def test_an_empty_monitor_reads_as_no_baseline_and_no_cursor():
    state = store()
    assert await state.load("suppliers-no") == {}
    assert state.cursor_update_id is None


async def test_a_round_trip_preserves_the_snapshot_and_the_cursor(load_json):
    state = store()
    saved = await state.save(
        "suppliers-no",
        {EQUINOR: snapshot(load_json)},
        cursor_update_id=25_175_397,
        updated_at="2026-09-08T09:00:00Z",
    )
    assert saved
    reloaded = await state.load("suppliers-no")
    assert reloaded[EQUINOR]["name"] == "EQUINOR ASA"
    assert state.cursor_update_id == 25_175_397


async def test_state_written_under_one_key_is_invisible_to_another(load_json):
    state = store()
    await state.save(
        "suppliers-no",
        {EQUINOR: snapshot(load_json)},
        cursor_update_id=500,
        updated_at="2026-09-08T09:00:00Z",
    )
    assert await state.load("customers-no") == {}
    assert state.cursor_update_id is None


async def test_a_foreign_or_future_envelope_is_ignored_rather_than_half_trusted(load_json):
    values = {state_key("suppliers-no"): {"schemaVersion": 99, "monitorKey": "suppliers-no"}}
    state = store(values)
    assert await state.load("suppliers-no") == {}
    assert state.cursor_update_id is None


async def test_a_snapshot_that_does_not_match_its_key_is_dropped(load_json):
    good = snapshot(load_json)
    values = {
        state_key("suppliers-no"): envelope(
            {EQUINOR: good, "984851006": good, "bad": {"organization_number": "bad"}}
        )
    }
    state = store(values)
    loaded = await state.load("suppliers-no")
    assert set(loaded) == {EQUINOR}


async def test_a_corrupt_cursor_stops_the_run_instead_of_skipping_an_interval(load_json):
    values = {state_key("suppliers-no"): envelope({EQUINOR: snapshot(load_json)}, cursor="500")}
    with pytest.raises(RuntimeError, match="cursor"):
        await store(values).load("suppliers-no")


async def test_an_oversized_write_is_refused_and_the_previous_state_survives(load_json):
    kvs = FakeKvs({state_key("suppliers-no"): envelope({EQUINOR: snapshot(load_json)})})
    state = MonitorStateStore(
        kvs, metrics=RunMetrics(actor="test"), logger=logging.getLogger("test")
    )
    bloated = {
        f"{900000000 + index:09d}": {**snapshot(load_json), "pad": "x" * 4096}
        for index in range(MAX_STATE_BYTES // 4096)
    }
    assert not await state.save(
        "suppliers-no", bloated, cursor_update_id=900, updated_at="2026-09-08T09:00:00Z"
    )
    assert kvs.writes == 0
    assert (await state.load("suppliers-no"))[EQUINOR]["name"] == "EQUINOR ASA"


async def test_an_unusable_delivery_receipt_is_preserved_for_recovery(load_json):
    values = {
        state_key("suppliers-no"): envelope({EQUINOR: snapshot(load_json)})
        | {"pendingDelivery": {"version": 1, "phase": "PENDING"}}
    }
    with pytest.raises(RuntimeError, match="pending delivery"):
        await store(values).load("suppliers-no")


async def test_a_valid_delivery_receipt_is_surfaced(load_json):
    receipt = {
        "version": 1,
        "phase": "PENDING",
        "run_id": "abc",
        "rows": [{"organization_number": EQUINOR}],
        "summary": {"watchlist_size": 1},
    }
    values = {
        state_key("suppliers-no"): envelope({EQUINOR: snapshot(load_json)})
        | {"pendingDelivery": receipt}
    }
    state = store(values)
    await state.load("suppliers-no")
    assert state.pending_delivery == receipt


async def test_chunk_failure_never_commits_the_new_cursor_or_receipt(load_json):
    previous = envelope({EQUINOR: snapshot(load_json)})

    class FailingKvs(FakeKvs):
        async def set_value(self, key, value):
            if "_DELIVERY_" in key and self.writes == 1:
                raise RuntimeError("Synthetic chunk failure")
            await super().set_value(key, value)

    kvs = FailingKvs({state_key("suppliers-no"): previous})
    state = MonitorStateStore(
        kvs, metrics=RunMetrics(actor="test"), logger=logging.getLogger("test")
    )
    await state.load("suppliers-no")
    receipt = {
        "version": 1,
        "phase": "PENDING",
        "run_id": "run",
        "summary": {},
        "rows": [{"index": i, "text": "x" * 600_000} for i in range(3)],
    }
    with pytest.raises(RuntimeError, match="chunk failure"):
        await state.save(
            "suppliers-no",
            {EQUINOR: snapshot(load_json)},
            cursor_update_id=900,
            updated_at="2026-09-08T09:00:00Z",
            pending_delivery=receipt,
        )
    assert kvs.values[state_key("suppliers-no")] == previous
    await state.load("suppliers-no")
    assert state.cursor_update_id == 500
    assert state.pending_delivery is None


async def test_missing_delivery_chunk_fails_closed_and_keeps_the_cursor(load_json):
    state = store()
    receipt = {
        "version": 1,
        "phase": "PENDING",
        "run_id": "run",
        "summary": {},
        "rows": [{"index": i, "text": "x" * 600_000} for i in range(3)],
    }
    await state.save(
        "suppliers-no",
        {EQUINOR: snapshot(load_json)},
        cursor_update_id=900,
        updated_at="2026-09-08T09:00:00Z",
        pending_delivery=receipt,
    )
    manifest = state._kvs.values[state_key("suppliers-no")]
    state._kvs.values.pop(manifest["pendingDelivery"]["rowChunks"][1])
    with pytest.raises(RuntimeError, match="delivery chunk"):
        await state.load("suppliers-no")
    assert manifest["cursorUpdateId"] == 900


async def test_legacy_recovery_redacts_street_data_before_surfacing_rows(load_json):
    receipt = {
        "version": 1,
        "phase": "PENDING",
        "run_id": "old",
        "summary": {},
        "rows": [
            {
                "source_patch": [
                    {"op": "replace", "path": "/postadresse/adresse/0", "value": "PRIVATE STREET"}
                ]
            }
        ],
    }
    state = store(
        {
            state_key("suppliers-no"): envelope({EQUINOR: snapshot(load_json)})
            | {"pendingDelivery": receipt}
        }
    )
    await state.load("suppliers-no")
    assert "PRIVATE STREET" not in str(state.pending_delivery)
    assert state.pending_delivery["rows"][0]["source_patch"][0]["value_redacted"]


async def test_retired_chunks_are_removed_only_after_switching_the_manifest(load_json):
    state = store()
    receipt = {
        "version": 1,
        "phase": "PENDING",
        "run_id": "run",
        "summary": {},
        "rows": [{"index": i, "text": "x" * 600_000} for i in range(3)],
    }
    await state.save(
        "suppliers-no",
        {EQUINOR: snapshot(load_json)},
        cursor_update_id=500,
        updated_at="2026-09-08T09:00:00Z",
        pending_delivery=receipt,
    )
    old_keys = state._kvs.values[state_key("suppliers-no")]["pendingDelivery"]["rowChunks"]
    await state.save(
        "suppliers-no",
        {EQUINOR: snapshot(load_json)},
        cursor_update_id=900,
        updated_at="2026-09-08T10:00:00Z",
    )
    assert all(state._kvs.values[key] is None for key in old_keys)
    assert state._kvs.values[state_key("suppliers-no")]["pendingDelivery"] is None
