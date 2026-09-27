"""The Apify edge: Dataset writes, KVS state, charging and run health.

The platform is faked, the source is faked, and nothing here reaches a network.
"""

from __future__ import annotations

import json
import logging
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import pytest

from common.delivery import DELIVERY_CHECKPOINT
from norway_brreg_company_change_monitor import main as runtime
from norway_brreg_company_change_monitor.billing import (
    COMPANY_MONITORED_PRICE_USD,
    PricingContractError,
    affordable_targets,
)
from norway_brreg_company_change_monitor.models import EVENT_COMPANY_MONITORED
from norway_brreg_company_change_monitor.normalize import normalize_entity
from norway_brreg_company_change_monitor.state import (
    MAX_DELIVERY_BYTES,
    MAX_STATE_BYTES,
    MonitorStateStore,
    state_key,
)

from .conftest import FakeSource, event, update_page

EQUINOR = "923609016"
DNB = "984851006"
GHOST = "810864402"
PRICES = {EVENT_COMPANY_MONITORED: COMPANY_MONITORED_PRICE_USD}


class MemoryKvs:
    def __init__(self):
        self.values: dict = {}
        self.writes = 0
        self.fail_writes = False

    async def get_value(self, key):
        return deepcopy(self.values.get(key))

    async def set_value(self, key, value):
        if self.fail_writes:
            raise RuntimeError("Synthetic KVS failure")
        self.values[key] = deepcopy(value)
        self.writes += 1


class FakeActor:
    def __init__(self, *, run_id="run-1", pay_per_event=False, budget="10", accepted=None):
        self.configuration = SimpleNamespace(actor_run_id=run_id)
        self.values: dict = {}
        self.rows: list[dict] = []
        self.failed: list[str] = []
        self.status: list[str] = []
        self.charges: list[tuple[str, int]] = []
        self.pay_per_event = pay_per_event
        self.budget = Decimal(budget)
        self.spent = Decimal(0)
        self.prices = dict(PRICES)
        # Number of charge attempts the platform accepts; None means all.
        self.accepted = accepted
        self.kvs = MemoryKvs()

    async def set_value(self, key, value):
        self.values[key] = deepcopy(value)

    async def get_value(self, key):
        return deepcopy(self.values.get(key))

    async def push_data(self, row, charged_event_name=None):
        self.rows.append(deepcopy(row))
        if charged_event_name is None:
            return None
        return await self.charge(charged_event_name, count=1)

    async def charge(self, event_name, *, count):
        self.charges.append((event_name, count))
        allowed = count
        if self.accepted is not None:
            allowed = max(0, min(count, self.accepted - sum(c for _, c in self.charges[:-1])))
        self.spent += self.prices[event_name] * allowed
        return SimpleNamespace(charged_count=allowed)

    async def set_status_message(self, message):
        self.status.append(message)

    async def fail(self, *, status_message):
        self.failed.append(status_message)

    async def get_input(self):
        return {"organizationNumbers": [EQUINOR]}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    def get_charging_manager(self):
        return SimpleNamespace(
            get_pricing_info=lambda: SimpleNamespace(
                is_pay_per_event=self.pay_per_event,
                per_event_prices=self.prices,
                max_total_charge_usd=self.budget,
            ),
            calculate_total_charged_amount=lambda: self.spent,
        )


@pytest.fixture
def platform(monkeypatch):
    actor = FakeActor()

    async def open_store(*, metrics, logger, name=None):
        return MonitorStateStore(actor.kvs, metrics=metrics, logger=logger)

    monkeypatch.setattr(runtime, "Actor", actor)
    monkeypatch.setattr(runtime.MonitorStateStore, "open", staticmethod(open_store))
    return actor


@pytest.fixture
def brreg(monkeypatch):
    source = FakeSource()
    original = runtime.BrregClient

    def build(**kwargs):
        return original(transport=source.transport, sleep=_no_sleep, **kwargs)

    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(runtime, "BrregClient", build)
    return source


def quiet_baseline(source: FakeSource, entity: dict) -> None:
    source.updates = [
        (200, update_page([event(500, EQUINOR)], total=1)),
        (200, update_page([event(500, EQUINOR)], total=1)),
        (200, update_page([])),
    ]
    source.entities = {EQUINOR: (200, entity)}


async def run(actor_input: dict) -> str | None:
    return await runtime._run(
        runtime.ActorInput.model_validate(actor_input), logging.getLogger("test")
    )


# -- happy path ------------------------------------------------------------


async def test_a_baseline_run_writes_rows_state_and_a_summary(platform, brreg, load_json):
    quiet_baseline(brreg, load_json("entity_current.json"))
    assert await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"}) is None

    assert [row["record_type"] for row in platform.rows] == ["BASELINE"]
    summary = platform.values["RUN_SUMMARY"]
    assert summary["cursor_after"] == 500
    assert summary["state_updated"] is True
    assert summary["operational"]["status"] == "SUCCESS"
    stored = platform.kvs.values[state_key("no")]
    assert stored["cursorUpdateId"] == 500
    assert stored["targets"][EQUINOR]["name"] == "EQUINOR ASA"
    assert platform.values["DELIVERY_CHECKPOINT"]["phase"] == "COMPLETE"


async def test_rows_come_back_in_watchlist_order_including_rejected_entries(
    platform, brreg, load_json
):
    quiet_baseline(brreg, load_json("entity_current.json"))
    await run({"organizationNumbers": ["not-a-number", EQUINOR, GHOST], "monitorKey": "no"})
    assert [row["submitted_organization_number"] for row in platform.rows] == [
        "not-a-number",
        EQUINOR,
        GHOST,
    ]
    assert platform.rows[0]["status"] == "INVALID_INPUT"
    assert platform.rows[0]["error"]["category"] == "INPUT"
    assert platform.rows[2]["status"] == "NOT_FOUND"


async def test_a_second_quiet_run_writes_nothing_and_keeps_the_cursor(platform, brreg, load_json):
    quiet_baseline(brreg, load_json("entity_current.json"))
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})
    platform.rows.clear()
    platform.values.pop("DELIVERY_CHECKPOINT")

    brreg.updates = [(200, update_page([event(900, EQUINOR)], total=1)), (200, update_page([]))]
    assert await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"}) is None
    assert platform.rows == []
    assert platform.values["RUN_SUMMARY"]["cursor_after"] == 900
    assert platform.values["RUN_SUMMARY"]["unchanged"] == 1


# -- failure semantics -----------------------------------------------------


async def test_an_unreachable_source_fails_the_run_and_preserves_state(platform, brreg, load_json):
    quiet_baseline(brreg, load_json("entity_current.json"))
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})
    before = deepcopy(platform.kvs.values[state_key("no")])
    writes = platform.kvs.writes
    platform.values.pop("DELIVERY_CHECKPOINT")

    brreg.updates = [(503, None)]
    failure = await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})

    assert failure is not None
    assert platform.kvs.values[state_key("no")] == before
    assert platform.kvs.writes == writes
    summary = platform.values["RUN_SUMMARY"]
    assert summary["pass_failure"]
    assert summary["cursor_after"] == 500
    assert summary["operational"]["sourceState"] == "SOURCE_FAILED"
    assert summary["charged_targets"] == 0
    assert [row["status"] for row in platform.rows[-1:]] == ["SOURCE_FAILED"]


async def test_a_kvs_write_failure_fails_the_run(platform, brreg, load_json):
    quiet_baseline(brreg, load_json("entity_current.json"))
    platform.kvs.fail_writes = True
    with pytest.raises(RuntimeError, match="Synthetic KVS failure"):
        await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})


async def test_an_interrupted_delivery_is_replayed_once_without_charging(
    platform, brreg, load_json
):
    quiet_baseline(brreg, load_json("entity_current.json"))
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})
    # Simulate a crash between the durable write and the acknowledgement.
    stored = platform.kvs.values[state_key("no")]
    stored["pendingDelivery"]["phase"] = "PENDING"
    platform.rows.clear()
    platform.values.pop("DELIVERY_CHECKPOINT")
    platform.pay_per_event = True

    assert await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"}) is None
    assert [row["record_type"] for row in platform.rows] == ["BASELINE"]
    assert platform.charges == []
    summary = platform.values["RUN_SUMMARY"]
    assert summary["recovery_only"] is True
    assert summary["operational"]["sourceState"] == "SOURCE_NOT_ATTEMPTED"
    assert platform.kvs.values[state_key("no")]["pendingDelivery"]["phase"] == "COMPLETE"


async def test_a_completed_delivery_checkpoint_stops_a_restarted_run(platform):
    platform.values[DELIVERY_CHECKPOINT] = {"version": 1, "phase": "COMPLETE"}
    await runtime.main()
    assert platform.failed == []
    assert platform.status


async def test_five_thousand_companies_fit_storage_and_recover_without_new_charges(
    platform, brreg, load_json
):
    numbers = [str(900000000 + index) for index in range(5000)]
    entity = load_json("entity_current.json")
    quiet_baseline(brreg, entity)
    brreg.entities = {
        number: (200, {**entity, "organisasjonsnummer": number}) for number in numbers
    }
    platform.pay_per_event = True
    data = {"organizationNumbers": numbers, "monitorKey": "large"}
    assert await run(data) is None
    assert len(platform.rows) == 5000
    assert len(platform.charges) == 5000
    assert all(
        len(json.dumps(value, indent=2).encode()) <= MAX_STATE_BYTES
        for value in platform.kvs.values.values()
    )
    manifest = platform.kvs.values[state_key("large")]
    assert len(manifest["targets"]) == 5000
    assert manifest["pendingDelivery"]["version"] == 2
    assert len(manifest["pendingDelivery"]["rowChunks"]) > 1

    manifest["pendingDelivery"]["phase"] = "PENDING"
    platform.rows.clear()
    platform.charges.clear()
    platform.values.pop("DELIVERY_CHECKPOINT")
    brreg.requests.clear()
    assert await run(data) is None
    assert [row["organization_number"] for row in platform.rows] == numbers
    assert platform.charges == []
    assert brreg.requests == []
    assert platform.values["RUN_SUMMARY"]["recovery_only"]


async def test_oversized_delivery_is_refused_before_billing_or_output(platform, brreg, load_json):
    quiet_baseline(brreg, load_json("entity_current.json"))
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})
    before = deepcopy(platform.kvs.values)
    platform.rows.clear()
    platform.values.pop("DELIVERY_CHECKPOINT")
    platform.pay_per_event = True
    brreg.updates = [
        (200, update_page([event(900, EQUINOR)])),
        (200, update_page([event(600, EQUINOR)])),
    ]
    brreg.entities = {
        EQUINOR: (200, load_json("entity_current.json") | {"navn": "x" * (MAX_DELIVERY_BYTES + 1)})
    }
    failure = await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})
    assert "storage limit" in failure
    assert platform.rows == []
    assert platform.charges == []
    assert platform.kvs.values == before


# -- billing ---------------------------------------------------------------


async def test_each_monitored_organization_is_charged_exactly_once(platform, brreg, load_json):
    platform.pay_per_event = True
    quiet_baseline(brreg, load_json("entity_current.json"))
    brreg.entities[DNB] = (200, load_json("entity_current.json") | {"organisasjonsnummer": DNB})

    await run({"organizationNumbers": [EQUINOR, DNB, EQUINOR, "bad"], "monitorKey": "no"})

    assert platform.charges == [(EVENT_COMPANY_MONITORED, 1)] * 2
    assert platform.values["RUN_SUMMARY"]["charged_targets"] == 2
    assert platform.values["RUN_SUMMARY"]["billable_targets"] == 2


async def test_an_unchanged_organization_is_charged_even_when_no_row_is_written(
    platform, brreg, load_json
):
    platform.pay_per_event = True
    quiet_baseline(brreg, load_json("entity_current.json"))
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})
    platform.charges.clear()
    platform.rows.clear()
    platform.values.pop("DELIVERY_CHECKPOINT")

    brreg.updates = [(200, update_page([event(900, EQUINOR)], total=1)), (200, update_page([]))]
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})
    assert platform.charges == [(EVENT_COMPANY_MONITORED, 1)]
    assert platform.rows == []


async def test_an_unverified_organization_is_never_charged(platform, brreg, load_json):
    platform.pay_per_event = True
    brreg.updates = [
        (200, update_page([event(500, EQUINOR)], total=1)),
        (200, update_page([event(500, EQUINOR)], total=1)),
        (200, update_page([])),
    ]
    brreg.entities = {EQUINOR: [(500, None), (500, None), (500, None)]}
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})
    assert platform.charges == []
    assert platform.rows[0]["status"] == "SOURCE_FAILED"


async def test_a_failed_interval_charges_nothing_at_all(platform, brreg, load_json):
    platform.pay_per_event = True
    brreg.updates = [(503, None)]
    failure = await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})
    assert failure is not None
    assert platform.charges == []


async def test_a_budget_that_cannot_fund_the_watchlist_skips_without_touching_state(
    platform, brreg, load_json
):
    platform.pay_per_event = True
    # Enough for one organization only.
    platform.budget = COMPANY_MONITORED_PRICE_USD
    quiet_baseline(brreg, load_json("entity_current.json"))
    brreg.entities[DNB] = (200, load_json("entity_current.json") | {"organisasjonsnummer": DNB})

    await run({"organizationNumbers": [EQUINOR, DNB], "monitorKey": "no"})

    statuses = [row["status"] for row in platform.rows]
    assert statuses == ["SUCCESS", "SKIPPED"]
    assert platform.charges == [(EVENT_COMPANY_MONITORED, 1)]
    summary = platform.values["RUN_SUMMARY"]
    assert summary["budget_limit_reached"] is True
    # The skipped organization was never covered by this interval, so the cursor
    # may not move past it.
    assert summary["cursor_advanced"] is False
    assert summary["cursor_after"] is None
    assert platform.kvs.values[state_key("no")]["cursorUpdateId"] is None


async def test_a_refused_charge_downgrades_the_row_and_pins_the_cursor(platform, brreg, load_json):
    platform.pay_per_event = True
    platform.accepted = 0
    quiet_baseline(brreg, load_json("entity_current.json"))
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})

    # One row per organization: a refused charge replaces the observation, it
    # never publishes a full unpaid record alongside it.
    assert [row["status"] for row in platform.rows] == ["SKIPPED"]
    skipped = platform.rows[0]
    assert skipped["current"] is None
    assert skipped["fingerprint"] is None
    summary = platform.values["RUN_SUMMARY"]
    assert summary["cursor_advanced"] is False
    assert state_key("no") not in platform.kvs.values


def test_the_pricing_contract_is_verified_before_any_source_work():
    manager = SimpleNamespace(
        get_pricing_info=lambda: SimpleNamespace(
            is_pay_per_event=True,
            per_event_prices={EVENT_COMPANY_MONITORED: 0.0005, "dataset-item": 0.001},
            max_total_charge_usd=Decimal("5"),
        ),
        calculate_total_charged_amount=lambda: Decimal(0),
    )
    with pytest.raises(PricingContractError, match="dataset-item"):
        affordable_targets(manager)


def test_a_platform_synthetic_start_event_is_tolerated_and_deducted():
    manager = SimpleNamespace(
        get_pricing_info=lambda: SimpleNamespace(
            is_pay_per_event=True,
            per_event_prices={EVENT_COMPANY_MONITORED: 0.0005, "actor-start": 0.00005},
            max_total_charge_usd=Decimal("1"),
        ),
        calculate_total_charged_amount=lambda: Decimal("0.5"),
    )
    assert affordable_targets(manager) == 1000


def test_a_rental_plan_is_unmetered():
    manager = SimpleNamespace(
        get_pricing_info=lambda: SimpleNamespace(
            is_pay_per_event=False, per_event_prices={}, max_total_charge_usd=Decimal("Infinity")
        ),
        calculate_total_charged_amount=lambda: Decimal(0),
    )
    assert affordable_targets(manager) is None


# -- privacy ---------------------------------------------------------------


async def test_no_contact_detail_ever_reaches_the_dataset(platform, brreg, load_json):
    entity = load_json("entity_current.json") | {
        "epostadresse": "post@example.no",
        "mobil": "40000000",
        "telefon": "51990000",
    }
    quiet_baseline(brreg, entity)
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})
    published = str(platform.rows) + str(platform.kvs.values)
    for leaked in ("post@example.no", "40000000", "51990000"):
        assert leaked not in published


async def test_a_contact_change_publishes_the_path_without_the_value(platform, brreg, load_json):
    quiet_baseline(brreg, load_json("entity_current.json"))
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})
    platform.rows.clear()
    platform.values.pop("DELIVERY_CHECKPOINT")

    brreg.updates = [
        (200, update_page([event(900, EQUINOR)], total=1)),
        (
            200,
            update_page(
                [
                    event(
                        600,
                        EQUINOR,
                        changes=[
                            {"op": "replace", "path": "/epostadresse", "value": "new@example.no"},
                            {"op": "replace", "path": "/navn", "value": "EQUINOR NORGE ASA"},
                        ],
                    )
                ]
            ),
        ),
    ]
    brreg.entities = {
        EQUINOR: (
            200,
            load_json("entity_current.json")
            | {"navn": "EQUINOR NORGE ASA", "epostadresse": "new@example.no"},
        )
    }
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "no"})

    patch = platform.rows[0]["source_patch"]
    assert patch[0] == {"op": "replace", "path": "/epostadresse", "value_redacted": True}
    assert patch[1]["value"] == "EQUINOR NORGE ASA"
    assert "new@example.no" not in str(platform.rows)


# -- monitor key isolation -------------------------------------------------


async def test_two_monitor_keys_keep_independent_baselines(platform, brreg, load_json):
    quiet_baseline(brreg, load_json("entity_current.json"))
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "suppliers"})
    platform.values.pop("DELIVERY_CHECKPOINT")
    quiet_baseline(brreg, load_json("entity_current.json"))
    await run({"organizationNumbers": [EQUINOR], "monitorKey": "customers"})

    assert state_key("suppliers") in platform.kvs.values
    assert state_key("customers") in platform.kvs.values
    assert [row["record_type"] for row in platform.rows] == ["BASELINE", "BASELINE"]


def test_the_stored_snapshot_carries_no_run_metadata(load_json):
    stored = normalize_entity(
        load_json("entity_current.json"), organization_number=EQUINOR
    ).to_state()
    assert "scraped_at" not in stored
    assert "monitor_key" not in stored
