"""The whole monitoring pass, driven by a scripted BRREG and no Apify platform."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from norway_brreg_company_change_monitor.models import (
    ActorInput,
    BaselineMode,
    Mode,
    OutputStatus,
    RecordType,
    validate_organization,
)
from norway_brreg_company_change_monitor.monitor import (
    CHANGE_BANKRUPTCY,
    CHANGE_DELETION,
    CHANGE_NAME,
    CHANGE_VAT,
)
from norway_brreg_company_change_monitor.normalize import normalize_entity
from norway_brreg_company_change_monitor.service import (
    MonitorPassFailed,
    budget_skipped_outcome,
    build_run_result,
    collect_outcomes,
    failed_pass_result,
    run_summary,
)
from norway_brreg_company_change_monitor.source import BrregClient

from .conftest import event, update_page

EQUINOR = "923609016"
DNB = "984851006"
GHOST = "810864402"
REMOVED = "936127088"
OBSERVED_AT = "2026-09-08T09:00:00Z"


def actor_input(**overrides):
    return ActorInput.model_validate(
        {"organizationNumbers": ["923609016"], "monitorKey": "test", **overrides}
    )


def queries(*numbers):
    return [validate_organization(number, index=index) for index, number in enumerate(numbers)]


def client(source, **kwargs) -> BrregClient:
    async def no_sleep(_seconds: float) -> None:
        return None

    return BrregClient(transport=source.transport, sleep=no_sleep, **kwargs)


def newest(update_id: int):
    return (200, update_page([event(update_id, EQUINOR)], total=1))


async def run(
    source,
    targets,
    *,
    previous_states=None,
    previous_cursor=None,
    parsed=None,
):
    parsed = parsed or actor_input()
    previous_states = previous_states or {}
    async with client(source) as brreg:
        outcomes, cutoff, counters = await collect_outcomes(
            targets,
            client=brreg,
            previous_states=previous_states,
            previous_cursor=previous_cursor,
        )
    return build_run_result(
        targets,
        outcomes=outcomes,
        previous_states=previous_states,
        previous_cursor=previous_cursor,
        cutoff_id=cutoff,
        actor_input=parsed,
        observed_at=OBSERVED_AT,
        counters=counters,
    )


def row(result, number: str) -> dict:
    return next(
        item.record for item in result.results if item.record["organization_number"] == number
    )


# -- baseline --------------------------------------------------------------


async def test_a_first_run_baselines_every_target_and_captures_the_cursor(source, load_json):
    source.updates = [newest(500), newest(500), (200, update_page([]))]
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    result = await run(source, queries(EQUINOR))

    baseline = row(result, EQUINOR)
    assert baseline["record_type"] == str(RecordType.BASELINE)
    assert baseline["status"] == str(OutputStatus.SUCCESS)
    assert baseline["current"]["name"] == "EQUINOR ASA"
    assert baseline["event_id"] is None
    assert baseline["change_types"] == []
    assert result.cursor_after == 500
    assert result.cursor_advanced
    assert result.state_updates[EQUINOR]["found"] is True


async def test_store_only_baseline_persists_state_without_emitting(source, load_json):
    source.updates = [newest(500), newest(500), (200, update_page([]))]
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    result = await run(
        source,
        queries(EQUINOR),
        parsed=actor_input(baselineMode=str(BaselineMode.STORE_ONLY)),
    )
    assert [item.emit for item in result.results] == [False]
    assert result.state_updates[EQUINOR]["fingerprint"]
    assert result.cursor_after == 500


async def test_a_change_during_the_baseline_window_is_reconciled_before_commit(source, load_json):
    stale = load_json("entity_current.json")
    fresh = stale | {"konkurs": True}
    source.updates = [
        newest(500),
        newest(540),
        (200, update_page([event(521, EQUINOR, changes=[{"op": "replace", "path": "/konkurs"}])])),
    ]
    source.entities = {EQUINOR: [(200, stale), (200, fresh)]}
    result = await run(source, queries(EQUINOR))

    baseline = row(result, EQUINOR)
    # The baseline is the reconciled record, not the one read before the event.
    assert baseline["current"]["bankrupt"] is True
    assert baseline["event_id"] == 521
    assert result.state_updates[EQUINOR]["bankrupt"] is True
    assert result.cursor_after == 540
    barrier = [
        request for request in source.requests if request.url.params.get("oppdateringsid") == "501"
    ]
    assert len(barrier) == 1


async def test_a_failed_barrier_persists_no_cursor_and_no_state(source, load_json):
    source.updates = [newest(500), newest(540), (500, None), (500, None), (500, None)]
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    with pytest.raises(MonitorPassFailed) as raised:
        await run(source, queries(EQUINOR))
    assert raised.value.code == "INTERVAL_INCOMPLETE"


async def test_one_absent_number_does_not_corrupt_the_other_targets(source, load_json):
    source.updates = [newest(500), newest(500), (200, update_page([]))]
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    result = await run(source, queries(EQUINOR, GHOST))

    assert row(result, EQUINOR)["status"] == str(OutputStatus.SUCCESS)
    ghost = row(result, GHOST)
    assert ghost["status"] == str(OutputStatus.NOT_FOUND)
    assert ghost["record_type"] == str(RecordType.NOT_FOUND)
    assert ghost["current"]["found"] is False
    assert ghost["current"]["name"] is None
    assert result.cursor_advanced


async def test_a_baseline_that_is_already_removed_from_open_data_keeps_no_record(source, load_json):
    source.updates = [newest(500), newest(500), (200, update_page([]))]
    source.entities = {REMOVED: (410, load_json("entity_removed_410.json"))}
    result = await run(source, queries(REMOVED))

    record = row(result, REMOVED)
    assert record["record_type"] == str(RecordType.REMOVED)
    assert record["current"]["deleted"] is True
    assert record["current"]["name"] is None
    stored = result.state_updates[REMOVED]
    assert stored["found"] is False
    assert stored["name"] is None


async def test_one_failed_baseline_does_not_deny_the_others_their_state(source, load_json):
    source.updates = [newest(500), newest(500), (200, update_page([]))]
    source.entities = {
        EQUINOR: (200, load_json("entity_current.json")),
        DNB: [(500, None), (500, None), (500, None)],
    }
    result = await run(source, queries(EQUINOR, DNB))

    assert row(result, DNB)["status"] == str(OutputStatus.SOURCE_FAILED)
    assert row(result, DNB)["record_type"] == str(RecordType.ERROR)
    assert DNB not in result.state_updates
    assert EQUINOR in result.state_updates
    # The failed target has no snapshot, so it is simply baselined again next run.
    assert result.cursor_advanced


# -- recurring monitoring --------------------------------------------------


@pytest.fixture
def stored(load_json):
    return {
        EQUINOR: normalize_entity(
            load_json("entity_current.json"), organization_number=EQUINOR
        ).to_state()
    }


async def test_a_quiet_second_run_emits_nothing_and_fetches_no_entity(source, stored):
    source.updates = [newest(900), (200, update_page([]))]
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)

    assert [item.emit for item in result.results] == [False]
    assert row(result, EQUINOR)["change_types"] == []
    assert row(result, EQUINOR)["status"] == str(OutputStatus.SUCCESS)
    assert result.cursor_after == 900
    assert not [r for r in source.requests if "/enheter/" in r.url.path]
    # One cutoff read plus one watchlist chunk. This is the whole recurring cost.
    assert len(source.requests) == 2


async def test_snapshot_mode_republishes_the_stored_record_without_re_fetching(source, stored):
    source.updates = [newest(900), (200, update_page([]))]
    result = await run(
        source,
        queries(EQUINOR),
        previous_states=stored,
        previous_cursor=500,
        parsed=actor_input(mode=str(Mode.SNAPSHOT_AND_CHANGES)),
    )
    record = row(result, EQUINOR)
    assert [item.emit for item in result.results] == [True]
    assert record["record_type"] == str(RecordType.SNAPSHOT)
    assert record["current"]["name"] == "EQUINOR ASA"
    assert record["fingerprint"] == stored[EQUINOR]["fingerprint"]


@pytest.mark.parametrize(
    ("override", "path", "expected"),
    [
        ({"navn": "EQUINOR NORGE ASA"}, "/navn", CHANGE_NAME),
        ({"registrertIMvaregisteret": False}, "/registrertIMvaregisteret", CHANGE_VAT),
        ({"konkurs": True}, "/konkurs", CHANGE_BANKRUPTCY),
    ],
)
async def test_a_real_change_produces_one_typed_row(
    source, load_json, stored, override, path, expected
):
    source.updates = [
        newest(900),
        (200, update_page([event(600, EQUINOR, changes=[{"op": "replace", "path": path}])])),
    ]
    source.entities = {EQUINOR: (200, load_json("entity_current.json") | override)}
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)

    record = row(result, EQUINOR)
    assert record["record_type"] == str(RecordType.CHANGE)
    assert record["status"] == str(OutputStatus.SUCCESS)
    assert record["change_types"] == [expected]
    assert record["source_change_type"] == "ENTITY_CHANGED"
    assert record["event_id"] == 600
    assert record["source_id"] == "brreg-update:600"
    assert record["source_patch"] == [{"op": "replace", "path": path}]
    assert result.cursor_after == 900


async def test_the_watchlist_chunk_starts_one_past_the_stored_cursor(source, stored):
    source.updates = [newest(900), (200, update_page([]))]
    await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)
    chunk = source.requests[1]
    assert chunk.url.params["oppdateringsid"] == "501"
    assert chunk.url.params["organisasjonsnummer"] == EQUINOR
    assert chunk.url.params["includeChanges"] == "true"


async def test_several_events_reconcile_to_one_row_with_every_event_id(source, load_json, stored):
    source.updates = [
        newest(900),
        (
            200,
            update_page(
                [
                    event(600, EQUINOR, changes=[{"op": "replace", "path": "/navn"}]),
                    event(700, EQUINOR, changes=[{"op": "replace", "path": "/konkurs"}]),
                ]
            ),
        ),
    ]
    source.entities = {
        EQUINOR: (200, load_json("entity_current.json") | {"navn": "X ASA", "konkurs": True})
    }
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)

    record = row(result, EQUINOR)
    assert record["event_ids"] == [600, 700]
    assert record["event_id"] == 700
    assert sorted(record["change_types"]) == sorted([CHANGE_NAME, CHANGE_BANKRUPTCY])
    assert len(record["source_patch"]) == 2
    assert len([r for r in source.requests if "/enheter/" in r.url.path]) == 1


async def test_a_deletion_event_becomes_a_typed_deletion_change(source, load_json, stored):
    source.updates = [
        newest(900),
        (200, update_page([event(600, EQUINOR, change_type="Sletting")])),
    ]
    source.entities = {
        EQUINOR: (200, load_json("entity_deleted.json") | {"organisasjonsnummer": EQUINOR})
    }
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)

    record = row(result, EQUINOR)
    assert record["source_change_type"] == "ENTITY_DELETED"
    assert CHANGE_DELETION in record["change_types"]
    assert record["current"]["deleted"] is True
    assert record["current"]["deletion_date"] == "2021-07-01"
    assert result.state_updates[EQUINOR]["deleted"] is True


async def test_removal_purges_the_cached_record_from_state(source, load_json, stored):
    source.updates = [newest(900), (200, update_page([event(600, EQUINOR, change_type="Fjernet")]))]
    source.entities = {
        EQUINOR: (410, load_json("entity_removed_410.json") | {"organisasjonsnummer": EQUINOR})
    }
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)

    record = row(result, EQUINOR)
    assert record["record_type"] == str(RecordType.REMOVED)
    assert record["source_change_type"] == "REMOVED_FROM_OPEN_DATA"
    stored_after = result.state_updates[EQUINOR]
    assert stored_after["name"] is None
    assert stored_after["business_address_city"] is None
    assert stored_after["employees"] is None
    assert stored_after["deleted"] is True


async def test_a_new_registration_event_is_reported_as_added(source, load_json):
    source.updates = [
        newest(500),
        newest(540),
        (200, update_page([event(520, EQUINOR, change_type="Ny")])),
    ]
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    result = await run(source, queries(EQUINOR))
    assert row(result, EQUINOR)["source_change_type"] == "ENTITY_ADDED"


async def test_an_unknown_source_type_is_preserved_not_guessed(source, load_json, stored):
    source.updates = [
        newest(900),
        (200, update_page([event(600, EQUINOR, change_type="Ukjent")])),
    ]
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)
    assert row(result, EQUINOR)["source_change_type"] == "UNKNOWN_SOURCE_CHANGE"


async def test_an_unmapped_patch_path_emits_evidence_without_inventing_before_after(
    source, load_json, stored
):
    source.updates = [
        newest(900),
        (
            200,
            update_page(
                [
                    event(
                        600,
                        EQUINOR,
                        changes=[{"op": "replace", "path": "/sisteInnsendteAarsregnskap"}],
                    )
                ]
            ),
        ),
    ]
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)

    record = row(result, EQUINOR)
    assert record["change_types"] == ["OTHER_CHANGED"]
    assert record["changes"] == {}
    assert record["patch_change_types"] == ["OTHER_CHANGED"]
    assert record["record_type"] == str(RecordType.CHANGE)
    assert result.results[0].emit
    assert result.cursor_after == 900


async def test_a_patch_contradicting_the_record_is_partial_and_does_not_advance_the_snapshot(
    source, load_json, stored
):
    source.updates = [
        newest(900),
        (
            200,
            update_page(
                [
                    event(
                        600,
                        EQUINOR,
                        changes=[{"op": "replace", "path": "/konkurs", "value": True}],
                    )
                ]
            ),
        ),
    ]
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)

    record = row(result, EQUINOR)
    assert record["status"] == str(OutputStatus.PARTIAL)
    assert record["error"]["code"] == "PATCH_MISMATCH"
    assert record["error"]["fields"] == ["bankrupt"]
    assert result.state_updates[EQUINOR] == stored[EQUINOR]
    assert result.disagreements == 1
    assert not [item for item in result.results if item.billable]
    assert result.results[0].emit
    assert result.cursor_after == 500

    # The record catches up without any additional registry event. The old
    # interval must still be read and the company fetched again.
    source.updates = [newest(950), (200, update_page([event(600, EQUINOR)]))]
    source.entities = {EQUINOR: (200, load_json("entity_current.json") | {"konkurs": True})}
    recovered = await run(
        source,
        queries(EQUINOR),
        previous_states=result.state_updates,
        previous_cursor=result.cursor_after,
    )
    assert row(recovered, EQUINOR)["change_types"] == [CHANGE_BANKRUPTCY]
    assert recovered.state_updates[EQUINOR]["bankrupt"] is True
    assert recovered.cursor_after == 950


async def test_malformed_known_record_pins_cursor_until_normalization_succeeds(
    source, load_json, stored
):
    source.updates = [newest(900), (200, update_page([event(600, EQUINOR)]))]
    source.entities = {EQUINOR: (200, load_json("entity_current.json") | {"antallAnsatte": "bad"})}
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)
    assert row(result, EQUINOR)["error"]["code"] == "MALFORMED_ENTITY"
    assert result.cursor_after == 500
    assert result.state_updates == stored


async def test_pinned_cursor_does_not_repeat_a_successful_peers_source_only_event(
    source, load_json, stored
):
    stored[DNB] = {**stored[EQUINOR], "organization_number": DNB}
    page = update_page(
        [
            event(600, EQUINOR, changes=[{"op": "replace", "path": "/konkurs", "value": True}]),
            event(
                700,
                DNB,
                changes=[{"op": "replace", "path": "/sisteInnsendteAarsregnskap", "value": "2025"}],
            ),
        ]
    )
    source.updates = [newest(900), (200, page)]
    source.entities = {
        EQUINOR: (200, load_json("entity_current.json")),
        DNB: (200, load_json("entity_current.json") | {"organisasjonsnummer": DNB}),
    }
    first = await run(source, queries(EQUINOR, DNB), previous_states=stored, previous_cursor=500)
    assert first.cursor_after == 500
    assert row(first, DNB)["change_types"] == ["OTHER_CHANGED"]
    source.updates = [newest(950), (200, page)]
    source.entities[EQUINOR] = (200, load_json("entity_current.json") | {"konkurs": True})
    source.requests.clear()
    second = await run(
        source, queries(EQUINOR, DNB), previous_states=first.state_updates, previous_cursor=500
    )
    assert not second.results[1].emit
    assert second.cursor_after == 950
    assert not any(request.url.path.endswith("/" + DNB) for request in source.requests)


async def test_patch_publication_limit_does_not_hide_a_late_disagreement(source, load_json, stored):
    patch = [{"op": "replace", "path": "/sisteInnsendteAarsregnskap", "value": "2025"}] * 200
    patch.append({"op": "replace", "path": "/konkurs", "value": True})
    source.updates = [newest(900), (200, update_page([event(600, EQUINOR, changes=patch)]))]
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)
    record = row(result, EQUINOR)
    assert record["status"] == "PARTIAL"
    assert record["error"]["fields"] == ["bankrupt"]
    assert record["source_patch_truncated"]
    assert len(record["source_patch"]) == 200
    assert result.cursor_after == 500


async def test_an_event_the_snapshot_already_reflects_is_a_quiet_success(source, load_json, stored):
    """Intervals legitimately overlap the previous run's re-fetch. Measured live."""
    source.updates = [
        newest(900),
        (
            200,
            update_page(
                [
                    event(
                        600,
                        EQUINOR,
                        changes=[{"op": "replace", "path": "/antallAnsatte", "value": 21239}],
                    )
                ]
            ),
        ),
    ]
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)

    record = row(result, EQUINOR)
    assert record["status"] == str(OutputStatus.SUCCESS)
    assert record["record_type"] == str(RecordType.SNAPSHOT)
    assert record["change_types"] == []
    assert result.disagreements == 0
    assert result.cursor_after == 900
    assert result.state_updates[EQUINOR]["fingerprint"] == stored[EQUINOR]["fingerprint"]
    assert [item.billable for item in result.results] == [True]


async def test_a_source_patch_can_be_switched_off(source, load_json, stored):
    source.updates = [
        newest(900),
        (200, update_page([event(600, EQUINOR, changes=[{"op": "replace", "path": "/navn"}])])),
    ]
    source.entities = {EQUINOR: (200, load_json("entity_current.json") | {"navn": "X ASA"})}
    result = await run(
        source,
        queries(EQUINOR),
        previous_states=stored,
        previous_cursor=500,
        parsed=actor_input(includeSourcePatch=False),
    )
    assert row(result, EQUINOR)["source_patch"] == []
    assert row(result, EQUINOR)["change_types"] == [CHANGE_NAME]


# -- failure semantics -----------------------------------------------------


async def test_a_failed_update_chunk_never_reports_a_quiet_watchlist(source, stored):
    source.updates = [newest(900), (503, None), (503, None), (503, None)]
    with pytest.raises(MonitorPassFailed) as raised:
        await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)
    assert raised.value.code == "INTERVAL_INCOMPLETE"

    result = failed_pass_result(
        queries(EQUINOR),
        failure=raised.value,
        previous_states=stored,
        previous_cursor=500,
        actor_input=actor_input(),
        observed_at=OBSERVED_AT,
    )
    assert row(result, EQUINOR)["status"] == str(OutputStatus.SOURCE_FAILED)
    assert row(result, EQUINOR)["change_types"] == []
    assert result.cursor_after == 500
    assert not result.cursor_advanced
    assert result.state_updates[EQUINOR] == stored[EQUINOR]
    assert not [item for item in result.results if item.billable]


async def test_a_failed_reconciliation_abandons_the_pass_rather_than_skipping_the_event(
    source, stored
):
    source.updates = [newest(900), (200, update_page([event(600, EQUINOR)]))]
    source.entities = {EQUINOR: [(500, None), (500, None), (500, None)]}
    with pytest.raises(MonitorPassFailed) as raised:
        await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)
    assert raised.value.code == "RECONCILE_FAILED"


async def test_a_cursor_that_moves_backward_stops_the_run(source, stored):
    source.updates = [newest(100)]
    with pytest.raises(MonitorPassFailed) as raised:
        await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)
    assert raised.value.code == "CURSOR_WENT_BACKWARD"


async def test_an_unreachable_stream_fails_the_pass_without_touching_state(source, stored):
    source.updates = [(503, None), (503, None), (503, None)]
    with pytest.raises(MonitorPassFailed):
        await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)


async def test_a_stored_snapshot_without_a_cursor_is_re_baselined_not_compared(
    source, load_json, stored
):
    source.updates = [newest(500), newest(500), (200, update_page([]))]
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=None)
    assert row(result, EQUINOR)["record_type"] == str(RecordType.BASELINE)
    assert result.cursor_after == 500


# -- watchlist churn -------------------------------------------------------


async def test_adding_a_target_baselines_it_without_disturbing_the_others(
    source, load_json, stored
):
    source.updates = [
        newest(900),
        newest(940),
        (200, update_page([])),
        (200, update_page([])),
    ]
    source.entities = {DNB: (200, load_json("entity_current.json") | {"organisasjonsnummer": DNB})}
    result = await run(source, queries(EQUINOR, DNB), previous_states=stored, previous_cursor=500)

    assert row(result, EQUINOR)["record_type"] == str(RecordType.SNAPSHOT)
    assert row(result, EQUINOR)["change_types"] == []
    assert row(result, DNB)["record_type"] == str(RecordType.BASELINE)
    assert set(result.state_updates) == {EQUINOR, DNB}
    starts = sorted(
        request.url.params["oppdateringsid"]
        for request in source.requests
        if "organisasjonsnummer" in request.url.params
    )
    assert starts == ["501", "901"]


async def test_removing_a_target_drops_its_state_without_a_fake_deletion_event(source, stored):
    stored = dict(stored) | {DNB: {**stored[EQUINOR], "organization_number": DNB}}
    source.updates = [newest(900), (200, update_page([]))]
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)
    assert set(result.state_updates) == {EQUINOR}
    assert all(record.record["change_types"] == [] for record in result.results)


# -- billing eligibility ---------------------------------------------------


async def test_a_monitored_organization_is_billable_once_even_when_unchanged(source, stored):
    source.updates = [newest(900), (200, update_page([]))]
    result = await run(
        source, queries(EQUINOR, EQUINOR), previous_states=stored, previous_cursor=500
    )
    assert [item.billable for item in result.results] == [True, True]


@pytest.mark.parametrize("status", [OutputStatus.SOURCE_FAILED, OutputStatus.PARTIAL])
def test_only_a_conclusive_answer_is_billable(status):
    assert status not in {OutputStatus.SUCCESS, OutputStatus.NOT_FOUND}


def test_a_budget_skipped_target_is_not_billable_and_pins_the_cursor(stored):
    targets = queries(EQUINOR)
    result = build_run_result(
        targets,
        outcomes={EQUINOR: budget_skipped_outcome()},
        previous_states=stored,
        previous_cursor=500,
        cutoff_id=900,
        actor_input=actor_input(),
        observed_at=OBSERVED_AT,
        budget_skipped=1,
    )
    record = row(result, EQUINOR)
    assert record["status"] == str(OutputStatus.SKIPPED)
    assert record["current"] is None
    assert record["fingerprint"] is None
    assert not [item for item in result.results if item.billable]
    assert result.cursor_after == 500
    assert not result.cursor_advanced
    assert result.state_updates[EQUINOR] == stored[EQUINOR]


# -- summary ---------------------------------------------------------------


async def test_the_run_summary_reports_the_cursor_and_the_work_done(source, load_json, stored):
    source.updates = [
        newest(900),
        (200, update_page([event(600, EQUINOR, changes=[{"op": "replace", "path": "/navn"}])])),
    ]
    source.entities = {EQUINOR: (200, load_json("entity_current.json") | {"navn": "X ASA"})}
    result = await run(source, queries(EQUINOR), previous_states=stored, previous_cursor=500)
    started = datetime(2026, 9, 8, 9, 0, tzinfo=UTC)
    summary = run_summary(
        actor_input=actor_input(),
        result=result,
        invalid=[],
        submitted=1,
        duplicates=0,
        source_requests=3,
        source_retries=0,
        started_at=started,
        finished_at=started,
        state_written=True,
    )
    assert summary["cursor_before"] == 500
    assert summary["cursor_after"] == 900
    assert summary["cursor_advanced"] is True
    assert summary["changed_targets"] == 1
    assert summary["change_rows"] == 1
    assert summary["update_chunks"] == 1
    assert summary["update_pages"] == 1
    assert summary["source_events"] == 1
    assert summary["refetched_targets"] == 1
    assert summary["billable_targets"] == 1
    assert summary["change_types"] == {CHANGE_NAME: 1}
    assert summary["pass_failure"] is None


async def test_a_transport_error_on_the_cutoff_read_is_never_a_quiet_watchlist(source):
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route", request=request)

    source.updates = refuse
    with pytest.raises(MonitorPassFailed):
        await run(source, queries(EQUINOR))
