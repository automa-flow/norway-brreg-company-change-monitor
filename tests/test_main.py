"""The Apify edge without the platform: watchlist preparation and a refused charge."""

from __future__ import annotations

from norway_brreg_company_change_monitor.main import _prepare, _skip_target
from norway_brreg_company_change_monitor.models import ActorInput, OutputStatus, RecordType
from norway_brreg_company_change_monitor.normalize import normalize_entity
from norway_brreg_company_change_monitor.service import TargetOutcome, build_run_result

EQUINOR = "923609016"
DNB = "984851006"
OBSERVED_AT = "2026-09-08T09:00:00Z"


def actor_input(numbers: list[object]) -> ActorInput:
    return ActorInput.model_validate({"organizationNumbers": numbers, "monitorKey": "test"})


def test_each_entry_is_validated_alone_and_a_repeat_is_monitored_once() -> None:
    parsed = actor_input([EQUINOR, "NO 923 609 016 MVA", "12", {"organizationNumber": DNB}, 5, DNB])
    valid, invalid, duplicates = _prepare(parsed)
    assert [query.organization_number for query in valid] == [EQUINOR, DNB]
    assert [query.index for query in valid] == [0, 3]
    assert [item.index for item in invalid] == [2, 4]
    assert duplicates == 2


def _changed_run(load_json, *, previous_states):
    record = load_json("entity_current.json")
    snapshot = normalize_entity(record | {"konkurs": True}, organization_number=EQUINOR)
    parsed = actor_input([EQUINOR])
    (query,), _, _ = _prepare(parsed)
    result = build_run_result(
        [query],
        outcomes={EQUINOR: TargetOutcome(snapshot=snapshot)},
        previous_states=previous_states,
        previous_cursor=500,
        cutoff_id=900,
        actor_input=parsed,
        observed_at=OBSERVED_AT,
    )
    return result, parsed


def test_a_refused_charge_publishes_no_data_restores_state_and_pins_the_cursor(load_json) -> None:
    stored = {
        EQUINOR: normalize_entity(
            load_json("entity_current.json"), organization_number=EQUINOR
        ).to_state()
    }
    result, parsed = _changed_run(load_json, previous_states=stored)
    assert result.results[0].record["change_types"] and result.cursor_after == 900

    _skip_target(result, 0, stored, actor_input=parsed, observed_at=OBSERVED_AT)

    skipped = result.results[0]
    assert skipped.status is OutputStatus.SKIPPED
    assert not skipped.billable
    assert skipped.record["record_type"] == str(RecordType.ERROR)
    assert skipped.record["current"] is None
    assert skipped.record["change_types"] == []
    assert skipped.record["fingerprint"] is None
    assert result.state_updates[EQUINOR] == stored[EQUINOR]
    assert not result.cursor_advanced
    assert result.cursor_after == 500


def test_a_refused_charge_on_a_new_company_stores_no_baseline(load_json) -> None:
    result, parsed = _changed_run(load_json, previous_states={})
    assert EQUINOR in result.state_updates

    _skip_target(result, 0, {}, actor_input=parsed, observed_at=OBSERVED_AT)

    assert EQUINOR not in result.state_updates
    assert result.results[0].status is OutputStatus.SKIPPED
