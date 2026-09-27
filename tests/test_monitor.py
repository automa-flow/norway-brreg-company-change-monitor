from __future__ import annotations

from copy import deepcopy

import pytest

from norway_brreg_company_change_monitor.models import SourceChangeType
from norway_brreg_company_change_monitor.monitor import (
    ALL_CHANGE_TYPES,
    CHANGE_ADDRESS,
    CHANGE_BANKRUPTCY,
    CHANGE_DELETION,
    CHANGE_EMPLOYEES,
    CHANGE_GROUP,
    CHANGE_INDUSTRY,
    CHANGE_LEGAL_FORM,
    CHANGE_LIQUIDATION,
    CHANGE_NAME,
    CHANGE_OTHER,
    CHANGE_REGISTRATION_DATA,
    CHANGE_SHARE_CAPITAL,
    CHANGE_VAT,
    FIELD_CHANGE_TYPES,
    detect_changes,
    patch_change_types,
    redact_patch,
    source_change_type,
)
from norway_brreg_company_change_monitor.normalize import SEMANTIC_FIELDS, normalize_entity

EQUINOR = "923609016"


@pytest.fixture
def baseline(load_json):
    return normalize_entity(
        load_json("entity_current.json"), organization_number=EQUINOR
    ).to_state()


def moved(load_json, **override):
    return normalize_entity(
        load_json("entity_current.json") | override, organization_number=EQUINOR
    ).to_state()


# -- source event vocabulary ----------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Ny", SourceChangeType.ENTITY_ADDED),
        ("Endring", SourceChangeType.ENTITY_CHANGED),
        ("Sletting", SourceChangeType.ENTITY_DELETED),
        ("Fjernet", SourceChangeType.REMOVED_FROM_OPEN_DATA),
        ("Ukjent", SourceChangeType.UNKNOWN_SOURCE_CHANGE),
        ("SomethingBrregAddsLater", SourceChangeType.UNKNOWN_SOURCE_CHANGE),
    ],
)
def test_every_source_event_type_maps_or_is_preserved_as_unknown(raw, expected):
    assert source_change_type(raw) == str(expected)


# -- typed changes from the record ----------------------------------------


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ({"navn": "EQUINOR NORGE ASA"}, CHANGE_NAME),
        ({"organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"}}, CHANGE_LEGAL_FORM),
        ({"naeringskode1": {"kode": "06.200"}}, CHANGE_INDUSTRY),
        ({"antallAnsatte": 21240}, CHANGE_EMPLOYEES),
        ({"registrertIMvaregisteret": False}, CHANGE_VAT),
        ({"konkurs": True}, CHANGE_BANKRUPTCY),
        ({"underAvvikling": True}, CHANGE_LIQUIDATION),
        ({"underTvangsavviklingEllerTvangsopplosning": True}, CHANGE_LIQUIDATION),
        ({"erIKonsern": False}, CHANGE_GROUP),
        ({"kapital": {"belop": 1.0, "valuta": "NOK"}}, CHANGE_SHARE_CAPITAL),
        ({"registreringsdatoEnhetsregisteret": "1995-03-13"}, CHANGE_REGISTRATION_DATA),
    ],
)
def test_each_watched_field_produces_its_own_typed_change(load_json, baseline, override, expected):
    decision = detect_changes(baseline, moved(load_json, **override))
    assert decision.change_types == (expected,)
    assert decision.changed


def test_a_relocation_is_one_address_change_with_both_fields(load_json, baseline):
    current = moved(
        load_json,
        forretningsadresse={"poststed": "OSLO", "kommunenummer": "0301"},
    )
    decision = detect_changes(baseline, current)
    assert decision.change_types == (CHANGE_ADDRESS,)
    assert decision.changes["business_address_city"] == {
        "previous": "STAVANGER",
        "current": "OSLO",
    }
    assert decision.changes["business_address_municipality_code"]["current"] == "0301"


def test_an_unchanged_record_produces_nothing(load_json, baseline):
    decision = detect_changes(baseline, moved(load_json))
    assert decision.change_types == ()
    assert decision.changes == {}
    assert not decision.changed


def test_deletion_is_reported_as_its_own_typed_change(load_json, baseline):
    deleted = normalize_entity(
        load_json("entity_deleted.json"), organization_number="981276957"
    ).to_state()
    decision = detect_changes(baseline, {**deleted, "organization_number": EQUINOR})
    assert CHANGE_DELETION in decision.change_types
    assert decision.changes["deleted"] == {"previous": False, "current": True}


def test_leaving_the_open_register_reports_only_the_presence_transition(load_json, baseline):
    gone = {**baseline, "found": False, "deleted": True, "deletion_date": "2026-09-05"}
    decision = detect_changes(baseline, gone)
    assert decision.change_types == (CHANGE_DELETION,)
    assert set(decision.changes) == {"found", "deleted", "deletion_date"}


def test_appearing_in_the_register_is_registration_data(load_json, baseline):
    decision = detect_changes({**baseline, "found": False, "name": None}, baseline)
    assert decision.change_types == (CHANGE_REGISTRATION_DATA,)
    assert decision.changes["found"] == {"previous": False, "current": True}


def test_several_fields_moving_produce_several_types_in_a_stable_order(load_json, baseline):
    current = moved(load_json, konkurs=True, navn="EQUINOR ASA UNDER KONKURS", antallAnsatte=0)
    decision = detect_changes(baseline, current)
    assert decision.change_types == (CHANGE_NAME, CHANGE_EMPLOYEES, CHANGE_BANKRUPTCY)
    assert list(decision.change_types) == sorted(decision.change_types, key=ALL_CHANGE_TYPES.index)


def test_every_stored_field_has_a_declared_change_type():
    assert set(FIELD_CHANGE_TYPES) == set(SEMANTIC_FIELDS) - {"found"}
    assert set(FIELD_CHANGE_TYPES.values()) <= set(ALL_CHANGE_TYPES)


# -- the official patch as evidence ---------------------------------------


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/navn", CHANGE_NAME),
        ("/historiskeNavn/-", CHANGE_NAME),
        ("/organisasjonsform/kode", CHANGE_LEGAL_FORM),
        ("/forretningsadresse/adresse/0", CHANGE_ADDRESS),
        ("/postadresse/postnummer", CHANGE_ADDRESS),
        ("/naeringskode1/kode", CHANGE_INDUSTRY),
        ("/antallAnsatte", CHANGE_EMPLOYEES),
        ("/registreringsdatoAntallAnsatteNAVAaregisteret", CHANGE_EMPLOYEES),
        ("/registrertIMvaregisteret", CHANGE_VAT),
        ("/registreringsdatoMerverdiavgiftsregisteret", CHANGE_VAT),
        ("/konkurs", CHANGE_BANKRUPTCY),
        ("/konkursdato", CHANGE_BANKRUPTCY),
        ("/underAvviklingDato", CHANGE_LIQUIDATION),
        ("/tvangsopplostPgaManglendeRegnskapDato", CHANGE_LIQUIDATION),
        ("/erIKonsern", CHANGE_GROUP),
        ("/kapital/belop", CHANGE_SHARE_CAPITAL),
        ("/registreringsdatoForetaksregisteret", CHANGE_REGISTRATION_DATA),
        ("/slettedato", CHANGE_DELETION),
    ],
)
def test_live_patch_paths_classify_without_guessing(path, expected):
    assert patch_change_types([{"op": "replace", "path": path}]) == (expected,)


@pytest.mark.parametrize(
    "path",
    ["/sisteInnsendteAarsregnskap", "/vedtektsdato", "/aktivitet/0", "/somethingBrregAddsIn2030"],
)
def test_an_unmapped_path_is_other_changed_and_never_invented(path):
    assert patch_change_types([{"op": "replace", "path": path}]) == (CHANGE_OTHER,)


def test_contact_values_are_redacted_while_the_path_survives():
    patch = redact_patch(
        [
            {"op": "replace", "path": "/epostadresse", "value": "someone@example.no"},
            {"op": "replace", "path": "/mobil", "value": "40000000"},
            {"op": "remove", "path": "/telefon", "value": "51990000"},
            {"op": "replace", "path": "/navn", "value": "EQUINOR ASA"},
        ]
    )
    assert [entry["path"] for entry in patch] == [
        "/epostadresse",
        "/mobil",
        "/telefon",
        "/navn",
    ]
    assert all(entry.get("value_redacted") for entry in patch[:3])
    assert all("value" not in entry for entry in patch[:3])
    assert patch[3]["value"] == "EQUINOR ASA"


@pytest.mark.parametrize("root", ["forretningsadresse", "postadresse", "beliggenhetsadresse"])
def test_address_values_are_minimized_for_leaf_array_and_object_operations(root):
    address = {"adresse": ["SYNTHETIC PRIVATE STREET"], "poststed": "OSLO", "kommunenummer": "0301"}
    operations = [
        {"op": "replace", "path": f"/{root}/adresse/0", "value": "SYNTHETIC PRIVATE STREET"},
        {"op": "add", "path": f"/{root}/adresse", "value": ["SYNTHETIC PRIVATE STREET"]},
        {"op": "replace", "path": f"/{root}", "value": address},
        {
            "op": "replace",
            "path": "",
            "value": {root: address, "epostadresse": "private@example.invalid"},
        },
        {
            "op": "copy",
            "from": f"/{root}/adresse/0",
            "path": "/navn",
            "value": "SYNTHETIC PRIVATE STREET",
        },
    ]
    original = deepcopy(operations)
    redacted = redact_patch(operations)
    assert "SYNTHETIC PRIVATE STREET" not in str(redacted)
    assert "private@example.invalid" not in str(redacted)
    assert redacted[2]["value"] == {"poststed": "OSLO", "kommunenummer": "0301"}
    assert all(operation["value_redacted"] for operation in redacted)
    assert operations == original


def test_a_record_older_than_the_event_it_should_contain_is_flagged(load_json, baseline):
    decision = detect_changes(
        baseline,
        moved(load_json),
        patch=[{"op": "replace", "path": "/konkurs", "value": True}],
    )
    assert decision.disagreement == ("bankrupt",)


def test_an_event_the_snapshot_already_reflects_is_not_a_contradiction(load_json, baseline):
    """The live case: an interval replays an event the baseline already contains.

    Measured 2026-09-08 against Equinor and DNB. The patch says antallAnsatte was
    set to the value the stored snapshot already holds, because the snapshot was
    fetched after that event. Flagging it would stall a perfectly good snapshot.
    """
    decision = detect_changes(
        baseline,
        moved(load_json),
        patch=[
            {"op": "replace", "path": "/antallAnsatte", "value": 21239},
            {
                "op": "replace",
                "path": "/registreringsdatoAntallAnsatteNAVAaregisteret",
                "value": "2026-08-10",
            },
        ],
    )
    assert decision.change_types == ()
    assert decision.disagreement == ()


def test_a_record_that_moved_past_the_stated_value_is_not_a_contradiction(load_json, baseline):
    decision = detect_changes(
        baseline,
        moved(load_json, antallAnsatte=21999),
        patch=[{"op": "replace", "path": "/antallAnsatte", "value": 21500}],
    )
    assert decision.change_types == (CHANGE_EMPLOYEES,)
    assert decision.disagreement == ()


def test_a_numeric_value_matches_across_int_and_float(load_json, baseline):
    decision = detect_changes(
        baseline,
        moved(load_json),
        patch=[{"op": "replace", "path": "/kapital/belop", "value": 5976872600}],
    )
    assert decision.disagreement == ()


def test_patch_text_is_compared_the_way_the_snapshot_stores_it(load_json, baseline):
    # The snapshot keeps trimmed text and never an empty string. A patch value that
    # differs only in that way is the same value; flagging it would create a PARTIAL
    # that no later record can resolve, pinning the cursor for good.
    padded = [{"op": "replace", "path": "/navn", "value": f"  {baseline['name']} "}]
    assert detect_changes(baseline, moved(load_json), patch=padded).disagreement == ()
    empty = [{"op": "replace", "path": "/slettedato", "value": ""}]
    assert detect_changes(baseline, moved(load_json), patch=empty).disagreement == ()


def test_a_removal_operation_expects_an_absent_value(load_json, baseline):
    cleared = moved(load_json, naeringskode2=None)
    assert (
        detect_changes(
            baseline, cleared, patch=[{"op": "remove", "path": "/naeringskode2/kode"}]
        ).disagreement
        == ()
    )
    assert detect_changes(
        baseline, moved(load_json), patch=[{"op": "remove", "path": "/naeringskode2/kode"}]
    ).disagreement == ("industry_code_2",)


def test_the_last_operation_for_a_field_states_the_expected_value(load_json, baseline):
    decision = detect_changes(
        baseline,
        moved(load_json, navn="FINAL ASA"),
        patch=[
            {"op": "replace", "path": "/navn", "value": "INTERIM ASA"},
            {"op": "replace", "path": "/navn", "value": "FINAL ASA"},
        ],
    )
    assert decision.change_types == (CHANGE_NAME,)
    assert decision.disagreement == ()


def test_a_patch_about_an_unstored_field_never_contradicts_the_record(load_json, baseline):
    decision = detect_changes(
        baseline,
        moved(load_json),
        patch=[
            {"op": "replace", "path": "/sisteInnsendteAarsregnskap", "value": "2025"},
            {"op": "replace", "path": "/registreringsdatoMerverdiavgiftsregisteret"},
            {"op": "replace", "path": "/forretningsadresse"},
        ],
    )
    assert decision.disagreement == ()


def test_a_field_that_moved_without_a_patch_path_is_not_a_contradiction(load_json, baseline):
    decision = detect_changes(baseline, moved(load_json, konkurs=True), patch=[])
    assert decision.change_types == (CHANGE_BANKRUPTCY,)
    assert decision.disagreement == ()


def test_every_path_the_live_stream_produced_classifies_deterministically(load_json):
    """The whole measured vocabulary, so a future BRREG field cannot pass silently."""
    observed = load_json("observed_change_paths.json")
    classified = {
        path: patch_change_types([{"op": "replace", "path": path}])[0] for path in observed["paths"]
    }
    assert len(classified) == 110
    unmapped = {path for path, kind in classified.items() if kind == CHANGE_OTHER}
    # These are real registry data this Actor deliberately does not normalize:
    # activity and articles text, contact details, the last filed annual account,
    # sector code, language, endorsements and the audit opt-out.
    assert {path.split("/")[1] for path in unmapped} == {
        "aktivitet",
        "epostadresse",
        "fravalgRevisjonBeslutningsDato",
        "fravalgRevisjonDato",
        "hjemmeside",
        "institusjonellSektorkode",
        "maalform",
        "mobil",
        "paategninger",
        "sisteInnsendteAarsregnskap",
        "telefon",
        "vedtektsdato",
        "vedtektsfestetFormaal",
    }
    assert set(classified.values()) - {CHANGE_OTHER} <= set(ALL_CHANGE_TYPES)
