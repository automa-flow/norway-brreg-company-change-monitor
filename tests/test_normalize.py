from __future__ import annotations

import pytest

from norway_brreg_company_change_monitor.normalize import (
    SEMANTIC_FIELDS,
    EntityValidationError,
    absent,
    normalize_entity,
    removed,
)

EQUINOR = "923609016"


def test_a_current_entity_normalizes_to_the_monitored_field_set(load_json):
    snapshot = normalize_entity(load_json("entity_current.json"), organization_number=EQUINOR)
    assert snapshot.found is True
    assert snapshot.deleted is False
    assert snapshot.name == "EQUINOR ASA"
    assert snapshot.organization_form_code == "ASA"
    assert snapshot.organization_form_description == "Allmennaksjeselskap"
    assert (snapshot.industry_code_1, snapshot.industry_code_2) == ("06.100", "06.200")
    assert snapshot.employees == 21239
    assert snapshot.vat_registered is True
    assert snapshot.bankrupt is False
    assert snapshot.under_liquidation is False
    assert snapshot.forced_liquidation_or_dissolution is False
    assert snapshot.part_of_group is True
    assert snapshot.share_capital_amount == 5976872600.0
    assert snapshot.share_capital_currency == "NOK"
    assert snapshot.business_address_city == "STAVANGER"
    assert snapshot.business_address_municipality_code == "1103"
    assert snapshot.registration_date == "1995-03-12"
    assert snapshot.deletion_date is None


def test_no_personal_or_contact_field_reaches_the_output(load_json):
    raw = load_json("entity_current.json") | {
        "epostadresse": "someone@example.no",
        "mobil": "40000000",
        "telefon": "51990000",
        "hjemmeside": "www.example.no",
    }
    row = normalize_entity(raw, organization_number=EQUINOR).to_output()
    serialized = str(row)
    for leaked in ("someone@example.no", "40000000", "51990000", "example.no"):
        assert leaked not in serialized
    assert not {"epostadresse", "mobil", "telefon", "hjemmeside"} & set(row)


def test_a_street_line_is_never_carried(load_json):
    row = normalize_entity(load_json("entity_current.json"), organization_number=EQUINOR)
    assert "Forusbeen" not in str(row.to_output())


def test_a_deleted_entity_keeps_only_what_the_register_still_publishes(load_json):
    snapshot = normalize_entity(load_json("entity_deleted.json"), organization_number="981276957")
    assert snapshot.found is True
    assert snapshot.deleted is True
    assert snapshot.deletion_date == "2021-07-01"
    assert snapshot.name == "DNB ASA"
    assert snapshot.employees is None
    assert snapshot.bankrupt is None


def test_removal_keeps_identity_and_the_removal_date_and_nothing_else(load_json):
    snapshot = removed("936127088", load_json("entity_removed_410.json"))
    assert snapshot.found is False
    assert snapshot.deleted is True
    assert snapshot.deletion_date == "2026-09-05"
    assert snapshot.name is None
    state = snapshot.to_state()
    assert {key for key, value in state.items() if value not in (None, False)} == {
        "organization_number",
        "deleted",
        "deletion_date",
        "fingerprint",
    }


def test_an_absent_number_is_a_conclusive_empty_snapshot():
    snapshot = absent("810864402")
    assert snapshot.found is False
    assert snapshot.deleted is False
    assert snapshot.name is None


def test_the_fingerprint_is_deterministic_and_excludes_observation_time(load_json):
    raw = load_json("entity_current.json")
    first = normalize_entity(raw, organization_number=EQUINOR)
    second = normalize_entity(dict(reversed(list(raw.items()))), organization_number=EQUINOR)
    assert first.fingerprint == second.fingerprint
    assert set(first.semantic()) == set(SEMANTIC_FIELDS)
    assert "scraped_at" not in first.to_output()


def test_a_presentation_field_never_moves_the_fingerprint(load_json):
    raw = load_json("entity_current.json")
    base = normalize_entity(raw, organization_number=EQUINOR)
    renamed = normalize_entity(
        raw | {"organisasjonsform": {"kode": "ASA", "beskrivelse": "Something else"}},
        organization_number=EQUINOR,
    )
    assert base.fingerprint == renamed.fingerprint


def test_a_semantic_field_always_moves_the_fingerprint(load_json):
    raw = load_json("entity_current.json")
    base = normalize_entity(raw, organization_number=EQUINOR)
    bankrupt = normalize_entity(raw | {"konkurs": True}, organization_number=EQUINOR)
    assert base.fingerprint != bankrupt.fingerprint


@pytest.mark.parametrize(
    "override",
    [
        {"navn": ""},
        {"navn": 7},
        {"antallAnsatte": "many"},
        {"konkurs": "false"},
        {"kapital": "5 000"},
        {"forretningsadresse": []},
        {"naeringskode1": "06.100"},
        {"registreringsdatoEnhetsregisteret": ""},
    ],
)
def test_a_wrong_type_is_a_validation_error_not_a_silent_absence(load_json, override):
    raw = load_json("entity_current.json") | override
    with pytest.raises(EntityValidationError):
        normalize_entity(raw, organization_number=EQUINOR)


def test_absent_optional_fields_are_allowed(load_json):
    raw = {
        "organisasjonsnummer": EQUINOR,
        "navn": "EQUINOR ASA",
        "respons_klasse": "Enhet",
    }
    snapshot = normalize_entity(raw, organization_number=EQUINOR)
    assert snapshot.found is True
    assert snapshot.employees is None
    assert snapshot.business_address_city is None
