from __future__ import annotations

import pytest
from pydantic import ValidationError

from norway_brreg_company_change_monitor.main import _prepare
from norway_brreg_company_change_monitor.models import (
    MAX_ORGANIZATION_NUMBERS,
    ActorInput,
    BaselineMode,
    CompanyQuery,
    InvalidCompany,
    Mode,
    normalize_organization_number,
    validate_organization,
)


def test_defaults_are_the_scheduled_monitoring_shape():
    parsed = ActorInput.model_validate({"organizationNumbers": ["923609016"]})
    assert parsed.monitor_key == "default"
    assert parsed.mode is Mode.CHANGES_ONLY
    assert parsed.baseline_mode is BaselineMode.EMIT_SNAPSHOT
    assert parsed.include_source_patch is True


def test_empty_watchlist_is_rejected():
    with pytest.raises(ValidationError):
        ActorInput.model_validate({"organizationNumbers": []})


def test_watchlist_ceiling_is_enforced():
    with pytest.raises(ValidationError):
        ActorInput.model_validate(
            {"organizationNumbers": ["923609016"] * (MAX_ORGANIZATION_NUMBERS + 1)}
        )


@pytest.mark.parametrize("key", ["", "a" * 81, "has space", "semi;colon"])
def test_invalid_monitor_key_is_rejected(key):
    with pytest.raises((ValidationError, ValueError)):
        ActorInput.model_validate({"organizationNumbers": ["923609016"], "monitorKey": key})


def test_unknown_field_is_rejected():
    with pytest.raises(ValidationError):
        ActorInput.model_validate({"organizationNumbers": ["923609016"], "concurrency": 20})


@pytest.mark.parametrize(
    "raw",
    ["923609016", "923 609 016", "923-609-016", "NO923609016", "NO 923 609 016 MVA", "923.609.016"],
)
def test_common_spellings_normalize_to_one_identity(raw):
    assert normalize_organization_number(raw) == "923609016"


@pytest.mark.parametrize("raw", ["", "12345", "1234567890", "92360901X", "  ", "NO12345MVA"])
def test_malformed_numbers_are_rejected(raw):
    with pytest.raises(ValueError):
        normalize_organization_number(raw)


def test_one_bad_entry_does_not_deny_the_rest_their_answer():
    parsed = ActorInput.model_validate(
        {"organizationNumbers": ["923609016", "12345", {"organizationNumber": "984851006"}]}
    )
    valid, invalid, duplicates = _prepare(parsed)
    assert [query.organization_number for query in valid] == ["923609016", "984851006"]
    assert [item.code for item in invalid] == ["INVALID_ORGANIZATION_NUMBER"]
    assert invalid[0].index == 1
    assert duplicates == 0


def test_duplicates_collapse_to_one_monitored_organization_preserving_order():
    parsed = ActorInput.model_validate(
        {"organizationNumbers": ["984851006", "NO 923 609 016", "923609016", "984851006"]}
    )
    valid, invalid, duplicates = _prepare(parsed)
    assert [query.organization_number for query in valid] == ["984851006", "923609016"]
    assert [query.index for query in valid] == [0, 1]
    assert not invalid
    assert duplicates == 2


def test_a_non_string_entry_is_reported_not_dropped():
    prepared = validate_organization(42, index=3)
    assert isinstance(prepared, InvalidCompany)
    assert prepared.code == "INVALID_ENTRY"
    assert prepared.submitted is None


def test_the_submitted_spelling_is_echoed_back():
    prepared = validate_organization("NO 923 609 016 MVA", index=0)
    assert isinstance(prepared, CompanyQuery)
    assert prepared.submitted == "NO 923 609 016 MVA"
    assert prepared.organization_number == "923609016"
