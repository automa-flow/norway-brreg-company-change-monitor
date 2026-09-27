"""Raw BRREG entity to a canonical, comparable snapshot. Pure functions.

A field is normalized when the register publishes it reliably and a KYB,
supplier-risk or master-data workflow acts on it. Everything else is left in the
source, not copied into a published row.

The register also publishes ``epostadresse``, ``mobil``, ``telefon``,
``hjemmeside``, roles, officers and beneficial owners. For a sole proprietorship
those contact fields are a natural person's own details, so none of them is
normalized, stored or emitted, and the role endpoints are never called.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from norway_brreg_company_change_monitor.models import (
    SCHEMA_VERSION,
    entity_source_url,
)

#: Everything a change may be derived from. The fingerprint hashes exactly these,
#: so the stored snapshot and the comparison can never drift apart. Observation
#: time and run metadata are excluded by construction.
SEMANTIC_FIELDS = (
    "found",
    "deleted",
    "name",
    "organization_form_code",
    "industry_code_1",
    "industry_code_2",
    "industry_code_3",
    "employees",
    "vat_registered",
    "bankrupt",
    "under_liquidation",
    "forced_liquidation_or_dissolution",
    "part_of_group",
    "share_capital_amount",
    "share_capital_currency",
    "business_address_city",
    "business_address_municipality_code",
    "postal_address_city",
    "postal_address_municipality_code",
    "registration_date",
    "deletion_date",
)


class EntityValidationError(ValueError):
    """A source record cannot safely become a conclusive observation."""


@dataclass(frozen=True, slots=True)
class Snapshot:
    """One conclusive answer about one organization number."""

    organization_number: str
    found: bool
    deleted: bool = False
    name: str | None = None
    organization_form_code: str | None = None
    #: Presentation only. BRREG restates the description with the code, so
    #: comparing it would report the same change twice.
    organization_form_description: str | None = None
    industry_code_1: str | None = None
    industry_code_2: str | None = None
    industry_code_3: str | None = None
    employees: int | None = None
    vat_registered: bool | None = None
    bankrupt: bool | None = None
    under_liquidation: bool | None = None
    forced_liquidation_or_dissolution: bool | None = None
    part_of_group: bool | None = None
    share_capital_amount: float | None = None
    share_capital_currency: str | None = None
    business_address_city: str | None = None
    business_address_municipality_code: str | None = None
    postal_address_city: str | None = None
    postal_address_municipality_code: str | None = None
    registration_date: str | None = None
    deletion_date: str | None = None

    def semantic(self) -> dict[str, Any]:
        return {field: getattr(self, field) for field in SEMANTIC_FIELDS}

    @property
    def fingerprint(self) -> str:
        return fingerprint_of(self.organization_number, self.semantic())

    def to_state(self) -> dict[str, Any]:
        """The compact record kept in the KVS and compared on the next run."""
        semantic = self.semantic()
        return {
            "organization_number": self.organization_number,
            **semantic,
            "organization_form_description": self.organization_form_description,
            "fingerprint": fingerprint_of(self.organization_number, semantic),
        }

    def to_output(self) -> dict[str, Any]:
        """The ``current`` block published on a row."""
        return {
            "organization_number": self.organization_number,
            **self.semantic(),
            "organization_form_description": self.organization_form_description,
            "source_url": entity_source_url(self.organization_number),
            "schema_version": SCHEMA_VERSION,
            "fingerprint": self.fingerprint,
        }


def fingerprint_of(organization_number: str, semantic: dict[str, Any]) -> str:
    """SHA-256 over the semantic fields only, in a canonical encoding."""
    payload = {
        "organization_number": organization_number,
        **{field: semantic.get(field) for field in SEMANTIC_FIELDS},
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def absent(organization_number: str) -> Snapshot:
    """The register conclusively has no record for this organization number."""
    return Snapshot(organization_number=organization_number, found=False)


def removed(organization_number: str, raw: dict[str, Any]) -> Snapshot:
    """Removed from open data. Identity and removal date only, by source design.

    ``found`` stays False so a removed company can never be compared field by
    field against the record it used to have; the cached record is deleted from
    state on the same run.
    """
    return Snapshot(
        organization_number=organization_number,
        found=False,
        deleted=True,
        deletion_date=_text(raw.get("slettedato")),
    )


def normalize_entity(raw: dict[str, Any], *, organization_number: str) -> Snapshot:
    """Normalize one BRREG entity record. Deterministic for a given input.

    A ``SlettetEnhet`` legitimately carries only identity, name, legal form and
    ``slettedato``; the absent operational fields are the register's answer, not
    a parse failure.
    """
    deleted = raw.get("respons_klasse") == "SlettetEnhet"
    name = _text(raw.get("navn"))
    if not name:
        raise EntityValidationError("The BRREG entity record has no usable name.")
    form = raw.get("organisasjonsform")
    if form is not None and not isinstance(form, dict):
        raise EntityValidationError("The BRREG field 'organisasjonsform' must be an object.")
    business = _address(raw.get("forretningsadresse"), "forretningsadresse")
    postal = _address(raw.get("postadresse"), "postadresse")
    capital = raw.get("kapital")
    if capital is not None and not isinstance(capital, dict):
        raise EntityValidationError("The BRREG field 'kapital' must be an object.")
    capital = capital or {}
    return Snapshot(
        organization_number=organization_number,
        found=True,
        deleted=deleted,
        name=name,
        organization_form_code=_text((form or {}).get("kode")),
        organization_form_description=_text((form or {}).get("beskrivelse")),
        industry_code_1=_industry(raw, "naeringskode1"),
        industry_code_2=_industry(raw, "naeringskode2"),
        industry_code_3=_industry(raw, "naeringskode3"),
        employees=_integer(raw, "antallAnsatte"),
        vat_registered=_boolean(raw, "registrertIMvaregisteret"),
        bankrupt=_boolean(raw, "konkurs"),
        under_liquidation=_boolean(raw, "underAvvikling"),
        forced_liquidation_or_dissolution=_boolean(
            raw, "underTvangsavviklingEllerTvangsopplosning"
        ),
        part_of_group=_boolean(raw, "erIKonsern"),
        share_capital_amount=_number(capital, "belop"),
        share_capital_currency=_text(capital.get("valuta")),
        business_address_city=business["city"],
        business_address_municipality_code=business["municipality_code"],
        postal_address_city=postal["city"],
        postal_address_municipality_code=postal["municipality_code"],
        registration_date=_date(raw, "registreringsdatoEnhetsregisteret"),
        deletion_date=_date(raw, "slettedato"),
    )


def _address(raw: Any, key: str) -> dict[str, str | None]:
    """Only the city and the municipality code.

    A street line is the home address of a sole proprietor often enough that
    carrying it would collect a personal field the monitoring workflow does not
    need. City and municipality code answer "did this company relocate" without
    it.
    """
    if raw is None:
        return {"city": None, "municipality_code": None}
    if not isinstance(raw, dict):
        raise EntityValidationError(f"The BRREG field '{key}' must be an object.")
    for field in ("poststed", "kommunenummer"):
        if field in raw and raw[field] is not None and not isinstance(raw[field], str):
            raise EntityValidationError(f"The BRREG field '{key}.{field}' must be a string.")
    return {
        "city": _text(raw.get("poststed")),
        "municipality_code": _text(raw.get("kommunenummer")),
    }


def _industry(raw: dict[str, Any], key: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise EntityValidationError(f"The BRREG field '{key}' must be an object.")
    return _text(value.get("kode"))


def _text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return value.strip() or None


def _date(raw: dict[str, Any], key: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise EntityValidationError(f"The BRREG field '{key}' must be a non-empty date string.")
    return value.strip()


def _boolean(raw: dict[str, Any], key: str) -> bool | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise EntityValidationError(f"The BRREG field '{key}' must be a boolean.")
    return value


def _integer(raw: dict[str, Any], key: str) -> int | None:
    value = raw.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise EntityValidationError(f"The BRREG field '{key}' must be an integer.")
    return value


def _number(raw: dict[str, Any], key: str) -> float | None:
    value = raw.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise EntityValidationError(f"The BRREG field '{key}' must be a number.")
    return float(value)
