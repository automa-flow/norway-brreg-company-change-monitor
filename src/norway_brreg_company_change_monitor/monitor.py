"""Change classification. All pure functions.

Two vocabularies, deliberately kept apart:

* ``source_change_type`` is BRREG's own ``endringstype``, mapped one to one;
* ``change_types`` is the Actor's stable semantic contract, derived from the diff
  between the last successful stored snapshot and the newly fetched record.

The official patch is *evidence*. The normalized diff is the *contract*. When the
patch claims a field moved and the snapshot diff shows nothing, or the other way
round, the record is PARTIAL: both are published, the discrepancy is counted, and
the stored snapshot is not advanced.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from norway_brreg_company_change_monitor.models import SOURCE_CHANGE_TYPES, SourceChangeType

CHANGE_NAME = "NAME_CHANGED"
CHANGE_LEGAL_FORM = "LEGAL_FORM_CHANGED"
CHANGE_ADDRESS = "ADDRESS_CHANGED"
CHANGE_INDUSTRY = "INDUSTRY_CHANGED"
CHANGE_EMPLOYEES = "EMPLOYEE_COUNT_CHANGED"
CHANGE_VAT = "VAT_STATUS_CHANGED"
CHANGE_BANKRUPTCY = "BANKRUPTCY_STATUS_CHANGED"
CHANGE_LIQUIDATION = "LIQUIDATION_STATUS_CHANGED"
CHANGE_GROUP = "GROUP_STATUS_CHANGED"
CHANGE_SHARE_CAPITAL = "SHARE_CAPITAL_CHANGED"
CHANGE_REGISTRATION_DATA = "REGISTRATION_DATA_CHANGED"
#: Not in the brief's list. The register publishes deletion explicitly - HTTP 200
#: with ``respons_klasse: "SlettetEnhet"`` and a ``slettedato``, 431 events in one
#: measured 10,000-id window - so this is a proven state rather than meaning
#: invented for an unfamiliar path. Filing it under OTHER_CHANGED would hide the
#: single most consequential thing a supplier watchlist can report.
CHANGE_DELETION = "DELETION_STATUS_CHANGED"
CHANGE_OTHER = "OTHER_CHANGED"

ALL_CHANGE_TYPES = (
    CHANGE_NAME,
    CHANGE_LEGAL_FORM,
    CHANGE_ADDRESS,
    CHANGE_INDUSTRY,
    CHANGE_EMPLOYEES,
    CHANGE_VAT,
    CHANGE_BANKRUPTCY,
    CHANGE_LIQUIDATION,
    CHANGE_GROUP,
    CHANGE_SHARE_CAPITAL,
    CHANGE_REGISTRATION_DATA,
    CHANGE_DELETION,
    CHANGE_OTHER,
)

#: Normalized field -> semantic change type. Every field in the stored snapshot
#: appears here exactly once, so a new field cannot silently produce no type.
FIELD_CHANGE_TYPES: dict[str, str] = {
    "name": CHANGE_NAME,
    "organization_form_code": CHANGE_LEGAL_FORM,
    "business_address_city": CHANGE_ADDRESS,
    "business_address_municipality_code": CHANGE_ADDRESS,
    "postal_address_city": CHANGE_ADDRESS,
    "postal_address_municipality_code": CHANGE_ADDRESS,
    "industry_code_1": CHANGE_INDUSTRY,
    "industry_code_2": CHANGE_INDUSTRY,
    "industry_code_3": CHANGE_INDUSTRY,
    "employees": CHANGE_EMPLOYEES,
    "vat_registered": CHANGE_VAT,
    "bankrupt": CHANGE_BANKRUPTCY,
    "under_liquidation": CHANGE_LIQUIDATION,
    "forced_liquidation_or_dissolution": CHANGE_LIQUIDATION,
    "part_of_group": CHANGE_GROUP,
    "share_capital_amount": CHANGE_SHARE_CAPITAL,
    "share_capital_currency": CHANGE_SHARE_CAPITAL,
    "registration_date": CHANGE_REGISTRATION_DATA,
    "deleted": CHANGE_DELETION,
    "deletion_date": CHANGE_DELETION,
}

#: Source patch path -> semantic change type, for the paths the live stream
#: actually produces. Measured over a 10,000-id window: 110 distinct paths.
#: Matching is by whole JSON-pointer segment, so ``/konkurs`` never swallows
#: ``/konkursdato`` - each sibling BRREG actually emits is listed on its own.
#: Everything unlisted stays OTHER_CHANGED with the patch attached; no meaning is
#: invented for a path this Actor has not seen.
PATCH_PREFIX_CHANGE_TYPES: tuple[tuple[str, str], ...] = (
    ("/navn", CHANGE_NAME),
    ("/historiskeNavn", CHANGE_NAME),
    ("/organisasjonsform", CHANGE_LEGAL_FORM),
    ("/forretningsadresse", CHANGE_ADDRESS),
    ("/postadresse", CHANGE_ADDRESS),
    ("/beliggenhetsadresse", CHANGE_ADDRESS),
    ("/naeringskode1", CHANGE_INDUSTRY),
    ("/naeringskode2", CHANGE_INDUSTRY),
    ("/naeringskode3", CHANGE_INDUSTRY),
    ("/hjelpeenhetskode", CHANGE_INDUSTRY),
    ("/antallAnsatte", CHANGE_EMPLOYEES),
    ("/harRegistrertAntallAnsatte", CHANGE_EMPLOYEES),
    ("/registreringsdatoAntallAnsatteEnhetsregisteret", CHANGE_EMPLOYEES),
    ("/registreringsdatoAntallAnsatteNAVAaregisteret", CHANGE_EMPLOYEES),
    ("/registrertIMvaregisteret", CHANGE_VAT),
    ("/registreringsdatoMerverdiavgiftsregisteret", CHANGE_VAT),
    ("/registreringsdatoMerverdiavgiftsregisteretEnhetsregisteret", CHANGE_VAT),
    ("/registreringsdatoFrivilligMerverdiavgiftsregisteret", CHANGE_VAT),
    ("/frivilligMvaRegistrertBeskrivelser", CHANGE_VAT),
    ("/konkurs", CHANGE_BANKRUPTCY),
    ("/konkursdato", CHANGE_BANKRUPTCY),
    ("/underAvvikling", CHANGE_LIQUIDATION),
    ("/underAvviklingDato", CHANGE_LIQUIDATION),
    ("/underTvangsavviklingEllerTvangsopplosning", CHANGE_LIQUIDATION),
    ("/tvangsopplostPgaManglendeRegnskapDato", CHANGE_LIQUIDATION),
    ("/tvangsopplostPgaManglendeRevisorDato", CHANGE_LIQUIDATION),
    ("/erIKonsern", CHANGE_GROUP),
    ("/kapital", CHANGE_SHARE_CAPITAL),
    ("/registreringsdatoEnhetsregisteret", CHANGE_REGISTRATION_DATA),
    ("/registreringsdatoForetaksregisteret", CHANGE_REGISTRATION_DATA),
    ("/registreringsdatoFrivillighetsregisteret", CHANGE_REGISTRATION_DATA),
    ("/registreringsdatoStiftelsesregisteret", CHANGE_REGISTRATION_DATA),
    ("/registrertIForetaksregisteret", CHANGE_REGISTRATION_DATA),
    ("/registrertIStiftelsesregisteret", CHANGE_REGISTRATION_DATA),
    ("/registrertIFrivillighetsregisteret", CHANGE_REGISTRATION_DATA),
    ("/registrertIPartiregisteret", CHANGE_REGISTRATION_DATA),
    ("/slettedato", CHANGE_DELETION),
    ("/respons_klasse", CHANGE_DELETION),
)

#: Patch paths whose value is a natural person's contact detail for a sole
#: proprietorship. The path is kept, because "a contact detail moved" is useful;
#: the value is dropped, because publishing it would collect a personal field
#: this Actor has no function for.
PRIVATE_FIELDS = frozenset({"epostadresse", "mobil", "telefon", "hjemmeside", "adresse"})
ADDRESS_FIELDS = frozenset({"forretningsadresse", "postadresse", "beliggenhetsadresse"})
PUBLIC_ADDRESS_FIELDS = frozenset({"poststed", "kommunenummer"})

#: Exact leaf paths that map one-to-one onto a stored normalized field. Only
#: these can contradict the record, and only in one direction: the patch says
#: this exact field moved, and the current record still equals the snapshot.
#:
#: Container paths such as ``/forretningsadresse`` (the whole object replaced) and
#: paths for data this Actor deliberately does not store - the VAT and employee
#: *registration dates*, ``/sisteInnsendteAarsregnskap``, ``/vedtektsdato`` - are
#: absent on purpose. They are evidence about fields with no stored counterpart,
#: so they can never disagree with one.
PATCH_PATH_FIELDS: dict[str, str] = {
    "/navn": "name",
    "/organisasjonsform/kode": "organization_form_code",
    "/forretningsadresse/poststed": "business_address_city",
    "/forretningsadresse/kommunenummer": "business_address_municipality_code",
    "/postadresse/poststed": "postal_address_city",
    "/postadresse/kommunenummer": "postal_address_municipality_code",
    "/naeringskode1/kode": "industry_code_1",
    "/naeringskode2/kode": "industry_code_2",
    "/naeringskode3/kode": "industry_code_3",
    "/antallAnsatte": "employees",
    "/registrertIMvaregisteret": "vat_registered",
    "/konkurs": "bankrupt",
    "/underAvvikling": "under_liquidation",
    "/underTvangsavviklingEllerTvangsopplosning": "forced_liquidation_or_dissolution",
    "/erIKonsern": "part_of_group",
    "/kapital/belop": "share_capital_amount",
    "/kapital/valuta": "share_capital_currency",
    "/registreringsdatoEnhetsregisteret": "registration_date",
    "/slettedato": "deletion_date",
}

PARTIAL_MESSAGE = (
    "The official BRREG patch states a value for a monitored field, and the current record "
    "shows neither that value nor any movement away from the stored snapshot. The record read "
    "is older than the event it should already contain, so both are published, the stored "
    "snapshot is left where it was, and the next run compares against that state."
)


@dataclass(frozen=True, slots=True)
class ChangeDecision:
    change_types: tuple[str, ...]
    changes: dict[str, Any]
    #: Monitored fields whose fetched value matches neither the value the
    #: official patch states nor any movement from the stored snapshot.
    #: Non-empty means the record is PARTIAL.
    disagreement: tuple[str, ...] = ()

    @property
    def changed(self) -> bool:
        return bool(self.change_types)


def source_change_type(raw: str) -> str:
    """Map BRREG's ``endringstype``, preserving anything unrecognised."""
    return str(SOURCE_CHANGE_TYPES.get(raw, SourceChangeType.UNKNOWN_SOURCE_CHANGE))


def patch_change_types(operations: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    """Classify a source patch by path prefix. Unfamiliar paths become OTHER_CHANGED."""
    types: list[str] = []
    for operation in operations:
        path = operation.get("path")
        if not isinstance(path, str):
            continue
        matched = next(
            (
                change_type
                for prefix, change_type in PATCH_PREFIX_CHANGE_TYPES
                if path == prefix or path.startswith(f"{prefix}/")
            ),
            CHANGE_OTHER,
        )
        if matched not in types:
            types.append(matched)
    return tuple(types)


def redact_patch(operations: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Minimize leaf operations and whole-object replacements without mutating input."""
    redacted = []
    for operation in operations:
        entry = dict(operation)
        if "value" in entry:
            if _private_path(entry.get("path", "")) or _private_path(entry.get("from", "")):
                entry.pop("value")
                entry["value_redacted"] = True
            else:
                value = entry["value"]
                segments = _pointer_segments(entry.get("path", ""))
                if segments and segments[-1] in ADDRESS_FIELDS and isinstance(value, Mapping):
                    value = {
                        key: item for key, item in value.items() if key in PUBLIC_ADDRESS_FIELDS
                    }
                cleaned = _redact_value(value)
                if cleaned != entry["value"]:
                    entry["value_redacted"] = True
                entry["value"] = cleaned
        redacted.append(entry)
    return redacted


def _pointer_segments(path: str) -> list[str]:
    return [part.replace("~1", "/").replace("~0", "~") for part in path.split("/")[1:]]


def _private_path(path: str) -> bool:
    segments = _pointer_segments(path)
    return any(
        part in PRIVATE_FIELDS
        or (
            part in ADDRESS_FIELDS
            and index + 1 < len(segments)
            and segments[index + 1] not in PUBLIC_ADDRESS_FIELDS
        )
        for index, part in enumerate(segments)
    )


def _redact_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            if key in PRIVATE_FIELDS:
                continue
            if key in ADDRESS_FIELDS and isinstance(item, Mapping):
                item = {
                    field: part for field, part in item.items() if field in PUBLIC_ADDRESS_FIELDS
                }
            result[key] = _redact_value(item)
        return result
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    return value


def detect_changes(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
    *,
    patch: Sequence[Mapping[str, Any]] = (),
) -> ChangeDecision:
    """Compare two conclusive snapshots of the same organization number."""
    change_types: list[str] = []
    changes: dict[str, Any] = {}
    for field, change_type in FIELD_CHANGE_TYPES.items():
        before = previous.get(field)
        after = current.get(field)
        if before == after:
            continue
        changes[field] = {"previous": before, "current": after}
        if change_type not in change_types:
            change_types.append(change_type)

    was_found = bool(previous.get("found"))
    is_found = bool(current.get("found"))
    if was_found != is_found:
        # Every operational field is undefined on one side of this transition, so
        # the per-field entries would be noise dressed as information. Presence
        # in the open register is itself the deletion-status field.
        changes = {
            key: value for key, value in changes.items() if key in ("deleted", "deletion_date")
        }
        changes["found"] = {"previous": was_found, "current": is_found}
        change_types = [CHANGE_DELETION if was_found else CHANGE_REGISTRATION_DATA]

    ordered = tuple(sorted(change_types, key=ALL_CHANGE_TYPES.index))
    return ChangeDecision(ordered, changes, disagreement=_disagreement(previous, current, patch))


def _disagreement(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
    patch: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    """Fields where the fetched record cannot be reconciled with the official patch.

    The test is deliberately narrow, and the narrowing is evidence-driven. A live
    replay on 2026-09-08 showed the obvious rule - "the patch names a field the
    diff does not" - firing on healthy data: an interval legitimately contains
    events the stored snapshot already reflects, because the snapshot was fetched
    *after* the previous cutoff was captured. Flagging those would stall a
    perfectly good snapshot on every overlap.

    What cannot be explained that way is a record that shows neither the value the
    patch states nor any movement at all. Then the record is older than an event
    it should already contain. A record that moved past the stated value is fine
    too: a change after this run's cutoff explains it.

    The reverse direction is never checked. ``Ny``, ``Sletting`` and ``Fjernet``
    carry no ``endringer`` at all, so a field moving without a patch naming it is
    normal.
    """
    stated: dict[str, Any] = {}
    for operation in patch:
        field = PATCH_PATH_FIELDS.get(operation.get("path", ""))
        if field is None:
            continue
        # Later operations win: the last one states the value the record should
        # have reached by the end of this interval.
        value = None if operation.get("op") == "remove" else operation.get("value")
        if isinstance(value, str):
            # Normalization stores trimmed text and no empty strings; compare the
            # patch the same way, or a difference no record can resolve would pin
            # the cursor for good.
            value = value.strip() or None
        stated[field] = value
    return tuple(
        sorted(
            field
            for field, value in stated.items()
            if not _equal(current.get(field), value)
            and _equal(current.get(field), previous.get(field))
        )
    )


def _equal(left: Any, right: Any) -> bool:
    """Compare a normalized value with a raw patch value.

    BRREG writes share capital as a bare number and this Actor stores a float, so
    ``5976872600`` and ``5976872600.0`` are the same amount. Booleans are never
    treated as numbers.
    """
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, int | float) and isinstance(right, int | float):
        return float(left) == float(right)
    return left == right
