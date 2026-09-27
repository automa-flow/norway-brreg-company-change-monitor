"""Input validation and the stable public vocabulary of the Actor.

The organization number is the only identity this Actor recognises. BRREG rejects
a whole comma-separated filter chunk with HTTP 400 if one value in it is not nine
digits, so a malformed entry that reached the source client would deny 199
well-formed companies their answer. Validation therefore happens per entry,
before anything is requested.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

ACTOR_NAME = "norway-brreg-company-change-monitor"
SOURCE_NAME = "no_brreg_enhetsregisteret"
SOURCE_BASE_URL = "https://data.brreg.no/enhetsregisteret/api"
UPDATES_ENDPOINT = f"{SOURCE_BASE_URL}/oppdateringer/enheter"
ENTITY_ENDPOINT = f"{SOURCE_BASE_URL}/enheter"
SCHEMA_VERSION = 1
STATE_VERSION = 1

EVENT_COMPANY_MONITORED = "company-monitored"

#: MVP watchlist ceiling. 5,000 companies is 25 update-stream chunks and, after
#: the baseline, usually 25 requests for the whole run.
MAX_ORGANIZATION_NUMBERS = 5000
#: Chunk width for the ``organisasjonsnummer`` filter. Measured 2026-09-08: 1,000
#: numbers (12,106 encoded characters) still answered 200 and 2,000 (24,106) was
#: rejected with an HTML 400 by the front-end proxy. 200 numbers is about 2,520
#: characters - an order of magnitude of headroom, chosen by URL length rather
#: than by an arbitrary count.
MAX_ORGNR_PER_CHUNK = 200
#: The API rejects ``size * (page + 1) > 10_000``, so pages are walked by cursor
#: and a page never needs to be large. 1,000 keeps one page near 350 kB.
UPDATE_PAGE_SIZE = 1000
#: Hard stop on one chunk's interval, so a monitor that has not run for a very
#: long time cannot hold a paid container open indefinitely.
MAX_UPDATE_PAGES_PER_CHUNK = 50
#: Concurrency. BRREG publishes no rate limit and returned no rate-limit headers,
#: so these are deliberately small for a free public service.
MAX_ENTITY_REQUESTS_IN_FLIGHT = 6
MAX_UPDATE_REQUESTS_IN_FLIGHT = 3

_MONITOR_KEY = re.compile(r"[A-Za-z0-9_-]{1,80}\Z")
_SEPARATORS = re.compile(r"[\s.\-/]+")
_ORGANIZATION_NUMBER = re.compile(r"[0-9]{9}\Z")


class Mode(StrEnum):
    CHANGES_ONLY = "changesOnly"
    SNAPSHOT_AND_CHANGES = "snapshotAndChanges"


class BaselineMode(StrEnum):
    EMIT_SNAPSHOT = "emitSnapshot"
    STORE_ONLY = "storeOnly"


class RecordType(StrEnum):
    BASELINE = "BASELINE"
    CHANGE = "CHANGE"
    NOT_FOUND = "NOT_FOUND"
    REMOVED = "REMOVED"
    #: Extensions to the four record types in the brief, required by this
    #: repository's failure-semantics invariant: an unchanged company in
    #: snapshotAndChanges is not a CHANGE, and a rejected entry or an unreachable
    #: source needs a shape that can never be read as an observation of a company.
    SNAPSHOT = "SNAPSHOT"
    ERROR = "ERROR"


class OutputStatus(StrEnum):
    SUCCESS = "SUCCESS"
    NOT_FOUND = "NOT_FOUND"
    PARTIAL = "PARTIAL"
    SOURCE_FAILED = "SOURCE_FAILED"
    INVALID_INPUT = "INVALID_INPUT"
    SKIPPED = "SKIPPED"


#: The register gave a definite answer about this company. Only these may update
#: stored state, and only these may be charged.
CONCLUSIVE_STATUSES = frozenset({OutputStatus.SUCCESS, OutputStatus.NOT_FOUND})


class SourceChangeType(StrEnum):
    ENTITY_ADDED = "ENTITY_ADDED"
    ENTITY_CHANGED = "ENTITY_CHANGED"
    ENTITY_DELETED = "ENTITY_DELETED"
    REMOVED_FROM_OPEN_DATA = "REMOVED_FROM_OPEN_DATA"
    UNKNOWN_SOURCE_CHANGE = "UNKNOWN_SOURCE_CHANGE"


#: BRREG's own ``endringstype`` vocabulary. ``Ukjent`` is its legacy backfill
#: marker and still appears in 2018 events; anything outside this map is
#: preserved as UNKNOWN_SOURCE_CHANGE rather than guessed at.
SOURCE_CHANGE_TYPES = {
    "Ny": SourceChangeType.ENTITY_ADDED,
    "Endring": SourceChangeType.ENTITY_CHANGED,
    "Sletting": SourceChangeType.ENTITY_DELETED,
    "Fjernet": SourceChangeType.REMOVED_FROM_OPEN_DATA,
    "Ukjent": SourceChangeType.UNKNOWN_SOURCE_CHANGE,
}


class ActorInput(BaseModel):
    """Global input only.

    ``organizationNumbers`` stays untyped here on purpose: one malformed entry
    must not deny the rest of the watchlist its answer.
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    organization_numbers: list[Any] = Field(
        alias="organizationNumbers", min_length=1, max_length=MAX_ORGANIZATION_NUMBERS
    )
    monitor_key: str = Field(default="default", alias="monitorKey")
    mode: Mode = Mode.CHANGES_ONLY
    baseline_mode: BaselineMode = Field(default=BaselineMode.EMIT_SNAPSHOT, alias="baselineMode")
    include_source_patch: bool = Field(default=True, alias="includeSourcePatch")

    def model_post_init(self, _context: object) -> None:
        if not _MONITOR_KEY.fullmatch(self.monitor_key):
            raise ValueError(
                "monitorKey must be 1-80 characters using only letters, digits, "
                "underscores and hyphens"
            )


class _OrganizationEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    organization_number: str = Field(alias="organizationNumber", min_length=1, max_length=32)


@dataclass(frozen=True, slots=True)
class CompanyQuery:
    index: int
    organization_number: str
    #: What the user typed, echoed back so a row joins to their own watchlist.
    submitted: str


@dataclass(frozen=True, slots=True)
class InvalidCompany:
    index: int
    submitted: str | None
    code: str
    message: str


def normalize_organization_number(raw: str) -> str:
    """Return the exact nine-digit organization number BRREG accepts.

    Spaces, dots, dashes and slashes are stripped, and a ``NO`` prefix with an
    optional ``MVA`` suffix is dropped, because ``NO 923 609 016 MVA`` is how the
    same company is written on a Norwegian invoice. No check digit is computed:
    existence is the register's answer to give, and a nine-digit number that
    fails the mod-11 rule is simply one the register reports as absent.
    """
    compact = _SEPARATORS.sub("", raw).upper()
    if not compact:
        raise ValueError("organizationNumber must not be empty")
    if compact.startswith("NO"):
        compact = compact[2:]
    if compact.endswith("MVA"):
        compact = compact[:-3]
    if not compact.isascii() or not _ORGANIZATION_NUMBER.fullmatch(compact):
        raise ValueError(
            "organizationNumber must be a Norwegian organization number: exactly 9 digits, "
            "optionally written as NO + 9 digits + MVA"
        )
    return compact


def validate_organization(raw: Any, *, index: int) -> CompanyQuery | InvalidCompany:
    """Validate one watchlist entry without letting it terminate the batch."""
    if isinstance(raw, str):
        raw = {"organizationNumber": raw}
    submitted = raw.get("organizationNumber") if isinstance(raw, dict) else None
    safe = submitted if isinstance(submitted, str) and len(submitted) <= 64 else None
    try:
        entry = _OrganizationEntry.model_validate(raw)
    except ValidationError:
        return InvalidCompany(
            index=index,
            submitted=safe,
            code="INVALID_ENTRY",
            message=(
                "Each watchlist entry must be a Norwegian organization number string, or an "
                "object with an organizationNumber field."
            ),
        )
    try:
        number = normalize_organization_number(entry.organization_number)
    except ValueError as exc:
        return InvalidCompany(
            index=index, submitted=safe, code="INVALID_ORGANIZATION_NUMBER", message=str(exc)
        )
    return CompanyQuery(
        index=index, organization_number=number, submitted=entry.organization_number
    )


def entity_source_url(organization_number: str) -> str:
    """The official record this observation came from, for provenance and replay."""
    return f"{ENTITY_ENDPOINT}/{organization_number}"


def update_source_id(update_id: int) -> str:
    """Stable identity of one source event, so a replay cannot duplicate output."""
    return f"brreg-update:{update_id}"


def error_record(category: str, code: str, message: str, *, retryable: bool) -> dict[str, Any]:
    return {"category": category, "code": code, "message": message, "retryable": retryable}
