"""Transport and shape validation for the public BRREG Enhetsregisteret API.

Two endpoints, both open data, both plain HTTP: the update stream and the entity
record. This module knows nothing about monitoring, change detection or the
Dataset. It answers two questions and validates the answers hard enough that
"nothing changed" can never be a disguised failure:

* which watched organizations were touched in a bounded interval of update ids;
* what does the register currently publish about one organization.

Every measured fact in the comments comes from
``experiments/norway-brreg-company-change-monitor``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from email.utils import parsedate_to_datetime
from itertools import pairwise
from typing import Any

import httpx

from common.http import AsyncHttpClient, HttpConfig, HttpError, HttpStatusError
from common.monitoring import RunMetrics
from common.retry import RetryPolicy
from norway_brreg_company_change_monitor.models import (
    ENTITY_ENDPOINT,
    MAX_ENTITY_REQUESTS_IN_FLIGHT,
    MAX_ORGNR_PER_CHUNK,
    MAX_UPDATE_PAGES_PER_CHUNK,
    MAX_UPDATE_REQUESTS_IN_FLIGHT,
    SOURCE_NAME,
    UPDATE_PAGE_SIZE,
    UPDATES_ENDPOINT,
    error_record,
)

#: A ``size=10000`` page measured 2.11 MB. Eight megabytes bounds the JSON parse
#: with ample headroom if the source ever changes shape.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
#: BRREG rejects a filter wider than roughly 24,000 encoded characters with an
#: HTML 400 from its front-end proxy. This guard keeps the client inside the
#: measured-good range rather than discovering the ceiling in production.
MAX_ENCODED_URL_LENGTH = 8000

RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
#: Statuses this client reads rather than letting the shared client raise on.
#: 404 (no such entity) and 410 (removed from open data) are answers, not errors.
INSPECTED_STATUS = (400, 403, 404, 410, *sorted(RETRYABLE_STATUS))

#: Backoff is owned here rather than by the shared HTTP client, because
#: Retry-After has to win over the computed delay. The shared client therefore
#: runs with a single attempt.
DEFAULT_RETRY = RetryPolicy(
    max_attempts=3,
    initial_delay=1.0,
    max_delay=8.0,
    multiplier=2.0,
    jitter=0.2,
    retry_on_status=RETRYABLE_STATUS,
)
#: An honoured Retry-After is still bounded: a source asking us to sleep for an
#: hour is telling us to end the run, not to hold a paid container open.
MAX_RETRY_AFTER_SECONDS = 60.0

ENTITY_CLASS_CURRENT = "Enhet"
ENTITY_CLASS_DELETED = "SlettetEnhet"


class BrregSourceError(Exception):
    """A BRREG answer that arrived but cannot be used."""

    def __init__(self, code: str, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable

    def as_error(self, category: str = "SOURCE") -> dict[str, Any]:
        return error_record(category, self.code, str(self), retryable=self.retryable)


class SourceProtocolError(BrregSourceError):
    """The register answered outside its own published contract."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message, retryable=False)


@dataclass(frozen=True, slots=True)
class UpdateEvent:
    """One validated entry of the official update stream."""

    update_id: int
    organization_number: str
    published_at: str
    source_change_type: str
    changes: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class UpdateInterval:
    """Everything the stream reported for one chunk over one bounded interval."""

    organization_numbers: tuple[str, ...]
    from_update_id: int
    to_update_id: int
    events: tuple[UpdateEvent, ...] = ()
    succeeded: bool = True
    error: dict[str, Any] | None = None
    pages: int = 0


class EntityState:
    CURRENT = "CURRENT"
    DELETED = "DELETED"
    ABSENT = "ABSENT"
    REMOVED = "REMOVED"


@dataclass(frozen=True, slots=True)
class EntityResult:
    """One conclusive or failed answer about one organization number."""

    organization_number: str
    state: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    error: dict[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.state is not None


def split_chunks(numbers: list[str], size: int = MAX_ORGNR_PER_CHUNK) -> list[tuple[str, ...]]:
    """Chunk by width, then assert the encoded URL stays inside the measured range."""
    chunks = [tuple(numbers[start : start + size]) for start in range(0, len(numbers), size)]
    for chunk in chunks:
        encoded = len(UPDATES_ENDPOINT) + len(",".join(chunk)) + 128
        if encoded > MAX_ENCODED_URL_LENGTH:
            raise ValueError(
                f"An update-stream chunk would encode to about {encoded} characters, over the "
                f"{MAX_ENCODED_URL_LENGTH} this client will send. Reduce the chunk width."
            )
    return chunks


class BrregClient:
    """One shared async HTTP client per run, bounded concurrency, no proxy."""

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        metrics: RunMetrics | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        retry_policy: RetryPolicy | None = None,
        updates_endpoint: str = UPDATES_ENDPOINT,
        entity_endpoint: str = ENTITY_ENDPOINT,
        page_size: int = UPDATE_PAGE_SIZE,
        max_update_in_flight: int = MAX_UPDATE_REQUESTS_IN_FLIGHT,
        max_entity_in_flight: int = MAX_ENTITY_REQUESTS_IN_FLIGHT,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self.metrics = metrics
        self.updates_endpoint = updates_endpoint
        self.entity_endpoint = entity_endpoint
        self.page_size = page_size
        self.policy = retry_policy or DEFAULT_RETRY
        self.request_count = 0
        self.retry_count = 0
        self.page_count = 0
        self._updates = asyncio.Semaphore(max_update_in_flight)
        self._entities = asyncio.Semaphore(max_entity_in_flight)
        # Injectable so retry tests assert the backoff schedule instead of
        # waiting it out.
        self._sleep = sleep or asyncio.sleep
        self._log = logger or logging.getLogger(__name__)

        def record_request(_source: str, _duration: float, status: int | None) -> None:
            self.request_count += 1
            if self.metrics is None:
                return
            self.metrics.increment("source_requests")
            if status == 429:
                self.metrics.increment("source_rate_limited")

        self.http = AsyncHttpClient(
            HttpConfig(
                timeout=90.0,
                connect_timeout=10.0,
                headers={"accept": "application/json"},
                # Retries live in this class so that Retry-After can win.
                retry=RetryPolicy(max_attempts=1),
            ),
            source=SOURCE_NAME,
            logger=logger,
            transport=transport,
            on_request_complete=record_request,
        )

    async def __aenter__(self) -> BrregClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self.http.aclose()

    # -- update stream -----------------------------------------------------

    async def latest_update_id(self) -> int:
        """The newest update id in the global stream. The run's cutoff.

        Bounding the interval by an id rather than by ``updatedBefore`` keeps the
        cursor and the cutoff on the same field. ``updatedBefore`` filters on
        ``dato``; an event inside the id interval whose ``dato`` sits at or after
        a timestamp cutoff would be excluded while the cursor still advanced past
        it, which is a permanently missed change.
        """
        async with self._updates:
            response = await self._get(
                self.updates_endpoint,
                {"size": 1, "sort": "id,DESC"},
                operation="brreg.latest",
            )
        events, total = parse_update_page(response)
        if not events:
            raise SourceProtocolError(
                "EMPTY_STREAM",
                f"The BRREG update stream reported {total} events but returned none, so no "
                "run cutoff can be established.",
            )
        return events[0].update_id

    async def fetch_interval(
        self,
        organization_numbers: tuple[str, ...],
        *,
        from_update_id: int,
        to_update_id: int,
        include_changes: bool = True,
    ) -> UpdateInterval:
        """Walk one chunk's interval to completion by cursor. Never raises."""
        base = UpdateInterval(organization_numbers, from_update_id, to_update_id)
        if not organization_numbers or from_update_id > to_update_id:
            return base
        collected: dict[int, UpdateEvent] = {}
        cursor = from_update_id
        pages = 0
        try:
            for _ in range(MAX_UPDATE_PAGES_PER_CHUNK):
                async with self._updates:
                    response = await self._get(
                        self.updates_endpoint,
                        {
                            "organisasjonsnummer": ",".join(organization_numbers),
                            "oppdateringsid": cursor,
                            "sort": "id,ASC",
                            "size": self.page_size,
                            **({"includeChanges": "true"} if include_changes else {}),
                        },
                        operation="brreg.updates",
                    )
                pages += 1
                self.page_count += 1
                events, total = parse_update_page(response)
                if any(left.update_id > right.update_id for left, right in pairwise(events)):
                    raise SourceProtocolError(
                        "UNSORTED_PAGE", "BRREG did not return the requested ascending update ids."
                    )
                for event in events:
                    # The cursor is inclusive and the sort is ascending, so an id
                    # below it means the page did not move. Without this, a stuck
                    # page would be walked until the page budget ran out.
                    if event.update_id < cursor:
                        raise SourceProtocolError(
                            "PAGINATION_STALLED",
                            f"The update stream returned id {event.update_id}, below the "
                            f"requested cursor {cursor}. The interval cannot be trusted, so "
                            "nothing is reported as changed.",
                        )
                    if event.organization_number not in organization_numbers:
                        raise SourceProtocolError(
                            "UNEXPECTED_ORGANIZATION",
                            "The update stream answered about an organization this chunk did "
                            "not request, so the exact filter did not behave as documented.",
                        )
                    if event.update_id <= to_update_id:
                        collected[event.update_id] = event
                if not events:
                    break
                highest = max(event.update_id for event in events)
                if highest >= to_update_id or len(events) == total:
                    break
                cursor = highest + 1
            else:
                raise SourceProtocolError(
                    "INTERVAL_TOO_LONG",
                    f"This watchlist chunk still had updates after {MAX_UPDATE_PAGES_PER_CHUNK} "
                    "pages. Run the monitor more often, or split the watchlist.",
                )
        except BrregSourceError as exc:
            return UpdateInterval(
                organization_numbers,
                from_update_id,
                to_update_id,
                succeeded=False,
                error=exc.as_error(),
                pages=pages,
            )
        ordered = tuple(collected[key] for key in sorted(collected))
        return UpdateInterval(
            organization_numbers, from_update_id, to_update_id, events=ordered, pages=pages
        )

    # -- entity record -----------------------------------------------------

    async def fetch_entity(self, organization_number: str) -> EntityResult:
        """Ask the register what it currently publishes about one company.

        Never raises, and never infers a state from missing fields: BRREG carries
        its own discriminator in ``respons_klasse``, and removal is an HTTP 410.
        """
        try:
            async with self._entities:
                response = await self._get(
                    f"{self.entity_endpoint}/{organization_number}",
                    None,
                    operation="brreg.entity",
                )
            if self.metrics is not None:
                self.metrics.increment("bytes_downloaded", len(response.content))
            if response.status_code == 404:
                return EntityResult(
                    organization_number, state=_absent_state(response, organization_number)
                )
            if response.status_code == 410:
                return EntityResult(
                    organization_number,
                    state=EntityState.REMOVED,
                    payload=parse_removed_entity(response, organization_number),
                )
            payload = parse_entity(response, organization_number)
            state = (
                EntityState.DELETED
                if payload["respons_klasse"] == ENTITY_CLASS_DELETED
                else EntityState.CURRENT
            )
            return EntityResult(organization_number, state=state, payload=payload)
        except BrregSourceError as exc:
            return EntityResult(organization_number, error=exc.as_error())

    async def fetch_entities(self, numbers: list[str]) -> dict[str, EntityResult]:
        """Fetch each unique organization exactly once, isolating failures."""
        unique = list(dict.fromkeys(numbers))
        results = await asyncio.gather(*(self.fetch_entity(number) for number in unique))
        return {result.organization_number: result for result in results}

    # -- transport ---------------------------------------------------------

    async def _get(
        self, url: str, params: dict[str, Any] | None, *, operation: str
    ) -> httpx.Response:
        last: BrregSourceError | None = None
        for attempt in range(1, self.policy.max_attempts + 1):
            delay: float | None = None
            try:
                response = await self.http.get(
                    url, params=params, allow_status=INSPECTED_STATUS, operation=operation
                )
            except HttpStatusError as exc:
                # Every status worth retrying is in INSPECTED_STATUS and comes
                # back as a response, so anything that still raises here is a
                # deterministic rejection. Retrying it would only add load.
                raise SourceProtocolError(
                    f"HTTP_{exc.status_code}",
                    f"BRREG rejected the request with HTTP {exc.status_code}.",
                ) from exc
            except HttpError as exc:
                last = BrregSourceError("TRANSPORT_ERROR", f"BRREG request failed: {exc}")
                delay = self.policy.delay_for(attempt)
            else:
                if response.status_code not in RETRYABLE_STATUS:
                    return _checked(response)
                last = BrregSourceError(
                    f"HTTP_{response.status_code}",
                    f"BRREG returned HTTP {response.status_code} after {attempt} attempt(s).",
                )
                delay = _retry_after(response) or self.policy.delay_for(attempt)
            if attempt == self.policy.max_attempts:
                break
            self.retry_count += 1
            if self.metrics is not None:
                self.metrics.increment("source_retries")
            await self._sleep(delay)
        assert last is not None
        raise last


def _checked(response: httpx.Response) -> httpx.Response:
    """Turn a definite non-2xx answer into a typed, non-retryable failure."""
    if response.status_code in (200, 404, 410):
        if len(response.content) > MAX_RESPONSE_BYTES:
            raise SourceProtocolError(
                "RESPONSE_TOO_LARGE",
                f"BRREG returned {len(response.content)} bytes, over the "
                f"{MAX_RESPONSE_BYTES} byte limit this Actor will parse.",
            )
        return response
    detail = _error_detail(response)
    raise SourceProtocolError(
        f"HTTP_{response.status_code}",
        f"BRREG rejected the request with HTTP {response.status_code}"
        + (f" ({detail})." if detail else "."),
    )


def _absent_state(response: httpx.Response, organization_number: str) -> str:
    """Only an empty 404 is the register saying "no such organization".

    Measured 2026-09-27: an unknown organization number is answered with HTTP 404
    and an empty body, while a request the API cannot route (an unknown path) is
    a 404 with an ``application/problem+json`` body. Reading the second as absence
    would charge for, store and later report a company that was never checked.
    """
    if response.content.strip():
        raise SourceProtocolError(
            "UNEXPECTED_NOT_FOUND",
            f"BRREG answered HTTP 404 with a body for {organization_number}. That is how it "
            "reports a request it cannot route, not an unknown organization, so the company "
            "is reported as unverified rather than absent.",
        )
    return EntityState.ABSENT


def _error_detail(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return ""
    if not isinstance(body, dict):
        return ""
    errors = body.get("valideringsfeil")
    if isinstance(errors, list) and errors and isinstance(errors[0], dict):
        return str(errors[0].get("feilmelding", ""))[:160]
    return str(body.get("feilmelding", ""))[:160]


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("retry-after")
    if not raw:
        return None
    try:
        seconds = float(raw)
    except ValueError:
        try:
            moment = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            return None
        seconds = (moment - datetime.now(moment.tzinfo)).total_seconds()
    return max(0.0, min(seconds, MAX_RETRY_AFTER_SECONDS))


def _decode(response: httpx.Response, *, what: str) -> Any:
    try:
        return AsyncHttpClient.decode_json(response)
    except HttpError as exc:
        # The 2,000-number filter is answered with an HTML error page, so a
        # non-JSON body is a real production case and must never read as "no
        # changes".
        raise SourceProtocolError(
            "NON_JSON_RESPONSE",
            f"BRREG returned a body that is not JSON for the {what} ({exc}). Treated as a "
            "source failure, never as an absent change.",
        ) from exc


def parse_update_page(response: httpx.Response) -> tuple[tuple[UpdateEvent, ...], int]:
    """Validate one update page hard enough that emptiness really means emptiness.

    An empty interval is a real, frequent answer: BRREG returns ``200`` with
    ``page.totalElements: 0`` and **no** ``_embedded`` key at all. That shape is
    an empty success. A missing ``_embedded`` with a non-zero count is not.
    """
    body = _decode(response, what="update stream")
    if not isinstance(body, dict):
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE", "The BRREG update stream returned JSON that is not an object."
        )
    page = body.get("page")
    if not isinstance(page, dict):
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE", "The BRREG update response has no 'page' metadata."
        )
    total = page.get("totalElements")
    if not isinstance(total, int) or isinstance(total, bool) or total < 0:
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE", "The BRREG update response has no usable 'totalElements' count."
        )
    embedded = body.get("_embedded")
    if embedded is None:
        if total:
            raise SourceProtocolError(
                "TRUNCATED_RESPONSE",
                f"The BRREG update stream reported {total} events and returned none. The page "
                "is incomplete, so this interval cannot be reported as quiet.",
            )
        return (), total
    if not isinstance(embedded, dict) or not isinstance(
        raw_events := embedded.get("oppdaterteEnheter"), list
    ):
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE", "The BRREG update response has no 'oppdaterteEnheter' list."
        )
    size = page.get("size")
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        raise SourceProtocolError("UNEXPECTED_SHAPE", "The update page has no positive size.")
    if page.get("number") != 0:
        raise SourceProtocolError(
            "UNEXPECTED_PAGE", "BRREG did not return the requested first page."
        )
    if len(raw_events) != min(size, total):
        raise SourceProtocolError(
            "TRUNCATED_RESPONSE", "The update list does not match its page size and total count."
        )
    return tuple(_update_event(raw) for raw in raw_events), total


def _update_event(raw: Any) -> UpdateEvent:
    if not isinstance(raw, dict):
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE", "The BRREG update stream returned an event that is not an object."
        )
    update_id = raw.get("oppdateringsid")
    if not isinstance(update_id, int) or isinstance(update_id, bool) or update_id < 0:
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE", "A BRREG update event has no integer 'oppdateringsid'."
        )
    number = raw.get("organisasjonsnummer")
    if not isinstance(number, str) or len(number) != 9 or not number.isdigit():
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE",
            "A BRREG update event has no nine-digit 'organisasjonsnummer'.",
        )
    published_at = raw.get("dato")
    if not isinstance(published_at, str) or not published_at:
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE", "A BRREG update event has no 'dato' timestamp."
        )
    change_type = raw.get("endringstype")
    if not isinstance(change_type, str) or not change_type:
        raise SourceProtocolError("UNEXPECTED_SHAPE", "A BRREG update event has no 'endringstype'.")
    return UpdateEvent(
        update_id=update_id,
        organization_number=number,
        published_at=published_at,
        source_change_type=change_type,
        changes=_patch(raw.get("endringer")),
    )


def _patch(raw: Any) -> tuple[dict[str, Any], ...]:
    """Keep the source patch only when it is well formed.

    Only ``Endring`` events carry ``endringer`` (4,495 of 4,495 in the measured
    window; ``Ny``, ``Sletting`` and ``Fjernet`` never do), so absence is normal
    and is not an error. A malformed patch is, because it would be published as
    evidence.
    """
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE", "A BRREG update event has an 'endringer' that is not a list."
        )
    operations = []
    for entry in raw:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("op"), str)
            or not isinstance(entry.get("path"), str)
        ):
            raise SourceProtocolError(
                "UNEXPECTED_SHAPE", "A BRREG change operation has no string 'op' and 'path'."
            )
        operation: dict[str, Any] = {"op": entry["op"], "path": entry["path"]}
        if "value" in entry:
            operation["value"] = entry["value"]
        if isinstance(entry.get("from"), str):
            operation["from"] = entry["from"]
        operations.append(operation)
    return tuple(operations)


def parse_entity(response: httpx.Response, organization_number: str) -> dict[str, Any]:
    """Validate one entity record, including its own deleted/current discriminator."""
    body = _decode(response, what="entity record")
    if not isinstance(body, dict):
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE", "BRREG returned an entity record that is not an object."
        )
    if body.get("organisasjonsnummer") != organization_number:
        raise SourceProtocolError(
            "UNEXPECTED_ORGANIZATION",
            "BRREG answered with a different organization number than the one requested.",
        )
    entity_class = body.get("respons_klasse")
    if entity_class not in (ENTITY_CLASS_CURRENT, ENTITY_CLASS_DELETED):
        raise SourceProtocolError(
            "UNKNOWN_ENTITY_CLASS",
            "BRREG returned an entity whose 'respons_klasse' is neither Enhet nor SlettetEnhet, "
            "so whether the company is current or deleted cannot be established.",
        )
    if not isinstance(body.get("navn"), str) or not body["navn"].strip():
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE", "BRREG returned an entity record with no usable 'navn'."
        )
    if entity_class == ENTITY_CLASS_DELETED and not isinstance(body.get("slettedato"), str):
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE", "BRREG returned a SlettetEnhet with no 'slettedato'."
        )
    return body


def parse_removed_entity(response: httpx.Response, organization_number: str) -> dict[str, Any]:
    """An HTTP 410 body: identity and removal date only, by design."""
    body = _decode(response, what="removed entity")
    if not isinstance(body, dict) or body.get("organisasjonsnummer") != organization_number:
        raise SourceProtocolError(
            "UNEXPECTED_SHAPE",
            "BRREG answered HTTP 410 without naming the requested organization number.",
        )
    return body
