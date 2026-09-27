from __future__ import annotations

import httpx
import pytest

from norway_brreg_company_change_monitor.models import UPDATES_ENDPOINT
from norway_brreg_company_change_monitor.source import (
    BrregClient,
    EntityState,
    SourceProtocolError,
    split_chunks,
)

from .conftest import event, update_page

EQUINOR = "923609016"
DNB = "984851006"


def client(source, **kwargs) -> BrregClient:
    async def no_sleep(_seconds: float) -> None:
        return None

    kwargs.setdefault("page_size", 3)
    return BrregClient(transport=source.transport, sleep=no_sleep, **kwargs)


# -- update stream ---------------------------------------------------------


async def test_latest_update_id_reads_the_run_cutoff(source, load_json):
    source.updates = [(200, load_json("updates_newest.json"))]
    async with client(source) as brreg:
        assert await brreg.latest_update_id() == 25175397


async def test_empty_interval_is_an_empty_success_not_a_failure(source, load_json):
    source.updates = [(200, load_json("updates_empty.json"))]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=100)
    assert interval.succeeded
    assert interval.events == ()
    assert interval.error is None


async def test_a_missing_embedded_with_a_nonzero_count_is_a_source_failure(source):
    source.updates = [(200, {"_links": {}, "page": {"size": 10, "totalElements": 7}})]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=100)
    assert not interval.succeeded
    assert interval.error["code"] == "TRUNCATED_RESPONSE"


async def test_change_operations_survive_the_round_trip(source, load_json):
    # This capture is the newest three events in descending order from a larger
    # history. Model a finite ASC interval without altering the source capture.
    captured = load_json("updates_with_changes.json")["_embedded"]["oppdaterteEnheter"]
    source.updates = [(200, update_page(sorted(captured, key=lambda item: item["oppdateringsid"])))]
    async with client(source, page_size=1000) as brreg:
        interval = await brreg.fetch_interval(
            (EQUINOR, DNB), from_update_id=1, to_update_id=99_999_999
        )
    assert interval.succeeded
    assert [e.update_id for e in interval.events] == sorted(e.update_id for e in interval.events)
    equinor = [e for e in interval.events if e.organization_number == EQUINOR]
    assert {e.source_change_type for e in equinor} == {"Endring"}
    paths = {change["path"] for e in equinor for change in e.changes}
    assert paths >= {"/sisteInnsendteAarsregnskap", "/antallAnsatte"}
    assert all(change["op"] == "replace" for e in equinor for change in e.changes)


async def test_unknown_change_type_is_preserved_verbatim(source, load_json):
    source.updates = [(200, load_json("updates_mixed_types.json"))]
    async with client(source, page_size=1000) as brreg:
        interval = await brreg.fetch_interval(
            ("917549249", "938282382", "936127088", EQUINOR),
            from_update_id=1,
            to_update_id=99_999_999,
        )
    assert [e.source_change_type for e in interval.events] == [
        "Sletting",
        "Ny",
        "Fjernet",
        "Ukjent",
    ]


async def test_pagination_walks_by_cursor_and_stops_at_the_cutoff(source):
    pages = [
        (200, update_page([event(i, EQUINOR) for i in (10, 11, 12)], total=7, size=3)),
        (200, update_page([event(i, EQUINOR) for i in (13, 14, 15)], total=4, size=3)),
        (200, update_page([event(16, EQUINOR)], size=3)),
    ]
    source.updates = pages
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=10, to_update_id=16)
    assert [e.update_id for e in interval.events] == [10, 11, 12, 13, 14, 15, 16]
    assert interval.pages == 3
    cursors = [request.url.params.get("oppdateringsid") for request in source.requests]
    assert cursors == ["10", "13", "16"]


async def test_events_past_the_cutoff_are_not_reported(source):
    source.updates = [(200, update_page([event(i, EQUINOR) for i in (10, 11, 12)]))]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=10, to_update_id=11)
    assert [e.update_id for e in interval.events] == [10, 11]


async def test_a_replayed_update_id_is_reported_once(source):
    source.updates = [
        (200, update_page([event(10, EQUINOR), event(10, EQUINOR), event(11, EQUINOR)]))
    ]
    async with client(source, page_size=1000) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=10, to_update_id=20)
    assert [e.update_id for e in interval.events] == [10, 11]


async def test_a_page_that_does_not_advance_is_refused(source):
    source.updates = [
        (200, update_page([event(i, EQUINOR) for i in (10, 11, 12)], total=9, size=3)),
        (200, update_page([event(i, EQUINOR) for i in (9, 10, 11)])),
    ]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=10, to_update_id=500)
    assert not interval.succeeded
    assert interval.error["code"] == "PAGINATION_STALLED"


async def test_an_unrequested_organization_invalidates_the_chunk(source):
    source.updates = [(200, update_page([event(10, DNB)]))]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=500)
    assert not interval.succeeded
    assert interval.error["code"] == "UNEXPECTED_ORGANIZATION"


async def test_html_served_in_place_of_json_is_a_source_failure(source):
    source.updates = [(200, "<html><body><h1>400 Bad request</h1></body></html>")]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=500)
    assert not interval.succeeded
    assert interval.error["code"] == "NON_JSON_RESPONSE"


async def test_a_malformed_event_is_a_source_failure_not_an_empty_interval(source):
    source.updates = [(200, update_page([{"oppdateringsid": 10, "organisasjonsnummer": EQUINOR}]))]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=500)
    assert not interval.succeeded
    assert interval.error["code"] == "UNEXPECTED_SHAPE"


async def test_a_rejected_request_carries_the_registers_own_message(source, load_json):
    source.updates = [(400, load_json("error_400_bad_date.json"))]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=500)
    assert not interval.succeeded
    assert interval.error["code"] == "HTTP_400"
    assert not interval.error["retryable"]
    assert "gyldig dato" in interval.error["message"]


async def test_the_page_budget_bounds_one_interval(source):
    def endless(request: httpx.Request) -> httpx.Response:
        cursor = int(request.url.params["oppdateringsid"])
        return httpx.Response(
            200,
            json=update_page(
                [event(cursor + offset, EQUINOR) for offset in range(3)], total=10**9, size=3
            ),
        )

    source.updates = endless
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=10**9)
    assert not interval.succeeded
    assert interval.error["code"] == "INTERVAL_TOO_LONG"
    assert interval.pages == 50


@pytest.mark.parametrize("ids", [[], [10]])
async def test_incomplete_embedded_list_is_a_failure_even_when_present(source, ids):
    source.updates = [(200, update_page([event(i, EQUINOR) for i in ids], total=7, size=3))]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=100)
    assert not interval.succeeded
    assert interval.error["code"] == "TRUNCATED_RESPONSE"
    assert interval.events == ()


async def test_unsorted_page_cannot_terminate_the_interval_at_a_false_cutoff(source):
    source.updates = [(200, update_page([event(i, EQUINOR) for i in (101, 10, 20)], size=3))]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=100)
    assert not interval.succeeded
    assert interval.error["code"] == "UNSORTED_PAGE"


async def test_a_smaller_server_page_does_not_end_an_incomplete_interval(source):
    source.updates = [
        (200, update_page([event(10, EQUINOR)], total=2, size=1)),
        (200, update_page([event(20, EQUINOR)], total=1, size=1)),
    ]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=100)
    assert interval.succeeded
    assert [item.update_id for item in interval.events] == [10, 20]


# -- retries ---------------------------------------------------------------


@pytest.mark.parametrize("status", [429, 500, 503])
async def test_transient_statuses_are_retried_then_succeed(source, status):
    source.updates = [(status, None), (status, None), (200, update_page([]))]
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=5)
        assert interval.succeeded
        assert brreg.retry_count == 2
    assert len(source.requests) == 3


async def test_a_timeout_is_retried_and_then_reported(source):
    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("too slow", request=request)

    source.updates = timeout
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=5)
    assert not interval.succeeded
    assert interval.error["code"] == "TRANSPORT_ERROR"
    assert interval.error["retryable"]
    assert len(source.requests) == 3


async def test_a_deterministic_rejection_is_not_retried(source):
    source.updates = [(400, {"feilmelding": "nope"})]
    async with client(source) as brreg:
        await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=5)
    assert len(source.requests) == 1


async def test_retry_after_wins_over_the_computed_backoff(source):
    slept: list[float] = []

    async def record(seconds: float) -> None:
        slept.append(seconds)

    def throttled(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"retry-after": "4"})

    source.updates = throttled
    brreg = BrregClient(transport=source.transport, sleep=record)
    async with brreg:
        await brreg.fetch_interval((EQUINOR,), from_update_id=1, to_update_id=5)
    assert slept == [4.0, 4.0]


# -- entity records --------------------------------------------------------


async def test_a_current_entity_is_classified_by_its_own_discriminator(source, load_json):
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    async with client(source) as brreg:
        result = await brreg.fetch_entity(EQUINOR)
    assert result.state == EntityState.CURRENT
    assert result.payload["navn"] == "EQUINOR ASA"


async def test_a_deleted_entity_is_never_inferred_from_missing_fields(source, load_json):
    source.entities = {"981276957": (200, load_json("entity_deleted.json"))}
    async with client(source) as brreg:
        result = await brreg.fetch_entity("981276957")
    assert result.state == EntityState.DELETED
    assert result.payload["slettedato"] == "2021-07-01"


async def test_an_absent_entity_is_not_found_not_failed(source):
    source.entities = {}
    async with client(source) as brreg:
        result = await brreg.fetch_entity("810864402")
    assert result.state == EntityState.ABSENT
    assert result.error is None


async def test_removal_from_open_data_is_its_own_state(source, load_json):
    source.entities = {"936127088": (410, load_json("entity_removed_410.json"))}
    async with client(source) as brreg:
        result = await brreg.fetch_entity("936127088")
    assert result.state == EntityState.REMOVED
    assert result.payload["slettedato"] == "2026-09-05"


async def test_an_entity_answering_about_someone_else_is_refused(source, load_json):
    source.entities = {EQUINOR: (200, load_json("entity_deleted.json"))}
    async with client(source) as brreg:
        result = await brreg.fetch_entity(EQUINOR)
    assert not result.succeeded
    assert result.error["code"] == "UNEXPECTED_ORGANIZATION"


async def test_an_unknown_entity_class_is_refused(source, load_json):
    body = load_json("entity_current.json") | {"respons_klasse": "NoeHeltAnnet"}
    source.entities = {EQUINOR: (200, body)}
    async with client(source) as brreg:
        result = await brreg.fetch_entity(EQUINOR)
    assert not result.succeeded
    assert result.error["code"] == "UNKNOWN_ENTITY_CLASS"


async def test_invalid_json_never_becomes_an_absent_company(source):
    source.entities = {EQUINOR: (200, "{not json")}
    async with client(source) as brreg:
        result = await brreg.fetch_entity(EQUINOR)
    assert not result.succeeded
    assert result.error["code"] == "NON_JSON_RESPONSE"


async def test_each_unique_organization_is_fetched_once(source, load_json):
    source.entities = {EQUINOR: (200, load_json("entity_current.json"))}
    async with client(source) as brreg:
        results = await brreg.fetch_entities([EQUINOR, EQUINOR, EQUINOR])
    assert set(results) == {EQUINOR}
    assert len(source.requests) == 1


# -- chunking --------------------------------------------------------------


def test_chunking_keeps_the_encoded_url_inside_the_measured_range():
    numbers = [f"{800000000 + n:09d}" for n in range(450)]
    chunks = split_chunks(numbers)
    assert [len(chunk) for chunk in chunks] == [200, 200, 50]
    widest = len(UPDATES_ENDPOINT) + len(",".join(chunks[0])) + 128
    assert widest < 8000


def test_a_chunk_wider_than_the_url_limit_is_refused():
    with pytest.raises(ValueError, match="encode"):
        split_chunks([f"{800000000 + n:09d}" for n in range(1000)], size=1000)


async def test_the_watchlist_filter_is_sent_as_the_exact_comma_separated_list(source):
    source.updates = [(200, update_page([]))]
    async with client(source) as brreg:
        await brreg.fetch_interval((EQUINOR, DNB), from_update_id=7, to_update_id=9)
    params = source.requests[0].url.params
    assert params["organisasjonsnummer"] == f"{EQUINOR},{DNB}"
    assert params["sort"] == "id,ASC"
    assert params["includeChanges"] == "true"


async def test_an_empty_chunk_costs_no_request(source):
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((), from_update_id=1, to_update_id=5)
    assert interval.succeeded
    assert not source.requests


async def test_an_already_covered_interval_costs_no_request(source):
    async with client(source) as brreg:
        interval = await brreg.fetch_interval((EQUINOR,), from_update_id=10, to_update_id=9)
    assert interval.succeeded
    assert not source.requests


def test_a_protocol_error_is_never_retryable():
    error = SourceProtocolError("X", "y").as_error()
    assert error["retryable"] is False
