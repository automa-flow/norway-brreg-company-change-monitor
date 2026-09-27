from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def load_json() -> Callable[[str], Any]:
    def load(name: str) -> Any:
        return json.loads((FIXTURES / name).read_text(encoding="utf-8"))

    return load


class FakeSource:
    """A scripted BRREG, keyed by path, so tests assert behaviour not transport.

    Handlers are ``(status, body)`` or a callable taking the request and returning
    one. A path may be given a list of responses to serve in order, which is how
    retry, pagination and "the record changed between two runs" are expressed.
    """

    def __init__(self) -> None:
        self.updates: list[Any] = []
        self.entities: dict[str, Any] = {}
        self.requests: list[httpx.Request] = []

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path.endswith("/oppdateringer/enheter"):
            return _respond(_next(self.updates, request))
        number = path.rsplit("/", 1)[-1]
        if number not in self.entities:
            return httpx.Response(404, request=request)
        return _respond(_next(self.entities[number], request))


def _next(handler: Any, request: httpx.Request) -> Any:
    if isinstance(handler, list):
        return handler.pop(0) if len(handler) > 1 else handler[0]
    if callable(handler):
        return handler(request)
    return handler


def _respond(value: Any) -> httpx.Response:
    if isinstance(value, httpx.Response):
        return value
    status, body = value
    if isinstance(body, str | bytes):
        return httpx.Response(status, content=body)
    if body is None:
        return httpx.Response(status)
    return httpx.Response(status, json=body)


@pytest.fixture
def source() -> FakeSource:
    return FakeSource()


def update_page(
    events: list[dict[str, Any]], *, total: int | None = None, size: int = 1000
) -> dict[str, Any]:
    """The exact envelope BRREG returns, including its empty-interval shape."""
    count = len(events) if total is None else total
    page = {"size": size, "totalElements": count, "totalPages": 1 if count else 0, "number": 0}
    if not events and total is None:
        # Measured: an empty interval carries no _embedded key at all.
        return {"_links": {}, "page": page}
    return {"_embedded": {"oppdaterteEnheter": events}, "_links": {}, "page": page}


def event(
    update_id: int,
    number: str,
    *,
    change_type: str = "Endring",
    changes: list[dict[str, Any]] | None = None,
    published_at: str = "2026-09-08T08:00:00.000Z",
) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "oppdateringsid": update_id,
        "dato": published_at,
        "organisasjonsnummer": number,
        "endringstype": change_type,
    }
    if changes is not None:
        raw["endringer"] = changes
    return raw
