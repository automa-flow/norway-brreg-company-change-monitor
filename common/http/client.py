"""Async HTTP client wrapper: timeouts, a real user agent, retries, optional
proxy, structured logging and typed errors.

This is intentionally *not* a scraping framework. It has no queue, no
scheduler, no session pool and no page/HTML awareness. Actors compose it with
their own source-specific logic.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Self

import httpx

from common.http.errors import (
    HttpError,
    HttpStatusError,
    HttpTimeoutError,
    HttpTransportError,
)
from common.logging import log_event
from common.retry import RetryPolicy, retry_async

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


@dataclass(frozen=True, slots=True)
class HttpConfig:
    """Everything that shapes an outgoing request."""

    timeout: float = 30.0
    connect_timeout: float | None = 10.0
    user_agent: str = DEFAULT_USER_AGENT
    headers: Mapping[str, str] = field(default_factory=dict)
    # Full proxy URL (http://user:pass@host:port) or None for a direct connection.
    proxy_url: str | None = None
    follow_redirects: bool = True
    retry: RetryPolicy = field(default_factory=RetryPolicy)


class AsyncHttpClient:
    """Async HTTP client with retries and structured request logging.

    ``source`` shows up in every log line and lets one Actor talk to several
    upstreams while keeping the logs attributable.
    """

    def __init__(
        self,
        config: HttpConfig | None = None,
        *,
        source: str = "http",
        logger: logging.Logger | None = None,
        cookies: httpx.Cookies | dict[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        on_request_complete: Callable[[str, float, int | None], None] | None = None,
    ) -> None:
        self.config = config or HttpConfig()
        self.source = source
        self._log = logger or logging.getLogger(__name__)
        self._on_request_complete = on_request_complete
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                self.config.timeout,
                connect=self.config.connect_timeout or self.config.timeout,
            ),
            headers={"user-agent": self.config.user_agent, **dict(self.config.headers)},
            proxy=self.config.proxy_url,
            follow_redirects=self.config.follow_redirects,
            cookies=cookies,
            transport=transport,
        )

    # -- lifecycle ---------------------------------------------------------

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    @property
    def cookies(self) -> httpx.Cookies:
        return self._client.cookies

    # -- requests ----------------------------------------------------------

    async def request(
        self,
        method: str,
        url: str,
        *,
        allow_status: Sequence[int] | None = None,
        request_id: str | None = None,
        operation: str | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send a request, retrying transient failures.

        ``allow_status`` lists non-2xx codes the caller handles itself (404 is
        the usual one). Everything else >= 400 raises HttpStatusError.
        """
        rid = request_id or uuid.uuid4().hex[:12]
        op = operation or f"http.{method.lower()}"
        accepted = set(allow_status or ())
        policy = self.config.retry

        async def attempt(attempt_no: int) -> httpx.Response:
            started = time.monotonic()
            try:
                response = await self._client.request(method, url, **kwargs)
            except httpx.TimeoutException as exc:
                self._record(op, rid, url, time.monotonic() - started, None, attempt_no, exc)
                raise HttpTimeoutError(
                    f"{method} {url} timed out after {self.config.timeout}s",
                    url=url,
                    request_id=rid,
                ) from exc
            except httpx.HTTPError as exc:
                self._record(op, rid, url, time.monotonic() - started, None, attempt_no, exc)
                raise HttpTransportError(
                    f"{method} {url} failed: {type(exc).__name__}: {exc}",
                    url=url,
                    request_id=rid,
                ) from exc

            duration = time.monotonic() - started
            self._record(op, rid, url, duration, response.status_code, attempt_no, None)
            return response

        def needs_retry(response: httpx.Response) -> bool:
            return response.status_code not in accepted and policy.should_retry_status(
                response.status_code
            )

        def note_retry(attempt_no: int, delay: float, exc: BaseException | None) -> None:
            log_event(
                self._log,
                f"{op}.retry",
                level=logging.WARNING,
                request_id=rid,
                source=self.source,
                attempt=attempt_no,
                delay=delay,
                status=type(exc).__name__ if exc else "retryable_status",
            )

        response = await retry_async(
            attempt,
            policy=policy,
            retryable_exceptions=(HttpTransportError,),
            result_needs_retry=needs_retry,
            on_retry=note_retry,
        )

        if response.status_code >= 400 and response.status_code not in accepted:
            raise HttpStatusError(
                f"{method} {url} returned HTTP {response.status_code}",
                status_code=response.status_code,
                url=url,
                request_id=rid,
                body=response.text,
            )
        return response

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self.request("POST", url, **kwargs)

    async def get_json(self, url: str, **kwargs: Any) -> Any:
        return self.decode_json(await self.get(url, **kwargs))

    @staticmethod
    def decode_json(response: httpx.Response) -> Any:
        """Decode a JSON body, or raise an error that says what arrived instead.

        Public because callers that build their own requests still want the
        readable failure when a WAF or login page is served in place of JSON.
        """
        try:
            return response.json()
        except ValueError as exc:
            content_type = response.headers.get("content-type", "unknown")
            raise HttpError(
                f"expected JSON but got {content_type}",
                url=str(response.request.url),
            ) from exc

    def _record(
        self,
        operation: str,
        request_id: str,
        url: str,
        duration: float,
        status: int | None,
        attempt: int,
        exc: BaseException | None,
    ) -> None:
        if self._on_request_complete is not None:
            self._on_request_complete(self.source, duration, status)
        if status is None:
            status_field: object = type(exc).__name__ if exc else "error"
        else:
            status_field = status
        log_event(
            self._log,
            operation,
            level=logging.DEBUG if status is not None and status < 400 else logging.WARNING,
            request_id=request_id,
            source=self.source,
            # Query strings can carry identifiers; the path alone is enough to
            # debug and keeps the log free of payload data.
            url=str(httpx.URL(url).copy_with(query=None)),
            duration=duration,
            status=status_field,
            attempt=attempt,
        )
