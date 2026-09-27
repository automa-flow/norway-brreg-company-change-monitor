"""Exceptions raised by :mod:`common.http`.

Actors distinguish "the source said no" from "we could not reach the source",
because those map to different user-visible outcomes (NOT_FOUND vs FAILED).
"""

from __future__ import annotations


class HttpError(Exception):
    """Base class for every failure raised by :class:`common.http.AsyncHttpClient`."""

    def __init__(
        self,
        message: str,
        *,
        url: str | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.request_id = request_id

    def __str__(self) -> str:
        base = super().__str__()
        return f"{base} (url={self.url})" if self.url else base


class HttpTransportError(HttpError):
    """The request never produced a response: DNS, TLS, proxy or connection error."""


class HttpTimeoutError(HttpTransportError):
    """The request exceeded its timeout."""


class HttpStatusError(HttpError):
    """The server responded with a status the caller did not accept."""

    #: How much of the body to keep on the exception. Enough to debug, small
    #: enough not to dump a whole HTML error page into the run log.
    BODY_PREVIEW_CHARS = 500

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        url: str | None = None,
        request_id: str | None = None,
        body: str | None = None,
    ) -> None:
        super().__init__(message, url=url, request_id=request_id)
        self.status_code = status_code
        self.body_preview = (body or "")[: self.BODY_PREVIEW_CHARS]
