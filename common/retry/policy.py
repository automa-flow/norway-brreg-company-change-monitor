"""Retry with exponential backoff.

Deliberately tiny: one policy object and one driver function. Callers decide
what is retryable by passing exception types and/or a result predicate, so this
module has no knowledge of HTTP, browsers or any particular source.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

#: Status codes that are worth retrying for a read-only scraping workload.
#: 429 and 5xx are transient; 408/425 are timeout-ish. 403 is *not* here on
#: purpose - for geo/WAF-blocked sources it means "you are blocked", and
#: hammering it makes things worse.
DEFAULT_RETRYABLE_STATUS: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How many times to retry and how long to wait between attempts."""

    max_attempts: int = 3
    initial_delay: float = 0.5
    max_delay: float = 20.0
    multiplier: float = 2.0
    #: Random fraction of the computed delay added on top, to avoid lockstep
    #: retries across concurrent workers. ``0`` makes delays deterministic.
    jitter: float = 0.2
    retry_on_status: frozenset[int] = field(default=DEFAULT_RETRYABLE_STATUS)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.initial_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be >= 0")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")

    def delay_for(self, attempt: int) -> float:
        """Delay in seconds to wait *after* a failed ``attempt`` (1-based)."""
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        base = min(self.initial_delay * (self.multiplier ** (attempt - 1)), self.max_delay)
        if self.jitter:
            # Not cryptographic: this only de-synchronizes concurrent workers.
            base += random.uniform(0, base * self.jitter)
        return base

    def should_retry_status(self, status_code: int) -> bool:
        return status_code in self.retry_on_status


async def retry_async[T](
    operation: Callable[[int], Awaitable[T]],
    *,
    policy: RetryPolicy,
    retryable_exceptions: tuple[type[BaseException], ...] = (),
    result_needs_retry: Callable[[T], bool] | None = None,
    on_retry: Callable[[int, float, BaseException | None], None] | None = None,
) -> T:
    """Run ``operation(attempt)`` until it succeeds or attempts run out.

    ``operation`` receives the 1-based attempt number, which is handy for
    logging and for rotating proxy sessions between attempts.

    The last failure is re-raised as-is; no wrapper exception is introduced so
    callers keep whatever error type they already handle. If the final attempt
    fails only because of ``result_needs_retry``, that last result is returned -
    the caller asked for a retry, not for an exception.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, policy.max_attempts + 1):
        last_exc = None
        try:
            result = await operation(attempt)
        except retryable_exceptions as exc:
            last_exc = exc
            if attempt == policy.max_attempts:
                raise
        else:
            if result_needs_retry is None or not result_needs_retry(result):
                return result
            if attempt == policy.max_attempts:
                return result

        delay = policy.delay_for(attempt)
        if on_retry is not None:
            on_retry(attempt, delay, last_exc)
        await asyncio.sleep(delay)

    # Unreachable: the loop either returns or raises on the final attempt.
    raise AssertionError("retry_async fell through")  # pragma: no cover
