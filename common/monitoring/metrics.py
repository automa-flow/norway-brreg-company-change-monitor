"""Health metrics for a single Actor run.

Deliberately just an in-memory counter bag plus a formatter. Logs and the Apify
run record are the monitoring system; there is no separate service here, and
there should not be one until several Actors actually need cross-run analysis.

The counters also double as the input to future Pay-per-Event pricing: cost per
company is ``requests / processed``, and browser bootstraps are the expensive
part of a KRZ-style run.
"""

from __future__ import annotations

import logging
import time
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from common.logging import log_event


class Outcome(StrEnum):
    """How one unit of work ended. Mirrors the per-record ``checkStatus``."""

    SUCCESS = "SUCCESS"
    NOT_FOUND = "NOT_FOUND"
    FAILED = "FAILED"


@dataclass(slots=True)
class RunMetrics:
    """Counters and timings for one run."""

    actor: str
    source: str = ""
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    outcomes: Counter[str] = field(default_factory=Counter)
    counters: Counter[str] = field(default_factory=Counter)
    _timings: dict[str, list[float]] = field(default_factory=dict, repr=False)
    #: Error class names -> count, so a run summary shows *why* things failed.
    errors: Counter[str] = field(default_factory=Counter)

    # -- recording ---------------------------------------------------------

    def record_outcome(self, outcome: Outcome | str) -> None:
        self.outcomes[str(outcome)] += 1

    def record_error(self, exc: BaseException | str) -> None:
        name = exc if isinstance(exc, str) else type(exc).__name__
        self.errors[name] += 1

    def increment(self, name: str, amount: int = 1) -> None:
        """Bump a free-form counter, e.g. ``http_requests`` or ``browser_bootstraps``."""
        self.counters[name] += amount

    def observe(self, name: str, seconds: float) -> None:
        self._timings.setdefault(name, []).append(seconds)

    @contextmanager
    def timer(self, name: str) -> Iterator[None]:
        started = time.monotonic()
        try:
            yield
        finally:
            self.observe(name, time.monotonic() - started)

    def finish(self) -> None:
        self.finished_at = datetime.now(UTC)

    # -- derived -----------------------------------------------------------

    @property
    def processed(self) -> int:
        return sum(self.outcomes.values())

    @property
    def duration_seconds(self) -> float:
        end = self.finished_at or datetime.now(UTC)
        return (end - self.started_at).total_seconds()

    @property
    def is_empty_result(self) -> bool:
        """No successful record at all - usually means the source or input is broken."""
        return self.outcomes.get(str(Outcome.SUCCESS), 0) == 0

    @property
    def total_failed(self) -> int:
        return self.outcomes.get(str(Outcome.FAILED), 0)

    def average(self, name: str) -> float | None:
        values = self._timings.get(name)
        return sum(values) / len(values) if values else None

    def per_unit(self, counter: str) -> float | None:
        """Counter value per processed record, e.g. requests per company."""
        return self.counters[counter] / self.processed if self.processed else None

    # -- output ------------------------------------------------------------

    def summary(self) -> dict[str, Any]:
        return {
            "actor": self.actor,
            "source": self.source,
            "startedAt": self.started_at.isoformat(),
            "finishedAt": (self.finished_at or datetime.now(UTC)).isoformat(),
            "durationSeconds": round(self.duration_seconds, 3),
            "processed": self.processed,
            "outcomes": dict(self.outcomes),
            "counters": dict(self.counters),
            "errors": dict(self.errors),
            "averages": {
                name: round(sum(values) / len(values), 3)
                for name, values in self._timings.items()
                if values
            },
            "emptyResult": self.is_empty_result,
        }

    def format_summary(self, extra_labels: dict[str, str] | None = None) -> str:
        """Human-readable block for the end of the run log.

        ``extra_labels`` maps counter names to display labels so an Actor can
        surface domain counters ("Active proceedings") without this module
        knowing anything about the domain.
        """
        lines = [
            f"Processed: {self.processed}",
            f"Successful: {self.outcomes.get(str(Outcome.SUCCESS), 0)}",
            f"Not found: {self.outcomes.get(str(Outcome.NOT_FOUND), 0)}",
            f"Failed: {self.total_failed}",
        ]
        for counter, label in (extra_labels or {}).items():
            lines.append(f"{label}: {self.counters.get(counter, 0)}")
        lines.append(f"Duration: {self.duration_seconds:.1f}s")
        if self.errors:
            top = ", ".join(f"{name}={count}" for name, count in self.errors.most_common(5))
            lines.append(f"Errors: {top}")
        return "\n".join(lines)

    def log_summary(
        self,
        logger: logging.Logger,
        *,
        extra_labels: dict[str, str] | None = None,
    ) -> None:
        log_event(
            logger,
            "run.summary",
            level=logging.INFO,
            source=self.source,
            duration=self.duration_seconds,
            records=self.processed,
            status="EMPTY" if self.is_empty_result and self.processed else "OK",
            message="\n" + self.format_summary(extra_labels),
        )
