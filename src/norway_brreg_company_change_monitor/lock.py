"""A fail-closed, non-expiring mutex for one monitor's read/compare/write cycle.

Two concurrent runs on the same monitor key would both read the cursor, both walk
the interval and both write: the loser's observations vanish from state, its
cursor advance is lost, and its changes are re-reported on the next run. KVS has
no compare-and-swap, but Request Queue unique keys provide atomic insertion, so
that is the mutex.

Never expire or automatically steal this marker: a paused writer could resume.
After a hard crash, stop all runs for the key before removing its marker by hand.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx

from norway_brreg_company_change_monitor.state import state_key

LOCK_QUEUE = "norway-brreg-company-change-monitor-locks"
#: Lets a private staging twin keep its own lock queue.
LOCK_QUEUE_ENV = "NORWAY_BRREG_LOCK_QUEUE"


def configured_lock_queue() -> str:
    name = os.environ.get(LOCK_QUEUE_ENV, LOCK_QUEUE).strip()
    if not name:
        raise ValueError(f"{LOCK_QUEUE_ENV} must not be empty")
    return name


class MonitorBusyError(RuntimeError):
    pass


class MonitorLock:
    def __init__(
        self,
        monitor_key: str,
        *,
        queue: Any = None,
        directory: Path | None = None,
        delete_request: Callable[[str], Awaitable[None]] | None = None,
        queue_name: str = LOCK_QUEUE,
    ) -> None:
        self.key = state_key(monitor_key)
        self.queue_name = queue_name
        self._queue = queue
        self._directory = directory
        self._request_id: str | None = None
        self._delete_request = delete_request

    @classmethod
    async def open(cls, monitor_key: str) -> MonitorLock:
        from apify import Actor  # noqa: PLC0415

        queue_name = configured_lock_queue()
        if not Actor.configuration.is_at_home:
            return cls(
                monitor_key,
                directory=Path(Actor.configuration.storage_dir) / queue_name,
                queue_name=queue_name,
            )
        # In particular, NEVER retry DELETE. A lost response followed by a retry
        # could delete a new owner's marker (the request ID derives from
        # uniqueKey). The client rejects zero retries, so DELETE gets its own
        # single-attempt transport. Retrying insertion is safe: uncertainty
        # leaves the lock closed.
        client = Actor.new_client()
        queue = await client.request_queues().get_or_create(name=queue_name)
        api_url = Actor.configuration.api_base_url.rstrip("/")
        token = Actor.configuration.token

        async def delete_request(request_id: str) -> None:
            async with httpx.AsyncClient(
                transport=httpx.AsyncHTTPTransport(retries=0),
                timeout=30,
                headers={"Authorization": f"Bearer {token}"},
            ) as http:
                response = await http.delete(
                    f"{api_url}/v2/request-queues/{queue.id}/requests/{request_id}"
                )
                response.raise_for_status()

        return cls(
            monitor_key,
            queue=client.request_queue(queue.id),
            delete_request=delete_request,
            queue_name=queue_name,
        )

    async def __aenter__(self) -> MonitorLock:
        if self._directory is not None:
            self._directory.mkdir(parents=True, exist_ok=True)
            try:
                with (self._directory / self.key).open("x", encoding="utf-8") as marker:
                    marker.write("Stop all runs for this monitor before removing this marker.\n")
            except FileExistsError as exc:
                raise self._busy() from exc
        else:
            # This URL is an inert mutex identity. No crawler ever visits it.
            result = await self._queue.add_request(
                {"url": "https://example.invalid/monitor-lock", "uniqueKey": self.key}
            )
            if result.was_already_present:
                raise self._busy()
            self._request_id = result.request_id
        return self

    def _busy(self) -> MonitorBusyError:
        return MonitorBusyError(
            f"Monitor is locked ({self.queue_name}, uniqueKey={self.key}). "
            "Retry after its active run finishes. If the owner crashed, stop all runs "
            "for this monitor before deleting only this lock marker; keep the KVS state."
        )

    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        # Cancellation or timeout leaves the marker in place. An interrupted
        # remote write may still be in flight; releasing now would permit a
        # stale writer to overwrite a newer cursor.
        if exc_type is not None:
            return
        if self._directory is not None:
            (self._directory / self.key).unlink()
        elif self._request_id is not None:
            delete = self._delete_request or self._queue.delete_request
            await asyncio.shield(delete(self._request_id))
