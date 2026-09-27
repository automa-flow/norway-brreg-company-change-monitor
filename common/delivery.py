"""Fail-closed guard for runs whose output cannot be atomically reconciled."""

from __future__ import annotations

from typing import Any, Literal

DELIVERY_CHECKPOINT = "DELIVERY_CHECKPOINT"

DeliveryGate = Literal["FRESH", "STOP", "RESUME"]


async def delivery_gate(actor: Any, *, allow_resume: bool = False) -> DeliveryGate:
    """Decide whether this process may publish, must stop, or may resume.

    `RESUME` is only ever returned for an interrupted run that recorded itself as
    resumable, which requires the Actor to reconstruct what it already delivered
    from durable storage before publishing anything else.
    """
    checkpoint = await actor.get_value(DELIVERY_CHECKPOINT)
    if checkpoint is None:
        return "FRESH"
    if isinstance(checkpoint, dict) and checkpoint.get("phase") == "COMPLETE":
        if checkpoint.get("outcome") == "FAILED":
            await actor.fail(
                status_message=checkpoint.get("statusMessage") or "Previously failed run retained."
            )
            return "STOP"
        await actor.set_status_message(
            "Previously completed delivery retained; no repeated charges."
        )
        return "STOP"
    if allow_resume and isinstance(checkpoint, dict) and checkpoint.get("resumable"):
        return "RESUME"
    await actor.fail(
        status_message="DELIVERY_UNCERTAIN: interrupted output retained; start a new run "
        "to obtain a fresh observation. This run will not replay charges."
    )
    return "STOP"


async def delivery_already_started(actor: Any) -> bool:
    return await delivery_gate(actor) != "FRESH"


async def begin_delivery(actor: Any, *, resumable: bool = False) -> None:
    """Persist intent before *any* Dataset or billing side effect."""
    checkpoint: dict[str, Any] = {"version": 2 if resumable else 1, "phase": "PUBLISHING"}
    if resumable:
        checkpoint["resumable"] = True
    await actor.set_value(DELIVERY_CHECKPOINT, checkpoint)


async def complete_delivery(
    actor: Any, *, outcome: str | None = None, status_message: str | None = None
) -> None:
    """Mark complete only after all intended output, summary and state are durable."""
    checkpoint = {"version": 1, "phase": "COMPLETE"}
    if outcome is not None:
        checkpoint["outcome"] = outcome
    if status_message is not None:
        checkpoint["statusMessage"] = status_message
    await actor.set_value(DELIVERY_CHECKPOINT, checkpoint)
