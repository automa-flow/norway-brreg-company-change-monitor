"""Small operational envelope; source-specific counts stay owned by each Actor."""

from typing import Any


def operational_summary(
    *,
    requested: int,
    completed: int,
    failed: int,
    invalid: int,
    ambiguous: int,
    source_state: str,
    budget_limited: bool,
    output_writes: int,
    duration_seconds: float,
    partial_source: bool = False,
) -> dict[str, Any]:
    if source_state == "SOURCE_FAILED":
        status = "SOURCE_FAILED"
    elif budget_limited:
        status = "BUDGET_LIMITED"
    elif failed or invalid or ambiguous or completed != requested or partial_source:
        status = "PARTIAL"
    elif source_state != "SOURCE_SUCCESS":
        status = "UNVERIFIED"
    else:
        status = "SUCCESS"
    return {
        "schemaVersion": 1,
        "status": status,
        "sourceState": source_state,
        "requested": requested,
        "completed": completed,
        "failed": failed,
        "invalid": invalid,
        "ambiguous": ambiguous,
        "budgetLimited": budget_limited,
        "outputWrites": output_writes,
        "durationSeconds": round(duration_seconds, 3),
    }
