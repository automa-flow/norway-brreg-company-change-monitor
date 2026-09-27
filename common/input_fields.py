"""Separate the input keys an Actor declares from the ones it does not.

Apify starts a run with whatever JSON the caller sends; the Input Schema does
not reject unknown keys (verified 2026-09-16: a run with ``debug: true`` was
created with HTTP 201). Every Actor input model here uses ``extra="forbid"``,
so a stray key from an API integration, an agent or a stale example turned a
perfectly good request into a FAILED run in the public statistics. Dropping
unknown top-level keys before validation keeps the strict model for everything
it declares and turns the stray key into a logged warning instead.

Nested objects keep their own rules: an unknown key inside a watchlist item is
still the caller's mistake about that item, and the models decide that.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


def declared_input_keys(model: type[BaseModel]) -> frozenset[str]:
    """Field names plus every alias the model accepts on input."""
    keys: set[str] = set()
    for name, field in model.model_fields.items():
        keys.add(name)
        for alias in (field.alias, field.validation_alias):
            if isinstance(alias, str):
                keys.add(alias)
    return frozenset(keys)


def split_unknown_input(raw: Any, model: type[BaseModel]) -> tuple[Any, list[str]]:
    """Return the input without undeclared top-level keys and the keys dropped.

    Anything that is not a mapping is returned untouched so the model reports
    the real problem itself.
    """
    if not isinstance(raw, dict):
        return raw, []
    known = declared_input_keys(model)
    ignored = sorted(str(key) for key in raw if key not in known)
    if not ignored:
        return raw, []
    return {key: value for key, value in raw.items() if key in known}, ignored
