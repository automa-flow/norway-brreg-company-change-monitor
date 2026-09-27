"""A private staging twin keeps its own named storages; production keeps the defaults."""

from __future__ import annotations

import pytest

from norway_brreg_company_change_monitor.lock import (
    LOCK_QUEUE,
    LOCK_QUEUE_ENV,
    configured_lock_queue,
)
from norway_brreg_company_change_monitor.state import (
    DEFAULT_STATE_STORE,
    STATE_STORE_ENV,
    configured_state_store,
)


def test_production_uses_the_published_storage_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(STATE_STORE_ENV, raising=False)
    monkeypatch.delenv(LOCK_QUEUE_ENV, raising=False)
    assert configured_state_store() == DEFAULT_STATE_STORE
    assert configured_lock_queue() == LOCK_QUEUE


def test_a_staging_twin_can_point_both_storages_elsewhere(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(STATE_STORE_ENV, " norway-brreg-company-change-monitor-staging-state ")
    monkeypatch.setenv(LOCK_QUEUE_ENV, "norway-brreg-company-change-monitor-staging-locks")
    assert configured_state_store() == "norway-brreg-company-change-monitor-staging-state"
    assert configured_lock_queue() == "norway-brreg-company-change-monitor-staging-locks"


def test_an_empty_override_is_refused_rather_than_sharing_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(STATE_STORE_ENV, "  ")
    monkeypatch.setenv(LOCK_QUEUE_ENV, "")
    with pytest.raises(ValueError, match=STATE_STORE_ENV):
        configured_state_store()
    with pytest.raises(ValueError, match=LOCK_QUEUE_ENV):
        configured_lock_queue()
