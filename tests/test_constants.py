"""Tests for clients/constants.py — the one place a number lives."""

from __future__ import annotations

import pytest

from clients import constants


def test_declared_returns_the_dict_value():
    assert constants.declared("failure_rate_tolerance") == 0.05


def test_declared_rejects_an_unknown_key():
    with pytest.raises(KeyError):
        constants.declared("no_such_constant")


def test_a_scoped_name_without_its_scope_is_not_a_key():
    # the scope is part of the key; the bare name is not declared
    with pytest.raises(KeyError):
        constants.declared("lane_budget_s")


def test_env_override_wins_and_is_typed_as_float(monkeypatch):
    monkeypatch.setenv("LW_DECLARED_FAILURE_RATE_TOLERANCE", "0.10")
    assert constants.declared("failure_rate_tolerance") == 0.10


def test_lane_budget_is_declared_once_per_lane():
    assert constants.declared("lane_budget_s/corporate-actions") == 3 * 60 * 60
    assert constants.declared("lane_budget_s/cmdty") == 30 * 60


def test_lane_budget_env_override_is_per_lane(monkeypatch):
    monkeypatch.setenv("LW_DECLARED_LANE_BUDGET_S_CORPORATE_ACTIONS", "7200")
    assert constants.declared("lane_budget_s/corporate-actions") == 7200
    # a different lane is untouched
    assert constants.declared("lane_budget_s/cmdty") == 30 * 60


def test_a_non_numeric_override_fails_loudly(monkeypatch):
    """A typo'd override must raise, never fall back to the declared value."""
    monkeypatch.setenv("LW_DECLARED_FAILURE_RATE_TOLERANCE", "five percent")
    with pytest.raises(ValueError):
        constants.declared("failure_rate_tolerance")


def test_every_declared_entry_is_a_value_and_a_nonempty_unit():
    for key, (value, unit) in constants.DECLARED.items():
        assert isinstance(value, (int, float))
        assert isinstance(unit, str) and unit
        assert key == key.strip() and "//" not in key


def test_env_keys_are_unique_across_declared():
    # '/' and '-' both flatten to '_', so two different keys could in principle
    # collide on one env var name. Assert they do not.
    env_keys = [constants._env_key(k) for k in constants.DECLARED]
    assert len(set(env_keys)) == len(env_keys)


def test_split_scope_separates_the_scope_from_the_name():
    assert constants.split_scope("lane_budget_s/corporate-actions") == ("lane_budget_s", "corporate-actions")
    assert constants.split_scope("failure_rate_tolerance") == ("failure_rate_tolerance", "")


def test_every_lane_in_lane_order_has_a_declared_budget():
    from livewire_scripts.run_daily_update_job import LANE_ORDER

    for lane in LANE_ORDER:
        assert f"lane_budget_s/{lane}" in constants.DECLARED


def test_declared_lane_budgets_cover_exactly_the_lane_set():
    budget_lanes = {key.split("/", 1)[1] for key in constants.DECLARED if key.startswith("lane_budget_s/")}

    assert budget_lanes == set(constants.LANE_ORDER) | {"default"}


def test_ib_only_lanes_are_a_subset_of_the_lane_order():
    assert set(constants.IB_ONLY_LANES) <= set(constants.LANE_ORDER)


def test_the_lake_lock_wait_is_declared_globally():
    """The acceptable wait is a property of the lock, not of one lane."""
    assert constants.DECLARED["lake_lock_wait_s"] == (2340, "s")
    assert constants.split_scope("lake_lock_wait_s") == ("lake_lock_wait_s", "")


def test_the_lake_lock_poll_intervals_are_scoped_by_job():
    poll_scopes = {key.split("/", 1)[1] for key in constants.DECLARED if key.startswith("lake_lock_poll_s/")}

    assert poll_scopes == {"daily", "intraday"}


def test_the_daily_job_polls_the_lake_lock_more_often_than_the_intraday_job():
    """Priority IS the poll interval: the low-priority job looks less often (spec section 3)."""
    assert constants.declared("lake_lock_poll_s/daily") < constants.declared("lake_lock_poll_s/intraday")


def test_the_lake_lock_keys_carry_a_working_env_override():
    import os

    os.environ["LW_DECLARED_LAKE_LOCK_POLL_S_INTRADAY"] = "5"
    try:
        assert constants.declared("lake_lock_poll_s/intraday") == 5.0
    finally:
        del os.environ["LW_DECLARED_LAKE_LOCK_POLL_S_INTRADAY"]
