from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from livewire_scripts.corporate_action_cursor import (
    build_identity,
    default_cursor_path,
    open_cursor,
)


def _utc(day: int) -> datetime:
    return datetime(2026, 7, day, 12, tzinfo=UTC)


def test_default_paths_isolate_modes_and_ticker_sets(tmp_path):
    base = build_identity(tmp_path, ["AAPL"], full_reconcile=False, dry_run=False)
    dry = build_identity(tmp_path, ["AAPL"], full_reconcile=False, dry_run=True)
    full = build_identity(tmp_path, ["AAPL"], full_reconcile=True, dry_run=False)
    other = build_identity(tmp_path, ["MSFT"], full_reconcile=False, dry_run=False)

    paths = {default_cursor_path(tmp_path, item) for item in (base, dry, full, other)}

    assert len(paths) == 4
    assert all(path.parent == tmp_path / "cursors/corporate_actions" for path in paths)


def test_ticker_identity_is_normalized_and_order_independent(tmp_path):
    first = build_identity(tmp_path, ["msft", "AAPL", "AAPL"], full_reconcile=True, dry_run=False)
    second = build_identity(tmp_path, ["AAPL", "MSFT"], full_reconcile=True, dry_run=False)

    assert first == second
    assert first.ticker_count == 2


def test_resume_missing_starts_and_incomplete_cross_date_resumes(tmp_path):
    identity = build_identity(tmp_path, ["AAPL", "MSFT"], full_reconcile=True, dry_run=False)
    path = tmp_path / "cursor.json"
    cursor = open_cursor(path, identity, resume=True, now=_utc(13))
    cursor.mark_completed("AAPL", now=_utc(13))

    resumed = open_cursor(path, identity, resume=True, now=_utc(14))

    assert resumed.completed == {"AAPL"}
    assert resumed.started_at == _utc(13)
    assert json.loads(path.read_text())["completed"] == ["AAPL"]


def test_fresh_run_replaces_matching_completed_cursor(tmp_path):
    identity = build_identity(tmp_path, ["AAPL"], full_reconcile=True, dry_run=False)
    path = tmp_path / "cursor.json"
    cursor = open_cursor(path, identity, resume=False, now=_utc(13))
    cursor.mark_completed("AAPL", now=_utc(13))
    cursor.mark_run_completed(now=_utc(13))

    fresh = open_cursor(path, identity, resume=False, now=_utc(14))

    assert fresh.completed == set()
    assert fresh.run_completed_at is None
    assert fresh.started_at == _utc(14)


def test_completed_cursor_rejects_resume(tmp_path):
    identity = build_identity(tmp_path, ["AAPL"], full_reconcile=True, dry_run=False)
    path = tmp_path / "cursor.json"
    cursor = open_cursor(path, identity, resume=False, now=_utc(13))
    cursor.mark_completed("AAPL", now=_utc(13))
    cursor.mark_run_completed(now=_utc(13))

    with pytest.raises(ValueError, match="already complete"):
        open_cursor(path, identity, resume=True, now=_utc(14))


def test_incompatible_cursor_rejects_resume(tmp_path):
    identity = build_identity(tmp_path, ["AAPL"], full_reconcile=False, dry_run=False)
    other = build_identity(tmp_path, ["MSFT"], full_reconcile=False, dry_run=False)
    path = tmp_path / "cursor.json"
    open_cursor(path, identity, resume=False, now=_utc(13))

    with pytest.raises(ValueError, match="incompatible"):
        open_cursor(path, other, resume=True, now=_utc(13))


def test_malformed_cursor_rejects_resume(tmp_path):
    path = tmp_path / "cursor.json"
    path.write_text("not json")
    identity = build_identity(tmp_path, ["AAPL"], full_reconcile=False, dry_run=False)

    with pytest.raises(ValueError, match="malformed"):
        open_cursor(path, identity, resume=True, now=_utc(13))


def test_cursor_contains_no_unexpected_fields(tmp_path):
    identity = build_identity(tmp_path, ["AAPL"], full_reconcile=False, dry_run=False)
    path = tmp_path / "cursor.json"
    cursor = open_cursor(path, identity, resume=False, now=_utc(13))
    cursor.mark_completed("AAPL", now=_utc(13))

    payload = json.loads(path.read_text())

    assert set(payload) == {
        "completed",
        "dry_run",
        "full_reconcile",
        "root",
        "run_completed_at",
        "schema_version",
        "started_at",
        "started_on_ny",
        "ticker_count",
        "ticker_sha256",
    }
    assert "key" not in path.read_text().lower()
