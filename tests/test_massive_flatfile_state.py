from datetime import date

import pytest

from clients.massive_flatfile_state import MAX_RETAINED_SCOPES, MassiveFlatfileState


def test_state_records_raw_completion_atomically(tmp_path):
    state = MassiveFlatfileState(tmp_path)
    state.mark_raw_completed(date(2026, 6, 5))
    assert state.raw_completed(date(2026, 6, 5))
    assert MassiveFlatfileState(tmp_path).raw_completed(date(2026, 6, 5))
    assert '"event": "raw_completed"' in state.manifest_path.read_text()


def test_publish_completion_is_scoped_and_idempotent(tmp_path):
    state = MassiveFlatfileState(tmp_path)
    state.mark_ticker_completed("history-a", 3, "AAPL")
    state.mark_ticker_completed("history-a", 3, "AAPL")
    state.mark_bucket_completed("history-a", 3)

    loaded = MassiveFlatfileState(tmp_path)
    assert loaded.ticker_completed("history-a", 3, "AAPL")
    assert loaded.bucket_completed("history-a", 3)
    assert not loaded.bucket_completed("history-b", 3)
    assert state.manifest_path.read_text().count('"event": "ticker_completed"') == 1
    state.reset_publish_scope("history-a")
    assert not state.bucket_completed("history-a", 3)


def test_state_records_string_failure_and_unavailable_dates(tmp_path):
    state = MassiveFlatfileState(tmp_path)
    state.mark_raw_failed("2026-06-04", "failed")
    state.mark_raw_unavailable("2026-06-05", "missing")
    manifest = state.manifest_path.read_text()
    assert '"event": "raw_failed"' in manifest
    assert '"event": "raw_unavailable"' in manifest


def test_malformed_state_is_rejected(tmp_path):
    (tmp_path / "massive_flatfile_state.json").write_text("{bad")
    with pytest.raises(ValueError, match="Malformed"):
        MassiveFlatfileState(tmp_path)


def test_ticker_completion_does_not_rewrite_the_snapshot(tmp_path):
    """Per-ticker save() re-serialised the whole 5 MB state file ~12K times.

    That write amplification is why the publish phase looked like a hang. The
    durable record is the append-only manifest; the snapshot is a resume cache
    flushed at bucket boundaries.
    """
    state = MassiveFlatfileState(tmp_path)
    state.mark_bucket_completed("scope", 0)  # forces an initial snapshot
    before = state.state_path.stat().st_mtime_ns

    for i in range(50):
        state.mark_ticker_completed("scope", 1, f"T{i}")

    assert state.state_path.stat().st_mtime_ns == before
    # ...but every one is durably in the manifest.
    manifest = state.manifest_path.read_text(encoding="utf-8")
    assert manifest.count('"event": "ticker_completed"') == 50
    # ...and is visible in-memory for the rest of the run.
    assert state.ticker_completed("scope", 1, "T7")


def test_bucket_completion_persists_pending_ticker_marks(tmp_path):
    state = MassiveFlatfileState(tmp_path)
    state.mark_ticker_completed("scope", 1, "AAPL")
    state.mark_bucket_completed("scope", 1)

    reloaded = MassiveFlatfileState(tmp_path)
    assert reloaded.ticker_completed("scope", 1, "AAPL")


def test_publish_scopes_are_pruned(tmp_path):
    """The scope key changes nightly, so nothing ever removed the old ones."""
    state = MassiveFlatfileState(tmp_path)
    for i in range(MAX_RETAINED_SCOPES + 5):
        state.mark_bucket_completed(f"scope-{i:03d}", 0)

    reloaded = MassiveFlatfileState(tmp_path)
    kept = reloaded.data["publish_scopes"]
    assert len(kept) == MAX_RETAINED_SCOPES
    # The newest survive; the oldest are dropped.
    assert "scope-014" in kept
    assert "scope-000" not in kept
