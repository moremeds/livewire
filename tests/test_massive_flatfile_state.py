from datetime import date

import pytest

from clients.massive_flatfile_state import MassiveFlatfileState


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


def test_malformed_state_is_rejected(tmp_path):
    (tmp_path / "massive_flatfile_state.json").write_text("{bad")
    with pytest.raises(ValueError, match="Malformed"):
        MassiveFlatfileState(tmp_path)
