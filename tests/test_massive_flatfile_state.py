from datetime import date

from clients.massive_flatfile_state import MassiveFlatfileState


def test_state_records_raw_completion_atomically(tmp_path):
    state = MassiveFlatfileState(tmp_path)
    state.mark_raw_completed(date(2026, 6, 5))
    assert state.raw_completed(date(2026, 6, 5))
    assert MassiveFlatfileState(tmp_path).raw_completed(date(2026, 6, 5))
    assert '"event": "raw_completed"' in state.manifest_path.read_text()
