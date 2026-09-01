from datetime import date

import pytest

from clients.coverage_denominator import ExpectedSeries
from clients.gap_engine import (
    classify,
    load_unresolved,
    massive_floor_for,
    record_unresolved,
    repair_source,
    suppress_unresolved,
)

SESSIONS = (date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28))
FLOOR = date(2021, 7, 12)  # measured Massive entitlement floor, docs/audits/2026-07-11


def _series() -> ExpectedSeries:
    return ExpectedSeries("MUNJ", "equity", "1d", SESSIONS)


def test_missing_file_is_g3():
    findings = classify(_series(), present=set(), massive_floor=FLOOR)
    assert [f.gap for f in findings] == ["G3"]
    assert findings[0].sessions == SESSIONS


def test_missing_latest_session_is_g1():
    present = {date(2026, 8, 26), date(2026, 8, 27)}
    findings = classify(_series(), present=present, massive_floor=FLOOR)
    assert [f.gap for f in findings] == ["G1"]
    assert findings[0].sessions == (date(2026, 8, 28),)


def test_missing_middle_session_is_g2():
    present = {date(2026, 8, 26), date(2026, 8, 28)}
    findings = classify(_series(), present=present, massive_floor=FLOOR)
    assert [f.gap for f in findings] == ["G2"]
    assert findings[0].sessions == (date(2026, 8, 27),)


def test_complete_series_yields_nothing():
    assert classify(_series(), present=set(SESSIONS), massive_floor=FLOOR) == []


def test_heal_by_is_headroom_above_the_massive_floor():
    findings = classify(_series(), present=set(), massive_floor=FLOOR)
    # earliest missing session is 2026-08-26; headroom is days above the floor
    assert findings[0].heal_by_days == (date(2026, 8, 26) - FLOOR).days


def test_session_below_the_floor_has_negative_headroom():
    old = ExpectedSeries("MUNJ", "equity", "1d", (date(2019, 3, 1),))
    findings = classify(old, present=set(), massive_floor=FLOOR)
    assert findings[0].heal_by_days < 0, "pre-floor sessions are IB-only"


def test_tier_follows_the_massive_window():
    """Inside the window Massive repairs unattended; below it only IB can, and
    IB is 2FA-gated, so it is a decision rather than an automatic repair."""
    inside = classify(_series(), present=set(), massive_floor=FLOOR)
    assert inside[0].tier == "A"

    old = ExpectedSeries("MUNJ", "equity", "1d", (date(2019, 3, 1),))
    below = classify(old, present=set(), massive_floor=FLOOR)
    assert below[0].tier == "B", "pre-floor gaps must not claim unattended repair"


def test_unresolved_sessions_are_not_re_reported(tmp_path):
    """Cause 5: the same unsourceable symbols must not be re-litigated every round."""
    ledger = tmp_path / "unresolved.json"
    record_unresolved(ledger, "MUNJ", date(2026, 8, 27), reason="delisted, no source", as_of=date(2026, 8, 31))
    findings = classify(_series(), present={date(2026, 8, 26), date(2026, 8, 28)}, massive_floor=FLOOR)
    kept = suppress_unresolved(findings, load_unresolved(ledger))
    assert kept == [], "a recorded unresolved session must not re-report"


def test_unresolved_ledger_keeps_the_reason(tmp_path):
    ledger = tmp_path / "unresolved.json"
    record_unresolved(ledger, "MUNJ", date(2026, 8, 27), reason="delisted, no source", as_of=date(2026, 8, 31))
    assert ("MUNJ", "equity", "1d", date(2026, 8, 27)) in load_unresolved(ledger)
    assert "delisted, no source" in ledger.read_text()


def test_partially_unresolved_finding_keeps_its_other_sessions(tmp_path):
    ledger = tmp_path / "unresolved.json"
    record_unresolved(ledger, "MUNJ", date(2026, 8, 27), reason="x", as_of=date(2026, 8, 31))
    findings = classify(_series(), present=set(), massive_floor=FLOOR)
    kept = suppress_unresolved(findings, load_unresolved(ledger))
    assert kept and kept[0].sessions == (date(2026, 8, 26), date(2026, 8, 28))


def test_absent_ledger_is_empty_not_an_error(tmp_path):
    """The first scheduled run points --unresolved at a file that does not
    exist yet; that must read as 'nothing suppressed', not blow up."""
    assert load_unresolved(tmp_path / "never-written.json") == set()


def test_unresolved_is_scoped_to_one_timeframe(tmp_path):
    """Keyed on (symbol, session) alone, marking 1d unresolved also silenced 1h."""
    ledger = tmp_path / "unresolved.json"
    record_unresolved(
        ledger,
        "MUNJ",
        date(2026, 8, 27),
        reason="x",
        as_of=date(2026, 8, 31),
        asset_class="equity",
        timeframe="1d",
    )
    hourly = ExpectedSeries("MUNJ", "equity", "1h", (date(2026, 8, 27),))
    findings = classify(hourly, present=set(), massive_floor=FLOOR)
    assert suppress_unresolved(findings, load_unresolved(ledger)) == findings


def test_ib_sourced_lanes_are_never_tier_a():
    """futures/cmdty come from IB, which is 2FA-gated and never auto-retries.
    A recent futures gap is still a decision, not an unattended repair."""
    for asset_class in ("futures", "cmdty"):
        series = ExpectedSeries("GC_202612", asset_class, "1d", SESSIONS)
        findings = classify(series, present=set(), massive_floor=FLOOR)
        assert findings[0].tier == "B", asset_class
        assert findings[0].source == "ib"
        assert findings[0].heal_by_days is None, "IB has no rolling window"


def test_deep_history_sources_have_no_expiry():
    """Yahoo/CBOE/FRED serve deep history, so the repair path never expires."""
    for asset_class, source in (("fx", "yahoo"), ("volatility", "cboe"), ("rates", "fred")):
        series = ExpectedSeries("DXY", asset_class, "1d", (date(2005, 3, 1),))
        findings = classify(series, present=set(), massive_floor=FLOOR)
        assert findings[0].tier == "A", asset_class
        assert findings[0].source == source
        assert findings[0].heal_by_days is None


def test_equity_still_rides_the_massive_floor():
    below = ExpectedSeries("MUNJ", "equity", "1d", (date(2019, 3, 1),))
    assert classify(below, present=set(), massive_floor=FLOOR)[0].tier == "B"
    assert classify(_series(), present=set(), massive_floor=FLOOR)[0].tier == "A"


def test_an_unmapped_asset_class_fails_closed():
    series = ExpectedSeries("XYZ", "options", "1d", SESSIONS)
    with pytest.raises(ValueError, match="no mapped repair source"):
        classify(series, present=set(), massive_floor=FLOOR)


def test_repair_source_names_the_documented_provider():
    assert repair_source("equity") == "massive"
    assert repair_source("futures") == "ib"


def test_the_massive_floor_rolls_with_the_scan_date():
    """Measured 2026-07-29: 2021-07-27 -> 403, 2021-07-28 -> OK, i.e. 1827 days."""
    assert massive_floor_for(date(2026, 7, 29)) == date(2021, 7, 28)
    later = massive_floor_for(date(2026, 8, 29))
    assert later > massive_floor_for(date(2026, 7, 29)), "the floor must roll forward"


def test_sessions_before_the_first_ever_bar_are_g13_not_g2():
    """Backfill not reaching that far is routine; a session written then lost is
    an incident. Both used to be G2, which hid the incident."""
    series = ExpectedSeries("MUNJ", "equity", "1d", SESSIONS)
    findings = classify(series, present={SESSIONS[2]}, massive_floor=FLOOR)
    assert [f.gap for f in findings] == ["G13"]
    assert findings[0].sessions == (SESSIONS[0], SESSIONS[1])


def test_head_and_interior_are_reported_separately():
    window = (date(2026, 8, 24), date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28))
    series = ExpectedSeries("MUNJ", "equity", "1d", window)
    # first-ever bar is 08-25; 08-24 is head, 08-26/27 are a real interior loss
    findings = classify(series, present={window[1], window[4]}, massive_floor=FLOOR)
    by_gap = {f.gap: f.sessions for f in findings}
    assert by_gap["G13"] == (window[0],)
    assert by_gap["G2"] == (window[2], window[3])
