from datetime import date

from clients.coverage_denominator import ExpectedSeries
from clients.gap_engine import classify

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
