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


def _terminus_series(symbol="EQR", sessions=()):
    return ExpectedSeries(symbol, "equity", "1d", tuple(sessions))


def test_a_terminus_is_g14_and_never_tier_a():
    # EQR left the tape on 2026-08-18. No source can supply bars for an
    # instrument that is not printing, so Tier A would queue a repair that
    # fetches nothing, forever.
    sessions = (date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20))
    findings = classify(
        _terminus_series(sessions=sessions),
        present={date(2026, 8, 17)},
        massive_floor=FLOOR,
        terminus=date(2026, 8, 18),
    )
    assert [f.gap for f in findings] == ["G14"]
    assert findings[0].tier == "B"
    assert findings[0].heal_by_days is None


def test_a_terminus_swallows_the_tail_rather_than_emitting_both():
    # Without this the same sessions are reported twice: once as a repairable G1
    # and once as an unrepairable G14, and the Tier A queue gets a job it cannot
    # do.
    sessions = (date(2026, 8, 18), date(2026, 8, 19))
    findings = classify(
        _terminus_series(sessions=sessions),
        present={date(2026, 8, 17)},
        massive_floor=FLOOR,
        terminus=date(2026, 8, 18),
    )
    assert len(findings) == 1


def test_a_missing_file_with_a_terminus_is_g14_not_g3():
    # BK is in sp500.json, has no 1d.parquet, and has never been on the tape.
    findings = classify(
        _terminus_series(symbol="BK", sessions=(date(2026, 8, 18),)),
        present=set(),
        massive_floor=FLOOR,
        terminus=date(2026, 8, 3),
    )
    assert [f.gap for f in findings] == ["G14"]
    assert findings[0].tier == "B"


def test_a_missing_file_with_no_terminus_is_still_g3_tier_a():
    # The acceptance-criterion-2 case: a symbol that never landed but IS on the
    # tape is a real, repairable gap. This must not regress.
    findings = classify(
        _terminus_series(sessions=(date(2026, 8, 18),)),
        present=set(),
        massive_floor=FLOOR,
        terminus=None,
    )
    assert [f.gap for f in findings] == ["G3"]
    assert findings[0].tier == "A"


def test_interior_and_head_gaps_are_no_longer_emitted():
    # G2 and G13 produced zero true findings out of 501 on the first production
    # run. Only the tail is reported.
    sessions = (date(2026, 8, 3), date(2026, 8, 5), date(2026, 8, 7))
    findings = classify(
        _terminus_series(sessions=sessions),
        present={date(2026, 8, 4), date(2026, 8, 6)},
        massive_floor=FLOOR,
        terminus=None,
    )
    assert [f.gap for f in findings] == ["G1"]
    assert findings[0].sessions == (date(2026, 8, 7),)


def test_a_terminus_does_not_swallow_repairable_sessions_before_it():
    """An ingestion outage that happens to precede a delisting stays repairable.

    classify used to return the G14 finding alone, so sessions BEFORE the
    terminus -- when the instrument was demonstrably still printing, and whose
    bars therefore exist at the provider -- were silently discarded. Two
    different facts, two findings.
    """
    sessions = tuple(date(2026, 8, d) for d in (10, 11, 12, 13, 14, 17, 18, 19))
    series = ExpectedSeries("EQR", "equity", "1d", sessions)
    findings = classify(series, present={date(2026, 8, 10)}, massive_floor=FLOOR, terminus=date(2026, 8, 17))

    by_gap = {f.gap: f for f in findings}
    assert set(by_gap) == {"G1", "G14"}
    assert by_gap["G14"].sessions == (date(2026, 8, 17), date(2026, 8, 18), date(2026, 8, 19))
    assert by_gap["G14"].tier == "B"
    # The pre-terminus run is an ordinary repairable tail.
    assert by_gap["G1"].sessions == (date(2026, 8, 11), date(2026, 8, 12), date(2026, 8, 13), date(2026, 8, 14))
    assert by_gap["G1"].tier == "A"


def test_an_unconfirmed_terminus_keeps_its_gap_class_but_loses_tier_a():
    """A qualifying absence run nobody could explain is not an unattended repair.

    The gap really is a tail, so the class stays G1; but Tier A means "a job can
    fetch this tonight without a human", and an instrument that may have stopped
    printing would make that job return nothing every night forever.
    """
    findings = classify(_series(), present={SESSIONS[0]}, massive_floor=FLOOR, unconfirmed=True)
    assert [(f.gap, f.tier, f.heal_by_days) for f in findings] == [("G1", "B", None)]
