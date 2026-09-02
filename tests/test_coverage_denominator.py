import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from clients.coverage_denominator import build_denominator, session_due_at

PRESETS = Path(__file__).resolve().parents[1] / "presets"


def test_denominator_does_not_depend_on_disk():
    """A preset symbol with no parquet file must still be expected.

    This is the whole point: coverage_report.py globs the disk, so a symbol
    that never landed is invisible to it.
    """
    series = build_denominator(
        [PRESETS / "volatility.json"],
        asset_class="volatility",
        timeframe="1d",
        start=date(2026, 8, 26),
        end=date(2026, 8, 28),
        as_of=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
    )
    assert series, "volatility preset must yield expected series"
    # every expected series carries the three real XNYS sessions in range
    for s in series:
        assert s.sessions == (date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28))


def test_expired_futures_contract_is_not_expected():
    """June-2026 index contracts have expired as of 2026-08-31.

    The plan named ES_202506 against presets/futures-active.json; that preset
    holds no expired contract (its nearest is GC_202608, still live on the
    as-of date), so the assertion would have passed vacuously. The real
    already-expired contracts live in presets/futures-index.json as _202606.
    """
    tickers = build_denominator(
        [PRESETS / "futures-index.json"],
        asset_class="futures",
        timeframe="1d",
        start=date(2026, 8, 26),
        end=date(2026, 8, 28),
        as_of=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
    )
    symbols = {s.symbol for s in tickers}
    assert symbols, "futures-index preset must yield live contracts"
    expired = {s for s in symbols if s.endswith("_202606")}
    assert not expired, f"expired contracts still expected: {expired}"
    assert any(s.endswith("_202609") for s in symbols), "live contracts were dropped"


def test_overlapping_presets_yield_each_symbol_once():
    """sp500 and ndx100 overlap by 87 real symbols. Emitting a series per
    occurrence puts every gap for those 87 into the repair manifest twice —
    two repair instructions against one parquet path."""
    series = build_denominator(
        [PRESETS / "sp500.json", PRESETS / "ndx100.json"],
        asset_class="equity",
        timeframe="1d",
        start=date(2026, 8, 26),
        end=date(2026, 8, 28),
        as_of=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
    )
    symbols = [s.symbol for s in series]
    assert len(symbols) == len(set(symbols)), "duplicate expected series"


def test_a_session_not_yet_closed_is_not_expected():
    """An --end in the future must not manufacture phantom missing sessions."""
    series = build_denominator(
        [PRESETS / "volatility.json"],
        asset_class="volatility",
        timeframe="1d",
        start=date(2026, 8, 26),
        end=date(2026, 9, 30),
        as_of=datetime(2026, 8, 28, 10, 0, tzinfo=UTC),
    )
    assert max(s.sessions[-1] for s in series) < date(2026, 8, 28)


def test_an_archived_symbol_a_preset_still_claims_stays_expected():
    """The delisted archive is NOT authoritative and must never filter the
    denominator.

    Measured on the production warehouse 2026-09-01: bronze-delisted holds 8,620
    symbols, and 234 of them are still claimed by a preset -- BK (a current S&P
    500 member, in sp500.json and two sector presets), 63 ADRs including ORAN,
    TEF, ERJ and ABB, and 157 ETFs. BK has no 1d.parquet in live bronze, so it
    is a real G3 hole *today*; an archive-driven filter would have hidden it
    permanently, which is exactly the invisible-gap failure this denominator
    exists to remove.

    This is a guard, not a feature test: it passes today because
    build_denominator has no delisted branch, and it fails the moment someone
    adds one that subtracts.

    It asserts the invariant rather than one symbol. It used to pin BK, and the
    2026-09-02 index refresh removed BK from sp500.json -- it was renamed to BNY,
    not delisted -- so the example evaporated while the rule it stood for did
    not. Every ticker the preset claims must reach the denominator; naming one
    of them makes the guard expire the next time the universe moves.
    """
    claimed = set(json.loads((PRESETS / "sp500.json").read_text())["tickers"])
    series = build_denominator(
        [PRESETS / "sp500.json"],
        asset_class="equity",
        timeframe="1d",
        start=date(2026, 8, 26),
        end=date(2026, 8, 28),
        as_of=datetime(2026, 8, 31, 10, 0, tzinfo=UTC),
    )
    assert claimed and claimed <= {s.symbol for s in series}


def test_session_is_due_after_the_delivery_allowance_the_following_day():
    # The denominator allows four hours after the 06:00 UTC filling job starts.
    assert session_due_at(date(2026, 8, 31)) == datetime(2026, 9, 1, 15, 0, tzinfo=UTC)


def test_rates_is_due_a_day_later_than_equity():
    # Spec section 8.1: FRED publishes a session behind, so the rates row is T+2.
    # A uniform T+1 expects DGS10 for session S at 15:00 UTC on S+1, when the
    # series legitimately does not exist yet -- one phantom gap per series, daily.
    session = date(2026, 8, 28)
    assert session_due_at(session, lag_days=2) - session_due_at(session) == timedelta(days=1)


def test_a_closed_but_not_yet_due_session_is_not_expected(tmp_path):
    # The 2026-09-01 04:21 UTC production run: session 2026-08-31 had closed and
    # its job had not started. 497 of 501 findings were this.
    preset = tmp_path / "p.json"
    preset.write_text('{"name": "p", "tickers": ["AAPL"]}')
    series = build_denominator(
        [preset],
        "equity",
        "1d",
        date(2026, 8, 27),
        date(2026, 8, 31),
        as_of=datetime(2026, 9, 1, 4, 21, tzinfo=UTC),
    )
    assert date(2026, 8, 31) not in series[0].sessions
    assert date(2026, 8, 28) in series[0].sessions


def test_the_same_session_is_expected_once_the_deadline_passes(tmp_path):
    preset = tmp_path / "p.json"
    preset.write_text('{"name": "p", "tickers": ["AAPL"]}')
    series = build_denominator(
        [preset],
        "equity",
        "1d",
        date(2026, 8, 27),
        date(2026, 8, 31),
        as_of=datetime(2026, 9, 1, 15, 0, tzinfo=UTC),
    )
    assert date(2026, 8, 31) in series[0].sessions


def test_a_naive_as_of_is_rejected_rather_than_assumed_utc(tmp_path):
    preset = tmp_path / "p.json"
    preset.write_text('{"name": "p", "tickers": ["AAPL"]}')
    with pytest.raises(ValueError, match="tz-aware"):
        build_denominator(
            [preset],
            "equity",
            "1d",
            date(2026, 8, 27),
            date(2026, 8, 31),
            as_of=datetime(2026, 9, 1, 10, 0),
        )
