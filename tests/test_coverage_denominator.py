from datetime import date
from pathlib import Path

from clients.coverage_denominator import build_denominator

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
        as_of=date(2026, 8, 31),
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
        as_of=date(2026, 8, 31),
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
        as_of=date(2026, 8, 31),
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
        as_of=date(2026, 8, 28),
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
    """
    series = build_denominator(
        [PRESETS / "sp500.json"],
        asset_class="equity",
        timeframe="1d",
        start=date(2026, 8, 26),
        end=date(2026, 8, 28),
        as_of=date(2026, 8, 31),
    )
    assert "BK" in {s.symbol for s in series}
