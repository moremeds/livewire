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
