"""Tests for the 2021-06-18 single-bar point repair.

Fixtures are REAL ServiceNow (NOW) daily bars around 2021-06-18, frozen 2026-07-18:
bronze holds a corrupt 06-18 (the modern 5:1-split-adjusted value 106.75 in an otherwise
raw series); Yahoo returns the split-adjusted 106.748 plus NOW's real 2025-12-18 5:1 split,
so raw = 106.748 x 5 = 533.74 — continuous with the raw neighbours 530.40 / 534.75.
No network: the Yahoo client is a stub returning frozen bars.
"""

from __future__ import annotations

from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.bronze_client import BronzeClient
from clients.yahoo_client import YahooNotFound, YahooOHLCVBar, YahooSplit
from livewire_scripts import repair_single_bar as R

# Real NOW bronze window (raw), with the corrupt 06-18 as it exists on disk today.
_NOW_BRONZE = [
    ("2021-06-16", 507.70, 521.50, 502.1510, 510.08, 2030210),
    ("2021-06-17", 505.80, 531.00, 505.8000, 530.40, 1876285),
    ("2021-06-18", 105.76, 107.54, 105.0600, 106.75, 4448395),  # corrupt: 5:1-adjusted
    ("2021-06-21", 531.32, 538.00, 525.0200, 534.75, 1107510),
    ("2021-06-22", 536.00, 547.00, 535.0000, 546.03, 1250507),
]
# Yahoo split-adjusted OHLCV for 2021-06-18 (== raw / 5) + NOW's real 5:1 split.
_NOW_YAHOO_0618 = YahooOHLCVBar(date(2021, 6, 18), 105.764, 107.544, 105.056, 106.748, 11231000)
_NOW_SPLIT = YahooSplit(date(2025, 12, 18), 5.0, 1.0)


class _StubYahoo:
    """Returns a caller-supplied (bars, splits) or raises YahooNotFound."""

    def __init__(self, bars: list[YahooOHLCVBar], splits: list[YahooSplit], *, not_found: bool = False):
        self._bars, self._splits, self._not_found = bars, splits, not_found

    def get_daily_ohlcv(self, symbol, start, end):
        if self._not_found:
            raise YahooNotFound(symbol)
        return list(self._bars), list(self._splits)


def _seed(root, rows=_NOW_BRONZE, symbol="NOW"):
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    bronze.replace_ticker_rows(
        symbol,
        [
            {
                "trade_date": td,
                "open": o,
                "high": h,
                "low": lo,
                "close": c,
                "adj_close": c,
                "volume": v,
                "source": "legacy",
                "price_basis": "raw",
            }
            for td, o, h, lo, c, v in rows
        ],
    )
    return bronze


def _read_0618(bronze, symbol="NOW"):
    row = next(r for r in bronze.read_symbol_rows(symbol) if r["trade_date"] == "2021-06-18")
    return row


# ---- validate_bar (the hard gate) ----


def test_validate_accepts_continuous_close():
    cand = {"open": 528.82, "high": 537.72, "low": 525.28, "close": 533.74}
    ok, reason = R.validate_bar(cand, {"close": 530.40}, {"close": 534.75})
    assert ok and reason == "ok"


def test_validate_rejects_close_outside_neighbour_band():
    cand = {"open": 100.0, "high": 108.0, "low": 100.0, "close": 106.75}  # the corrupt value
    ok, reason = R.validate_bar(cand, {"close": 530.40}, {"close": 534.75})
    assert not ok and "outside neighbour band" in reason


def test_validate_rejects_inconsistent_ohlc():
    cand = {"open": 528.0, "high": 500.0, "low": 525.0, "close": 533.0}  # high < close
    ok, reason = R.validate_bar(cand, {"close": 530.40}, {"close": 534.75})
    assert not ok and "OHLC inconsistent" in reason


def test_validate_rejects_nonpositive():
    cand = {"open": 0.0, "high": 1.0, "low": 0.0, "close": 0.0}
    ok, reason = R.validate_bar(cand, {"close": 530.40}, {"close": 534.75})
    assert not ok and "nonpositive" in reason


# ---- recover_raw_bar ----


def test_recover_reconstructs_raw_from_split_multiplier():
    client = _StubYahoo([_NOW_YAHOO_0618], [_NOW_SPLIT])
    got = R.recover_raw_bar(client, "NOW", date(2021, 6, 18), date(2026, 7, 18))
    assert got["close"] == pytest.approx(533.74)
    assert got["adj_close"] == pytest.approx(533.74)  # raw: adj_close == close
    assert got["volume"] == 2246200  # 11231000 / 5
    assert (got["source"], got["price_basis"]) == ("yahoo", "raw")


def test_recover_returns_none_when_yahoo_lacks_the_date():
    client = _StubYahoo([], [_NOW_SPLIT])
    assert R.recover_raw_bar(client, "NOW", date(2021, 6, 18), date(2026, 7, 18)) is None


# ---- run: dry-run / apply / needs-review ----


def test_dry_run_reports_without_writing(tmp_path):
    bronze = _seed(tmp_path)
    rc = R.run(
        ["--tickers", "NOW", "--target-date", "2021-06-18", "--output-dir", str(tmp_path / "out"), "--pause", "0"],
        data_lake_root=tmp_path,
        client_factory=lambda: _StubYahoo([_NOW_YAHOO_0618], [_NOW_SPLIT]),
        as_of_date=date(2026, 7, 18),
    )
    assert rc == 0
    assert _read_0618(bronze)["close"] == pytest.approx(106.75)  # unchanged
    assert not (tmp_path / "out" / "backup").exists()  # dry-run never backs up


def test_apply_overwrites_only_the_bad_bar_and_backs_up(tmp_path):
    bronze = _seed(tmp_path)
    rc = R.run(
        [
            "--tickers",
            "NOW",
            "--target-date",
            "2021-06-18",
            "--output-dir",
            str(tmp_path / "out"),
            "--apply",
            "--pause",
            "0",
        ],
        data_lake_root=tmp_path,
        client_factory=lambda: _StubYahoo([_NOW_YAHOO_0618], [_NOW_SPLIT]),
        as_of_date=date(2026, 7, 18),
    )
    assert rc == 0
    fixed = _read_0618(bronze)
    assert fixed["close"] == pytest.approx(533.74)
    assert fixed["source"] == "yahoo"  # proves "yahoo" is now an accepted EQUITY_SOURCE
    # neighbours untouched
    rows = {r["trade_date"]: r for r in bronze.read_symbol_rows("NOW")}
    assert rows["2021-06-17"]["close"] == pytest.approx(530.40)
    assert rows["2021-06-21"]["close"] == pytest.approx(534.75)
    # backup preserves the pre-repair bytes → rollback is possible
    backup = tmp_path / "out" / "backup" / "NOW.1d.parquet"
    assert backup.exists()
    old = {str(r["trade_date"]): r for r in pq.read_table(backup).to_pylist()}
    assert old["2021-06-18"]["close"] == pytest.approx(106.75)


def test_out_of_band_recovery_is_needs_review_not_written(tmp_path):
    bronze = _seed(tmp_path)
    # Yahoo close 40 x mult 5 = 200 raw — far below the 530/534 neighbours → refuse.
    bad = YahooOHLCVBar(date(2021, 6, 18), 39.0, 41.0, 38.0, 40.0, 11231000)
    rc = R.run(
        [
            "--tickers",
            "NOW",
            "--target-date",
            "2021-06-18",
            "--output-dir",
            str(tmp_path / "out"),
            "--apply",
            "--pause",
            "0",
        ],
        data_lake_root=tmp_path,
        client_factory=lambda: _StubYahoo([bad], [_NOW_SPLIT]),
        as_of_date=date(2026, 7, 18),
    )
    assert rc == 0  # needs-review is not a failure
    assert _read_0618(bronze)["close"] == pytest.approx(106.75)  # not overwritten
    assert not (tmp_path / "out" / "backup" / "NOW.1d.parquet").exists()


def test_yahoo_not_found_is_needs_review(tmp_path):
    bronze = _seed(tmp_path)
    R.run(
        [
            "--tickers",
            "NOW",
            "--target-date",
            "2021-06-18",
            "--output-dir",
            str(tmp_path / "out"),
            "--apply",
            "--pause",
            "0",
        ],
        data_lake_root=tmp_path,
        client_factory=lambda: _StubYahoo([], [], not_found=True),
        as_of_date=date(2026, 7, 18),
    )
    assert _read_0618(bronze)["close"] == pytest.approx(106.75)


def test_symbols_for_date_filters_flags(tmp_path):
    flags = tmp_path / "flags.parquet"
    pq.write_table(
        pa.table({"symbol": ["NOW", "GE", "AAPL"], "trade_date": ["2021-06-18", "2021-06-18", "2023-01-03"]}),
        flags,
    )
    assert R._symbols_for_date(flags, date(2021, 6, 18)) == ["GE", "NOW"]


def test_run_via_audit_flags(tmp_path):
    bronze = _seed(tmp_path)
    flags = tmp_path / "flags.parquet"
    pq.write_table(pa.table({"symbol": ["NOW"], "trade_date": ["2021-06-18"]}), flags)
    rc = R.run(
        [
            "--audit-flags",
            str(flags),
            "--target-date",
            "2021-06-18",
            "--output-dir",
            str(tmp_path / "out"),
            "--apply",
            "--pause",
            "0",
        ],
        data_lake_root=tmp_path,
        client_factory=lambda: _StubYahoo([_NOW_YAHOO_0618], [_NOW_SPLIT]),
        as_of_date=date(2026, 7, 18),
    )
    assert rc == 0 and _read_0618(bronze)["close"] == pytest.approx(533.74)


def test_run_requires_tickers_or_flags(tmp_path):
    with pytest.raises(ValueError, match="--tickers or --audit-flags"):
        R.run(
            ["--target-date", "2021-06-18", "--output-dir", str(tmp_path / "out"), "--pause", "0"],
            data_lake_root=tmp_path,
            client_factory=lambda: _StubYahoo([], []),
            as_of_date=date(2026, 7, 18),
        )


def test_resume_skips_completed_symbol(tmp_path):
    _seed(tmp_path)
    args = [
        "--tickers",
        "NOW",
        "--target-date",
        "2021-06-18",
        "--output-dir",
        str(tmp_path / "out"),
        "--apply",
        "--pause",
        "0",
    ]
    R.run(
        args,
        data_lake_root=tmp_path,
        client_factory=lambda: _StubYahoo([_NOW_YAHOO_0618], [_NOW_SPLIT]),
        as_of_date=date(2026, 7, 18),
    )

    class _Boom:
        def get_daily_ohlcv(self, *a, **k):
            raise AssertionError("resume must not re-fetch a completed symbol")

    rc = R.run([*args, "--resume"], data_lake_root=tmp_path, client_factory=_Boom, as_of_date=date(2026, 7, 18))
    assert rc == 0


def test_unexpected_exception_marks_failed(tmp_path):
    bronze = _seed(tmp_path)

    class _Raiser:
        def get_daily_ohlcv(self, *a, **k):
            raise RuntimeError("boom")

    rc = R.run(
        [
            "--tickers",
            "NOW",
            "--target-date",
            "2021-06-18",
            "--output-dir",
            str(tmp_path / "out"),
            "--apply",
            "--pause",
            "0",
        ],
        data_lake_root=tmp_path,
        client_factory=_Raiser,
        as_of_date=date(2026, 7, 18),
    )
    assert rc == 1  # a failed symbol fails the run
    assert _read_0618(bronze)["close"] == pytest.approx(106.75)  # not written


def test_resume_requires_matching_target(tmp_path):
    _seed(tmp_path)
    args = ["--tickers", "NOW", "--output-dir", str(tmp_path / "out"), "--pause", "0"]
    R.run(
        [*args, "--target-date", "2021-06-18"],
        data_lake_root=tmp_path,
        client_factory=lambda: _StubYahoo([_NOW_YAHOO_0618], [_NOW_SPLIT]),
        as_of_date=date(2026, 7, 18),
    )
    with pytest.raises(ValueError, match="does not match"):
        R.run(
            [*args, "--target-date", "2021-06-15", "--resume"],
            data_lake_root=tmp_path,
            client_factory=lambda: _StubYahoo([_NOW_YAHOO_0618], [_NOW_SPLIT]),
            as_of_date=date(2026, 7, 18),
        )
