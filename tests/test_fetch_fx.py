"""Unit tests for the DXY / FX ingest lane.

Prices are REAL, frozen 2026-07-27: DX-Y.NYB daily OHLCV for 2025-07-09..11 and
Massive C:EURUSD hourly bars for 2026-07-22. No network — providers are stubbed.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pyarrow.parquet as pq
import pytest

from clients.massive_client import MassiveAuthError, MassiveIntradayBar
from clients.yahoo_client import YahooNotFound, YahooOHLCV
from livewire_scripts import fetch_fx

# Real DX-Y.NYB daily OHLCV (an index: Yahoo really does report volume 0).
_DXY_BARS = [
    YahooOHLCV(
        datetime(2025, 7, 9, 13, 30, tzinfo=UTC), 97.55000305175781, 97.75, 97.45999908447266, 97.47000122070312, 0
    ),
    YahooOHLCV(
        datetime(2025, 7, 10, 13, 30, tzinfo=UTC),
        97.44000244140625,
        97.91999816894531,
        97.2699966430664,
        97.6500015258789,
        0,
    ),
    YahooOHLCV(
        datetime(2025, 7, 11, 13, 30, tzinfo=UTC),
        97.56999969482422,
        97.95999908447266,
        97.55999755859375,
        97.8499984741211,
        0,
    ),
]

# Real Massive C:EURUSD hourly bars.
_EURUSD_BARS = [
    MassiveIntradayBar(datetime(2026, 7, 22, 0, 0, tzinfo=UTC), 1.14019, 1.1408, 1.14, 1.14035, 4655, "EURUSD"),
    MassiveIntradayBar(datetime(2026, 7, 22, 1, 0, tzinfo=UTC), 1.1403, 1.1407, 1.1398, 1.1402, 4047, "EURUSD"),
]


class _StubYahoo:
    def __init__(self, *, daily=None, intraday=None, missing=()):
        self._daily = _DXY_BARS if daily is None else daily
        self._intraday = _DXY_BARS if intraday is None else intraday
        self._missing = set(missing)
        self.daily_calls: list[tuple[str, date | None]] = []
        self.intraday_calls: list[tuple[str, str]] = []

    def get_daily_ohlcv(self, symbol, start=None, end=None):
        if symbol in self._missing:
            raise YahooNotFound(symbol)
        self.daily_calls.append((symbol, start))
        return self._daily

    def get_intraday(self, symbol, interval):
        if symbol in self._missing:
            raise YahooNotFound(symbol)
        self.intraday_calls.append((symbol, interval))
        return self._intraday


class _StubMassive:
    """Records every chunk asked for, and 403s anything starting before ``floor``."""

    def __init__(self, floor: date | None = None, bars=None):
        self._floor = floor
        self._bars = _EURUSD_BARS if bars is None else bars
        self.calls: list[tuple[str, str, date, date]] = []

    def get_fx_intraday_bars(self, pair, timeframe, start, end):
        self.calls.append((pair, timeframe, start, end))
        if self._floor is not None and start < self._floor:
            raise MassiveAuthError("not entitled", status_code=403)
        return self._bars


# ---------------------------------------------------------------------------
# symbol mapping
# ---------------------------------------------------------------------------


def test_dxy_maps_to_its_ice_symbol_and_pairs_take_the_uniform_suffix():
    assert fetch_fx.yahoo_symbol("DXY") == "DX-Y.NYB"
    assert fetch_fx.yahoo_symbol("EURUSD") == "EURUSD=X"
    # USD-base pairs need no special case: USDJPY=X and JPY=X return the same series.
    assert fetch_fx.yahoo_symbol("USDJPY") == "USDJPY=X"
    assert fetch_fx.yahoo_symbol("USDKRW") == "USDKRW=X"


# ---------------------------------------------------------------------------
# row shaping
# ---------------------------------------------------------------------------


def test_daily_rows_carry_adj_close_equal_to_close():
    """FX and index quotes have no splits or dividends, so there is nothing to adjust."""
    rows = fetch_fx.daily_rows(_DXY_BARS, symbol_id=123)

    assert [row["trade_date"] for row in rows] == [date(2025, 7, 9), date(2025, 7, 10), date(2025, 7, 11)]
    assert all(row["adj_close"] == row["close"] for row in rows)
    assert rows[0]["close"] == pytest.approx(97.47000122070312)
    assert rows[0]["symbol_id"] == 123


def test_intraday_rows_accept_either_providers_bar_shape():
    """Yahoo names the field `timestamp`; Massive names it `bar_timestamp`."""
    from_yahoo = fetch_fx.intraday_rows(_DXY_BARS, symbol_id=7)
    from_massive = fetch_fx.intraday_rows(_EURUSD_BARS, symbol_id=9)

    assert from_yahoo[0]["bar_timestamp"] == datetime(2025, 7, 9, 13, 30, tzinfo=UTC)
    assert from_massive[0]["bar_timestamp"] == datetime(2026, 7, 22, 0, 0, tzinfo=UTC)
    assert from_massive[0]["close"] == pytest.approx(1.14035)


# ---------------------------------------------------------------------------
# chunked fetch against the rolling entitlement floor
# ---------------------------------------------------------------------------


def test_chunks_below_the_floor_are_skipped_so_the_seed_still_reaches_max_depth():
    """A 403 below the floor must not abort the walk — later chunks still have data."""
    massive = _StubMassive(floor=date(2026, 6, 1))
    bars = fetch_fx.fetch_massive_intraday(massive, "EURUSD", "1m", date(2026, 4, 1), date(2026, 7, 27))

    starts = [call[2] for call in massive.calls]
    assert starts[0] == date(2026, 4, 1)
    assert len(starts) > 1
    # Everything at or above the floor contributed; nothing raised.
    above = [call for call in massive.calls if call[2] >= date(2026, 6, 1)]
    assert bars == _EURUSD_BARS * len(above)


def test_chunk_span_is_sized_per_timeframe_to_limit_request_count():
    """Requests cost 12s each at the measured 5/min limit, so coarser bars chunk wider."""
    span_start, span_end = date(2025, 1, 1), date(2026, 1, 1)
    counts = {}
    for timeframe in ("1m", "5m", "30m"):
        massive = _StubMassive()
        fetch_fx.fetch_massive_intraday(massive, "EURUSD", timeframe, span_start, span_end)
        counts[timeframe] = len(massive.calls)

    assert counts["1m"] > counts["5m"] > counts["30m"]


def test_chunks_never_extend_past_the_requested_end():
    massive = _StubMassive()
    fetch_fx.fetch_massive_intraday(massive, "EURUSD", "1m", date(2026, 7, 1), date(2026, 7, 27))

    assert all(call[3] <= date(2026, 7, 27) for call in massive.calls)


# ---------------------------------------------------------------------------
# source routing
# ---------------------------------------------------------------------------


def test_pairs_take_yahoo_at_1h_because_it_reaches_past_the_massive_floor(tmp_path):
    yahoo, massive = _StubYahoo(), _StubMassive()
    fetch_fx.sync_intraday(["EURUSD"], ["1h"], tmp_path, yahoo, massive, days=7)

    assert yahoo.intraday_calls == [("EURUSD=X", "1h")]
    assert massive.calls == []


def test_pairs_take_massive_below_1h_where_its_window_is_deeper(tmp_path):
    yahoo, massive = _StubYahoo(), _StubMassive()
    fetch_fx.sync_intraday(["EURUSD"], ["5m"], tmp_path, yahoo, massive, days=7)

    assert yahoo.intraday_calls == []
    assert [call[:2] for call in massive.calls] == [("EURUSD", "5m")]


def test_dxy_always_takes_yahoo_because_massive_does_not_carry_it(tmp_path):
    yahoo, massive = _StubYahoo(), _StubMassive()
    fetch_fx.sync_intraday(["DXY"], ["1m", "5m", "1h"], tmp_path, yahoo, massive, days=7)

    assert [call[1] for call in yahoo.intraday_calls] == ["1m", "5m", "1h"]
    assert all(call[0] == "DX-Y.NYB" for call in yahoo.intraday_calls)
    assert massive.calls == []


# ---------------------------------------------------------------------------
# publication and accumulation
# ---------------------------------------------------------------------------


def test_sync_daily_publishes_bronze_parquet(tmp_path):
    assert fetch_fx.sync_daily(["DXY"], tmp_path, _StubYahoo(), days=None) == 0

    table = pq.read_table(tmp_path / "symbol=DXY" / "1d.parquet")
    assert table.num_rows == 3
    assert table.column("close")[0].as_py() == pytest.approx(97.47000122070312)


def test_sync_daily_without_days_requests_full_history(tmp_path):
    yahoo = _StubYahoo()
    fetch_fx.sync_daily(["DXY"], tmp_path, yahoo, days=None)

    assert yahoo.daily_calls == [("DX-Y.NYB", None)]


def test_a_symbol_yahoo_does_not_carry_is_reported_not_fatal(tmp_path):
    yahoo = _StubYahoo(missing={"DX-Y.NYB"})
    failures = fetch_fx.sync_daily(["DXY", "EURUSD"], tmp_path, yahoo, days=None)

    assert failures == 1
    # The healthy symbol still published.
    assert (tmp_path / "symbol=EURUSD" / "1d.parquet").exists()


def test_repeated_runs_accumulate_rather_than_replace(tmp_path):
    """Both providers serve rolling windows, so held history only grows by merging."""
    early = _EURUSD_BARS
    late = [
        MassiveIntradayBar(datetime(2026, 7, 22, 2, 0, tzinfo=UTC), 1.14033, 1.14101, 1.1399, 1.14053, 6563, "EURUSD"),
    ]

    fetch_fx.sync_intraday(["EURUSD"], ["5m"], tmp_path, _StubYahoo(), _StubMassive(bars=early), days=7)
    first = pq.read_table(tmp_path / "symbol=EURUSD" / "5m.parquet").num_rows

    fetch_fx.sync_intraday(["EURUSD"], ["5m"], tmp_path, _StubYahoo(), _StubMassive(bars=late), days=7)
    table = pq.read_table(tmp_path / "symbol=EURUSD" / "5m.parquet")

    assert first == 2
    # The window that rolled out of the provider's range is still held locally.
    assert table.num_rows == 3
    stamps = sorted(value.as_py() for value in table.column("bar_timestamp"))
    assert stamps[0] == datetime(2026, 7, 22, 0, 0, tzinfo=UTC)
    assert stamps[-1] == datetime(2026, 7, 22, 2, 0, tzinfo=UTC)


def test_preset_loads_the_declared_universe():
    tickers = fetch_fx.load_preset_tickers(fetch_fx.DEFAULT_PRESET)

    assert "DXY" in tickers
    assert "EURUSD" in tickers and "USDJPY" in tickers  # G10
    assert "USDKRW" in tickers and "USDBRL" in tickers  # NDF
    # The IB-era inverted spelling is gone.
    assert "USDEUR" not in tickers


def test_preset_without_tickers_is_rejected(tmp_path):
    empty = tmp_path / "empty.json"
    empty.write_text('{"name": "x", "tickers": []}')

    with pytest.raises(ValueError, match="no tickers"):
        fetch_fx.load_preset_tickers(empty)


# ---------------------------------------------------------------------------
# empty / degraded provider responses
# ---------------------------------------------------------------------------


def test_a_symbol_with_no_daily_bars_is_reported_not_published(tmp_path):
    failures = fetch_fx.sync_daily(["DXY"], tmp_path, _StubYahoo(daily=[]), days=None)

    assert failures == 0  # an empty window is not a failure
    assert not (tmp_path / "symbol=DXY" / "1d.parquet").exists()


def test_a_symbol_with_no_intraday_bars_is_reported_not_published(tmp_path):
    fetch_fx.sync_intraday(["EURUSD"], ["5m"], tmp_path, _StubYahoo(), _StubMassive(bars=[]), days=7)

    assert not (tmp_path / "symbol=EURUSD" / "5m.parquet").exists()


def test_pairs_are_skipped_when_massive_is_unavailable(tmp_path):
    """Yahoo-owned work must still publish when the Massive credential is missing."""
    fetch_fx.sync_intraday(["DXY", "EURUSD"], ["5m"], tmp_path, _StubYahoo(), None, days=7)

    assert (tmp_path / "symbol=DXY" / "5m.parquet").exists()
    assert not (tmp_path / "symbol=EURUSD" / "5m.parquet").exists()


def test_intraday_symbol_missing_from_yahoo_is_counted(tmp_path):
    yahoo = _StubYahoo(missing={"DX-Y.NYB"})
    failures = fetch_fx.sync_intraday(["DXY"], ["1h"], tmp_path, yahoo, None, days=7)

    assert failures == 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_parse_args_defaults_to_the_preset_and_every_timeframe():
    args = fetch_fx.parse_args([])

    assert args.preset == fetch_fx.DEFAULT_PRESET
    assert args.timeframes == list(fetch_fx.ALL_TIMEFRAMES)
    assert args.days is None  # omitted means seed maximum depth
    assert args.tickers is None


def test_explicit_tickers_override_the_preset(tmp_path, monkeypatch):
    seen: dict = {}

    def _sync_daily(symbols, bronze_dir, yahoo, *, days):
        seen["symbols"] = list(symbols)
        seen["days"] = days
        return 0

    monkeypatch.setattr(fetch_fx, "sync_daily", _sync_daily)
    rc = fetch_fx.run(["--tickers", "eurusd", "dxy", "--timeframes", "1d", "--warehouse", str(tmp_path)])

    assert rc == 0
    assert seen["symbols"] == ["EURUSD", "DXY"]  # upper-cased
    assert seen["days"] is None


def test_run_reports_failure_with_a_nonzero_exit_code(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch_fx, "sync_daily", lambda *a, **k: 2)
    rc = fetch_fx.run(["--tickers", "DXY", "--timeframes", "1d", "--warehouse", str(tmp_path)])

    assert rc == 1


def test_run_builds_a_throttled_massive_client_only_when_pairs_need_it(tmp_path, monkeypatch):
    built: list[float] = []

    class _Client:
        def __init__(self, *, min_interval_seconds):
            built.append(min_interval_seconds)

    monkeypatch.setattr(fetch_fx, "MassiveClient", _Client)
    monkeypatch.setattr(fetch_fx, "sync_intraday", lambda *a, **k: 0)

    # 1h is Yahoo-owned, so no Massive client is needed at all.
    fetch_fx.run(["--tickers", "EURUSD", "--timeframes", "1h", "--warehouse", str(tmp_path)])
    assert built == []

    # 5m is Massive-owned, and must be paced against the measured 5/min limit.
    fetch_fx.run(["--tickers", "EURUSD", "--timeframes", "5m", "--warehouse", str(tmp_path)])
    assert built == [fetch_fx.MASSIVE_MIN_INTERVAL_SECONDS]


def test_run_survives_a_missing_massive_credential(tmp_path, monkeypatch):
    def _raise(**kwargs):
        raise MassiveAuthError("MASSIVE_API_KEY is not set")

    monkeypatch.setattr(fetch_fx, "MassiveClient", _raise)
    monkeypatch.setattr(fetch_fx, "sync_intraday", lambda *a, **k: 0)
    rc = fetch_fx.run(["--tickers", "EURUSD", "--timeframes", "5m", "--warehouse", str(tmp_path)])

    assert rc == 1  # reported, not crashed


def test_dxy_only_run_needs_no_massive_client(tmp_path, monkeypatch):
    def _raise(**kwargs):  # pragma: no cover - must never be reached
        raise AssertionError("DXY is Yahoo-only; no Massive client should be built")

    monkeypatch.setattr(fetch_fx, "MassiveClient", _raise)
    monkeypatch.setattr(fetch_fx, "sync_intraday", lambda *a, **k: 0)

    assert fetch_fx.run(["--tickers", "DXY", "--timeframes", "1m", "--warehouse", str(tmp_path)]) == 0


def test_main_delegates_to_run(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch_fx, "sync_daily", lambda *a, **k: 0)
    assert fetch_fx.main(["--tickers", "DXY", "--timeframes", "1d", "--warehouse", str(tmp_path)]) == 0
