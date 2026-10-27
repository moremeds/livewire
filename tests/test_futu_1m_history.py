from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients import ledger
from clients.futu_client import FutuUnavailable
from clients.intraday_bronze_client import IntradayBronzeClient
from clients.timeframe_aggregator import aggregate_bars
from clients.trading_calendar import trading_dates_in_range
from livewire_scripts import futu_1m_history as h

FLOOR = date(2021, 6, 3)  # the Massive file's first session
SYMBOL_ID = 4242


def _bars(days, price, *, minutes=(0, 1, 2)):
    """Regular-session bars starting 09:30 New York (13:30Z in June, 14:30Z in winter)."""
    rows = []
    for day in days:
        open_utc = datetime(day.year, day.month, day.day, 9, 30, tzinfo=h.NEW_YORK).astimezone(UTC)
        for m in minutes:
            p = price(day) if callable(price) else price
            rows.append(
                {
                    "bar_timestamp": open_utc + timedelta(minutes=m),
                    "open": p,
                    "high": p,
                    "low": p,
                    "close": p,
                    "volume": 100,
                }
            )
    return rows


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.setattr(h, "warehouse_dir", lambda: tmp_path / "wh")
    monkeypatch.setattr(h, "data_lake_dir", lambda: tmp_path / "wh" / "data-lake")
    monkeypatch.setattr(h, "massive_ticker_events", lambda symbol: [(date(2003, 9, 10), symbol)])
    _raw_partition(tmp_path / "wh", FLOOR)
    bronze = tmp_path / "wh" / "data-lake" / "bronze" / "asset_class=equity"
    massive = _bars(trading_dates_in_range(FLOOR, date(2021, 8, 31)), 50.0)
    IntradayBronzeClient(bronze_dir=bronze, timeframe="1m").replace_ticker_rows(
        "MSTR", [r | {"symbol_id": SYMBOL_ID} for r in massive]
    )
    return bronze


def _raw_partition(warehouse, day, *, complete=True):
    folder = warehouse / "data-lake" / "raw" / "massive" / "us_stocks_sip" / "minute_aggs_v1" / f"date={day}"
    folder.mkdir(parents=True)
    if complete:
        (folder / "_SUCCESS").touch()


class FakeSession:
    def __init__(self, rows=None, *, unavailable=False):
        pre = _bars(trading_dates_in_range(date(2021, 4, 1), FLOOR - timedelta(days=1)), 40.0)
        post = _bars(trading_dates_in_range(FLOOR, date(2021, 8, 31)), 50.0)
        self.rows = rows if rows is not None else pre + post
        self.unavailable = unavailable
        self.requests = []

    def __call__(self, host, port):
        return self

    def __enter__(self):
        if self.unavailable:
            raise FutuUnavailable("OpenD unreachable")
        return self

    def __exit__(self, *exc):
        return None

    def require_logged_in(self):
        return None

    def history_1m(self, code, start, end):
        self.requests.append((code, start, end))
        return [r for r in self.rows if r["bar_timestamp"].date() <= end]


def _read(bronze, tf="1m"):
    return pq.read_table(bronze / "symbol=MSTR" / f"{tf}.parquet").to_pylist()


def _measured(name):
    return {
        r["scope"]: r["value"] for r in ledger.query(f"select scope, value from measurements where name = '{name}'")
    }


def test_apply_prepends_only_the_sessions_before_the_floor_and_keeps_every_massive_bar(lake):
    before = _read(lake)
    assert h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession()) == 0
    after = _read(lake)
    floor_start = datetime(2021, 6, 3, 13, 30, tzinfo=UTC)
    assert [r for r in after if r["bar_timestamp"] >= floor_start] == before
    added = [r for r in after if r["bar_timestamp"] < floor_start]
    assert {r["close"] for r in added} == {40.0}
    assert {r["symbol_id"] for r in added} == {SYMBOL_ID}
    assert len(added) == 3 * len(trading_dates_in_range(date(2021, 4, 1), date(2021, 6, 2)))
    assert _measured("futu_1m_prepend_bars") == {"MSTR": len(added)}
    assert _measured("futu_1m_pre_floor_return_pairs") == {"MSTR": 0.0}  # no 1d: the gate is skipped, visibly
    for tf in ("5m", "30m", "1h"):
        assert min(r["bar_timestamp"] for r in _read(lake, tf)).date() == date(2021, 4, 1)


def test_a_derived_bar_already_in_the_file_is_never_overwritten(lake):
    """Only the derived files can already hold a pre-floor timestamp; the existing bar wins."""
    sentinel = _bars([date(2021, 5, 3)], 777.0, minutes=(0,))[0] | {"symbol_id": SYMBOL_ID}
    massive_5m = aggregate_bars(_read(lake), source_tf="1m", target_tf="5m")
    IntradayBronzeClient(bronze_dir=lake, timeframe="5m").replace_ticker_rows("MSTR", [sentinel, *massive_5m])
    h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession())
    after = {r["bar_timestamp"]: r for r in _read(lake, "5m")}
    assert after[sentinel["bar_timestamp"]]["close"] == 777.0
    assert [after[r["bar_timestamp"]]["close"] for r in massive_5m] == [r["close"] for r in massive_5m]


def test_a_renamed_ticker_gets_only_the_days_since_its_rename(lake, monkeypatch):
    """BTBT was DNJR until 2020-10-15: Futu, keyed by security, serves both."""
    monkeypatch.setattr(h, "massive_ticker_events", lambda s: [(date(2018, 3, 1), "OLD"), (date(2021, 5, 3), s)])
    h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession())
    assert min(r["bar_timestamp"] for r in _read(lake)).date() == date(2021, 5, 3)
    assert min(r["bar_timestamp"] for r in _read(lake, "1h")).date() == date(2021, 5, 3)


def test_a_ticker_massive_has_no_history_for_is_refused(lake, monkeypatch):
    monkeypatch.setattr(h, "massive_ticker_events", lambda s: [(date(2020, 1, 2), "OTHER")])
    before = _read(lake)
    h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession())
    assert _read(lake) == before
    assert _measured("futu_1m_prepend_refused") == {"MSTR:no_massive_ticker_history": 1.0}


@pytest.mark.parametrize(
    ("ticker", "events", "since"),
    [
        ("MSTR", [], None),
        ("MSTR", [(date(2003, 9, 10), "MSTR")], date(2003, 9, 10)),
        ("DJT", [(date(2024, 3, 26), "DJT"), (date(2021, 9, 30), "DWAC")], date(2024, 3, 26)),
        ("X", [(date(2010, 1, 4), "X"), (date(2012, 1, 3), "Y"), (date(2014, 1, 2), "X")], date(2014, 1, 2)),
    ],
)
def test_ticker_since_is_the_start_of_the_current_run(ticker, events, since):
    assert h.ticker_since(events, ticker) == since


def test_a_lowercase_ticker_extends_the_canonical_file_and_asks_futu_for_it(lake):
    session = FakeSession()
    h.main(["--tickers", "mstr", "--apply"], session_factory=session)
    assert [code for code, *_ in session.requests] == ["US.MSTR"]
    assert sorted(p.name for p in lake.glob("symbol=*")) == ["symbol=MSTR"]
    assert min(r["bar_timestamp"] for r in _read(lake)).date() == date(2021, 4, 1)


def test_a_run_stopped_before_1m_is_completed_by_its_retry(lake, monkeypatch):
    real = IntradayBronzeClient.merge_ticker_rows

    def fail_1m(self, symbol, rows, **kw):
        if self._filename == "1m.parquet":
            raise OSError("disk")
        return real(self, symbol, rows, **kw)

    monkeypatch.setattr(IntradayBronzeClient, "merge_ticker_rows", fail_1m)
    assert h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession()) == 1
    monkeypatch.setattr(IntradayBronzeClient, "merge_ticker_rows", real)
    assert h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession()) == 0
    for tf in ("1m", "5m", "30m", "1h"):
        assert min(r["bar_timestamp"] for r in _read(lake, tf)).date() == date(2021, 4, 1)


def test_a_dry_run_writes_nothing_but_still_grades_the_overlap(lake):
    before = _read(lake)
    assert h.main(["--tickers", "MSTR"], session_factory=FakeSession()) == 0
    assert _read(lake) == before
    assert _measured("futu_1m_overlap_close_match") == {"MSTR": 1.0}
    assert "futu_1m_prepend_bars" not in {r["name"] for r in ledger.query("select name from measurements")}


def test_futu_disagreeing_with_massive_at_the_seam_is_refused(lake):
    """A basis or identity mismatch: Futu's floor-day closes are 10x Massive's."""
    pre = _bars(trading_dates_in_range(date(2021, 4, 1), date(2021, 6, 2)), 400.0)
    post = _bars(trading_dates_in_range(FLOOR, date(2021, 8, 31)), 500.0)
    before = _read(lake)
    h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession(pre + post))
    assert _read(lake) == before
    assert _measured("futu_1m_prepend_refused") == {"MSTR:overlap_close_mismatch": 1.0}


def test_a_short_overlap_is_refused(lake):
    rows = _bars(trading_dates_in_range(date(2021, 4, 1), date(2021, 6, 10)), lambda d: 40.0 if d < FLOOR else 50.0)
    h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession(rows))
    assert _measured("futu_1m_prepend_refused") == {"MSTR:overlap_too_short": 1.0}


def test_pre_floor_closes_that_do_not_move_with_bronze_1d_are_refused(lake):
    days = trading_dates_in_range(date(2021, 4, 1), date(2021, 6, 2))
    pq.write_table(
        pa.Table.from_pylist(
            [{"trade_date": d, "close": 10.0 + (i % 2)} for i, d in enumerate(days)]  # 1d zig-zags, Futu is flat
        ),
        lake / "symbol=MSTR" / "1d.parquet",
    )
    h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession())
    assert _measured("futu_1m_prepend_refused") == {"MSTR:pre_floor_daily_return_mismatch": 1.0}


def test_pre_floor_closes_at_another_basis_but_the_same_returns_are_accepted(lake):
    """Bronze 1d before the seed boundary can sit at a split-adjusted level (MSTR /10)."""
    days = trading_dates_in_range(date(2021, 4, 1), date(2021, 6, 2))
    pq.write_table(
        pa.Table.from_pylist([{"trade_date": d, "close": 4.0} for d in days]),
        lake / "symbol=MSTR" / "1d.parquet",
    )
    h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession())
    assert _measured("futu_1m_pre_floor_return_match") == {"MSTR": 1.0}
    assert "futu_1m_prepend_refused" not in {r["name"] for r in ledger.query("select name from measurements")}


def test_a_symbol_also_in_bronze_delisted_is_refused(lake):
    (lake.parent.parent / "bronze-delisted" / "asset_class=equity" / "symbol=MSTR").mkdir(parents=True)
    session = FakeSession()
    h.main(["--tickers", "MSTR", "--apply"], session_factory=session)
    assert session.requests == []
    assert _measured("futu_1m_prepend_refused") == {"MSTR:history_split_with_bronze_delisted": 1.0}


def test_a_symbol_with_no_massive_file_or_no_futu_rows_is_refused_and_the_next_still_runs(lake):
    h.main(["--tickers", "NOPE", "MSTR"], session_factory=FakeSession())
    h.main(["--tickers", "MSTR"], session_factory=FakeSession(rows=[]))
    assert _measured("futu_1m_prepend_refused") == {"NOPE:no_massive_1m": 1.0, "MSTR:futu_returned_nothing": 1.0}


def test_nothing_before_the_floor_is_not_a_refusal(lake):
    rows = _bars(trading_dates_in_range(FLOOR, date(2021, 8, 31)), 50.0)
    assert h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession(rows)) == 0
    assert _measured("futu_1m_prepend_sessions") == {}
    assert _measured("futu_1m_prepend_bars") == {"MSTR": 0.0}


def test_a_failing_symbol_fails_the_run_by_name_and_the_others_still_run(lake):
    class Broken(FakeSession):
        def history_1m(self, code, start, end):
            if code == "US.BAD":
                raise RuntimeError("boom")
            return super().history_1m(code, start, end)

    IntradayBronzeClient(bronze_dir=lake, timeframe="1m").replace_ticker_rows("BAD", _bars([FLOOR], 1.0))
    assert h.main(["--tickers", "BAD", "MSTR"], session_factory=Broken()) == 1
    assert _measured("futu_1m_prepend_failed") == {"BAD:RuntimeError": 1.0}
    assert "MSTR" in _measured("futu_1m_overlap_close_match")
    assert ledger.query("select verdict from runs where ended is not null") == [{"verdict": "FAILED"}]


def test_no_opend_session_is_a_skipped_run(lake):
    assert h.main(["--tickers", "MSTR"], session_factory=FakeSession(unavailable=True)) == h.EXIT_UNAVAILABLE
    assert ledger.query("select verdict, exit_code from runs where ended is not null") == [
        {"verdict": "DEGRADED", "exit_code": 86}
    ]


def test_an_interrupt_closes_the_run_as_failed(lake):
    class Interrupted(FakeSession):
        def history_1m(self, code, start, end):
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        h.main(["--tickers", "MSTR"], session_factory=Interrupted())
    assert ledger.query("select verdict from runs where ended is not null") == [{"verdict": "FAILED"}]


def test_the_preset_supplies_the_tickers(lake, tmp_path):
    preset = tmp_path / "p.json"
    preset.write_text('{"name": "p", "tickers": ["MSTR"]}')
    session = FakeSession()
    h.main(["--preset", str(preset)], session_factory=session)
    assert [code for code, *_ in session.requests] == ["US.MSTR"]


def test_a_file_that_starts_after_the_raw_floor_is_the_tickers_own_start_and_is_refused(lake, tmp_path):
    """DJT's file starts at its 2024 rename; Futu would hand it DWAC's SPAC history."""
    for day in (date(2021, 6, 1), date(2021, 6, 2)):
        _raw_partition(tmp_path / "wh", day, complete=False)  # incomplete partitions do not move the floor
    (
        tmp_path / "wh" / "data-lake" / "raw" / "massive" / "us_stocks_sip" / "minute_aggs_v1" / "._date=2021-05-01"
    ).touch()
    later = _bars(trading_dates_in_range(date(2021, 6, 4), date(2021, 8, 31)), 50.0)
    IntradayBronzeClient(bronze_dir=lake, timeframe="1m").replace_ticker_rows("DJT", later)
    session = FakeSession()
    h.main(["--tickers", "DJT", "MSTR"], session_factory=session)
    assert [code for code, *_ in session.requests] == ["US.MSTR"]
    assert _measured("futu_1m_prepend_refused") == {"DJT:massive_history_starts_after_the_raw_floor": 1.0}


def test_no_raw_partition_means_no_known_floor_and_every_symbol_is_refused(lake, tmp_path):
    (
        tmp_path
        / "wh"
        / "data-lake"
        / "raw"
        / "massive"
        / "us_stocks_sip"
        / "minute_aggs_v1"
        / f"date={FLOOR}"
        / "_SUCCESS"
    ).unlink()
    h.main(["--tickers", "MSTR", "--apply"], session_factory=FakeSession())
    assert _measured("futu_1m_prepend_refused") == {"MSTR:massive_history_starts_after_the_raw_floor": 1.0}
