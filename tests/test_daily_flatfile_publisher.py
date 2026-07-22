import csv
import gzip
from datetime import date

from clients.bronze_client import BronzeClient
from clients.massive_daily_flatfile_store import MassiveDailyFlatfileStore
from clients.massive_flatfile_state import MassiveFlatfileState
from livewire_scripts.daily_flatfile_publisher import publish_daily_dates

# Midnight UTC of 2024-06-03 and 2024-06-04 in epoch ns.
_TS_20240603 = 1717372800_000_000_000
_TS_20240604 = 1717459200_000_000_000


def _write_day(path, rows):
    with gzip.open(path, "wt", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"],
        )
        writer.writeheader()
        writer.writerows(rows)


def _row(ticker, ts_ns, vol=10):
    return {
        "ticker": ticker,
        "volume": vol,
        "open": 1,
        "close": 2,
        "high": 3,
        "low": 0.5,
        "window_start": ts_ns,
        "transactions": 1,
    }


def test_publish_daily_writes_per_ticker_bronze_1d_for_new_tickers(tmp_path):
    source = tmp_path / "day.csv.gz"
    _write_day(source, [_row("AAPL", _TS_20240603), _row("MSFT", _TS_20240603)])
    day = date(2024, 6, 3)
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=4)
    store.stage_gzip(day, source)
    state = MassiveFlatfileState(tmp_path / "cursors", name="massive_daily_flatfile")

    stats = publish_daily_dates(store, state, [day], tmp_path / "bronze", use_processes=False)
    assert stats == {"tickers": 2, "rows_1d": 2, "skipped_existing": 0}
    bronze = BronzeClient(tmp_path / "bronze", asset_class="equity")
    assert bronze.get_existing_symbols() == {"AAPL", "MSFT"}


def test_publish_daily_skips_protected_preset_tickers(tmp_path):
    """A preset ticker is owned by the `daily` command and left alone.

    Its pre-2003 history is deeper than day_aggs can supply.
    """
    bronze_dir = tmp_path / "bronze"
    pre_existing = BronzeClient(bronze_dir, asset_class="equity")
    pre_existing.replace_ticker_rows(
        "AAPL",
        [
            {
                "trade_date": "1980-12-12",
                "symbol_id": 42,
                "open": 0.1,
                "high": 0.11,
                "low": 0.09,
                "close": 0.105,
                "adj_close": 0.105,
                "volume": 1000,
            }
        ],
    )

    source = tmp_path / "day.csv.gz"
    _write_day(source, [_row("AAPL", _TS_20240603), _row("MSFT", _TS_20240603)])
    day = date(2024, 6, 3)
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=4)
    store.stage_gzip(day, source)
    state = MassiveFlatfileState(tmp_path / "cursors", name="massive_daily_flatfile")

    stats = publish_daily_dates(
        store,
        state,
        [day],
        bronze_dir,
        protected_symbols=frozenset({"AAPL"}),
        use_processes=False,
    )
    assert stats == {"tickers": 1, "rows_1d": 1, "skipped_existing": 1}

    aapl_rows = pre_existing.read_symbol_rows("AAPL")
    assert len(aapl_rows) == 1
    assert aapl_rows[0]["trade_date"] == "1980-12-12"


def test_publish_daily_updates_a_symbol_this_lane_created(tmp_path):
    """The freeze bug: an unprotected symbol must keep getting new bars.

    The old policy skipped every ticker that already had a 1d.parquet, so the
    ~17.5K non-preset SIP symbols were written exactly once — frozen at
    whatever window was in force the night they first appeared — while the run
    kept exiting 0.
    """
    bronze_dir = tmp_path / "bronze"
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=4)
    state = MassiveFlatfileState(tmp_path / "cursors", name="massive_daily_flatfile")

    first = tmp_path / "day1.csv.gz"
    _write_day(first, [_row("ZZZZ", _TS_20240603)])
    day_one = date(2024, 6, 3)
    store.stage_gzip(day_one, first)
    publish_daily_dates(store, state, [day_one], bronze_dir, scope="s1", use_processes=False)

    second = tmp_path / "day2.csv.gz"
    _write_day(second, [_row("ZZZZ", _TS_20240604)])
    day_two = date(2024, 6, 4)
    store.stage_gzip(day_two, second)
    stats = publish_daily_dates(store, state, [day_two], bronze_dir, scope="s2", use_processes=False)

    assert stats["tickers"] == 1
    rows = BronzeClient(bronze_dir, asset_class="equity").read_symbol_rows("ZZZZ")
    # Merged, not replaced — day one survives day two's publish. (Trade dates
    # come from the store's epoch->ET conversion, so assert the shape, not
    # hardcoded calendar days.)
    dates = sorted(r["trade_date"] for r in rows)
    assert len(dates) == 2
    assert dates[0] < dates[1]


def test_publish_daily_defaults_to_protecting_nothing(tmp_path):
    """Default must not be 'every symbol on disk' — that is what froze them."""
    source = tmp_path / "day.csv.gz"
    _write_day(source, [_row("AAPL", _TS_20240603), _row("MSFT", _TS_20240603)])
    day = date(2024, 6, 3)
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=4)
    store.stage_gzip(day, source)
    state = MassiveFlatfileState(tmp_path / "cursors", name="massive_daily_flatfile")

    stats = publish_daily_dates(store, state, [day], tmp_path / "bronze", use_processes=False)
    assert stats == {"tickers": 2, "rows_1d": 2, "skipped_existing": 0}
    bronze = BronzeClient(tmp_path / "bronze", asset_class="equity")
    assert bronze.get_existing_symbols() == {"AAPL", "MSFT"}


def test_publish_daily_resume_via_scope_cursor(tmp_path):
    source_a = tmp_path / "a.csv.gz"
    source_b = tmp_path / "b.csv.gz"
    _write_day(source_a, [_row("AAPL", _TS_20240603)])
    _write_day(source_b, [_row("AAPL", _TS_20240604)])

    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=1)
    store.stage_gzip(date(2024, 6, 3), source_a)
    store.stage_gzip(date(2024, 6, 4), source_b)
    state = MassiveFlatfileState(tmp_path / "cursors", name="massive_daily_flatfile")

    days = [date(2024, 6, 3), date(2024, 6, 4)]
    stats = publish_daily_dates(store, state, days, tmp_path / "bronze", scope="hist", use_processes=False)
    assert stats == {"tickers": 1, "rows_1d": 2, "skipped_existing": 0}

    # Same scope: bucket already marked complete → no work, AAPL stays in existing-set.
    again = publish_daily_dates(store, state, days, tmp_path / "bronze", scope="hist", use_processes=False)
    assert again == {"tickers": 0, "rows_1d": 0, "skipped_existing": 0}


def test_publish_daily_empty_days_is_noop(tmp_path):
    state = MassiveFlatfileState(tmp_path / "cursors", name="massive_daily_flatfile")
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=1)
    assert publish_daily_dates(store, state, [], tmp_path / "bronze") == {
        "tickers": 0,
        "rows_1d": 0,
        "skipped_existing": 0,
    }


def test_publish_daily_threadpool_path(tmp_path):
    """Smoke-test the parallel branch with use_processes=False (threadpool)."""
    source = tmp_path / "day.csv.gz"
    _write_day(source, [_row("AAPL", _TS_20240603), _row("MSFT", _TS_20240603)])
    day = date(2024, 6, 3)
    store = MassiveDailyFlatfileStore(tmp_path, bucket_count=2)
    store.stage_gzip(day, source)
    state = MassiveFlatfileState(tmp_path / "cursors", name="massive_daily_flatfile")

    stats = publish_daily_dates(store, state, [day], tmp_path / "bronze", workers=2, use_processes=False)
    assert stats["tickers"] == 2
    assert stats["skipped_existing"] == 0
