import csv
import gzip
from datetime import date

from clients.intraday_bronze_client import IntradayBronzeClient
from clients.massive_flatfile_state import MassiveFlatfileState
from clients.massive_flatfile_store import MassiveFlatfileStore
from livewire_scripts.flatfile_publisher import publish_dates


def test_publish_dates_writes_every_timeframe(tmp_path):
    source = tmp_path / "day.csv.gz"
    with gzip.open(source, "wt", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"]
        )
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "AAPL",
                "volume": 10,
                "open": 1,
                "close": 2,
                "high": 3,
                "low": 0.5,
                "window_start": 1717421400000000000,
                "transactions": 1,
            }
        )
    day = date(2024, 6, 3)
    store = MassiveFlatfileStore(tmp_path, bucket_count=4)
    store.stage_gzip(day, source)
    stats = publish_dates(store, MassiveFlatfileState(tmp_path / "cursors"), [day], tmp_path / "bronze")
    assert stats["tickers"] == 1
    for tf in ("1m", "5m", "30m", "1h"):
        assert IntradayBronzeClient(tmp_path / "bronze", tf).get_existing_symbols() == {"AAPL"}
