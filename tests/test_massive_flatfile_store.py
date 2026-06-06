import csv
import gzip
from datetime import date

from clients.massive_flatfile_store import MassiveFlatfileStore


def test_stage_gzip_preserves_all_symbols(tmp_path):
    source = tmp_path / "day.csv.gz"
    with gzip.open(source, "wt", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"],
        )
        writer.writeheader()
        for ticker in ["AAPL", "MSFT"]:
            writer.writerow(
                {
                    "ticker": ticker,
                    "volume": 10,
                    "open": 1,
                    "close": 2,
                    "high": 3,
                    "low": 0.5,
                    "window_start": 1717421400000000000,
                    "transactions": 1,
                }
            )
    store = MassiveFlatfileStore(tmp_path, bucket_count=4)
    stats = store.stage_gzip(date(2024, 6, 3), source)
    assert stats == {"rows": 2, "symbols": 2}
    assert store.has_raw_date(date(2024, 6, 3))
    assert store.symbols_for_date(date(2024, 6, 3)) == {"AAPL", "MSFT"}
