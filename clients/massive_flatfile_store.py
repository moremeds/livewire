"""Bucketed raw Parquet store for Massive whole-market minute files."""

from __future__ import annotations

import csv
import gzip
import shutil
import tempfile
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from clients.symbol_ids import stable_symbol_id

RAW_SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        ("bar_timestamp", pa.timestamp("us", tz="UTC")),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.int64()),
    ]
)


class MassiveFlatfileStore:
    def __init__(self, warehouse_dir: Path, bucket_count: int = 256):
        self.warehouse_dir = warehouse_dir
        self.bucket_count = bucket_count
        self.raw_root = warehouse_dir / "data-lake" / "raw" / "massive" / "us_stocks_sip" / "minute_aggs_v1"

    def raw_path(self, day: date) -> Path:
        return self.raw_root / f"date={day.isoformat()}"

    def has_raw_date(self, day: date) -> bool:
        return (self.raw_path(day) / "_SUCCESS").exists()

    def bucket_path(self, day: date, bucket: int) -> Path:
        return self.raw_path(day) / f"bucket={bucket:03d}.parquet"

    def stage_gzip(self, day: date, gzip_path: Path, *, replace: bool = False) -> dict[str, int]:
        final = self.raw_path(day)
        if self.has_raw_date(day) and not replace:
            return self.raw_stats(day)
        self.raw_root.mkdir(parents=True, exist_ok=True)
        temp = Path(tempfile.mkdtemp(prefix=f".date={day}.", dir=self.raw_root))
        spill = temp / ".spill"
        spill.mkdir()
        handles: dict[int, object] = {}
        writers: dict[int, csv.DictWriter] = {}
        symbols: set[str] = set()
        rows = 0
        try:
            with gzip.open(gzip_path, "rt", newline="", encoding="utf-8") as source:
                for row in csv.DictReader(source):
                    ticker = row["ticker"]
                    symbols.add(ticker)
                    bucket = stable_symbol_id(ticker) % self.bucket_count
                    if bucket not in handles:
                        handle = (spill / f"{bucket:03d}.csv").open("w", newline="", encoding="utf-8")
                        handles[bucket] = handle
                        writer = csv.DictWriter(handle, fieldnames=RAW_SCHEMA.names)
                        writer.writeheader()
                        writers[bucket] = writer
                    writers[bucket].writerow(
                        {
                            "ticker": ticker,
                            "bar_timestamp": datetime.fromtimestamp(int(row["window_start"]) / 1_000_000_000, tz=UTC).isoformat(),
                            "open": row["open"],
                            "high": row["high"],
                            "low": row["low"],
                            "close": row["close"],
                            "volume": row["volume"],
                        }
                    )
                    rows += 1
            for handle in handles.values():
                handle.close()
            for csv_path in sorted(spill.glob("*.csv")):
                bucket = int(csv_path.stem)
                table = pacsv.read_csv(csv_path).cast(RAW_SCHEMA)
                table = table.sort_by([("ticker", "ascending"), ("bar_timestamp", "ascending")])
                pq.write_table(table, temp / f"bucket={bucket:03d}.parquet", compression="snappy")
            pq.write_table(pa.table({"ticker": sorted(symbols)}), temp / "_symbols.parquet", compression="snappy")
            shutil.rmtree(spill)
            (temp / "_SUCCESS").write_text(f"rows={rows}\nsymbols={len(symbols)}\n", encoding="utf-8")
            if final.exists():
                old = final.with_name(f".old-{final.name}")
                if old.exists():
                    shutil.rmtree(old)
                final.rename(old)
                temp.rename(final)
                shutil.rmtree(old)
            else:
                temp.rename(final)
            return {"rows": rows, "symbols": len(symbols)}
        finally:
            for handle in handles.values():
                if not handle.closed:
                    handle.close()
            if temp.exists():
                shutil.rmtree(temp)

    def symbols_for_date(self, day: date) -> set[str]:
        path = self.raw_path(day) / "_symbols.parquet"
        return set(pq.read_table(path).column("ticker").to_pylist()) if path.exists() else set()

    def available_buckets(self, days: list[date]) -> set[int]:
        result: set[int] = set()
        for day in days:
            for path in self.raw_path(day).glob("bucket=*.parquet"):
                result.add(int(path.stem.split("=")[1]))
        return result

    def iter_bucket_tables(self, bucket: int, days: list[date]) -> Iterator[pa.Table]:
        for day in days:
            path = self.bucket_path(day, bucket)
            if path.exists():
                yield pq.read_table(path)

    def raw_stats(self, day: date) -> dict[str, int]:
        root = self.raw_path(day)
        rows = sum(pq.read_metadata(p).num_rows for p in root.glob("bucket=*.parquet"))
        return {"rows": rows, "symbols": len(self.symbols_for_date(day))}
