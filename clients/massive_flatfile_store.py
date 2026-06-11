"""Bucketed raw Parquet store for Massive whole-market minute files."""

from __future__ import annotations

import csv
import gzip
import heapq
import shutil
import tempfile
from collections import OrderedDict
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, TextIO

MAX_OPEN_BUCKETS = 64

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
        # LRU cache of open bucket writers — caps file descriptors at MAX_OPEN_BUCKETS so
        # bucket_count > rlimit doesn't EMFILE during streaming.
        open_buckets: OrderedDict[int, tuple[TextIO, csv.DictWriter]] = OrderedDict()
        header_written: set[int] = set()

        def writer_for(bucket: int) -> csv.DictWriter:
            if bucket in open_buckets:
                open_buckets.move_to_end(bucket)
                return open_buckets[bucket][1]
            while len(open_buckets) >= MAX_OPEN_BUCKETS:
                _, (evicted, _) = open_buckets.popitem(last=False)
                evicted.close()
            first_time = bucket not in header_written
            handle = (spill / f"{bucket:03d}.csv").open(
                "w" if first_time else "a", newline="", encoding="utf-8"
            )
            writer = csv.DictWriter(handle, fieldnames=RAW_SCHEMA.names)
            if first_time:
                writer.writeheader()
                header_written.add(bucket)
            open_buckets[bucket] = (handle, writer)
            return writer

        symbols: set[str] = set()
        rows = 0
        try:
            with gzip.open(gzip_path, "rt", newline="", encoding="utf-8") as source:
                for row in csv.DictReader(source):
                    ticker = row["ticker"]
                    symbols.add(ticker)
                    bucket = stable_symbol_id(ticker) % self.bucket_count
                    writer_for(bucket).writerow(
                        {
                            "ticker": ticker,
                            "bar_timestamp": datetime.fromtimestamp(
                                int(row["window_start"]) / 1_000_000_000, tz=UTC
                            ).isoformat(),
                            "open": row["open"],
                            "high": row["high"],
                            "low": row["low"],
                            "close": row["close"],
                            # Massive sometimes emits fractional volumes (split-adjusted or
                            # fractional-share aware); RAW_SCHEMA is int64, so truncate here.
                            "volume": int(float(row["volume"])),
                        }
                    )
                    rows += 1
            for handle, _ in open_buckets.values():
                handle.close()
            open_buckets.clear()
            # exFAT mounts spawn AppleDouble (`._foo.csv`) sidecars for any file with xattrs;
            # the glob would otherwise pick them up and int() would crash on the leading dot.
            for csv_path in sorted(p for p in spill.glob("*.csv") if not p.name.startswith("._")):
                bucket = int(csv_path.stem)
                table = pacsv.read_csv(csv_path).cast(RAW_SCHEMA)  # pyright: ignore[reportPrivateImportUsage]
                table = table.sort_by([("ticker", "ascending"), ("bar_timestamp", "ascending")])
                self._validate_table(table)
                pq.write_table(table, temp / f"bucket={bucket:03d}.parquet", compression="snappy")
            if rows == 0:
                raise ValueError(f"{day}: Massive flat file contained no rows")
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
            for handle, _ in open_buckets.values():
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

    def scan_bucket_by_ticker(
        self,
        bucket: int,
        days: list[date],
        *,
        batch_size: int = 256,
    ) -> Iterator[tuple[str, list[dict[str, Any]]]]:
        """K-way merge sorted daily bucket files, buffering one ticker at a time."""
        iterators = [
            self._iter_path_rows(self.bucket_path(day, bucket), batch_size)
            for day in days
            if self.bucket_path(day, bucket).exists()
        ]
        heap: list[tuple[str, datetime, int, dict[str, Any]]] = []
        for index, rows in enumerate(iterators):
            try:
                row = next(rows)
            except StopIteration:
                continue
            heapq.heappush(heap, (row["ticker"], row["bar_timestamp"], index, row))

        current_ticker: str | None = None
        current_rows: list[dict[str, Any]] = []
        previous_key: tuple[str, datetime] | None = None
        while heap:
            ticker, timestamp, index, row = heapq.heappop(heap)
            key = (ticker, timestamp)
            if key == previous_key:
                raise ValueError(f"duplicate raw flat-file key: {ticker} {timestamp.isoformat()}")
            previous_key = key
            if current_ticker is not None and ticker != current_ticker:
                yield current_ticker, current_rows
                current_rows = []
            current_ticker = ticker
            current_rows.append(row)
            try:
                following = next(iterators[index])
            except StopIteration:
                continue
            heapq.heappush(heap, (following["ticker"], following["bar_timestamp"], index, following))
        if current_ticker is not None:
            yield current_ticker, current_rows

    @staticmethod
    def _iter_path_rows(path: Path, batch_size: int) -> Iterator[dict[str, Any]]:
        for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size, columns=RAW_SCHEMA.names):
            yield from batch.to_pylist()

    @staticmethod
    def _validate_table(table: pa.Table) -> None:
        if table.num_rows == 0:
            raise ValueError("raw flat-file bucket cannot be empty")
        previous: tuple[str, datetime] | None = None
        for ticker, timestamp in zip(
            table.column("ticker").to_pylist(), table.column("bar_timestamp").to_pylist(), strict=True
        ):
            key = (ticker, timestamp)
            if key == previous:
                raise ValueError(f"duplicate raw flat-file key: {ticker} {timestamp.isoformat()}")
            if previous is not None and key < previous:
                raise ValueError("raw flat-file bucket is not sorted")
            previous = key

    def raw_stats(self, day: date) -> dict[str, Any]:
        root = self.raw_path(day)
        paths = list(root.glob("bucket=*.parquet"))
        rows = sum(pq.read_metadata(p).num_rows for p in paths)
        size_bytes = sum(p.stat().st_size for p in paths)
        bounds: list[datetime] = []
        for path in paths:
            table = pq.read_table(path, columns=["bar_timestamp"])
            values = table.column("bar_timestamp").to_pylist()
            if values:
                bounds.extend((values[0], values[-1]))
        return {
            "rows": rows,
            "symbols": len(self.symbols_for_date(day)),
            "size_bytes": size_bytes,
            "earliest": min(bounds) if bounds else None,
            "latest": max(bounds) if bounds else None,
        }
