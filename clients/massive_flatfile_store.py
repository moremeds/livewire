"""Bucketed raw Parquet store for Massive whole-market minute files."""

from __future__ import annotations

import heapq
import shutil
import tempfile
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from clients.parquet_io import PARQUET_COMPRESSION, PARQUET_COMPRESSION_LEVEL
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
        try:
            # pyarrow reads gzipped CSV in C — release the GIL for decompression and parsing.
            # The csv option classes / read_csv are publicly documented but live in a
            # private submodule, hence the reportPrivateImportUsage suppression.
            read_options = pacsv.ReadOptions(use_threads=True)  # pyright: ignore[reportPrivateImportUsage]
            parse_options = pacsv.ParseOptions(delimiter=",")  # pyright: ignore[reportPrivateImportUsage]
            convert_options = pacsv.ConvertOptions(  # pyright: ignore[reportPrivateImportUsage]
                column_types={
                    "ticker": pa.string(),
                    # Massive sometimes emits fractional volumes; read as float, truncate to int below.
                    "volume": pa.float64(),
                    "open": pa.float64(),
                    "high": pa.float64(),
                    "low": pa.float64(),
                    "close": pa.float64(),
                    "window_start": pa.int64(),
                },
                include_columns=["ticker", "volume", "open", "close", "high", "low", "window_start"],
            )
            try:
                src = pacsv.read_csv(  # pyright: ignore[reportPrivateImportUsage]
                    str(gzip_path),
                    read_options=read_options,
                    parse_options=parse_options,
                    convert_options=convert_options,
                )
            except pa.ArrowInvalid as exc:
                raise ValueError(f"{day}: invalid Massive flat-file CSV: {exc}") from exc

            rows = src.num_rows
            if rows == 0:
                raise ValueError(f"{day}: Massive flat file contained no rows")

            # Dictionary-encode the ticker column; compute bucket per unique value (~12K),
            # then map back to the full ~5M-row table. Avoids any per-row Python loop.
            ticker_dict = pc.dictionary_encode(src["ticker"].combine_chunks())
            unique_tickers = ticker_dict.dictionary.to_pylist()
            bucket_lookup = pa.array(
                [stable_symbol_id(t) % self.bucket_count for t in unique_tickers],
                type=pa.int32(),
            )
            bucket_col = pc.take(bucket_lookup, ticker_dict.indices)

            # window_start is nanoseconds since epoch → cast to UTC microsecond timestamps.
            bar_ts = pc.cast(
                pc.divide(src["window_start"], pa.scalar(1000, type=pa.int64())),
                pa.timestamp("us", tz="UTC"),
            )

            # Truncate fractional volume into RAW_SCHEMA's int64.
            volume_int = pc.cast(src["volume"], pa.int64(), safe=False)

            bronze = pa.table(
                {
                    "ticker": src["ticker"],
                    "bar_timestamp": bar_ts,
                    "open": src["open"],
                    "high": src["high"],
                    "low": src["low"],
                    "close": src["close"],
                    "volume": volume_int,
                }
            )

            # Partition by bucket, sort each partition, validate, write.
            for bucket in range(self.bucket_count):
                sub = bronze.filter(pc.equal(bucket_col, bucket))
                if sub.num_rows == 0:
                    continue
                sub = sub.sort_by([("ticker", "ascending"), ("bar_timestamp", "ascending")])
                self._validate_table(sub)
                pq.write_table(
                    sub,
                    temp / f"bucket={bucket:03d}.parquet",
                    compression=PARQUET_COMPRESSION,
                    compression_level=PARQUET_COMPRESSION_LEVEL,
                )

            symbols = set(unique_tickers)
            pq.write_table(
                pa.table({"ticker": sorted(symbols)}),
                temp / "_symbols.parquet",
                compression=PARQUET_COMPRESSION,
                compression_level=PARQUET_COMPRESSION_LEVEL,
            )
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
