"""Bucketed raw Parquet store for Massive whole-market daily aggregate files."""

from __future__ import annotations

import heapq
import shutil
import tempfile
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from clients.symbol_ids import stable_symbol_id

RAW_DAILY_SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        ("trade_date", pa.string()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.int64()),
    ]
)


class MassiveDailyFlatfileStore:
    def __init__(self, warehouse_dir: Path, bucket_count: int = 32):
        self.warehouse_dir = warehouse_dir
        self.bucket_count = bucket_count
        self.raw_root = warehouse_dir / "data-lake" / "raw" / "massive" / "us_stocks_sip" / "day_aggs_v1"

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
            read_options = pacsv.ReadOptions(use_threads=True)  # pyright: ignore[reportPrivateImportUsage]
            parse_options = pacsv.ParseOptions(delimiter=",")  # pyright: ignore[reportPrivateImportUsage]
            convert_options = pacsv.ConvertOptions(  # pyright: ignore[reportPrivateImportUsage]
                column_types={
                    "ticker": pa.string(),
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
                raise ValueError(f"{day}: invalid Massive daily flat-file CSV: {exc}") from exc

            rows = src.num_rows
            if rows == 0:
                raise ValueError(f"{day}: Massive daily flat file contained no rows")

            ticker_dict = pc.dictionary_encode(src["ticker"].combine_chunks())
            unique_tickers = ticker_dict.dictionary.to_pylist()
            bucket_lookup = pa.array(
                [stable_symbol_id(t) % self.bucket_count for t in unique_tickers],
                type=pa.int32(),
            )
            bucket_col = pc.take(bucket_lookup, ticker_dict.indices)

            # window_start is ns since epoch (midnight UTC of trade date) → ISO date string.
            ts_us = pc.cast(
                pc.divide(src["window_start"], pa.scalar(1000, type=pa.int64())),
                pa.timestamp("us", tz="UTC"),
            )
            trade_date_strs = pa.array(
                [_us_ts_to_iso_date(value) for value in ts_us.to_pylist()],
                type=pa.string(),
            )

            volume_int = pc.cast(src["volume"], pa.int64(), safe=False)

            bronze = pa.table(
                {
                    "ticker": src["ticker"],
                    "trade_date": trade_date_strs,
                    "open": src["open"],
                    "high": src["high"],
                    "low": src["low"],
                    "close": src["close"],
                    "volume": volume_int,
                }
            )

            for bucket in range(self.bucket_count):
                sub = bronze.filter(pc.equal(bucket_col, bucket))
                if sub.num_rows == 0:
                    continue
                sub = sub.sort_by([("ticker", "ascending"), ("trade_date", "ascending")])
                self._validate_table(sub)
                pq.write_table(sub, temp / f"bucket={bucket:03d}.parquet", compression="snappy")

            symbols = set(unique_tickers)
            pq.write_table(
                pa.table({"ticker": sorted(symbols)}),
                temp / "_symbols.parquet",
                compression="snappy",
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
        batch_size: int = 1024,
    ) -> Iterator[tuple[str, list[dict[str, Any]]]]:
        """K-way merge sorted daily bucket files, buffering one ticker at a time."""
        iterators = [
            self._iter_path_rows(self.bucket_path(day, bucket), batch_size)
            for day in days
            if self.bucket_path(day, bucket).exists()
        ]
        heap: list[tuple[str, str, int, dict[str, Any]]] = []
        for index, rows in enumerate(iterators):
            try:
                row = next(rows)
            except StopIteration:
                continue
            heapq.heappush(heap, (row["ticker"], row["trade_date"], index, row))

        current_ticker: str | None = None
        current_rows: list[dict[str, Any]] = []
        previous_key: tuple[str, str] | None = None
        while heap:
            ticker, trade_date, index, row = heapq.heappop(heap)
            key = (ticker, trade_date)
            if key == previous_key:
                raise ValueError(f"duplicate raw daily flat-file key: {ticker} {trade_date}")
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
            heapq.heappush(heap, (following["ticker"], following["trade_date"], index, following))
        if current_ticker is not None:
            yield current_ticker, current_rows

    @staticmethod
    def _iter_path_rows(path: Path, batch_size: int) -> Iterator[dict[str, Any]]:
        for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size, columns=RAW_DAILY_SCHEMA.names):
            yield from batch.to_pylist()

    @staticmethod
    def _validate_table(table: pa.Table) -> None:
        if table.num_rows == 0:
            raise ValueError("raw daily flat-file bucket cannot be empty")
        previous: tuple[str, str] | None = None
        for ticker, trade_date in zip(
            table.column("ticker").to_pylist(), table.column("trade_date").to_pylist(), strict=True
        ):
            key = (ticker, trade_date)
            if key == previous:
                raise ValueError(f"duplicate raw daily flat-file key: {ticker} {trade_date}")
            if previous is not None and key < previous:
                raise ValueError("raw daily flat-file bucket is not sorted")
            previous = key

    def raw_stats(self, day: date) -> dict[str, Any]:
        root = self.raw_path(day)
        paths = list(root.glob("bucket=*.parquet"))
        rows = sum(pq.read_metadata(p).num_rows for p in paths)
        size_bytes = sum(p.stat().st_size for p in paths)
        return {
            "rows": rows,
            "symbols": len(self.symbols_for_date(day)),
            "size_bytes": size_bytes,
        }


def _us_ts_to_iso_date(value: datetime | None) -> str:
    if value is None:
        raise ValueError("daily flat-file row missing window_start")
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).date().isoformat()
