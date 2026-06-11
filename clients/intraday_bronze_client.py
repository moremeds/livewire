"""Intraday parquet bronze client (1m, 1h, and 5m bars).

Stores per-ticker per-timeframe parquet files alongside the existing 1d.parquet
files written by BronzeClient. Schema is timestamp-keyed (bar_timestamp
TIMESTAMPTZ) rather than date-keyed.

Universal rule: all bar timestamps stored as UTC with timezone awareness.
Naive datetimes are rejected at write time.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from clients.parquet_io import publish_parquet
from clients.symbol_ids import stable_symbol_id
from clients.symbol_paths import decode_symbol, encode_symbol

log = logging.getLogger(__name__)

_DEFAULT_BRONZE_DIR = Path.home() / "market-warehouse" / "data-lake" / "bronze" / "asset_class=equity"

INTRADAY_TIMEFRAMES = ("1m", "1h", "5m", "30m")

INTRADAY_PARQUET_FILENAME = {
    "1m": "1m.parquet",
    "1h": "1h.parquet",
    "5m": "5m.parquet",
    "30m": "30m.parquet",
}

# IB historical request limits per timeframe
INTRADAY_MAX_REQUEST_DURATION = {
    "1m": "1 D",
    "1h": "1 M",
    "5m": "1 W",
    "30m": "1 M",
}

# Realistic IB data depth per timeframe (Massive Starter = 5yr all timeframes)
INTRADAY_MAX_DEPTH = {
    "1m": "5 Y",
    "1h": "5 Y",
    "5m": "5 Y",
    "30m": "5 Y",
}

# IB barSizeSetting strings
INTRADAY_IB_BAR_SIZE = {
    "1m": "1 min",
    "1h": "1 hour",
    "5m": "5 mins",
    "30m": "30 mins",
}

_INTRADAY_COLUMNS = (
    "bar_timestamp",
    "symbol_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

_INTRADAY_SCHEMA = pa.schema(
    [
        ("bar_timestamp", pa.timestamp("us", tz="UTC")),
        ("symbol_id", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.int64()),
    ]
)


class IntradayBronzeClient:
    """Per-ticker intraday bronze parquet client.

    Independent from BronzeClient. Daily code paths are never affected.
    """

    def __init__(
        self,
        bronze_dir: str | Path | None = None,
        timeframe: str = "5m",
    ):
        if timeframe not in INTRADAY_TIMEFRAMES:
            raise ValueError(f"unsupported timeframe: {timeframe!r}. Must be one of {INTRADAY_TIMEFRAMES}")
        self._bronze_dir = Path(bronze_dir or _DEFAULT_BRONZE_DIR)
        self._timeframe = timeframe
        self._filename = INTRADAY_PARQUET_FILENAME[timeframe]

    @property
    def timeframe(self) -> str:
        return self._timeframe

    @property
    def bronze_dir(self) -> Path:
        return self._bronze_dir

    def close(self) -> None:
        """No-op. Kept for API symmetry with BronzeClient."""

    def __enter__(self) -> IntradayBronzeClient:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def get_existing_symbols(self) -> set[str]:
        """Return symbols with bronze parquet snapshots at this timeframe."""
        if not self._bronze_dir.exists():
            return set()
        symbols: set[str] = set()
        for path in self._bronze_dir.glob(f"symbol=*/{self._filename}"):
            partition = path.parent.name
            if partition.startswith("symbol="):
                symbols.add(decode_symbol(partition.split("=", 1)[1]))
        return symbols

    def get_latest_timestamps(self) -> dict[str, datetime]:
        """Return ``{symbol: latest_bar_timestamp}`` for all symbols at this timeframe."""
        result: dict[str, datetime] = {}
        if not self._bronze_dir.exists():
            return result
        for path in self._bronze_dir.glob(f"symbol=*/{self._filename}"):
            symbol = decode_symbol(path.parent.name.split("=", 1)[1])
            table = pq.read_table(path, columns=["bar_timestamp"])
            values: list[datetime] = table.column("bar_timestamp").to_pylist()
            result[symbol] = max(values)
        return result

    def get_symbol_id(self, symbol: str) -> int:
        """Return existing symbol_id from parquet, or derive a stable one."""
        path = self._symbol_path(symbol)
        if not path.exists():
            return stable_symbol_id(symbol)
        table = pq.read_table(path, columns=["symbol_id"])
        return int(table.column("symbol_id")[0].as_py())

    def read_symbol_rows(self, symbol: str) -> list[dict[str, Any]]:
        """Read all rows for a symbol. Returns empty list if no parquet exists."""
        path = self._symbol_path(symbol)
        if not path.exists():
            return []
        table = pq.read_table(path, columns=list(_INTRADAY_COLUMNS))
        return table.to_pylist()

    def replace_ticker_rows(self, symbol: str, rows: list[dict[str, Any]]) -> int:
        """Atomically replace a symbol's parquet snapshot with *rows*.

        Returns the number of rows written. Raises ValueError on empty rows
        or invalid timestamps.
        """
        normalized = self._normalize_rows(rows, symbol)
        if not normalized:
            raise ValueError(f"{symbol}: cannot publish an empty parquet snapshot")
        self._publish(symbol, normalized)
        return len(normalized)

    def merge_ticker_rows(
        self,
        symbol: str,
        rows: list[dict[str, Any]],
        *,
        overwrite_existing: bool = True,
    ) -> int:
        """Merge *rows* into existing snapshot. Returns count of new rows added.

        Pyarrow-native: avoids round-tripping the existing parquet through
        Python ``list[dict]`` / ``dict[ts -> dict]``. For a 5-year 1m parquet
        (~500K rows), the old dict-based merge spent the bulk of its time on
        list/dict conversions, not actual I/O.
        """
        incoming_rows = self._normalize_rows(rows, symbol)
        if not incoming_rows:
            return 0

        incoming_table = pa.Table.from_pylist(incoming_rows, schema=_INTRADAY_SCHEMA)
        incoming_keys = incoming_table.column("bar_timestamp")

        path = self._symbol_path(symbol)
        if path.exists():
            existing_table = pq.read_table(path, columns=list(_INTRADAY_COLUMNS)).cast(_INTRADAY_SCHEMA)
        else:
            existing_table = pa.table(
                {col: pa.array([], type=_INTRADAY_SCHEMA.field(col).type) for col in _INTRADAY_COLUMNS},
                schema=_INTRADAY_SCHEMA,
            )

        existing_keys = existing_table.column("bar_timestamp")

        # "inserted" follows the original API: incoming rows whose timestamp is NOT
        # in the *original* existing parquet. Computed against existing_keys before
        # we filter anything below.
        new_in_incoming = pc.invert(pc.is_in(incoming_keys, value_set=existing_keys))
        inserted = int(pc.sum(pc.cast(new_in_incoming, pa.int64())).as_py() or 0)

        if not overwrite_existing:
            # Append-only: drop incoming rows whose timestamp is already in existing.
            incoming_table = incoming_table.filter(new_in_incoming)
            if incoming_table.num_rows == 0:
                return 0
        elif existing_table.num_rows > 0:
            # Overwrite: drop overlapping rows from existing so incoming wins on concat.
            keep_existing = pc.invert(pc.is_in(existing_keys, value_set=incoming_keys))
            existing_table = existing_table.filter(keep_existing)

        merged = pa.concat_tables([existing_table, incoming_table]).sort_by([("bar_timestamp", "ascending")])
        self._publish_table(symbol, merged)
        return inserted

    def _read_symbol_timestamps(self, symbol: str) -> set[datetime]:
        """Read only timestamp keys for a symbol snapshot."""
        path = self._symbol_path(symbol)
        if not path.exists():
            return set()
        table = pq.read_table(path, columns=["bar_timestamp"])
        keys: set[datetime] = set()
        for value in table.column("bar_timestamp").to_pylist():
            if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
                keys.add(value.replace(tzinfo=UTC))
            else:
                keys.add(value.astimezone(UTC))
        return keys

    def _symbol_path(self, symbol: str) -> Path:
        return self._bronze_dir / f"symbol={encode_symbol(symbol)}" / self._filename

    def _normalize_rows(self, rows: list[dict[str, Any]], symbol: str) -> list[dict[str, Any]]:
        # Use the row's own symbol_id if provided, otherwise derive a stable one.
        fallback_symbol_id = self.get_symbol_id(symbol)
        normalized: dict[datetime, dict[str, Any]] = {}

        for row in rows:
            ts = row["bar_timestamp"]
            if not isinstance(ts, datetime):
                raise ValueError(f"{symbol}: bar_timestamp must be a datetime, got {type(ts).__name__}")
            if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
                raise ValueError(f"{symbol}: bar_timestamp must be tz-aware (got naive {ts!r})")
            ts_utc = ts.astimezone(UTC)
            sid = int(row["symbol_id"]) if "symbol_id" in row else fallback_symbol_id
            normalized[ts_utc] = {
                "bar_timestamp": ts_utc,
                "symbol_id": sid,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
            }

        return [normalized[ts] for ts in sorted(normalized)]

    def _publish(self, symbol: str, rows: list[dict[str, Any]]) -> Path:
        out_path = self._symbol_path(symbol)
        table = pa.Table.from_pylist(rows, schema=_INTRADAY_SCHEMA)
        return self._publish_table(symbol, table, _out_path=out_path)

    def _publish_table(self, symbol: str, table: pa.Table, *, _out_path: Path | None = None) -> Path:
        out_path = _out_path or self._symbol_path(symbol)
        result = publish_parquet(out_path, table, sort_column="bar_timestamp")
        log.info("Published %s", result)
        return result
