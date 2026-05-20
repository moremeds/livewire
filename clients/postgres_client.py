"""Postgres analytical publish client."""

from __future__ import annotations

import os
import json
from collections.abc import Callable
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
import pyarrow.parquet as pq
from psycopg import sql
from psycopg.types.json import Jsonb

from clients.postgres_schema import iter_schema_statements, validate_schema_name

_DAILY_COLUMNS = [
    "trade_date",
    "symbol_id",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
]
_FUTURES_COLUMNS = [
    "trade_date",
    "contract_id",
    "root_symbol",
    "expiry_date",
    "open",
    "high",
    "low",
    "close",
    "settlement",
    "volume",
    "open_interest",
]
_INTRADAY_COLUMNS = [
    "bar_timestamp",
    "symbol_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
]
_DEFAULT_BATCH_SIZE = 50_000


class PostgresClient:
    """Client for replayable Postgres analytical tables."""

    def __init__(
        self,
        dsn: str | None = None,
        schema: str | None = None,
        connect_factory: Callable[[str], Any] | None = None,
    ):
        self._dsn = dsn or os.environ.get("MDW_POSTGRES_DSN")
        if not self._dsn:
            raise ValueError("Postgres DSN is required; pass dsn or set MDW_POSTGRES_DSN")
        self.schema = validate_schema_name(schema or os.environ.get("MDW_POSTGRES_SCHEMA", "md"))
        self._connect_factory = connect_factory or psycopg.connect
        self._conn = self._connect_factory(self._dsn)

    @property
    def conn(self) -> Any:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> PostgresClient:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def ensure_schema(self) -> None:
        """Create the analytical schema and tables if they do not exist."""
        with self._conn.transaction():
            with self._conn.cursor() as cur:
                for stmt in iter_schema_statements(self.schema):
                    cur.execute(stmt)

    def replace_equities_from_parquet(
        self,
        bronze_dir: str | Path,
        asset_class: str = "equity",
        venue: str = "SMART",
    ) -> dict[str, int]:
        """Replace daily equity-like rows from bronze parquet."""
        parquet_files = _parquet_files(bronze_dir, "1d.parquet")
        if not parquet_files:
            return {"symbols": 0, "rows": 0}

        symbol_rows: list[tuple[str, int, str, str]] = []
        for path in parquet_files:
            symbol = _symbol_from_path(path)
            first_symbol_id = _first_parquet_int(path, "symbol_id")
            if first_symbol_id is not None:
                symbol_rows.append((symbol, first_symbol_id, asset_class, venue))

        with self._conn.transaction():
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TEMP TABLE _lw_symbols_stage (
                        symbol text NOT NULL,
                        symbol_id bigint NOT NULL,
                        asset_class text NOT NULL,
                        venue text NOT NULL
                    ) ON COMMIT DROP
                    """
                )
                cur.execute(
                    """
                    CREATE TEMP TABLE _lw_equities_daily_stage (
                        trade_date date NOT NULL,
                        symbol_id bigint NOT NULL,
                        open double precision NOT NULL,
                        high double precision NOT NULL,
                        low double precision NOT NULL,
                        close double precision NOT NULL,
                        adj_close double precision NOT NULL,
                        volume bigint NOT NULL
                    ) ON COMMIT DROP
                    """
                )
                copied_symbols = _copy_rows(
                    cur,
                    "_lw_symbols_stage",
                    ["symbol", "symbol_id", "asset_class", "venue"],
                    symbol_rows,
                )
                copied_rows = _copy_rows(cur, "_lw_equities_daily_stage", _DAILY_COLUMNS, _daily_rows(parquet_files))
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.symbols (symbol_id, symbol, asset_class, venue)
                    SELECT DISTINCT symbol_id, symbol, asset_class, venue
                    FROM _lw_symbols_stage
                    ON CONFLICT (symbol_id) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        asset_class = EXCLUDED.asset_class,
                        venue = EXCLUDED.venue
                    """
                )
                cur.execute(
                    f"""
                    DELETE FROM {self.schema}.equities_daily d
                    USING {self.schema}.symbols s
                    WHERE d.symbol_id = s.symbol_id AND s.asset_class = %s
                    """,
                    (asset_class,),
                )
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.equities_daily
                        (trade_date, symbol_id, open, high, low, close, adj_close, volume)
                    SELECT trade_date, symbol_id, open, high, low, close, adj_close, volume
                    FROM _lw_equities_daily_stage
                    """
                )
        return {"symbols": copied_symbols, "rows": copied_rows}

    def replace_futures_from_parquet(self, bronze_dir: str | Path) -> dict[str, int]:
        """Replace futures daily rows from bronze parquet."""
        parquet_files = _parquet_files(bronze_dir, "1d.parquet")
        if not parquet_files:
            return {"rows": 0}

        with self._conn.transaction():
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TEMP TABLE _lw_futures_daily_stage (
                        trade_date date NOT NULL,
                        contract_id bigint NOT NULL,
                        root_symbol text NOT NULL,
                        expiry_date date NOT NULL,
                        open double precision NOT NULL,
                        high double precision NOT NULL,
                        low double precision NOT NULL,
                        close double precision NOT NULL,
                        settlement double precision NOT NULL,
                        volume bigint NOT NULL,
                        open_interest bigint NOT NULL
                    ) ON COMMIT DROP
                    """
                )
                copied_rows = _copy_rows(cur, "_lw_futures_daily_stage", _FUTURES_COLUMNS, _futures_rows(parquet_files))
                cur.execute(f"DELETE FROM {self.schema}.futures_daily")
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.futures_daily
                        (trade_date, contract_id, root_symbol, expiry_date,
                         open, high, low, close, settlement, volume, open_interest)
                    SELECT trade_date, contract_id, root_symbol, expiry_date,
                           open, high, low, close, settlement, volume, open_interest
                    FROM _lw_futures_daily_stage
                    """
                )
        return {"rows": copied_rows}

    def replace_equities_intraday_from_parquet(
        self,
        bronze_dir: str | Path,
        timeframe: str,
    ) -> dict[str, int]:
        """Replace intraday rows for the symbols staged from parquet."""
        if timeframe not in ("1m", "1h", "5m"):
            raise ValueError(f"unsupported intraday timeframe: {timeframe!r}")
        table = f"equities_{timeframe}"
        parquet_files = _parquet_files(bronze_dir, f"{timeframe}.parquet")
        if not parquet_files:
            return {"symbols": 0, "rows": 0}

        symbol_rows: list[tuple[str, int, str, str]] = []
        for path in parquet_files:
            symbol = _symbol_from_path(path)
            first_symbol_id = _first_parquet_int(path, "symbol_id")
            if first_symbol_id is not None:
                symbol_rows.append((symbol, first_symbol_id, "equity", "SMART"))

        with self._conn.transaction():
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TEMP TABLE _lw_symbols_stage (
                        symbol text NOT NULL,
                        symbol_id bigint NOT NULL,
                        asset_class text NOT NULL,
                        venue text NOT NULL
                    ) ON COMMIT DROP
                    """
                )
                cur.execute(
                    """
                    CREATE TEMP TABLE _lw_intraday_stage (
                        bar_timestamp timestamptz NOT NULL,
                        symbol_id bigint NOT NULL,
                        open double precision NOT NULL,
                        high double precision NOT NULL,
                        low double precision NOT NULL,
                        close double precision NOT NULL,
                        volume bigint NOT NULL
                    ) ON COMMIT DROP
                    """
                )
                copied_symbols = _copy_rows(
                    cur,
                    "_lw_symbols_stage",
                    ["symbol", "symbol_id", "asset_class", "venue"],
                    symbol_rows,
                )
                copied_rows = _copy_rows(cur, "_lw_intraday_stage", _INTRADAY_COLUMNS, _intraday_rows(parquet_files))
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.symbols (symbol_id, symbol, asset_class, venue)
                    SELECT DISTINCT symbol_id, symbol, asset_class, venue
                    FROM _lw_symbols_stage
                    ON CONFLICT (symbol_id) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        asset_class = EXCLUDED.asset_class,
                        venue = EXCLUDED.venue
                    """
                )
                cur.execute(
                    f"""
                    DELETE FROM {self.schema}.{table} d
                    USING _lw_symbols_stage s
                    WHERE d.symbol_id = s.symbol_id
                    """
                )
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.{table}
                        (bar_timestamp, symbol_id, open, high, low, close, volume)
                    SELECT bar_timestamp, symbol_id, open, high, low, close, volume
                    FROM _lw_intraday_stage
                    """
                )
        return {"symbols": copied_symbols, "rows": copied_rows}

    def replace_telemetry_from_jsonl(self, path: str | Path) -> dict[str, int]:
        """Replace telemetry event rows from a JSONL artifact."""
        rows, skipped = _telemetry_rows(path)
        if rows == [] and skipped == 0 and not Path(path).exists():
            return {"rows": 0, "skipped": 0}
        with self._conn.transaction():
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TEMP TABLE _lw_telemetry_stage (
                        ts timestamptz NOT NULL,
                        source text NOT NULL,
                        event text NOT NULL,
                        farm text,
                        state text,
                        code integer,
                        req_id integer,
                        message text,
                        payload jsonb NOT NULL
                    ) ON COMMIT DROP
                    """
                )
                copied = _copy_rows(
                    cur,
                    "_lw_telemetry_stage",
                    ["ts", "source", "event", "farm", "state", "code", "req_id", "message", "payload"],
                    rows,
                )
                cur.execute(f"DELETE FROM {self.schema}.telemetry_events")
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.telemetry_events
                        (ts, source, event, farm, state, code, req_id, message, payload)
                    SELECT ts, source, event, farm, state, code, req_id, message, payload
                    FROM _lw_telemetry_stage
                    """
                )
        return {"rows": copied, "skipped": skipped}

    def replace_quality_flags_from_jsonl(self, path: str | Path) -> dict[str, int]:
        """Replace quality-flag rows from a JSONL artifact."""
        rows, skipped = _quality_rows(path)
        if rows == [] and skipped == 0 and not Path(path).exists():
            return {"rows": 0, "skipped": 0}
        with self._conn.transaction():
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TEMP TABLE _lw_quality_flags_stage (
                        ts timestamptz NOT NULL,
                        source text NOT NULL,
                        ticker text,
                        timeframe text NOT NULL,
                        parquet_path text,
                        category text NOT NULL,
                        severity text NOT NULL,
                        detail jsonb NOT NULL,
                        payload jsonb NOT NULL
                    ) ON COMMIT DROP
                    """
                )
                copied = _copy_rows(
                    cur,
                    "_lw_quality_flags_stage",
                    [
                        "ts",
                        "source",
                        "ticker",
                        "timeframe",
                        "parquet_path",
                        "category",
                        "severity",
                        "detail",
                        "payload",
                    ],
                    rows,
                )
                cur.execute(f"DELETE FROM {self.schema}.quality_flags")
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.quality_flags
                        (ts, source, ticker, timeframe, parquet_path,
                         category, severity, detail, payload)
                    SELECT ts, source, ticker, timeframe, parquet_path,
                           category, severity, detail, payload
                    FROM _lw_quality_flags_stage
                    """
                )
        return {"rows": copied, "skipped": skipped}


def _copy_batch_size() -> int:
    return int(os.environ.get("MDW_POSTGRES_COPY_BATCH_SIZE", _DEFAULT_BATCH_SIZE))


def _parquet_files(bronze_dir: str | Path, filename: str) -> list[Path]:
    return sorted(Path(bronze_dir).glob(f"symbol=*/{filename}"))


def _symbol_from_path(path: Path) -> str:
    return path.parent.name.split("=", 1)[1]


def _iter_parquet_dicts(path: Path, columns: list[str], batch_size: int):
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
        yield from batch.to_pylist()


def _first_parquet_int(path: Path, column: str) -> int | None:
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(batch_size=1, columns=[column]):
        rows = batch.to_pylist()
        if rows:
            return int(rows[0][column])
    return None


def _daily_rows(paths: list[Path]):
    for path in paths:
        for row in _iter_parquet_dicts(path, _DAILY_COLUMNS, _copy_batch_size()):
            yield _daily_row(row)


def _futures_rows(paths: list[Path]):
    for path in paths:
        for row in _iter_parquet_dicts(path, _FUTURES_COLUMNS, _copy_batch_size()):
            yield _futures_row(row)


def _intraday_rows(paths: list[Path]):
    for path in paths:
        for row in _iter_parquet_dicts(path, _INTRADAY_COLUMNS, _copy_batch_size()):
            yield _intraday_row(row)


def _copy_rows(cur, table: str, columns: list[str], rows) -> int:
    stmt = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
    )
    copied = 0
    with cur.copy(stmt) as copy:
        for row in rows:
            copy.write_row(row)
            copied += 1
    return copied


def _daily_row(row: dict[str, Any]) -> tuple:
    return (
        _as_date(row["trade_date"]),
        int(row["symbol_id"]),
        float(row["open"]),
        float(row["high"]),
        float(row["low"]),
        float(row["close"]),
        float(row["adj_close"]),
        int(row["volume"]),
    )


def _futures_row(row: dict[str, Any]) -> tuple:
    return (
        _as_date(row["trade_date"]),
        int(row["contract_id"]),
        str(row["root_symbol"]),
        _as_date(row["expiry_date"]),
        float(row["open"]),
        float(row["high"]),
        float(row["low"]),
        float(row["close"]),
        float(row["settlement"]),
        int(row["volume"]),
        int(row["open_interest"]),
    )


def _intraday_row(row: dict[str, Any]) -> tuple:
    ts = row["bar_timestamp"]
    if not isinstance(ts, datetime):
        raise ValueError(f"bar_timestamp must be datetime, got {type(ts).__name__}")
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        raise ValueError(f"bar_timestamp must be tz-aware, got {ts!r}")
    return (
        ts.astimezone(timezone.utc),
        int(row["symbol_id"]),
        float(row["open"]),
        float(row["high"]),
        float(row["low"]),
        float(row["close"]),
        int(row["volume"]),
    )


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        ts = value
    else:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _json_ready(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


def _jsonb(value: Any) -> Jsonb:
    return Jsonb(_json_ready(value or {}))


def _read_jsonl(path: str | Path) -> tuple[list[dict[str, Any]], int]:
    artifact = Path(path)
    if not artifact.exists():
        return [], 0
    rows: list[dict[str, Any]] = []
    skipped = 0
    for line in artifact.read_text().splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            skipped += 1
    return rows, skipped


def _telemetry_rows(path: str | Path) -> tuple[list[tuple], int]:
    events, skipped = _read_jsonl(path)
    return [
        (
            _parse_ts(event["ts"]),
            str(event["source"]),
            str(event["event"]),
            event.get("farm"),
            event.get("state"),
            event.get("code"),
            event.get("req_id"),
            event.get("message"),
            _jsonb(event),
        )
        for event in events
    ], skipped


def _quality_rows(path: str | Path) -> tuple[list[tuple], int]:
    flags, skipped = _read_jsonl(path)
    return [
        (
            _parse_ts(flag["ts"]),
            str(flag["source"]),
            flag.get("ticker"),
            str(flag.get("timeframe", "1d")),
            flag.get("parquet_path"),
            str(flag["category"]),
            str(flag["severity"]),
            _jsonb(flag.get("detail", {})),
            _jsonb(flag),
        )
        for flag in flags
    ], skipped
