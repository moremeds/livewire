from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients import postgres_client as postgres_client_module
from clients.postgres_schema import POSTGRES_TABLES
from clients.postgres_client import PostgresClient


class FakeCursor:
    def __init__(self, conn: FakeConnection):
        self.conn = conn

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc) -> None:
        self.conn.cursor_closed = True

    def execute(self, stmt, params=None) -> None:
        self.conn.executed.append((stmt, params))

    def copy(self, stmt):
        self.conn.copy_statements.append(stmt)
        return FakeCopy(self.conn)


class FakeCopy:
    def __init__(self, conn: FakeConnection):
        self.conn = conn

    def __enter__(self) -> FakeCopy:
        return self

    def __exit__(self, *exc) -> None:
        pass

    def write_row(self, row) -> None:
        self.conn.copied_rows.append(row)


class FakeTransaction:
    def __init__(self, conn: FakeConnection):
        self.conn = conn

    def __enter__(self) -> FakeTransaction:
        self.conn.events.append("begin")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.conn.events.append("rollback" if exc_type else "commit")


class FakeConnection:
    def __init__(self):
        self.executed = []
        self.copy_statements = []
        self.copied_rows = []
        self.events = []
        self.closed = False
        self.cursor_closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    def close(self) -> None:
        self.closed = True


def test_missing_dsn_error_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MDW_POSTGRES_DSN", raising=False)

    with pytest.raises(ValueError, match="MDW_POSTGRES_DSN"):
        PostgresClient(connect_factory=lambda dsn: FakeConnection())


def test_reads_dsn_and_schema_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MDW_POSTGRES_DSN", "postgresql://example/livewire")
    monkeypatch.setenv("MDW_POSTGRES_SCHEMA", "market_data")
    seen = {}

    def connect_factory(dsn: str) -> FakeConnection:
        seen["dsn"] = dsn
        return FakeConnection()

    client = PostgresClient(connect_factory=connect_factory)

    assert seen == {"dsn": "postgresql://example/livewire"}
    assert client.schema == "market_data"


def test_context_manager_closes_connection() -> None:
    conn = FakeConnection()

    with PostgresClient(dsn="postgresql://example/livewire", connect_factory=lambda dsn: conn) as client:
        assert client.conn is conn

    assert conn.closed is True


def test_ensure_schema_starts_and_commits_transaction() -> None:
    conn = FakeConnection()
    client = PostgresClient(dsn="postgresql://example/livewire", connect_factory=lambda dsn: conn)

    client.ensure_schema()

    assert conn.events == ["begin", "commit"]
    assert conn.cursor_closed is True


def test_ensure_schema_executes_expected_table_statements() -> None:
    conn = FakeConnection()
    client = PostgresClient(
        dsn="postgresql://example/livewire",
        schema="market_data",
        connect_factory=lambda dsn: conn,
    )

    client.ensure_schema()

    executed_sql = "\n".join(str(stmt) for stmt, _params in conn.executed)
    assert "CREATE SCHEMA IF NOT EXISTS market_data" in executed_sql
    for table in POSTGRES_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS market_data.{table}" in executed_sql


def test_invalid_schema_name_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid Postgres schema name"):
        PostgresClient(
            dsn="postgresql://example/livewire",
            schema="market-data",
            connect_factory=lambda dsn: FakeConnection(),
        )


def test_ensure_schema_rolls_back_on_failure() -> None:
    class FailingCursor(FakeCursor):
        def execute(self, stmt, params=None) -> None:
            super().execute(stmt, params)
            raise RuntimeError("boom")

    class FailingConnection(FakeConnection):
        def cursor(self) -> FailingCursor:
            return FailingCursor(self)

    conn = FailingConnection()
    client = PostgresClient(dsn="postgresql://example/livewire", connect_factory=lambda dsn: conn)

    with pytest.raises(RuntimeError, match="boom"):
        client.ensure_schema()

    assert conn.events == ["begin", "rollback"]


def write_daily_parquet(bronze_dir: Path, symbol: str, rows: list[dict]) -> Path:
    path = bronze_dir / f"symbol={symbol}" / "1d.parquet"
    path.parent.mkdir(parents=True)
    table = pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                ("trade_date", pa.date32()),
                ("symbol_id", pa.int64()),
                ("open", pa.float64()),
                ("high", pa.float64()),
                ("low", pa.float64()),
                ("close", pa.float64()),
                ("adj_close", pa.float64()),
                ("volume", pa.int64()),
            ]
        ),
    )
    pq.write_table(table, path)
    return path


def write_futures_parquet(bronze_dir: Path, symbol: str, rows: list[dict]) -> Path:
    path = bronze_dir / f"symbol={symbol}" / "1d.parquet"
    path.parent.mkdir(parents=True)
    table = pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                ("trade_date", pa.date32()),
                ("contract_id", pa.int64()),
                ("root_symbol", pa.string()),
                ("expiry_date", pa.date32()),
                ("open", pa.float64()),
                ("high", pa.float64()),
                ("low", pa.float64()),
                ("close", pa.float64()),
                ("settlement", pa.float64()),
                ("volume", pa.int64()),
                ("open_interest", pa.int64()),
            ]
        ),
    )
    pq.write_table(table, path)
    return path


def write_intraday_parquet(bronze_dir: Path, symbol: str, timeframe: str, rows: list[dict]) -> Path:
    path = bronze_dir / f"symbol={symbol}" / f"{timeframe}.parquet"
    path.parent.mkdir(parents=True)
    table = pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                ("bar_timestamp", pa.timestamp("us", tz="UTC")),
                ("symbol_id", pa.int64()),
                ("open", pa.float64()),
                ("high", pa.float64()),
                ("low", pa.float64()),
                ("close", pa.float64()),
                ("volume", pa.int64()),
            ]
        ),
    )
    pq.write_table(table, path)
    return path


def make_client(conn: FakeConnection) -> PostgresClient:
    return PostgresClient(dsn="postgresql://example/livewire", connect_factory=lambda dsn: conn)


def test_empty_daily_bronze_returns_zero_without_delete(tmp_path: Path) -> None:
    conn = FakeConnection()

    counts = make_client(conn).replace_equities_from_parquet(tmp_path)

    assert counts == {"symbols": 0, "rows": 0}
    assert conn.executed == []


def test_equity_parquet_produces_expected_copy_payloads(tmp_path: Path) -> None:
    write_daily_parquet(
        tmp_path,
        "AAPL",
        [
            {
                "trade_date": date(2026, 1, 2),
                "symbol_id": 11,
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "adj_close": 1.5,
                "volume": 100,
            }
        ],
    )
    conn = FakeConnection()

    counts = make_client(conn).replace_equities_from_parquet(tmp_path)

    assert counts == {"symbols": 1, "rows": 1}
    assert ("AAPL", 11, "equity", "SMART") in conn.copied_rows
    assert (date(2026, 1, 2), 11, 1.0, 2.0, 0.5, 1.5, 1.5, 100) in conn.copied_rows


def test_volatility_daily_uses_cboe_metadata(tmp_path: Path) -> None:
    write_daily_parquet(
        tmp_path,
        "VIX",
        [
            {
                "trade_date": date(2026, 1, 2),
                "symbol_id": 22,
                "open": 10.0,
                "high": 12.0,
                "low": 9.0,
                "close": 11.0,
                "adj_close": 11.0,
                "volume": 0,
            }
        ],
    )
    conn = FakeConnection()

    counts = make_client(conn).replace_equities_from_parquet(
        tmp_path, asset_class="volatility", venue="CBOE"
    )

    assert counts == {"symbols": 1, "rows": 1}
    assert ("VIX", 22, "volatility", "CBOE") in conn.copied_rows


def test_daily_replace_rolls_back_on_copy_failure(tmp_path: Path) -> None:
    write_daily_parquet(
        tmp_path,
        "AAPL",
        [
            {
                "trade_date": date(2026, 1, 2),
                "symbol_id": 11,
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "adj_close": 1.5,
                "volume": 100,
            }
        ],
    )

    class FailingCopy(FakeCopy):
        def write_row(self, row) -> None:
            raise RuntimeError("copy failed")

    class FailingCursor(FakeCursor):
        def copy(self, stmt):
            return FailingCopy(self.conn)

    class FailingConnection(FakeConnection):
        def cursor(self) -> FailingCursor:
            return FailingCursor(self)

    conn = FailingConnection()

    with pytest.raises(RuntimeError, match="copy failed"):
        make_client(conn).replace_equities_from_parquet(tmp_path)

    assert conn.events == ["begin", "rollback"]


def test_futures_replace_streams_contract_fields(tmp_path: Path) -> None:
    write_futures_parquet(
        tmp_path,
        "ES_202606",
        [
            {
                "trade_date": date(2026, 1, 2),
                "contract_id": 33,
                "root_symbol": "ES",
                "expiry_date": date(2026, 6, 1),
                "open": 5000.0,
                "high": 5010.0,
                "low": 4990.0,
                "close": 5005.0,
                "settlement": 5005.0,
                "volume": 1000,
                "open_interest": 2000,
            }
        ],
    )
    conn = FakeConnection()

    counts = make_client(conn).replace_futures_from_parquet(tmp_path)

    assert counts == {"rows": 1}
    assert (
        date(2026, 1, 2),
        33,
        "ES",
        date(2026, 6, 1),
        5000.0,
        5010.0,
        4990.0,
        5005.0,
        5005.0,
        1000,
        2000,
    ) in conn.copied_rows


def test_empty_futures_bronze_returns_zero(tmp_path: Path) -> None:
    assert make_client(FakeConnection()).replace_futures_from_parquet(tmp_path) == {"rows": 0}


def test_intraday_replace_supports_1h_and_5m(tmp_path: Path) -> None:
    ts = datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc)
    write_intraday_parquet(
        tmp_path,
        "AAPL",
        "1h",
        [{"bar_timestamp": ts, "symbol_id": 11, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100}],
    )
    write_intraday_parquet(
        tmp_path,
        "MSFT",
        "5m",
        [{"bar_timestamp": ts, "symbol_id": 22, "open": 3.0, "high": 4.0, "low": 2.5, "close": 3.5, "volume": 200}],
    )
    conn = FakeConnection()
    client = make_client(conn)

    assert client.replace_equities_intraday_from_parquet(tmp_path, "1h") == {"symbols": 1, "rows": 1}
    assert client.replace_equities_intraday_from_parquet(tmp_path, "5m") == {"symbols": 1, "rows": 1}
    assert ("AAPL", 11, "equity", "SMART") in conn.copied_rows
    assert (ts, 11, 1.0, 2.0, 0.5, 1.5, 100) in conn.copied_rows
    assert ("MSFT", 22, "equity", "SMART") in conn.copied_rows
    assert (ts, 22, 3.0, 4.0, 2.5, 3.5, 200) in conn.copied_rows


def test_intraday_unsupported_timeframe_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported intraday timeframe"):
        make_client(FakeConnection()).replace_equities_intraday_from_parquet(tmp_path, "15m")


def test_intraday_empty_timeframe_returns_zero(tmp_path: Path) -> None:
    assert make_client(FakeConnection()).replace_equities_intraday_from_parquet(tmp_path, "1h") == {
        "symbols": 0,
        "rows": 0,
    }


def test_intraday_row_requires_datetime_and_timezone() -> None:
    with pytest.raises(ValueError, match="must be datetime"):
        postgres_client_module._intraday_row({"bar_timestamp": "2026-01-02"})

    with pytest.raises(ValueError, match="tz-aware"):
        postgres_client_module._intraday_row({"bar_timestamp": datetime(2026, 1, 2)})


def test_date_timestamp_and_json_normalization_helpers() -> None:
    assert postgres_client_module._as_date(datetime(2026, 1, 2, 3, 4)) == date(2026, 1, 2)
    assert postgres_client_module._as_date("2026-01-03") == date(2026, 1, 3)
    assert postgres_client_module._parse_ts(datetime(2026, 1, 2, 3, 4)) == datetime(
        2026, 1, 2, 3, 4, tzinfo=timezone.utc
    )
    payload = postgres_client_module._jsonb(
        {"ts": datetime(2026, 1, 2, tzinfo=timezone.utc), "path": Path("/tmp/a.parquet")}
    )
    assert payload.obj == {"ts": "2026-01-02T00:00:00+00:00", "path": "/tmp/a.parquet"}


def test_first_parquet_int_returns_none_for_empty_file(tmp_path: Path) -> None:
    path = tmp_path / "empty.parquet"
    pq.write_table(pa.Table.from_pylist([], schema=pa.schema([("symbol_id", pa.int64())])), path)

    assert postgres_client_module._first_parquet_int(path, "symbol_id") is None


def test_telemetry_jsonl_import_maps_payload(tmp_path: Path) -> None:
    path = tmp_path / "telemetry.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ts": "2026-01-02T03:04:05+00:00",
                        "source": "ib",
                        "event": "farm_state",
                        "farm": "usfarm",
                        "state": "ok",
                        "code": 2104,
                        "req_id": 7,
                        "message": "connected",
                    }
                ),
                "",
                "{bad json",
            ]
        )
    )
    conn = FakeConnection()

    counts = make_client(conn).replace_telemetry_from_jsonl(path)

    assert counts == {"rows": 1, "skipped": 1}
    row = conn.copied_rows[0]
    assert row[:8] == (
        datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        "ib",
        "farm_state",
        "usfarm",
        "ok",
        2104,
        7,
        "connected",
    )


def test_missing_telemetry_jsonl_returns_zero(tmp_path: Path) -> None:
    assert make_client(FakeConnection()).replace_telemetry_from_jsonl(tmp_path / "missing.jsonl") == {
        "rows": 0,
        "skipped": 0,
    }


def test_quality_jsonl_import_maps_detail_and_missing_file(tmp_path: Path) -> None:
    missing = make_client(FakeConnection()).replace_quality_flags_from_jsonl(tmp_path / "missing.jsonl")
    assert missing == {"rows": 0, "skipped": 0}

    path = tmp_path / "quality.jsonl"
    path.write_text(
        json.dumps(
            {
                "ts": "2026-01-02T03:04:05+00:00",
                "source": "ib",
                "ticker": "AAPL",
                "timeframe": "1h",
                "parquet_path": tmp_path / "a.parquet",
                "category": "interior_gaps",
                "severity": "warning",
                "detail": {"missing": [date(2026, 1, 2)]},
            },
            default=str,
        )
    )
    conn = FakeConnection()

    counts = make_client(conn).replace_quality_flags_from_jsonl(path)

    assert counts == {"rows": 1, "skipped": 0}
    row = conn.copied_rows[0]
    assert row[:7] == (
        datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        "ib",
        "AAPL",
        "1h",
        str(tmp_path / "a.parquet"),
        "interior_gaps",
        "warning",
    )
    assert row[7].obj == {"missing": ["2026-01-02"]}
