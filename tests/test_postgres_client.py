from __future__ import annotations

import os

import pytest

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
