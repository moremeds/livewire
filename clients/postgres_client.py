"""Postgres analytical publish client."""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import psycopg

from clients.postgres_schema import iter_schema_statements, validate_schema_name


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
