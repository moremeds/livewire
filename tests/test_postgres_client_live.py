from __future__ import annotations

import os
import uuid

import psycopg
import pytest
from psycopg import sql

from clients.postgres_client import PostgresClient
from clients.postgres_schema import POSTGRES_TABLES


@pytest.mark.postgres_live
def test_live_ensure_schema_creates_tables() -> None:
    dsn = os.environ.get("MDW_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("MDW_TEST_POSTGRES_DSN is not set")

    schema = f"md_test_{uuid.uuid4().hex}"
    with psycopg.connect(dsn) as conn:
        try:
            client = PostgresClient(dsn=dsn, schema=schema)
            client.ensure_schema()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    """,
                    (schema,),
                )
                tables = {row[0] for row in cur.fetchall()}
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
                )

    assert set(POSTGRES_TABLES).issubset(tables)
