#!/usr/bin/env python3
"""Smoke-check the Postgres analytical publish target."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.postgres_client import PostgresClient
from clients.postgres_schema import POSTGRES_TABLES, validate_schema_name


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=None, help="Postgres DSN; defaults to MDW_POSTGRES_DSN")
    parser.add_argument("--schema", default=None, help="Postgres schema; defaults to MDW_POSTGRES_SCHEMA or md")
    parser.add_argument("--ensure-schema", action="store_true", help="Create schema before counting tables")
    args = parser.parse_args(argv)

    dsn = args.dsn or os.environ.get("MDW_POSTGRES_DSN")
    if not dsn:
        print("MDW_POSTGRES_DSN is required unless --dsn is supplied", file=sys.stderr)
        return 2

    schema = validate_schema_name(args.schema or os.environ.get("MDW_POSTGRES_SCHEMA", "md"))
    with PostgresClient(dsn=dsn, schema=schema) as db:
        if args.ensure_schema:
            db.ensure_schema()
        with db.conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
            print("SELECT 1 ok")
            for table in POSTGRES_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                count = cur.fetchone()[0]
                print(f"{table}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
