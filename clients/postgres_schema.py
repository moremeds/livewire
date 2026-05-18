"""Postgres analytical schema definitions."""

from __future__ import annotations

import re
from collections.abc import Iterator


POSTGRES_TABLES = (
    "symbols",
    "equities_daily",
    "futures_daily",
    "equities_1h",
    "equities_5m",
    "telemetry_events",
    "quality_flags",
)

_SCHEMA_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_schema_name(schema: str) -> str:
    """Return a safe Postgres schema identifier or raise ValueError."""
    if not _SCHEMA_RE.fullmatch(schema):
        raise ValueError(f"Invalid Postgres schema name: {schema!r}")
    return schema


def iter_schema_statements(schema: str = "md") -> Iterator[str]:
    """Yield DDL statements for the analytical Postgres schema."""
    schema = validate_schema_name(schema)
    yield f"CREATE SCHEMA IF NOT EXISTS {schema}"
    yield f"""
CREATE TABLE IF NOT EXISTS {schema}.symbols (
    symbol_id bigint PRIMARY KEY,
    symbol text NOT NULL,
    asset_class text NOT NULL,
    venue text NOT NULL,
    UNIQUE (symbol, asset_class)
)""".strip()
    yield f"""
CREATE TABLE IF NOT EXISTS {schema}.equities_daily (
    trade_date date NOT NULL,
    symbol_id bigint NOT NULL REFERENCES {schema}.symbols(symbol_id),
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    adj_close double precision NOT NULL,
    volume bigint NOT NULL,
    PRIMARY KEY (trade_date, symbol_id)
)""".strip()
    yield f"""
CREATE TABLE IF NOT EXISTS {schema}.futures_daily (
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
    open_interest bigint NOT NULL,
    PRIMARY KEY (trade_date, contract_id)
)""".strip()
    yield f"""
CREATE TABLE IF NOT EXISTS {schema}.equities_1h (
    bar_timestamp timestamptz NOT NULL,
    symbol_id bigint NOT NULL REFERENCES {schema}.symbols(symbol_id),
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume bigint NOT NULL,
    PRIMARY KEY (bar_timestamp, symbol_id)
)""".strip()
    yield f"""
CREATE TABLE IF NOT EXISTS {schema}.equities_5m (
    bar_timestamp timestamptz NOT NULL,
    symbol_id bigint NOT NULL REFERENCES {schema}.symbols(symbol_id),
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume bigint NOT NULL,
    PRIMARY KEY (bar_timestamp, symbol_id)
)""".strip()
    yield f"""
CREATE TABLE IF NOT EXISTS {schema}.telemetry_events (
    id bigserial PRIMARY KEY,
    ts timestamptz NOT NULL,
    source text NOT NULL,
    event text NOT NULL,
    farm text,
    state text,
    code integer,
    req_id integer,
    message text,
    payload jsonb NOT NULL DEFAULT '{{}}'::jsonb
)""".strip()
    yield f"""
CREATE TABLE IF NOT EXISTS {schema}.quality_flags (
    id bigserial PRIMARY KEY,
    ts timestamptz NOT NULL,
    source text NOT NULL,
    ticker text,
    timeframe text NOT NULL DEFAULT '1d',
    parquet_path text,
    category text NOT NULL,
    severity text NOT NULL,
    detail jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    payload jsonb NOT NULL DEFAULT '{{}}'::jsonb
)""".strip()
