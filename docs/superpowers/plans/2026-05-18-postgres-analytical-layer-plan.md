# Postgres Analytical Layer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task.

**Goal:** Add Sub-B of the Livewire redesign: a Postgres analytical publish layer rebuilt from canonical bronze parquet and Sub-A reliability artifacts.

**Architecture:** Bronze parquet remains the system of record and the live ingestion write path remains parquet-first. Postgres is an analytical/query target populated by explicit rebuild/import commands, mirroring the existing DuckDB rebuild contract while adding durable relational access for market data, reliability events, and future Sub-C/Sub-E providers. The implementation must not make daily ingestion write directly to Postgres.

**Tech Stack:** Python 3.13, PyArrow/Parquet, `psycopg` for Postgres access, pytest with fake connection tests plus live-gated Postgres integration tests through `MDW_TEST_POSTGRES_DSN`, existing `rich` CLI style.

---

## Scope

Sub-B includes:
- Postgres schema for analytical tables under schema `md`.
- A `PostgresClient` that creates schema, replaces analytical tables from bronze parquet, and imports reliability JSONL artifacts.
- A CLI equivalent to `scripts/rebuild_duckdb_from_parquet.py`: `scripts/rebuild_postgres_from_parquet.py`.
- Documentation for local/operator usage, configuration, and verification.

Sub-B explicitly excludes:
- Making Postgres canonical for ingestion.
- Writing to Postgres from `fetch_ib_historical.py`, `daily_update.py`, `backfill_intraday.py`, or `run_ib_fetch_robust.py`.
- Removing DuckDB or replacing DuckDB-backed helper queries. DuckDB retirement is deferred to follow-up phase **Sub-B2: Retire DuckDB Analytical Layer** after Postgres is proven with real fixtures and operator smoke tests.
- Sub-C provider work (`Massive`, extended `UW`).
- Sub-D new timeframes beyond existing `1d`, `1h`, and `5m`.
- Sub-E options chains.
- Sub-F gold/factor tables.

## Design Decisions

1. **Postgres is a publish target, not a source of truth.**
   `~/market-warehouse/data-lake/bronze/...` stays canonical. Postgres rebuilds are replayable and destructive by default for selected tables, matching current DuckDB rebuild semantics.

2. **Use Python COPY from parsed parquet/JSONL, not Postgres parquet extensions.**
   Avoid requiring `parquet_fdw`, `COPY FROM PROGRAM`, superuser privileges, or server-side filesystem access. The client reads local parquet with PyArrow and streams rows through `psycopg`.

3. **Keep DuckDB intact during Sub-B.**
   DuckDB remains available for local ad hoc analytics and existing tests. Sub-B adds parallel Postgres publish commands rather than replacing the existing `DBClient` immediately.
   Full DuckDB dependency removal is intentionally out of scope for this plan because current runtime helpers still use DuckDB for direct parquet queries and row counts.

4. **Reliability artifacts move into queryable tables.**
   Sub-A JSONL files remain append-only artifacts. Sub-B imports them into `md.telemetry_events` and `md.quality_flags` so operators can query farms, quality flags, and market data in one database.

5. **Unit tests use fakes; SQL semantics use an optional live database gate.**
   Fake connection tests are enough for argument parsing, call ordering, and failure paths. Schema DDL, FK behavior, `jsonb`, temp tables, and `COPY` are validated by tests that skip unless `MDW_TEST_POSTGRES_DSN` points at a disposable Postgres database.

6. **COPY uses psycopg row-by-row adaptation, not CSV text generation.**
   Use `cursor.copy(COPY ... FROM STDIN)` and `copy.write_row(row)` with composed identifiers. Do not specify `FORMAT CSV`, delimiters, or null markers when using `write_row()`. Psycopg adapts Python `date`, tz-aware `datetime`, `int`, `float`, `str`, `None`, and `Jsonb(...)` values through its normal adaptation path.

7. **Replace counts mean staged rows, not retained table size.**
   Methods return the number of symbols/rows read from the current source artifact and written in that call. They do not report total table size after the call.

## Implementation Patterns To Follow

**COPY helper shape:**
```python
from psycopg import sql


def _copy_rows(cur, schema: str, table: str, columns: list[str], rows) -> int:
    stmt = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )
    copied = 0
    with cur.copy(stmt) as copy:
        for row in rows:
            copy.write_row(row)
            copied += 1
    return copied
```

**Parquet batch helper shape:**
```python
import pyarrow.parquet as pq


def _iter_parquet_dicts(path: Path, columns: list[str], batch_size: int):
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
        yield from batch.to_pylist()
```

**Type normalization rules:**
- Daily `trade_date`: pass Python `datetime.date`.
- Intraday `bar_timestamp`: require a tz-aware `datetime`, convert to UTC before `COPY`.
- Numeric OHLC values: `float(...)`.
- Volume/open interest IDs: `int(...)`.
- JSON payload/detail: recursively convert `date`, `datetime`, and `Path` to strings, then wrap with `psycopg.types.json.Jsonb`.
- Missing optional JSON payload/detail becomes `{}`.
- Never stringify dates/timestamps for typed date/timestamptz columns.

**Live test DSN contract:**
- `MDW_TEST_POSTGRES_DSN` must point at a disposable database where the test user can `CREATE SCHEMA` and `DROP SCHEMA`.
- Live tests must create a unique schema name such as `md_test_<uuid>` and pass it as `schema=...`.
- Live tests must never use `public`, `md`, or the operator's `MDW_POSTGRES_SCHEMA`.
- Teardown must run `DROP SCHEMA IF EXISTS <test_schema> CASCADE`.
- If `MDW_TEST_POSTGRES_DSN` is unset, tests marked `postgres_live` skip cleanly and the PR must state that live DB validation was skipped.

## Dependency Graph

- T0 -> T1 -> T2 -> T3
- T3 -> T4
- T3 -> T5
- T3 -> T6
- T3 -> T7
- T3 -> T9
- T4, T5, T6, T7 -> T8
- T8, T9 -> T10
- T10 -> T11 -> T12
- T12 -> T13

## Tasks

### Task 0: Preflight And Branch Setup

**Files:**
- No edits expected.

**Steps:**
1. Confirm the repo is on `main`, aligned with `origin/main`, and clean:
   ```bash
   git status --short --branch
   git fetch --prune
   git pull --ff-only
   ```
2. Run the current baseline gate:
   ```bash
   source ~/market-warehouse/.venv/bin/activate
   python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning
   ```
3. Create the feature branch:
   ```bash
   git checkout -b feat/postgres-analytical-layer
   ```
4. If the baseline gate fails before any Sub-B edits, stop and report the failure instead of starting implementation.

depends_on: []

### Task 1: Add Postgres Dependency And Configuration Surface

**Files:**
- Modify: `pyproject.toml`
- Modify: `.env.example`
- Modify: `scripts/setup_market_warehouse.sh`
- Test: no test-only dependency task

**Steps:**
1. Add `psycopg[binary]` as the Postgres client dependency in the project dependency configuration if `pyproject.toml` grows a dependency list; otherwise keep the project metadata unchanged and add the runtime package to `scripts/setup_market_warehouse.sh`, which is the current bootstrap install path.
   - Add a `postgres_live` pytest marker to `pyproject.toml` because the repo uses `--strict-markers`.
2. Install it into the active developer venv before running tests:
   ```bash
   source ~/market-warehouse/.venv/bin/activate
   python -m pip install 'psycopg[binary]'
   ```
3. Add non-secret env placeholders:
   - `MDW_POSTGRES_DSN=postgresql://user:password@localhost:5432/livewire`
   - `MDW_POSTGRES_SCHEMA=md`
   - `MDW_TEST_POSTGRES_DSN=postgresql://user:password@localhost:5432/livewire_test`
4. Do not add real credentials.
5. Run:
   ```bash
   source ~/market-warehouse/.venv/bin/activate
   python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing
   ```
6. Commit:
   ```bash
   git add pyproject.toml .env.example scripts/setup_market_warehouse.sh
   git commit -m "chore: add postgres analytical config"
   ```

depends_on: [T0]

### Task 2: Define Postgres Schema SQL

**Files:**
- Create: `clients/postgres_schema.py`
- Test: `tests/test_postgres_schema.py`

**Identifier safety:**
- Expose a `validate_schema_name(schema: str) -> str` helper that accepts only `^[A-Za-z_][A-Za-z0-9_]*$`.
- Build SQL through `psycopg.sql.Identifier` in `PostgresClient` when executing against a real connection. The SQL block below is the target shape, not permission to use raw f-string interpolation with untrusted schema text.

**Required tables:**
```sql
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.symbols (
    symbol_id bigint PRIMARY KEY,
    symbol text NOT NULL,
    asset_class text NOT NULL,
    venue text NOT NULL,
    UNIQUE (symbol, asset_class)
);

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
);

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
);

CREATE TABLE IF NOT EXISTS {schema}.equities_1h (
    bar_timestamp timestamptz NOT NULL,
    symbol_id bigint NOT NULL REFERENCES {schema}.symbols(symbol_id),
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume bigint NOT NULL,
    PRIMARY KEY (bar_timestamp, symbol_id)
);

CREATE TABLE IF NOT EXISTS {schema}.equities_5m (
    bar_timestamp timestamptz NOT NULL,
    symbol_id bigint NOT NULL REFERENCES {schema}.symbols(symbol_id),
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume bigint NOT NULL,
    PRIMARY KEY (bar_timestamp, symbol_id)
);

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
    payload jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS {schema}.quality_flags (
    id bigserial PRIMARY KEY,
    ts timestamptz NOT NULL,
    source text NOT NULL,
    ticker text,
    timeframe text NOT NULL DEFAULT '1d',
    parquet_path text,
    category text NOT NULL,
    severity text NOT NULL,
    detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb
);
```

**Tests:**
- `test_schema_sql_contains_expected_tables`
- `test_schema_name_is_quoted`
- `test_invalid_schema_name_rejected`
- `test_schema_sql_contains_quality_timeframe_and_detail_columns`
- `test_schema_sql_contains_telemetry_code_req_id_and_message_columns`

**Verification:**
```bash
python -m pytest tests/test_postgres_schema.py -q --cov=clients.postgres_schema --cov-report=term-missing
```

depends_on: [T1]

### Task 3: Add Postgres Client Lifecycle And Schema Creation

**Files:**
- Create: `clients/postgres_client.py`
- Modify: `clients/__init__.py`
- Test: `tests/test_postgres_client.py`
- Test: `tests/test_postgres_client_live.py`

**Behavior:**
- `PostgresClient(dsn: str | None = None, schema: str | None = None, connect_factory=None)`
- Reads `MDW_POSTGRES_DSN` and `MDW_POSTGRES_SCHEMA`.
- Raises a clear `ValueError` when no DSN is supplied.
- Supports context manager lifecycle.
- `ensure_schema()` executes all schema SQL in a transaction.

**Tests:**
- Fake connection/cursor records SQL calls, no live Postgres required.
- Missing DSN error is explicit.
- Context manager closes the connection.
- `ensure_schema()` starts and commits a transaction.
- Live-gated test `tests/test_postgres_client_live.py::test_live_ensure_schema_creates_tables` skips unless `MDW_TEST_POSTGRES_DSN` is set, creates a unique temporary schema, runs `ensure_schema()`, verifies the required tables exist, and drops the schema in teardown. Mark it with `@pytest.mark.postgres_live`.

**Verification:**
```bash
python -m pytest tests/test_postgres_client.py -q --cov=clients.postgres_client --cov-report=term-missing
```

depends_on: [T2]

### Task 4: Implement Daily Equity And Volatility Replace From Parquet

**Files:**
- Modify: `clients/postgres_client.py`
- Test: `tests/test_postgres_client.py`

**Behavior:**
- Add `replace_equities_from_parquet(bronze_dir, asset_class="equity", venue="SMART") -> dict[str, int]`.
- Read `symbol=*/1d.parquet`.
- Use `pyarrow.parquet.ParquetFile.iter_batches(batch_size=...)` for row streaming; default batch size is `50_000` and may be overridden by `MDW_POSTGRES_COPY_BATCH_SIZE`.
- Use a transaction:
  - create temporary staging tables
  - stream symbols and rows into staging
  - upsert staged symbols into `md.symbols` by `symbol_id`, updating `symbol`, `asset_class`, and `venue`
  - delete target `md.equities_daily` rows for all existing `symbol_id` values whose `md.symbols.asset_class = :asset_class`
  - insert staged daily rows into `md.equities_daily`
- Do not delete `md.symbols` during this task. `md.equities_1h` and `md.equities_5m` reference `md.symbols`, so deleting symbols during a daily rebuild can violate FKs after intraday data exists. Stale no-row symbols are acceptable in Sub-B and can be cleaned by a later maintenance task that checks all referencing tables.
- Return `{"symbols": staged_symbol_count, "rows": staged_daily_row_count}`. It is valid for the final `md.symbols` table to contain more rows because older symbols may be retained for FK safety.

**Tests:**
- Empty bronze dir returns zero counts without deleting unrelated asset classes.
- Equity parquet produces expected symbol and row COPY payloads.
- Volatility call stores `asset_class="volatility"` and `venue="CBOE"`.
- Rollback is called if COPY or insert fails.
- Live-gated test loads a tiny parquet fixture into a temporary schema and verifies actual row counts and FK validity.
- Live-gated regression: load intraday rows for a symbol, then run daily `replace_equities_from_parquet()` for that same symbol and verify the intraday rows still exist.
- Live-gated regression: seed an old daily row for an asset-class symbol absent from the new parquet input, run `replace_equities_from_parquet()`, and verify the old daily row is gone while the symbol row may remain.

**Verification:**
```bash
python -m pytest tests/test_postgres_client.py -q --cov=clients.postgres_client --cov-report=term-missing
```

depends_on: [T3]

### Task 5: Implement Futures Replace From Parquet

**Files:**
- Modify: `clients/postgres_client.py`
- Test: `tests/test_postgres_client.py`

**Behavior:**
- Add `replace_futures_from_parquet(bronze_dir) -> dict[str, int]`.
- Read `symbol=*/1d.parquet`.
- Replace `md.futures_daily` from parquet rows.
- Keep `md.symbols` untouched for futures, matching the DuckDB behavior.
- Use `pyarrow.parquet.ParquetFile.iter_batches(batch_size=...)`.
- For non-empty parquet input, delete all existing rows from `md.futures_daily` inside the same transaction before inserting staged rows.
- Return `{"rows": staged_futures_row_count}`.

**Tests:**
- Futures parquet rows are streamed with contract fields.
- Empty directory returns `{"rows": 0}`.
- Non-empty load replaces older futures rows instead of appending.
- Failure rolls back.
- Live-gated test loads a tiny futures parquet fixture into a temporary schema.

depends_on: [T3]

### Task 6: Implement Intraday Replace From Parquet

**Files:**
- Modify: `clients/postgres_client.py`
- Test: `tests/test_postgres_client.py`

**Behavior:**
- Add `replace_equities_intraday_from_parquet(bronze_dir, timeframe) -> dict[str, int]`.
- Support only `1h` and `5m`.
- Reject unsupported timeframes with `ValueError`.
- Populate `md.equities_1h` or `md.equities_5m`.
- Ensure referenced `symbol_id` values exist in `md.symbols`; if the target schema has no matching symbols, upsert minimal `md.symbols` rows from the parquet hive `symbol` partition with `asset_class='equity'` and `venue='SMART'` before loading intraday bars.
- Replace intraday rows for staged `symbol_id` values only, not the entire timeframe table. This lets `--timeframe 1h --bronze-dir <subset>` refresh a subset without deleting unrelated symbols.
- Use `pyarrow.parquet.ParquetFile.iter_batches(batch_size=...)`.

**Tests:**
- `1h` parquet populates `equities_1h`.
- `5m` parquet populates `equities_5m`.
- Unsupported timeframe raises.
- Live-gated tests load tiny `1h` and `5m` fixtures into a temporary schema and verify timestamptz roundtrip.
- Live-gated test verifies intraday loading into an empty schema creates the minimal symbol row required by the FK.

depends_on: [T3]

### Task 7: Import Reliability JSONL Into Postgres

**Files:**
- Modify: `clients/postgres_client.py`
- Test: `tests/test_postgres_client.py`

**Behavior:**
- Add `replace_telemetry_from_jsonl(path) -> dict[str, int]`.
- Add `replace_quality_flags_from_jsonl(path) -> dict[str, int]`.
- Missing files return zero counts.
- Malformed JSON lines are skipped and counted.
- Preserve full original event as JSONB `payload`.
- Map first-class telemetry fields into columns: `ts`, `source`, `event`, `farm`, `state`, `code`, `req_id`, `message`.
- Map first-class audit fields into columns: `ts`, `source`, `ticker`, `timeframe`, `parquet_path`, `category`, `severity`, `detail`.

**Tests:**
- Telemetry JSONL maps `ts/source/event/farm/state/payload`.
- Quality audit JSONL maps `ts/source/ticker/category/severity/payload`.
- Quality audit JSONL maps `timeframe`, `parquet_path`, and `detail`.
- Bad lines are skipped without aborting import.
- Live-gated test imports JSONL fixtures and verifies `jsonb` fields can be queried.

depends_on: [T3]

### Task 8: Add Postgres Rebuild CLI

**Files:**
- Create: `scripts/rebuild_postgres_from_parquet.py`
- Test: `tests/test_rebuild_postgres_from_parquet.py`

**CLI:**
```bash
python scripts/rebuild_postgres_from_parquet.py \
  --asset-class equity \
  --timeframe all \
  --bronze-dir ~/market-warehouse/data-lake/bronze/asset_class=equity
```

**Flags:**
- `--dsn`
- `--schema`
- `--asset-class {equity,volatility,futures}`
- `--timeframe {1d,1h,5m,all}`
- `--bronze-dir`
- `--include-reliability`
- `--telemetry-path`
- `--quality-audit-path`

**Tests:**
- Default bronze path derives from `--asset-class`.
- Equity `all` calls daily, `1h`, and `5m` loaders.
- Volatility calls daily loader with CBOE venue.
- Futures calls futures loader.
- `--include-reliability` calls both JSONL importers.
- Missing bronze dir raises the same style of `FileNotFoundError` as the DuckDB rebuild script.
- Missing optional timeframe data with `--timeframe all` prints a skip message for that timeframe instead of failing the whole run when at least one requested parquet family exists.
- Missing explicitly requested timeframe data, such as `--timeframe 5m`, raises `FileNotFoundError`.

depends_on: [T4, T5, T6, T7]

### Task 9: Add Optional Live Postgres Smoke Script

**Files:**
- Create: `scripts/smoke_postgres_analytical.py`
- Test: `tests/test_smoke_postgres_analytical.py`

**Behavior:**
- Requires `MDW_POSTGRES_DSN`.
- Runs `SELECT 1`, ensures schema, prints table counts.
- Prints per-table counts for `symbols`, `equities_daily`, `futures_daily`, `equities_1h`, `equities_5m`, `telemetry_events`, and `quality_flags`.
- Does not mutate market data unless `--ensure-schema` is supplied.

**Tests:**
- Missing DSN exits with a clear message.
- With fake client, prints expected counts.

depends_on: [T3]

### Task 10: Documentation Sweep

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `.codex/project-memory.md`

**Required content:**
- Postgres is an analytical publish target.
- Bronze parquet remains canonical.
- DuckDB remains supported.
- `MDW_POSTGRES_DSN` and `MDW_POSTGRES_SCHEMA`.
- Rebuild examples for equity, volatility, futures, intraday, and reliability imports.
- Explicit warning that live ingestion does not write Postgres.
- Rollback guidance: because Postgres is a replayable publish target, rollback means dropping/truncating the target schema and rerunning the rebuild from bronze parquet and JSONL artifacts.
- Notes that futures and intraday manual verification commands are conditional on corresponding bronze parquet existing.

depends_on: [T8, T9]

### Task 11: Full Test And Coverage Gate

**Files:**
- No edits expected.

**Run:**
```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning
```

**Expected:** 100% coverage.

depends_on: [T10]

### Task 12: Manual Verification Checklist

**Operator commands:**
```bash
export MDW_POSTGRES_DSN='postgresql://...'
python scripts/smoke_postgres_analytical.py --ensure-schema
python scripts/rebuild_postgres_from_parquet.py --asset-class equity --timeframe 1d
python scripts/rebuild_postgres_from_parquet.py --asset-class volatility
python scripts/rebuild_postgres_from_parquet.py --include-reliability

# Conditional: run only when corresponding bronze parquet exists.
test -n "$(find ~/market-warehouse/data-lake/bronze/asset_class=futures -path '*/1d.parquet' -print -quit 2>/dev/null)" && \
  python scripts/rebuild_postgres_from_parquet.py --asset-class futures
test -n "$(find ~/market-warehouse/data-lake/bronze/asset_class=equity -path '*/1h.parquet' -print -quit 2>/dev/null)" && \
  python scripts/rebuild_postgres_from_parquet.py --asset-class equity --timeframe 1h
test -n "$(find ~/market-warehouse/data-lake/bronze/asset_class=equity -path '*/5m.parquet' -print -quit 2>/dev/null)" && \
  python scripts/rebuild_postgres_from_parquet.py --asset-class equity --timeframe 5m
```

**Expected checks:**
- `md.symbols` row count matches published equity/volatility symbols.
- `md.equities_daily` count matches loaded parquet rows.
- `md.futures_daily` has futures rows when futures bronze exists.
- `md.telemetry_events` and `md.quality_flags` are queryable when JSONL files exist.
- Optional live integration test command passes when `MDW_TEST_POSTGRES_DSN` is configured:
  ```bash
  MDW_TEST_POSTGRES_DSN="$MDW_POSTGRES_DSN" python -m pytest tests/test_postgres_client_live.py -q
  ```

depends_on: [T11]

### Task 13: Ship Branch

**Run:**
```bash
git status --short
git log --oneline --decorate -5
git push -u origin feat/postgres-analytical-layer
gh pr create --title "Sub-B: Postgres Analytical Layer" --body-file /tmp/livewire-postgres-sub-b-pr.md
```

**PR body must include:**
- Summary of Postgres publish target.
- Statement that bronze parquet remains canonical.
- Test command and 100% coverage result.
- Manual Postgres smoke checklist status.

depends_on: [T12]

## Review Gates

- After T3: confirm schema/client shape before loading market data.
- After T8: confirm CLI semantics before docs.
- After T11: stop on any coverage or runtime-warning failure.
- Before T13: confirm no secrets in `.env`, docs, tests, or git diff.

## Follow-Up Candidates After Sub-B

- Sub-B2: Retire DuckDB Analytical Layer. Make Postgres the only analytical database, remove `DBClient`/`rebuild_duckdb_from_parquet.py`, replace in-memory DuckDB parquet helper queries with PyArrow/Polars-style readers, and remove DuckDB from bootstrap, docs, Docker, and tests.
- Sub-C: Massive/UW provider integration and real row-count anomaly detection.
- Sub-D: Additional intraday timeframes and Postgres tables if needed.
- Sub-F: Gold/factor tables built on top of Postgres analytical tables.
