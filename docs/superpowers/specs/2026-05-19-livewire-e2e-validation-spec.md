# Livewire E2E Validation Spec

## Purpose

Validate the implemented Livewire feature set through real operator entrypoints and real storage artifacts, not only pytest unit/integration coverage.

## Scope

This E2E pass covers:
- Sub-A reliability foundation: telemetry JSONL, quality-audit JSONL, quality report CLI, coverage report CLI, and watchdog marker behavior.
- Sub-B Postgres analytical publish target: rebuild from bronze parquet plus reliability artifacts, then smoke-check table counts.
- Sub-C Massive daily validation: explicit `daily --source massive` path, IB preflight bypass, Massive fetch, parquet merge, and quality sidecar/audit emission.
- DuckDB retirement: active command surface and active runtime/operator docs contain no DuckDB dependency path.

This E2E pass does not cover:
- IB historical live fetch quality, unless IB Gateway is explicitly selected for a live run.
- Intraday Sub-D live backfills.
- R2 upload/download.
- Email delivery to a real SMTP recipient. Marker and command behavior are checked; SMTP is a separate operations test.

## Environment Gates

- A local temp warehouse must be used for destructive write tests.
- `MASSIVE_API_KEY` must be configured before the live Sub-C Massive leg can run.
- A Postgres DSN must be available before the Sub-B publish leg can run.
- Live credentials must never be printed into logs or final output.
- DuckDB checks must scan active runtime/operator paths, while preserving historical design docs.

## Required Artifacts

The E2E run must create an isolated directory containing:
- `data-lake/bronze/asset_class=equity/symbol=<ticker>/1d.parquet`
- `data-lake/bronze/asset_class=equity/symbol=<ticker>/1h.parquet`
- `data-lake/bronze/asset_class=equity/symbol=<ticker>/5m.parquet`
- `logs/telemetry.jsonl`
- `logs/quality_audit.jsonl`
- `logs/coverage_<date>.log`
- `logs/quality_summary_<date>.marker`, when `report --email` is intentionally exercised

## E2E Matrix

### E2E-0 Fixture Build

Command shape:

```bash
python <fixture-builder>
```

Expected evidence:
- AAPL and MSFT daily parquet snapshots exist.
- AAPL intraday `1h.parquet` and `5m.parquet` snapshots exist.
- Telemetry JSONL contains at least one `ib` farm state and one `massive` request event.
- Quality audit JSONL contains at least one `massive` quality flag.

### E2E-1 Sub-A Reliability

Commands:

```bash
python scripts/livewire_quality.py report --view summary --since 30d --telemetry-path <tmp>/logs/telemetry.jsonl --audit-path <tmp>/logs/quality_audit.jsonl
python scripts/livewire_quality.py report --view quality --since 30d --severity warning --telemetry-path <tmp>/logs/telemetry.jsonl --audit-path <tmp>/logs/quality_audit.jsonl
MDW_WAREHOUSE_DIR=<tmp> python scripts/livewire_quality.py coverage --target-date 2024-01-03 --no-recover
```

Expected evidence:
- Summary output lists source event counts and quality flag categories.
- Quality output lists the seeded `massive/AAPL/1d` flag.
- Coverage output writes a coverage log under the temp warehouse and reports `1d`, `1h`, and `5m` coverage.

### E2E-2 Sub-B Postgres Publish

Commands:

```bash
python scripts/livewire_store.py rebuild-postgres --dsn <dsn> --schema <e2e_schema> --bronze-dir <tmp>/data-lake/bronze/asset_class=equity --timeframe all --include-reliability --telemetry-path <tmp>/logs/telemetry.jsonl --quality-audit-path <tmp>/logs/quality_audit.jsonl
python scripts/livewire_store.py smoke-postgres --dsn <dsn> --schema <e2e_schema> --ensure-schema
```

Expected evidence:
- Rebuild reports non-zero daily, 1h, 5m, telemetry, and quality rows.
- Smoke output returns `SELECT 1 ok`.
- Smoke table counts match fixture counts.

### E2E-3 Sub-C Massive Live Daily

Commands:

```bash
MDW_DATA_LAKE=<tmp>/data-lake MDW_TELEMETRY_PATH=<tmp>/logs/telemetry.jsonl MDW_QUALITY_AUDIT_PATH=<tmp>/logs/quality_audit.jsonl python scripts/livewire_ingest.py daily --asset-class equity --source massive --preset <tmp>/preset.json --target-date 2024-01-03
python <parquet-verifier>
```

Expected evidence:
- Command exits zero.
- Output reports `Source massive`, `Bars inserted`, and `Validation issues`.
- AAPL parquet has a `2024-01-03` row after the run.
- No IB preflight is required for this command.

If `MASSIVE_API_KEY` is unavailable, this leg is blocked, not passed.

### E2E-4 DuckDB Removal

Commands:

```bash
python scripts/livewire_store.py --help
rg -n "DuckDB|duckdb|DBClient|db_client|tmp_duckdb|rebuild-duckdb|rebuild_duckdb|market\\.duckdb" README.md CLAUDE.md AGENTS.md .codex/project-memory.md clients livewire_scripts scripts pyproject.toml tests --glob '!tests/test_duckdb_retirement.py'
```

Expected evidence:
- Storage help lists no DuckDB command.
- Active reference inventory has no matches outside the regression test.

## Pass/Fail Rules

- An E2E leg passes only when its commands run in this session and the expected artifact/output is observed.
- A live-gated leg with missing credentials or service access is `blocked`, not `passed`.
- Any command returning non-zero unexpectedly is `failed` unless the expected result is an absence scan such as `rg` returning 1 for no matches.
- Cleanup must remove temp schemas and generated temp files unless the user asks to keep evidence artifacts.
