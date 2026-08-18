# Livewire — Adaptation Implementation Plan

Status: Draft / proposed
Date: 2026-06-14
Implements: `livewire-adaptation.md`
Context: four-system decoupling (livewire · apex · argon · xenon)

## Goal

Make livewire a **clean, stable, available read source** for apex, without
changing what it is. Two outcomes:

1. **A versioned read contract** apex builds against (the integration-critical deliverable).
2. **The hot bronze off the flaky HDD** onto SSD (the availability fix).

Everything is **additive and reversible**. The per-ticker single-file physical
layout stays **frozen** (R2 + Postgres + external readers depend on it) — this
plan *codifies and enforces* that layout, it does not change it.

## Constraints (non-negotiable)

- 95% coverage gate (`pyproject.toml fail_under`), ruff + pyright, secrets scanner, **PR + green CI before merge** (branch protection now enforces it).
- Atomic publish (`temp → validate → os.replace`) unchanged.
- Defaults unchanged when new env vars are unset (no surprise behavior for existing runs).

## Sequencing

```
Phase 0  Read contract        ──▶ unblocks apex immediately (location-agnostic)
Phase 1  Path consolidation   ──▶ lets bronze live apart from raw
Phase 2  SSD migration        ──▶ depends on Phase 1
Phase 3  gold/ guardrail      ──▶ independent, cheap
Phase 4  Multi-file migration ──▶ parallel track, not integration-blocking
```

Phase 0 has **no dependency** on the others — apex can start building
`LivewireOhlcProvider` against the contract before any infra moves, because the
contract references the bronze root abstractly (`$MDW_BRONZE_DIR`), not a path.

---

## Phase 0 — Bronze read contract (apex-facing)

**What:** publish + enforce a stable contract describing how to read bronze, so
apex (and any consumer) binds to a documented interface, not internal quirks.

**Deliverables:**
- `docs/contracts/bronze-read-contract-v1.md` — canonical layout
  (`asset_class=<>/symbol=<>/{1d,1m,5m,30m,1h}.parquet`), per-timeframe column
  schema + dtypes, the time/sort column, partition keys, zstd note, and the
  **DuckDB read recipe**:
  ```sql
  SELECT * FROM read_parquet(
    '$MDW_BRONZE_DIR/asset_class=equity/symbol=*/1m.parquet',
    hive_partitioning => true   -- yields a `symbol` column
  );
  ```
- `sql/bronze_views.sql` — DuckDB view definitions per (asset_class, timeframe)
  apex can load verbatim.
- `tests/test_bronze_read_contract.py` — **enforcing test** against a tiny
  fixture bronze tree: asserts each timeframe file is discoverable by the
  documented glob, has the documented columns/dtypes, is sorted on the time
  column, and is dup-free. A real-lake check is added behind
  `@pytest.mark.integration`.

**Acceptance:** the contract test fails if anyone changes the layout/schema →
the frozen contract is now machine-enforced. apex can read a fixture lake via the
published recipe.

**Rollback:** docs/tests only; nothing to roll back.

---

## Phase 1 — Path config consolidation

**What:** one canonical, independently-configurable bronze root, so bronze can
live on a different disk than `raw/`. Today config is split (hardcoded
`~/market-warehouse` in `bronze_client.py`/`intraday_bronze_client.py` vs
`MDW_WAREHOUSE_DIR` elsewhere).

**Changes:**
- Add `livewire_scripts/paths.py` (or `clients/paths.py`) resolver:
  - `MDW_BRONZE_DIR` (default: `${MDW_WAREHOUSE_DIR:-~/market-warehouse}/data-lake/bronze`)
  - `MDW_RAW_DIR`, `MDW_GOLD_DIR` likewise.
- Repoint `_DEFAULT_BRONZE_DIR` in `bronze_client.py` + `intraday_bronze_client.py`
  to the resolver. Replace remaining hardcoded `~/market-warehouse` reads
  (`check_gaps`, `health_check`, `fetch_fred_rates`, flatfile ingest, etc.).
- Constructors keep their `bronze_dir=` override (unchanged); only the *default* moves to the resolver.

**Tests:** env-var resolution, default fallback when unset, override precedence. 95% coverage.

**Acceptance:** with no env set, behavior is byte-identical to today. Setting
`MDW_BRONZE_DIR` relocates all reads/writes consistently. Full suite green.

**Rollback:** unset `MDW_BRONZE_DIR` → reverts to default path.

---

## Phase 2 — SSD migration (hot bronze → APFS)

**What:** move the 41 GB equity bronze to the internal SSD (`~` is already on
APFS/disk3, 82 GB free); keep `raw/` Massive staging on the HDD.

**Pre-flight:** confirm ≥ 45 GB SSD free; pause ingestion (no launchd run mid-copy).

**Steps:**
1. Create SSD target, e.g. `~/livewire-bronze/` (APFS).
2. **Resumable** copy (flaky enclosure!): `rsync -a --partial --append-verify --info=progress2 <hdd>/bronze/ ~/livewire-bronze/` — retries survive an enclosure drop.
3. **Validate:** file-count + per-file row-count diff (reuse the contract test's schema check) HDD vs SSD. Must match exactly.
4. **Cutover:** set `MDW_BRONZE_DIR=~/livewire-bronze` in repo `.env`, `~/market-warehouse/.env`, and the launchd env path (`run-daily-job` already loads `~/market-warehouse/.env`).
5. Keep the HDD bronze copy as **cold backup** for N days; then archive/remove.

**Downstream:** `sync_to_r2`, `rebuild_postgres_from_parquet` already take
`bronze_dir` → they follow `MDW_BRONZE_DIR` automatically once Phase 1 lands.

**Acceptance:** a daily ingest + an R2 sync + a Postgres rebuild all run green
against the SSD bronze; coverage/health checks pass; HDD only touched for `raw/`.

**Rollback:** unset `MDW_BRONZE_DIR` → reads/writes return to the HDD copy (still present).

**Risks:** enclosure drop mid-rsync → `--partial` resumes. exFAT→APFS resets
file times (cosmetic; content/rows validated). SSD tight at ~41/82 GB — monitor.

---

## Phase 3 — `gold/` guardrail

**What:** lock in that `gold/` is **price-derived data-plane tables only**
(adjusted returns, factor tables, universe membership) — **never TA/signals**
(those are apex's, persisted in apex). Prevents re-coupling.

**Deliverables:** `docs/contracts/gold-tier-policy.md` + a one-line note in `CLAUDE.md`.

**Acceptance:** policy documented; no code.

---

## Phase 4 — Intraday multi-file migration (parallel track)

**What:** the year-file + daily-tail + compaction layout from
`docs/superpowers/specs/2026-06-13-intraday-multifile-migration-design.md`.

**Status:** **not integration-blocking.** It's a reliability/throughput win for
livewire's own ingest (removes the per-ticker rewrite storm). SSD (Phase 2)
already softens the rewrite-storm pain, so this can wait. If pursued, it must
keep the Phase 0 read contract intact (the contract abstracts over file count,
so update `bronze_views.sql` to a directory dataset and the contract test follows).

---

## What apex needs first

**Phase 0 only.** Once the read contract + `bronze_views.sql` exist, apex builds
`LivewireOhlcProvider` against `$MDW_BRONZE_DIR` immediately — the SSD migration
(Phases 1–2) then happens transparently underneath, because apex reads the
abstract root, never a hardcoded path.
