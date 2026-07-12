# Todo

Active task lists live here. Completed sections move to [archive.md](archive.md).

## Corporate-action ingestion R2 (2026-07-13)

Dependency graph: `T1 -> T2 -> T3 -> T4`

- [x] **T1** (`depends_on: []`): Add strict Massive split/dividend models,
  bounded same-origin pagination, validation, exports, and focused tests.
- [x] **T2** (`depends_on: [T1]`): Add the revision-aware canonical corporate
  action store with correction lineage, safe cancellation semantics, atomic
  publication, and focused tests.
- [ ] **T3** (`depends_on: [T2]`): Add targeted/universe reconciliation CLI,
  dry-run behavior, telemetry counters, dispatch coverage, and focused tests.
- [ ] **T4** (`depends_on: [T3]`): Document operation and scheduling boundaries,
  run focused and CI-equivalent verification, and prepare the implementation PR.

## Daily partial-bar audit and repair (2026-07-11)

Dependency graph: `T1 -> T2 -> T3 -> T4 -> T5 -> T6`

- [x] **T1** (`depends_on: []`): Confirm QQQ against staged day aggregates,
  summed minute data, REST, and ingestion logs; identify the pre-close target-date
  root cause and verify the June 24 prevention fix.
- [x] **T2** (`depends_on: [T1]`): Add tested tooling to audit staged `day_aggs`
  against every existing equity `1d.parquet`, write a rollback-capable mismatch
  manifest, and apply only manifest rows through locked atomic merges.
- [x] **T3** (`depends_on: [T2]`): Run the complete read-only overlap audit and
  review mismatch counts, date range, field distribution, and symbol scope.
- [x] **T4** (`depends_on: [T3]`): Repair only audited mismatches from staged
  authoritative rows and preserve the original values in the manifest.
- [x] **T5** (`depends_on: [T4]`): Re-run the complete audit and require zero
  remaining staged-overlap mismatches; validate QQQ and a liquid-ticker sample.
- [x] **T6** (`depends_on: [T5]`): Validate full-history structure and REST overlap
  for common and deterministic uncommon ticker samples, write the repair report,
  run repository gates, and prepare the PR.

## Post-lift cleanups (2026-06-11)

These were surfaced during the move of `data-lake/` from internal SSD to `/Volumes/DATA_LAKE` (exFAT, symlinked back).

### Atomic-publish orphan sweep on BronzeClient init

`BronzeClient` writes per-ticker parquet via temp → validate → `os.replace()`. If a writer is killed mid-publish, the temp file (`.<filename>.parquet.<pid>.<nanotime>.tmp`) is orphaned. On 2026-06-11 we found 19 such orphans (~135 MB) across equity bronze, traced to two killed-batch events (PIDs 8479, 44972). One-shot manual sweep cleaned them, but the underlying gap remains: `BronzeClient.__init__` should sweep its own asset-class dir on construction. ~5 lines.

Path: `clients/bronze_client.py` (and `clients/intraday_bronze_client.py` — same pattern).

### AppleDouble sidecar tolerance in glob patterns

On exFAT, macOS creates `._foo.parquet` sidecars (AppleDouble format) for any file with xattrs. These sidecars are pure metadata, not parquet — DuckDB / PyArrow will crash if it tries to read them as data.

Already fixed:
- `clients/massive_flatfile_store.py:110` — spill glob filters `._*`.

Still vulnerable (read-only diagnostic, low priority):
- `livewire_scripts/warehouse_health_report.py:149` — `bronze_root.glob("asset_class=*/symbol=*/*.parquet")`. The `*.parquet` leaf matches `._foo.parquet`. Add `if not p.name.startswith("._")` filter.

Operational mitigation in place: a `find … -name '._*' -delete` sweep is safe to run anytime. Done on 2026-06-11 (7,758 sidecars cleaned from equity bronze).

### Planner uses wrong filesystem for capacity check

`livewire_scripts/flatfile_planner.py:42` calls `shutil.disk_usage(warehouse_dir)` on `~/market-warehouse/` (internal SSD). After the lift, the actual data target is the symlinked `data-lake/` subdir (exFAT). Result: planner reports ~63 GiB free instead of the real 10 TiB. `repair` mode is harmless (skips `require_capacity()`), but `backfill` mode would falsely reject any future full-history run.

Fix:

```python
target = warehouse_dir / "data-lake"
if not target.exists():
    target = warehouse_dir if warehouse_dir.exists() else warehouse_dir.parent
usage = shutil.disk_usage(target)
```

### Ticker renames need a rename map (or re-archive cron)

Surfaced by VSCO → VSXY (Victoria's Secret rename, early June 2026). On 2026-06-11 we archived `symbol=VSCO/` to `bronze-delisted/` so daily-update stops failing on it. But the running 5-year backfill will recreate `symbol=VSCO/` with pre-rename intraday (Massive's flat-files use ticker-as-of-trade-date, which is data-correct).

Two follow-ups:
- **Short term**: re-archive `symbol=VSCO/` after the backfill completes.
- **Long term**: a `renames.json` config consulted at publish/query time. Equivalent to a primary-key remap table. Lets continuous-history queries `SELECT * FROM bronze WHERE symbol IN (rename_chain('VSXY'))` work cleanly.

### Equity union "exit 1 after completed summary"

`daily_backfill_equity_union` exits non-zero when any ticker fails after all retries, even if 1,548 succeeded. The orchestrator treats this as recoverable ("exited 1 after completed summary; continuing") — good — but the underlying tickers stay failing.

Persistent set across multiple days: KFS, SLNO, AMWD, BK, CVGW, FFIC, MCW, THR, TSEOF, VRE. All actively traded; not delisted. Most likely a symbol-normalization mismatch in Massive's SIP feed (suffixes, dot-class shares, ADR variants). Need to inspect Massive's actual ticker for each name and add a normalization map in `MassiveClient`.

## Per-day publish cost is HDD-bound (2026-06-11)

The DATA_LAKE volume is a spinning HDD with ~73 MB/s sustained sequential throughput. The current per-ticker monolithic intraday layout (`symbol=AAPL/{1m,5m,30m,1h}.parquet`) means every single-day append touches ~16 MB per ticker × ~12K tickers = ~192 GB read + 192 GB write. Disk-floor wall-clock = ~88 min for one day.

Real-world numbers from 2026-06-11:
- 5-year backfill staging: ~5 s/day (pyarrow-native, in `260cbf1`).
- 5-year backfill publish: completed inside the staged window — bound by the same HDD throughput.
- One-day repair for 2026-06-10: **48 min** with `--workers 4` and the new pyarrow-native merge (`95182c2`).

Three paths to actually break the ~88-min floor, none of which are in PR #28:

1. **Per-month partition layout** for intraday parquet: `symbol=AAPL/1m/year=2026/month=06.parquet`. Appending within a month rewrites only that month (~22 days, ~5 % of touch surface) → projected ~5 min/day at current HDD throughput. Requires updating every reader that opens intraday parquet by path (Postgres rebuild, warehouse health report, quality checks, any analytical script in this repo or downstream). Plus a one-time migration script that splits each existing `1m.parquet` into per-month files atomically. Multi-day effort.

2. **Hot/cold storage tier**: recent N months on internal SSD (`~/market-warehouse-hot/`), historical archive on the HDD volume. Daily catch-up only writes the hot tier; a separate compaction step rolls aging data into cold. Read path has to union both. Same downstream-touch surface as option 1, plus the operational complexity of two locations.

3. **Per-day delta sidecar**: keep the monolithic `1m.parquet` for reads as-is, but write incremental updates to `1m.delta/date=YYYY-MM-DD.parquet`. A separate compaction job folds the deltas into the monolith periodically (weekly?). Every reader has to union the monolith with whatever's in the delta dir at read time. Less invasive than option 1 — the monolithic file's API stays — but adds a non-trivial compaction service and a read-time correctness invariant.

Recommendation: option 1, when we're ready to coordinate the reader updates. Until then, daily catch-up takes ~50–90 min on this hardware and that is the actual ceiling, not a software bug.
