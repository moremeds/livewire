# Todo

Active task lists live here. Completed sections move to [archive.md](archive.md).

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
