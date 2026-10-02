# Commodity intraday backfill feasibility — IB × 103-contract manifest (Task, read-only)

**Scope**: can Livewire backfill IB intraday history for the 103 commodity futures in `docs/audits/price-discovery/COMMODITY_BACKFILL_MANIFEST_2026-09-23.json`? Read-only source/FS/API probes; **no edits, no Bronze writes, no cursor changes, no bulk backfill, no commits** — commit none.

**Captured**: 2026-09-23 ~06:05–06:25 UTC. **Commit**: `f83e3df0` worktree `price-discovery`.

## 1. Verdict

**Yes for a bounded recent-depth seed** — source support exists, IB serves 1m/5m/30m/1h TRADES bars for the manifest's contract months, and no entitlement errors were observed on any probed root. **Not "5 years deep"**: code defaults to `years=5` but verified intraday depth is far shallower — 1-minute reaches ≥9 months and fails at ~15 months; 1-hour reaches ≥2.2 years (sparse for far-dated months) and fails at ~4.7 years. The manifest's `head_timestamp` is **daily** depth, not intraday.

## 2. Supported command / timeframes (source-verified)

`livewire_scripts/backfill_intraday.py` → CLI `intraday-backfill`:

```
livewire_ingest.py intraday-backfill --asset-class futures \
    --timeframe {1m|5m|30m|1h} (--tickers CL_202611 ... | --preset futures-commodity-*) \
    [--years N | --days N] [--skip-existing|--existing-only] [--max-tickers N] \
    [--host 127.0.0.1 --port 4001] [--dry-run]
```

- Source: **IB only** (`--source` = `ib`); `what_to_show="TRADES"`; serial per ticker, serial per chunk (`backfill_intraday.py:304-360`).
- Defaults `years=5`; chunks: 1m=1-day req, 5m=1-week, 30m/1h=~1-month (`:243-271`); cursor `--after` watermark support only under `--preset` (`:28-53`, `cursors/preset-*.json`).
- **RTH-only for futures**: fetch `use_rth=True`, validator `require_rth=True` + `is_trading_day` + exact-grid check (`intraday_bronze_client.py:115-152`, `backfill_intraday.py:57-78`). Overnight Globex sessions are dropped by construction — material for CL/NG and ags; intraday lake would cover pit-hours only.
- Request-count reality at defaults: 1m×5y ≈ 1,826 chunks/ticker → ~190k requests across 103 contracts, most returning empty beyond depth — **do not run defaults**; bound with `--years`/`--days`.

## 3. Existing intraday Bronze coverage on mini — zero

Read-only `ssh macmini` `ls` of `~/market-warehouse/data-lake/bronze/asset_class=futures/`:

- **114 futures symbol dirs**, every one containing **`1d.parquet` only** (+ meta/locks); `1m/5m/30m/1h.parquet` count = **0**.
- Includes the newly seeded manifest dirs (`RB_20261*`, `HO_20261*`, `NG_20261*`, `COIL_2026*/2027*`) written today ~13:02–13:10 local (Sep 23) — daily bars only.
- Equity SPY does hold intraday files (different asset class path).

## 4. Representative IB probes — verified data

Connection: existing `ib_async` path, `127.0.0.1:4001`, dedicated clientId 7, serial, `finally` disconnect; contract by manifest `conId`. Script + raw output: `evidence/ib-energy/ib_depth*_probe.py`, `ib-intraday-depth-2026-09-23.json` (sha256 `557beeac…`).

| Contract | Request | Result |
|---|---|---|
| CLX6 / RBX6 / ZSX6 | 1h end 2022-01-15, 1 M | **0 rows, error 162** "HMDS query returned no data" |
| same 3 | 1m end 2024-06-15, 1 D | **0 rows, error 162** |
| CLX6 | 1h end 2024-06-15, 1 M | **7 rows** — all 2024-06-05 (back-month barely traded) |
| CLX6 | 1h end 2025-12-15, 1 M | 162 rows (Nov 13–Dec 12) |
| CLX6 | 1h end 2026-06/08-15, 1 M | 168/176 rows, full months |
| CLX6 | 1m end 2026-01/06/08/09-15, 1 D | 450 rows each (full RTH session) |

Earlier Task-3 probe adds 1h recent coverage for **RBX6, HOZ6, NGF27, COILX6** (16–25 bars per 2-day window) and **ZSX6 hourly** 12 bars — all entitlement-clean (`ib_energy_probe` / `ib-softs-probe-2026-09-23.json`).

**Depth summary**: 1m ≥ ~9 mo, fails ~15 mo; 1h ≥ ~2.2 y (thin for far-dated months), fails ~4.7 y. True boundaries unmapped between probes; per-contract listing date binds too.

## 5. Entitlement / pacing

- **No entitlement errors** on any probed energy/metal/ag contract (GC/SI/HG not individually re-probed intraday; daily already entitled). ICE (COIL/IPE), NYMEX, CBOT, NYBOT, CME, COMEX all serve.
- Error 162 doubles as empty-history/no-data; paced as serial `run_sync` requests (~1–2 s each). IB historical pacing (~6 concurrent / ~60 per 10 min per IB docs) is **not stress-tested here** — serialized code path stays under it; expect hours-scale wall time even bounded.
- Gateway link to IBKR degraded ~6 min during Task-3 probes then restored (1102) — transient upstream risk exists; probe scripts capture but don't auto-retry.

## 6. Code-path caveats (verified in source)

- `--preset` loads `contracts[]` → composite `ROOT_YYYYMM` tickers **but discards the preset's per-ticker `exchange_map`** (`backfill_intraday.py:46-48` `_`); contract resolution falls to `ROOT_EXCHANGE_MAP` then `CME` default (`ingestion_common.py:98`). **Worktree map now contains all manifest roots at verified venues** (`COIL:IPE`, `RB/HO:NYMEX`, `HG:COMEX`, softs:`NYBOT`, grains:`CBOT`, `LE/HE:CME`) — but the **deployed mini release `f83e3df0` still has the old map** (verified read-only over ssh): running intraday-backfill from the deployed checkout would query new roots at CME → error 200. Requires the updated code (or `--tickers` after deploy).
- `exchange` override exists only on the `historical` command path (`fetch_ib_historical.py:439-458`), not `intraday-backfill`.
- Bars are per-contract-month — good for aligned crack legs if all legs use the same delivery month.
- Nightly intraday is **not** currently scheduled for futures (only `daily`); intraday is backfill-only today.

## 7. Expected operational approach

1. Generate a `futures-commodity-*` preset from the manifest's 103 composite tickers (already shaped correctly) **after** the updated `ROOT_EXCHANGE_MAP` is deployed — or pass `--tickers` explicitly.
2. Seed bounded depth: `1h --years 2`, `5m --days 365`, `1m --days 270` per family's measured depth rather than defaults; `--max-tickers` + `--dry-run` on a 3-contract pilot first.
3. Expect error-162 empties beyond depth; chunk volume grows linearly with `--years` — keep requests serialized.
4. Maintain via `intraday-catchup` if desired — not currently scheduled for futures.

## 8. Evidence index

| Path | Content |
|---|---|
| `evidence/ib-energy/ib-intraday-depth-2026-09-23.json` | 14 probe results (contract/req/rows/range/errors), sha256 `557beeac…` |
| `evidence/ib-energy/ib_depth*_probe.py` | Exact probe scripts (conIds, timeframes, durations) |
| `evidence/ib-energy/ib_energy_probe.py` / `ib-softs-probe-2026-09-23.json` | Prior qualification + 1h/1d results for RB/HO/NG/COIL/ZS/… |
| `clients/intraday_bronze_client.py:44-54` | `INTRADAY_TIMEFRAMES`, bar-size/duration map |
| `livewire_scripts/backfill_intraday.py` | CLI args, chunking, RTH-only futures fetch/validate |
| Mini FS read-only `ls` | 114 futures dirs, 0 intraday parquets |

*All probes read-only; no Bronze writes, no cursor edits, no backfill executed; unprobed roots' intraday depth remains unverified.*
