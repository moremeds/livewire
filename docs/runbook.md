# Livewire runbook

Operator commands moved out of CLAUDE.md on 2026-09-02. Rules live in CLAUDE.md; incident history in docs/postmortems/.

---

## 1. Environment

Two Python environments, and they are not interchangeable:

- **dev / test** goes through `uv` — `uv sync --dev`, `uv run pytest`. This matches CI.
- **launchd runtime** is the venv at `~/market-warehouse/.venv/`. Every
  `source …/bin/activate` + `python …` example in this runbook runs against it.

```bash
source ~/market-warehouse/.venv/bin/activate
```

Python version: 3.13. Test deps installed in `~/market-warehouse/.venv/`:
`pytest`, `pytest-cov`, `responses`.

### Credential / `.env` locations

`scripts/livewire_ops.py run-daily-job` loads, in order: `~/.secrets`, the repo
`.env`, and `~/market-warehouse/.env`.

A release artifact carries **no `.env`** (gitignored, so `git archive` omits it),
so credentials must live in `~/market-warehouse/.env`, which
`livewire_scripts/scheduled_env.py` loads. `release promote` warns when that file
is absent.

`livewire_quality.py` loads the scheduled env for `health`, `watchdog` and
`coverage`, so a manual full scan from a bare shell still resolves the right
warehouse paths.

### Path resolution (`livewire_scripts/paths.py`)

| Variable            | Default                 | Meaning                   |
| ------------------- | ----------------------- | ------------------------- |
| `MDW_WAREHOUSE_DIR` | `~/market-warehouse`    | Warehouse root            |
| `MDW_DATA_LAKE`     | `<warehouse>/data-lake` | Canonical data-lake root  |
| `MDW_SILVER_DIR`    | `<data-lake>/silver`    | Silver publish root       |
| `MDW_LOG_DIR`       | `<warehouse>/logs`      | Operational log directory |

### IB connection

| Variable      | Default     | Meaning                         |
| ------------- | ----------- | ------------------------------- |
| `MDW_IB_HOST` | `127.0.0.1` | IB Gateway host (also `--host`) |
| `MDW_IB_PORT` | `4001`      | IB Gateway port (also `--port`) |

### Reliability foundation

| Variable                            | Default                                               | Meaning                                                                                            |
| ----------------------------------- | ----------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `MDW_TELEMETRY_PATH`                | `~/market-warehouse/logs/telemetry.jsonl`             | Telemetry JSONL append path; set to `none` to disable telemetry                                    |
| `MDW_QUALITY_AUDIT_PATH`            | `~/market-warehouse/logs/quality_audit.jsonl`         | Central quality-flag audit JSONL append path                                                       |
| `MDW_ALERT_SEVERITY_THRESHOLD`      | `warning`                                             | Minimum quality-flag severity that triggers per-flag email                                         |
| `MDW_ALERT_RATE_LIMIT_SECONDS`      | `300`                                                 | De-dup window for identical `(source, ticker, category)` alert emails                              |
| `MDW_ORCHESTRATOR_TIMEOUT_SECONDS`  | `300`                                                 | Per-ticker hard timeout for `livewire_ingest.py robust`                                            |
| `MDW_ORCHESTRATOR_MAX_ATTEMPTS`     | `3`                                                   | Per-ticker retry budget for `livewire_ingest.py robust`                                            |
| `MDW_ORCHESTRATOR_COOLDOWN_SECONDS` | `60`                                                  | Sleep between orchestrator retry attempts                                                          |
| `MDW_LOG_LEVEL`                     | `INFO`                                                | Logger root level for reliability tooling                                                          |
| `MDW_LOG_DIR`                       | `~/market-warehouse/logs/`                            | Runtime log directory                                                                               |
| `MDW_SOURCE_EVIDENCE`               | `on`                                                  | Set to `off`/`0`/`false`/`no` to stop `corporate-actions` collecting exact provider response bytes |

### Massive S3 flat files

| Variable                          | Default      | Meaning                                                                                                        |
| --------------------------------- | ------------ | -------------------------------------------------------------------------------------------------------------- |
| `MASSIVE_S3_ACCESS_KEY`           | — (required) | Massive S3 access key for equity intraday                                                                      |
| `MASSIVE_S3_SECRET_KEY`           | — (required) | Massive S3 secret key for equity intraday                                                                      |
| `MDW_FLATFILE_LOOKBACK_DAYS`      | `7`          | Direct `flatfile-ingest catch-up` lookback                                                                     |
| `MDW_FLATFILE_BUCKETS`            | `256`        | Raw ticker buckets per trading day                                                                             |
| `MDW_FLATFILE_STORAGE_MULTIPLIER` | `8`          | Capacity-planning multiplier for a full build                                                                  |
| `MDW_FLATFILE_DAILY_WORKERS`      | `4`          | Process-pool size for the `flatfile-ingest-daily` publish phase (also `--workers`)                             |
| `MDW_FLATFILE_DAILY_BUCKETS`      | `32`         | Ticker buckets per day for `flatfile-ingest-daily` (also `--buckets`)                                          |

### Orchestrator budgets

| Variable                           | Default      | Meaning                                                                           |
| ---------------------------------- | ------------ | --------------------------------------------------------------------------------- |
| `MDW_SYNC_PHASE_TIMEOUT_SECONDS`   | `21600` (6h) | Hard per-phase budget in `daily-backfill`                                         |
| `MDW_DAILY_BACKFILL_INTRADAY_DAYS` | `7`          | Whole-market flat-file catch-up window in `daily-backfill`                        |
| `MDW_DAILY_BACKFILL_DAY_AGGS_DAYS` | `7`          | `flatfile-ingest-daily catch-up` window in `daily-backfill`                       |

### Declared constants (`clients/constants.py`)

| Variable            | Default                    | Meaning                                                                                                                                                                                                       |
| ------------------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LW_DECLARED_<KEY>` | see `clients/constants.py` | Overrides any declared constant for one run. `<KEY>` is the `DECLARED` key upper-cased with `/` and `-` as `_` — e.g. `LW_DECLARED_FAILURE_RATE_TOLERANCE=0.10`, `LW_DECLARED_LANE_BUDGET_S_CORPORATE_ACTIONS=7200` |

Every key is emitted to the ledger as `measurements(source='declared')` at run
start; `status` WARNs when a lane's 14-day p95 `source='measured'` elapsed time
drifts more than 2× from its declared budget.

### DuckDB catalog

| Variable          | Default                               | Meaning                                                                    |
| ----------------- | ------------------------------------- | -------------------------------------------------------------------------- |
| `MDW_DUCKDB_PATH` | `~/market-warehouse/analytics.duckdb` | Catalog database holding the coverage table. Views need no database at all |

### Provider API keys

| Variable          | Used by                                                             |
| ----------------- | ------------------------------------------------------------------- |
| `MASSIVE_API_KEY` | `MassiveClient` (daily REST equity, splits/dividends, break triage) |
| `FRED_API_KEY`    | `livewire_ingest.py fred-rates`                                     |

---

## 2. IB Gateway — facts needed before running anything

IB Gateway + IBC run on the Mac mini, which is the host these sessions run on.
This repo does not install, configure, or restart the Gateway.

- **Connect to `127.0.0.1:4001`, never the LAN IP.** The LAN address is TCP-open
  so `nc -z` succeeds against it, but `TrustedTwsApiClientIPs` is empty and the
  API connection silently times out after ~4 minutes. The code default is already
  correct — do not override it.
- **Gateway version**: pinned to **10.45** (10.46 is incompatible).
- **Trading mode**: live. **2FA** is approved manually in IBKR Mobile on every
  fresh login; livewire cannot bypass this.
- **Do NOT** write order-management workflows, restart/manage the Gateway from
  this repo, or auto-retry on connection failure.

Check availability before assuming IB is up:

```bash
nc -z 127.0.0.1 "${MDW_IB_PORT:-4001}"
```

### Preflight and exit 86

IB-backed ingest commands run a preflight before connecting. If the Gateway is
unreachable they report status and exit cleanly rather than burning a 4-min IB
timeout, with `GATEWAY_DOWN_EXIT_CODE` = **86** (distinct from `1` and from
argparse's `2`). The lane is **skipped, not retried**; it logs
`=== Skipped <scope> ===` and the run is DEGRADED, not failed.

`daily --source massive` and `historical --backfill --source massive` are
explicit non-IB equity paths and bypass IB preflight.

Other IB gotchas:

- Empty IB head timestamps fall back to `IB_EARLIEST_DATE` — intentional, not an error.
- IB error `326` (client ID in use) is handled by auto-retry in `IBClient.connect()`.
  Don't manually reassign client IDs.
- IB returns no data on weekends/holidays — harmless no-ops.
- CBOE volatility uses CBOE's public API, not IB. Stale VIX/SPX is a `cboe-vol`
  question, not an IB-connectivity one.

---

## 3. Daily ingestion

### `daily` — lightweight scheduled daily run (~2,500 tickers)

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py daily                                  # Normal daily run
python scripts/livewire_ingest.py daily --dry-run                        # Report gaps without fetching
python scripts/livewire_ingest.py daily --force                          # Run on non-trading day
python scripts/livewire_ingest.py daily --target-date 2026-03-11        # Recover through a fixed trading date
python scripts/livewire_ingest.py daily --preset presets/sp500.json      # Limit to preset tickers
python scripts/livewire_ingest.py daily --asset-class equity --source massive  # Explicit Massive equity daily path
python scripts/livewire_ingest.py daily --host 127.0.0.1 --port 7497 --max-concurrent 4   # Custom IB config
python scripts/livewire_ingest.py daily --batch-size 25                  # Custom batch size
python scripts/livewire_ingest.py daily --asset-class volatility          # Daily update for volatility indices
python scripts/livewire_ingest.py daily --asset-class futures             # Daily update for futures contracts
```

Logs to `~/market-warehouse/logs/daily_update_YYYY-MM-DD.log`.

### `historical` — seed / backfill

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py historical                                  # Mag 7 default
python scripts/livewire_ingest.py historical --tickers AAPL NVDA              # Custom tickers
python scripts/livewire_ingest.py historical --preset presets/sp500.json      # From preset with cursor resume
python scripts/livewire_ingest.py historical --years 0 --skip-existing        # Inception, skip existing
python scripts/livewire_ingest.py historical --preset presets/sp500.json --backfill --source auto  # Backfill older equity data; auto keeps deep history on IB
python scripts/livewire_ingest.py historical --preset presets/volatility.json --asset-class volatility  # CBOE vol indices (IB backfill)
python scripts/livewire_ingest.py historical --preset presets/futures-index.json --asset-class futures  # CME/CBOT index futures
python scripts/livewire_ingest.py historical --preset presets/futures-energy.json --asset-class futures  # NYMEX energy futures
python scripts/livewire_ingest.py historical --host 192.168.1.50 --port 4001 --tickers AAPL            # Remote IB Gateway
```

**Backfill mode** (`--backfill`) fetches only missing older data for tickers
already in bronze parquet:

- Queries each ticker's oldest existing `trade_date` from parquet
- Fetches the inception → oldest_date gap. For equity, `--source auto` keeps this
  deep older-history path on IB. `--source massive` is explicit and does not
  complete the cursor if the returned range does not reach the requested start.
- Merges older rows into the canonical parquet snapshot
- Uses separate cursor JSON: `cursor_backfill_{name}.json`
- Skips tickers not in bronze parquet (use normal fetch first)

### Orchestrated runners

```bash
python scripts/livewire_ingest.py backfill-all   # Full warehouse build (Python orchestrator)
python scripts/livewire_ingest.py daily-backfill # Lightweight daily catch-up (Python orchestrator)
# Or via unified CLI:
python scripts/livewire.py backfill --full       # Same as backfill-all
python scripts/livewire.py sync --full           # Same as daily-backfill
```

- `backfill-all` (`livewire_scripts/backfill_runner.py`) is the default warehouse
  build: equity daily seed/backfill for `sp500`, `ndx100`, `r2k` (older-history
  daily phase IB-backed through `--source auto`), then FRED Treasury rates, then
  one maximum-entitled-history full-market Massive flat-file equity-intraday build
  in parallel with the CBOE/IB volatility lane, finishing with a DuckDB coverage
  refresh.
- `daily-backfill` (`livewire_scripts/sync_runner.py`) is the routine catch-up
  runner: Massive for recent equity daily gaps, one whole-market flat-file
  catch-up over `MDW_DAILY_BACKFILL_INTRADAY_DAYS` (7), the full-universe
  `flatfile-ingest-daily catch-up` day_aggs lane
  (`MDW_DAILY_BACKFILL_DAY_AGGS_DAYS`, 7) before the intraday flatfile phase, then
  side lanes (FRED rates, CBOE daily volatility, IB volatility intraday), and the
  DuckDB coverage refresh last. Prints one `SUMMARY_JSON` line at the end.

### `robust` — per-ticker IB orchestrator for bulk runs

Use it instead of direct `historical` loops for any IB bulk run larger than
roughly five tickers.

```bash
python scripts/livewire_ingest.py robust --preset presets/sp500.json --mode seed
python scripts/livewire_ingest.py robust --preset presets/sp500.json --mode backfill
python scripts/livewire_ingest.py robust --tickers AAPL MSFT --mode seed --timeout 300 --max-attempts 3 --cooldown 60
```

### Output paths

Per-ticker bronze Parquet at
`data-lake/bronze/asset_class=equity/symbol=<ticker>/{1d,1m,5m,30m,1h}.parquet`;
volatility/index bronze under
`data-lake/bronze/asset_class=volatility/symbol=<ticker>/`; futures under
`asset_class=futures/symbol=ES_202506/1d.parquet`; FRED rates under
`asset_class=rates/symbol=<series>/1d.parquet`.

Delisted symbols that should no longer participate in future syncs or backfills
are archived outside the canonical sync path under
`data-lake/bronze-delisted/asset_class=equity/symbol=<ticker>/1d.parquet`.

### Futures preset format

Futures presets use a `contracts` array instead of `tickers`:

```json
{
  "name": "futures-index",
  "asset_class": "futures",
  "contracts": [
    { "root": "ES", "exchange": "CME", "expiry": "202506" },
    { "root": "NQ", "exchange": "CME", "expiry": "202506" }
  ]
}
```

`load_preset()` flattens these into composite tickers (`ES_202506`) and returns an
exchange map for contract construction.

---

## 4. Non-equity lanes

### CBOE volatility indices

Fetched from CBOE's public API
(`cdn.cboe.com/api/global/delayed_quotes/charts/historical/`), not IB. This is the
authoritative source for VIX, VVIX, VXHYG, VXSMH and all other CBOE volatility
indices. For `VIX` and `SPX`, `cboe-vol` also appends newer rows from CBOE's
official daily-price CSV backup when the chart JSON lags.

```bash
python scripts/livewire_ingest.py cboe-vol      # CBOE vol indices (daily sync, preferred)
```

### FRED Treasury yields

Uses `FRED_API_KEY`. Default series `DGS3`, `DGS5`, `DGS10`, `DGS30`; writes
`trade_date`, `symbol_id`, `tenor_years`, `yield_pct`, `source` to
`data-lake/bronze/asset_class=rates/symbol=<series>/1d.parquet`.

```bash
python scripts/livewire_ingest.py fred-rates    # DGS3/DGS5/DGS10/DGS30
```

### FX and DXY

`scripts/livewire_ingest.py fx` is the only writer of `asset_class=fx`. It is not
an IB lane. Source per (symbol, timeframe) — never mixed within one file:

|                | Daily            | 1m / 5m / 30m          | 1h        |
| -------------- | ---------------- | ---------------------- | --------- |
| Currency pairs | Yahoo `<PAIR>=X` | **Massive** `C:<PAIR>` | **Yahoo** |
| `DXY`          | Yahoo `DX-Y.NYB` | Yahoo                  | Yahoo     |

```bash
python scripts/livewire_ingest.py fx                       # seed maximum depth (~2h)
python scripts/livewire_ingest.py fx --days 7              # nightly catch-up
python scripts/livewire_ingest.py fx --tickers DXY EURUSD --timeframes 1d 1h
```

- `--days` bounds only Massive. Yahoo's chart API takes discrete `range=` values,
  so Yahoo-sourced series always fetch their full window regardless.
- History is **accumulated**: never replace an intraday fx file — a replace throws
  away everything that has already rolled out of the provider's window.

### IB intraday backfill (non-equity only)

Equity intraday uses `flatfile-ingest`. This command is IB-only.

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py intraday-backfill --timeframe 1m --asset-class futures --source ib --preset presets/futures-index.json
python scripts/livewire_ingest.py intraday-backfill --timeframe 5m --asset-class volatility --source ib --preset presets/volatility-intraday.json
```

- Per-timeframe cursor: `~/market-warehouse/cursors/cursor_intraday_{1m,5m,30m,1h}_{preset}.json`. Resumes after interrupt.
- IB error 162/200 ("HMDS no data" / ambiguous contract) marks the ticker complete and moves on.
- Default depth: 5 years for 1m, 2 years for 1h, 1 year for 5m (matches `INTRADAY_MAX_DEPTH`).
- `--days N` fetches only a recent calendar-day window, for daily catch-up jobs.
- `--skip-existing` consults `min(bar_timestamp)` in the existing per-ticker parquet and skips if it already covers the requested depth.
- Logs to `~/market-warehouse/logs/backfill_intraday_{1m,5m,30m,1h}_YYYY-MM-DD.log`.

### Asset-class contract selection

- `--asset-class volatility` uses `Index('SYMBOL', 'CBOE')` contracts instead of
  `Stock('SYMBOL', 'SMART')` and writes to `data-lake/bronze/asset_class=volatility/`.
- `--asset-class futures` uses `Future(root, expiry, exchange)` contracts with
  composite tickers (`ES_202506`), writes to `data-lake/bronze/asset_class=futures/`,
  and uses the futures parquet schema.

---

## 5. Massive flat files

### `flatfile-ingest` — equity intraday (the only equity-intraday path)

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py flatfile-ingest discover
python scripts/livewire_ingest.py flatfile-ingest backfill
python scripts/livewire_ingest.py flatfile-ingest catch-up --days 7
python scripts/livewire_ingest.py flatfile-ingest repair --dates 2026-06-05
```

Requires `MASSIVE_S3_ACCESS_KEY` and `MASSIVE_S3_SECRET_KEY`. `backfill` discovers
the provider-entitled range and refuses to download unless projected storage
leaves the configured reserve. Modes operate on every symbol in each selected
whole-market file; **ticker and preset filters are unsupported**.

- Raw partitions: `data-lake/raw/massive/us_stocks_sip/minute_aggs_v1/date=YYYY-MM-DD/`
- Resume state: `~/market-warehouse/cursors/`

### `flatfile-ingest-daily` — full-universe equity daily (~20K tickers)

Reads Massive's `day_aggs_v1` whole-market daily flat files and writes per-ticker
`1d.parquet` bronze via `BronzeClient` — no intraday derivation.

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py flatfile-ingest-daily discover
python scripts/livewire_ingest.py flatfile-ingest-daily backfill --workers 4
python scripts/livewire_ingest.py flatfile-ingest-daily catch-up --days 14
python scripts/livewire_ingest.py flatfile-ingest-daily repair --dates 2026-06-11
```

Requires `MASSIVE_S3_ACCESS_KEY` and `MASSIVE_S3_SECRET_KEY`.

- Raw partitions: `data-lake/raw/massive/us_stocks_sip/day_aggs_v1/date=YYYY-MM-DD/`
- Durable cursor: `~/market-warehouse/cursors/`
- Publish phase uses a process pool by default (`--workers`, env
  `MDW_FLATFILE_DAILY_WORKERS`, default 4) with 32 ticker buckets per day
  (`--buckets`, env `MDW_FLATFILE_DAILY_BUCKETS`).

Both commands can run side-by-side — separate raw paths and separate cursors.

**Caveats that change how you run these:**

- The flat-file **GET floor is a rolling 5 years**; LIST advertises a 2003 start
  that is not fetchable. `backfill` is therefore not a deep-history tool — it
  re-fetches inside the rolling window only.
- **Never delete raw partitions to reclaim space.** Anything older than the
  current floor cannot be re-downloaded.
- Massive's S3 `global_forex/` prefix lists back to 2010 but GETs 403 — the
  flat-file entitlement covers `us_stocks_sip` only.

---

## 6. Corporate actions + Silver

### Corporate actions

Artifacts live at
`data-lake/bronze/asset_class=corporate_action/symbol=<encoded_symbol>/events.parquet`.

```bash
python scripts/livewire_ingest.py corporate-actions --tickers NVDA AAPL SPY       # Targeted Massive split/dividend reconciliation
python scripts/livewire_ingest.py corporate-actions --full-reconcile              # Whole equity-bronze universe; may infer cancellations
python scripts/livewire_ingest.py corporate-actions --workers 4 --resume --full-reconcile  # Resume incomplete whole-universe reconciliation
python scripts/livewire_ingest.py corporate-actions --dry-run                     # Compare without publishing
```

Targeted runs never infer disappearance by default; full reconciliations may
append cancellation revisions.

#### Why was corporate-actions slow last night?

```bash
uv run python scripts/livewire_ops.py ledger query "select name, value, unit from measurements where scope = 'corporate-actions' and run_id = '<run_id>' order by name"
```

Read them together; no single one of them answers the question.

- `provider_wait_s` large → the lane is asleep, not working: throttled or
  retrying. More `--workers` will not help.
- `provider_throttled` large → the provider is pushing back. More workers make
  it worse; the lane needs preemptive pacing (`min_interval_seconds`) like fx
  has. The 5 req/min figure elsewhere in this repo is **fx-scoped**; this
  lane's ceiling has never been measured.
- `provider_errors` large → attempts that never got a response. Each costs a
  full request timeout plus a backoff and is invisible in response counts.
- `provider_latency_p95_ms` high with the three above near zero → the endpoint
  itself is slow. This is the only case where more `--workers` is the lever.

`provider_latency_p95_ms` is socket time per attempt only; it does not include
`provider_wait_s`, by construction — the sleeps happen outside the measured
window. Join on `run_id` to `lane_results.elapsed_s` for the lane's wall-clock.

### Silver rebuild

Silver artifacts publish beneath `MDW_SILVER_DIR` (default `data-lake/silver`).
Immutable revision manifests are written before `current.json`, the final
cross-file commit record.

```bash
python scripts/livewire_store.py rebuild-silver --tickers NVDA AAPL SPY   # Targeted adjusted daily/factor rebuild
python scripts/livewire_store.py rebuild-silver --full --dry-run          # Full comparison without publishing
python scripts/livewire_store.py rebuild-silver --full --dry-run --failure-output /tmp/rev3-dry.json
```

Trim controls:

- `--continuity-threshold` (default `6.0`) — the blind window scan over the
  adjusted series.
- `--continuity-allowlist <ISO_DATE>…` — exempt evidence-backed dates (global by
  date, not per-symbol).
- `--allow-window-regression` — overrides the withhold-on-regression gate.
  **Required exactly once, for the rev-3 bootstrap.**
- `--failure-output <path>` — writes the failure manifest that `resolve-yahoo-basis`
  consumes.

Quarantined symbols have their artifact **moved** to
`<silver>/evicted/<revision>/…`, not merely dropped from the manifest.

### Break triage

Classifies each break the audit recorded against Massive as a second source.

| Signal                                                  | Verdict          | Effect                                             |
| ------------------------------------------------------- | ---------------- | -------------------------------------------------- |
| Our jump present in Massive's **raw** series            | `real_move`      | keep — never trimmed                               |
| Our jump absent from Massive's raw series               | `bad_data`       | trim                                               |
| Massive's **adjusted÷raw** factor steps across the date | `missing_action` | trim (the record is what's missing, not the price) |
| Provider cannot answer                                  | `inconclusive`   | trim                                               |

```bash
python scripts/livewire_quality.py triage-breaks \
    --audit-manifest <.../audit.json> --output <lake>/repairs/triage/current.json --resume
```

- The verdict manifest is durable and **default-loaded** from
  `<data-lake-root>/repairs/triage/current.json`. The nightly job passes no flags.
  **Never delete the verdict store to "force a re-triage"** — a verdict obtained
  today may be unobtainable next year.
- `/v2/aggs` is entitled for a rolling ~5 years only; every older break is
  `inconclusive`. A large `inconclusive` count is the expected shape.
- Transient provider failures abort the run and are never checkpointed; `--resume`
  re-asks them.

### Price-basis command set

`scripts/livewire_quality.py calibrate-daily-basis` ·
`scripts/livewire_store.py migrate-price-basis` ·
`scripts/livewire_quality.py audit-split-basis` ·
`scripts/livewire_quality.py resolve-split-basis` ·
`scripts/livewire_store.py repair-split-basis` ·
`scripts/livewire_quality.py audit-legacy-basis` ·
`scripts/livewire_quality.py triage-breaks` ·
`scripts/livewire_store.py repair-legacy-basis` ·
`scripts/livewire_store.py rollback-legacy-basis`

Audit manifests record their resolved data-lake root; repair and rollback reject a
different active root before mutation, and reject a manifest with no root recorded
rather than failing open.

### Legacy-basis full repair → rev-3 (operator-reviewed, resumable)

Run the phases in order and **review the `mixed` count before touching IB**.

```bash
# Run these FROM THE REPO ROOT: --presets-dir defaults to a cwd-relative Path("presets"),
# so --priority-only elsewhere used to silently repair zero symbols and exit 0. It now errors.

# 1. Offline audit → manifest (read-only; never mutates bronze). REVIEW counts{clean,mixed,error}.
python scripts/livewire_quality.py audit-legacy-basis --full \
    --output <lake>/repairs/silver-legacy-basis/<stamp>/audit.json

# 2a. FIRST BATCH ONLY — sp500 + ndx100 + r2k members (defer the ~10.6K tail).
#     IB re-derivation is 2FA-gated and never auto-retries a connection failure.
#     --dry-run first: fetch, classify and self-check with no bronze write (status "would-repair").
python scripts/livewire_store.py repair-legacy-basis \
    --audit-manifest <.../audit.json> --output-dir <.../repair-batch1> --priority-only --dry-run
python scripts/livewire_store.py repair-legacy-basis \
    --audit-manifest <.../audit.json> --output-dir <.../repair-batch1> --priority-only --resume
# Every mutated parquet is copied verbatim to <output-dir>/backup/ FIRST; the sidecar
# records backup_sha256. To undo the batch (or one symbol):
python scripts/livewire_store.py rollback-legacy-basis --output-dir <.../repair-batch1> [--tickers NVDA]

# 2b. STOP GATE — quantify the tail BEFORE committing to it.
python -c "import json; from livewire_scripts.repair_legacy_basis import summarize_progress; \
print(json.dumps(summarize_progress(json.load(open('<.../audit.json>')), \
json.load(open('<.../repair-batch1>/summary.json')), \
cursor=json.load(open('<.../repair-batch1>/cursor.json'))), indent=2))"
# tail_mixed_exact appears only when the batch ran to completion; an aborted batch
# reports tail_mixed_lower_bound instead. Decide with tail_estimated_unrepairable:
#   small tail, low ambiguous rate  → run the full tail now (drop --priority-only, keep --resume,
#                                     reuse --output-dir so the cursor skips done symbols)
#   large tail / high ambiguous rate → schedule a dedicated 2FA-gated IB batch run first
python scripts/livewire_store.py repair-legacy-basis \
    --audit-manifest <.../audit.json> --output-dir <.../repair-batch1> --resume   # tail (no --priority-only)

# 3. Triage what repair cannot fix, so real market moves are not trimmed as corruption.
python scripts/livewire_quality.py triage-breaks \
    --audit-manifest <.../audit.json> --output <lake>/repairs/triage/current.json --resume

# 4. Review the trim BEFORE publishing: window_regressions is every symbol that would
#    lose published history. On the rev-3 bootstrap this is expected and large.
python scripts/livewire_store.py rebuild-silver --full --dry-run --failure-output /tmp/rev3-dry.json

# 5. Freeze the three writers, single rev-3 publish, restore writers EVEN IF rebuild fails.
WRITERS="com.livewire.daily-update com.livewire.intraday-catchup com.livewire.daily-update-watchdog"
for L in $WRITERS; do launchctl unload ~/Library/LaunchAgents/$L.plist; done
# --allow-window-regression is required EXACTLY ONCE, here: rev-2 published untrimmed
# history, so every intentional trim reads as a regression on this first run only.
python scripts/livewire_store.py rebuild-silver --full --allow-window-regression; RC=$?
for L in $WRITERS; do launchctl load ~/Library/LaunchAgents/$L.plist; done   # restore regardless of RC

# 6. After Apex adopts rev-3, smoke-test NVDA/AMZN/GOOGL/AGL (formerly corrupt) + INTC (fail-closed control).
#    Production runs APEX_LIVEWIRE_PRICE_MODE=raw, so it serves BRONZE — a production
#    smoke test proves nothing about the adjusted path. Use an adjusted-mode canary.
```

### Unknown-basis reconstruction (`resolve-yahoo-basis`)

Flips `unknown → raw` for the unknown-basis population so split-affected symbols
can stage in Silver. **Dry-run is the default.** `--apply` **requires
`--ib-verify`** — no publish without IB confirmation. IB unreachable **aborts** the
run; `--resume` re-asks.

```bash
# dry-run: Yahoo true-raw reconstruct + self-gate the split-affected unknown-basis failures
python scripts/livewire_store.py resolve-yahoo-basis \
    --failure-manifest <.../rev-dry.json> \
    --output <lake>/repairs/unknown-basis/<stamp>/manifest.json

# apply: publish only IB-anchor-verified reconstructions (2FA-gated)
python scripts/livewire_store.py resolve-yahoo-basis \
    --failure-manifest <.../rev-dry.json> --output <.../manifest.json> \
    --apply --output-dir <.../batch1> --allow-rewrite --ib-verify --priority-order --resume
```

Every non-`published` verdict (`ib_mismatch`, `ib_insufficient_overlap`,
`high_mismatch`, `split_mismatch`, `stage_fail`) leaves bronze **untouched** and
lands in the review queue. Writes go through the same verbatim-backup +
`rollback-legacy-basis` path as the other basis repairs.

Rollout is batched and 2FA-gated: batch-1 = the split-affected unknown-basis
failures from `rebuild-silver --full --dry-run --failure-output`, then a STOP GATE
on the published-vs-review ratio before the ~12K tail. Full design:
`docs/superpowers/specs/2026-07-19-unknown-basis-ib-verified-reconstruction-design.md`
(plan archived; see git history).

### Silver canary (read-only)

```bash
python livewire_scripts/validate_silver_canary.py --tickers NVDA AAPL SPY --control SYMBOL   # Read-only factor/OHLCV/bronze-integrity canary
```

### Rollback

```bash
python scripts/livewire_store.py rollback-legacy-basis --output-dir <.../batch1>              # Undo a repair batch from its backups
python scripts/livewire_store.py rollback-legacy-basis --output-dir <.../batch1> --tickers NVDA
```

---

## 7. Quality & observability

### Coverage (own launchd job, 11:00 UTC)

For each tracked timeframe (`1d`, `1m`, `1h`, `5m`, `30m`) it counts how many
symbols in the active bronze universe for that timeframe have bars current as-of
the target trading day, using parquet footer statistics. A symbol counts as
present if it is current OR absent from the day's raw traded set (no-trade is not
missing). Writes a one-line summary to
`~/market-warehouse/logs/coverage_YYYY-MM-DD.log`.

```bash
python scripts/livewire_quality.py coverage                                # Today's coverage + auto-recovery
python scripts/livewire_quality.py coverage --no-recover                   # Report only
python scripts/livewire_quality.py coverage --target-date 2026-04-06       # Specific trading day
python scripts/livewire_quality.py coverage --threshold 0.99               # Stricter threshold
python scripts/livewire_quality.py coverage --force                        # Run on a non-trading day
```

Gap-engine artifacts written by the same command:

```bash
python scripts/livewire_quality.py coverage --target-date 2026-08-28
# writes <data-lake>/repairs/tier_a_<date>.json and decisions_<date>.json,
# plus the `scan:` line in <log_dir>/coverage_<date>.log
```

- Below the declared `coverage_alert_threshold` (default `0.95`) it triggers a targeted
  backfill subprocess and re-checks. 1d recovery uses Massive daily REST; intraday
  recovery republishes the whole target-day flat file with
  `flatfile-ingest repair --dates <date>`.
- **Safety cap (default 100):** if more than N symbols are missing for any single
  timeframe, auto-recovery aborts and it emails immediately.
- Email goes out only when post-recovery gaps remain.

Gap classes actually emitted: `G3` (nothing on disk for the series), `G1` (missing
the newest sessions), `G14` (the instrument left the tape). `G2` (interior) and
`G13` (head) are named in the taxonomy and **not** emitted.

Tier follows the repair **source**, not gap severity:

| Asset class    | Source  | Tier                         | Floor               |
| -------------- | ------- | ---------------------------- | ------------------- |
| equity         | Massive | A inside the window, B below | rolling             |
| fx             | Yahoo   | A                            | none (deep history) |
| volatility     | CBOE    | A                            | none                |
| rates          | FRED    | A                            | none                |
| futures, cmdty | **IB**  | **always B**                 | n/a                 |

The unresolved ledger lives at `<data-lake>/repairs/unresolved.json`, keyed on
`(symbol, asset_class, timeframe, session)`.

### Status — one graded view

```bash
python scripts/livewire_ops.py status
```

Grades nine cheap signals and prints a fix command for anything not OK. It reads
only what the nightly jobs already produced — it never scans parquet. Exit code is
always 0. `Verdict` is ordered `OK < UNKNOWN < WARN < BAD`.

### Ledger

```bash
uv run python scripts/livewire_ops.py ledger query "select lane, outcome, elapsed_s from lane_results order by started desc limit 20"
uv run python scripts/livewire_ops.py ledger query "select scope, date '1970-01-01' + cast(value as int) as last_session from measurements where name = 'last_session'"
uv run python scripts/livewire_ops.py ledger emit --table evidence --json '{"evidence_hash":"…","kind":"request","subject":"silver:TSLA","payload_json":"{}","source_url":null,"fetched_at":"2026-09-02T06:00:00+00:00","proposer":"human","run_id":"manual-1"}'
# LW_LEDGER_ROOT overrides the root (default <lake>/ledger); LW_RUN_ID names the run.
```

### Weekly quality summary

Pure parser over the seven daily coverage logs from the previous ISO week.
Self-skips on non-Sunday, so the entrypoint can call it unconditionally every day.

```bash
python scripts/livewire_quality.py weekly            # Sunday: write the report; other days: noop
python scripts/livewire_quality.py weekly --force    # Render anyway
python scripts/livewire_quality.py weekly --week 2026-14
```

Output: `~/market-warehouse/logs/quality_weekly_YYYY-WW.md`.

### Health check (intraday) — not scheduled

Interior gap detection for intraday parquet, with optional suspected-halt
annotation. Default behaviour is **report-only**. An unfiltered run appends its
summary to `~/market-warehouse/logs/interior_gaps_YYYY-MM-DD.log`; a
`--symbol`-scoped run writes no log. When `--symbol`, `--since` and `--timeframe`
are all set, the command implicitly repairs that narrow window by shelling out to
`intraday-backfill` (there is no `--repair` flag — full scope means repair).

```bash
python scripts/livewire_quality.py health --intraday --timeframe 5m                       # Scan all symbols (~3115s)
python scripts/livewire_quality.py health --intraday --timeframe 5m --symbol AAPL         # Scan one symbol
python scripts/livewire_quality.py health --intraday --timeframe 5m --symbol AAPL --since 2026-04-01  # Repair window
```

### Telemetry / quality report

```bash
python scripts/livewire_quality.py report --view summary --since 24h
python scripts/livewire_quality.py report --view flap --since 24h --source ib
python scripts/livewire_quality.py report --view quality --since 24h --severity critical
python scripts/livewire_quality.py report --view summary --since 24h --email
```

Views are `summary`, `flap`, `quality`; `--source` accepts `all`, `ib`, `uw`, or
`massive`. Quality flags are emitted beside parquet as `<parquet>.meta.json`;
sidecar and audit JSONL schemas are in
`docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md`.

### Nightly digest

```bash
python scripts/livewire_quality.py digest --run-date YYYY-MM-DD --email
```

Renders the same ledger-backed checks as `status`; it does not parse job logs.

### Watchdog

```bash
python scripts/livewire_quality.py watchdog
```

Runs at 10:30 UTC daily and pages only when a ledger-backed status check is BAD.

### Alerts

```bash
python scripts/livewire_ops.py send-alert
```

Nodemailer CLI behind every failure page. Failed sends are recorded as ledger
execution rows. Alert values must use the single-token `--key=value` form.

### Daily-run outcome categories

Per-ticker outcomes emitted by `daily`, in the `SUMMARY_JSON` line (schema in
`livewire_scripts/daily_outcomes.py`):

| Outcome    | Meaning                                                       |
| ---------- | ------------------------------------------------------------- |
| `updated`  | Bars fetched and merged                                       |
| `no_trade` | No bars returned — the instrument didn't trade, not a failure |
| `partial`  | Target day filled, older gaps remain                          |
| `error`    | Exception / HTTP failure                                      |

Exit code is threshold-based (`resolve_exit_code`): a run fails only on systemic
failure — `error` count over `max(50, 5% of processed)`, or zero updates with any
error. `no_trade`/`partial` never fail a run.

### `robust` outcome categories

| Category  | Meaning                                                                                         |
| --------- | ----------------------------------------------------------------------------------------------- |
| `ok`      | Child exited cleanly and bronze exists; row delta is included when known.                       |
| `ok-noop` | Backfill exited cleanly with no row delta, treated as "no older history".                       |
| `skip`    | Seed skipped because bronze already exists, or backfill skipped because no seed parquet exists. |
| `fail`    | Child exited non-zero, exhausted retries, or exited zero without producing seed bronze.         |
| `timeout` | All attempts hit the hard timeout.                                                              |

### IB BarData → Bronze mapping (equity)

| IB BarData field   | Bronze column | Transform                                                      |
| ------------------ | ------------- | -------------------------------------------------------------- |
| `bar.date`         | `trade_date`  | `str(bar.date)`                                                |
| (from ticker)      | `symbol_id`   | Read existing parquet ID or derive stable ID                   |
| `bar.open`         | `open`        | Already float                                                  |
| `bar.high`         | `high`        | Already float                                                  |
| `bar.low`          | `low`         | Already float                                                  |
| `bar.close`        | `close`       | Already float                                                  |
| `bar.close`        | `adj_close`   | Same value                                                     |
| `bar.volume`       | `volume`      | `int(bar.volume)`                                              |
| provider           | `source`      | Row-level `ib`, `massive`, `nasdaq`, `stooq`, or `legacy`      |
| normalization gate | `price_basis` | Canonical `raw`; unresolved legacy/fallback rows use `unknown` |

### IB BarData → Futures Bronze mapping

| IB BarData field        | Bronze column   | Transform                                          |
| ----------------------- | --------------- | -------------------------------------------------- |
| `bar.date`              | `trade_date`    | `str(bar.date)`                                    |
| (from composite ticker) | `contract_id`   | Stable hash of composite ticker (e.g. `ES_202506`) |
| (from composite ticker) | `root_symbol`   | Parsed from `ticker.rsplit("_", 1)[0]`             |
| (from composite ticker) | `expiry_date`   | `YYYY-MM-01` derived from expiry code              |
| `bar.open`              | `open`          | Already float                                      |
| `bar.high`              | `high`          | Already float                                      |
| `bar.low`               | `low`           | Already float                                      |
| `bar.close`             | `close`         | Already float                                      |
| `bar.close`             | `settlement`    | Same value (IB doesn't provide settlement)         |
| `bar.volume`            | `volume`        | `int(bar.volume)`                                  |
| (default)               | `open_interest` | `0` (IB BarData doesn't include OI)                |

---

## 8. DuckDB analytical catalog

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_store.py duckdb views          # what the catalog exposes
python scripts/livewire_store.py duckdb build          # rebuild + publish the coverage table
python scripts/livewire_store.py duckdb freshness      # per-view staleness buckets
python scripts/livewire_store.py duckdb lag            # silver trailing or missing vs bronze
python scripts/livewire_store.py duckdb stale --days 30
python scripts/livewire_store.py duckdb bars --symbols NVDA HON      # direct-path read
python scripts/livewire_store.py duckdb sql "SELECT ... FROM bronze_equity_1d"
```

- Views are registered **on demand**, never eagerly; `duckdb sql` registers only
  the views its query text names.
- Symbol-scoped reads bypass views entirely — `duckdb bars` constructs
  `symbol=<TICKER>/<tf>.parquet` paths directly.
- Coverage is **daily-only**.
- `build` publishes by writing a staging database and `os.replace()`-ing it into
  place; concurrent `read_only` readers are fine.

---

## 9. Releases & scheduling

### Release artifacts

`scripts/livewire_ops.py release` builds the merged `origin/main` commit into
`<warehouse>/releases/<sha>/` (a `git archive` export plus its own
`uv sync --frozen --no-dev` virtualenv, then `chmod -R a-w`) and atomically
repoints `<warehouse>/current` at it.

```bash
python scripts/livewire_ops.py release promote            # build+serve origin/main
python scripts/livewire_ops.py release promote --dry-run  # decide without building
python scripts/livewire_ops.py release list               # `*` marks what is served
python scripts/livewire_ops.py release rollback           # serve the previous one
```

- **`git checkout main && git pull` before promoting anything that changes the
  promoter** — `promote` exports `origin/main` but runs the checkout's own builder.
- **Never `rm -rf` the release `current` points at.** Recover a dangling `current`
  with `release rollback`, then `promote`.
- `--allow-unverified` bypasses the CI gate; needed exactly once, to bootstrap the
  first release from a SHA predating the push trigger.
- `promote` runs `npm ci --omit=dev` between `build_venv` and `freeze`.

### launchd install

```bash
# The six warehouse jobs run the immutable release, so they take the WAREHOUSE path.
# The promoter is the one job that reads the repo — it is what builds the release.
WAREHOUSE=~/market-warehouse
for L in daily-update daily-update-watchdog intraday-catchup coverage; do
  sed "s|/path/to/warehouse|$WAREHOUSE|g" "launchd/com.livewire.$L.plist.example" \
    > ~/Library/LaunchAgents/com.livewire.$L.plist
done
# release-promote and universe-refresh both read the repo: one builds the
# release, the other writes presets/ (which a frozen release forbids).
for L in release-promote universe-refresh; do
  sed -e "s|/path/to/repo|$(pwd)|g" -e "s|/path/to/warehouse|$WAREHOUSE|g" \
    "launchd/com.livewire.$L.plist.example" > ~/Library/LaunchAgents/com.livewire.$L.plist
done
for L in daily-update daily-update-watchdog intraday-catchup coverage universe-refresh release-promote; do
  launchctl load ~/Library/LaunchAgents/com.livewire.$L.plist
done
```

`com.livewire.gap-scan` was retired; its plist template and its
`livewire_quality.py gap-scan` subcommand are both deleted. On any host that
installed it, run once:

```bash
launchctl unload ~/Library/LaunchAgents/com.livewire.gap-scan.plist && rm ~/Library/LaunchAgents/com.livewire.gap-scan.plist
```

All plists use `StartCalendarInterval` with Mac-local `Hour`/`Minute` (launchd has
no `TimeZone` key). On this Mac (`Asia/Hong_Kong`, UTC+8) the configured Hour
values map to the UTC targets below. See each plist's header comment for the
conversion table to other Mac timezones.

### Schedule (UTC)

| Job                                  | UTC time            | Entrypoint                                                                                  |
| ------------------------------------ | ------------------- | ------------------------------------------------------------------------------------------- |
| `com.livewire.release-promote`       | 04:30 daily         | `livewire_ops.py release promote` (reads the repo)                                          |
| `com.livewire.intraday-catchup`      | 05:00 daily         | `livewire_ops.py run-intraday-catchup-job`                                                  |
| `com.livewire.daily-update`          | 06:00 daily         | `livewire_ops.py run-daily-job`                                                             |
| `com.livewire.daily-update-watchdog` | 10:30 daily         | `livewire_quality.py watchdog`                                                              |
| `com.livewire.coverage`              | 11:00 daily         | `livewire_quality.py coverage` (also runs the windowed gap classifier)                      |
| `com.livewire.universe-refresh`      | Sunday 13:00 weekly | `livewire_ingest.py universe-sync && livewire_ingest.py shepherd-universe` (reads the repo) |

`run-daily-job` syncs equities, futures and cmdty via IB, then all volatility
indices via CBOE and DXY/FX via Yahoo+Massive, in a single invocation; pass
`--asset-class <name>` to run only one IB asset class (this skips both the CBOE
volatility and fx syncs). After a successful run it spawns the weekly quality
report, sends the nightly digest email, and runs the housekeeping retention sweep
last. Coverage is **not** here.

`run-intraday-catchup-job` calls the `daily-backfill` orchestrator (equity daily +
intraday 1m/5m/30m/1h, FRED rates, CBOE volatility daily, IB volatility intraday
30m/5m with 1h derived locally from 30m). Equity intraday requires
`MASSIVE_S3_ACCESS_KEY` and `MASSIVE_S3_SECRET_KEY`; missing credentials fail the
orchestrator before any phases run, and there is no REST or IB equity-intraday
fallback. The wrapper is single-attempt; on terminal failure it sends one alert
tagged `--job-name intraday_catchup`.

`com.livewire.universe-refresh` runs from the **repo**, not the release:
`release.freeze()` does `chmod -R a-w` and `universe_sync` writes `presets/*.json`.

Manual invocation of the universe chain:

```bash
python scripts/livewire_ingest.py universe-sync [--dry-run] [--skip-dead]
python scripts/livewire_ingest.py shepherd-universe scan --index <INDEX> [--preset <path>]
python scripts/livewire_ingest.py shepherd-universe import-decision --manifest <path>
python scripts/livewire_ingest.py shepherd-universe verify --index <INDEX> --revision <N> [--effective-at ...] [--as-of ...]
```

### Log file names

| Log                  | Path                                                                      |
| -------------------- | ------------------------------------------------------------------------- |
| Daily update         | `~/market-warehouse/logs/daily_update_YYYY-MM-DD.log`                     |
| Intraday catch-up    | `~/market-warehouse/logs/intraday_catchup_YYYY-MM-DD.log` (UTC date)      |
| IB intraday backfill | `~/market-warehouse/logs/backfill_intraday_{1m,5m,30m,1h}_YYYY-MM-DD.log` |
| Coverage             | `~/market-warehouse/logs/coverage_YYYY-MM-DD.log`                         |
| Interior gaps        | `~/market-warehouse/logs/interior_gaps_YYYY-MM-DD.log`                    |
| Weekly summary       | `~/market-warehouse/logs/quality_weekly_YYYY-WW.md`                       |
| Telemetry            | `~/market-warehouse/logs/telemetry.jsonl`                                 |
| Quality audit        | `~/market-warehouse/logs/quality_audit.jsonl`                             |

---

## 10. Housekeeping

`housekeeping` prunes logs (60d), releases (keep 3) and superseded evicted silver
revisions (keep 2). **Dry run is the default**; `release.prune` previews in it too.
`raw/` and `repairs/` are protected **by name**, never by an age rule.

```bash
python scripts/livewire_ops.py housekeeping                      # dry run (default)
python scripts/livewire_ops.py housekeeping --apply              # actually delete
python scripts/livewire_ops.py housekeeping --log-retention-days 60
python scripts/livewire_ops.py housekeeping --keep-releases 3
python scripts/livewire_ops.py housekeeping --keep-evicted 2
python scripts/livewire_ops.py housekeeping --log-dir <path> --data-lake <path>
python scripts/livewire_ops.py housekeeping --appledouble        # opt-in `._*` sweep
```

The AppleDouble sweep (`--appledouble`) is opt-in and must **never** go in the
nightly job — it `rglob`s the whole exFAT volume.

---

## 11. Testing & pre-commit hook

All new code in `clients/` and `scripts/` must have tests. Coverage is enforced at
95% (`fail_under = 95` in `pyproject.toml`, `--cov-fail-under=95` in CI) for the
source currently included by `pyproject.toml`; `clients/ib_client.py` is omitted
from the fail-under gate and covered by focused tests separately.

```bash
uv run pytest tests/ -v                                                        # Run all (matches CI)
uv run pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing  # With coverage
uv run pytest tests/ -v -m "not integration"                                   # Unit tests only
uv run pytest tests/ -v -W error::RuntimeWarning                               # Catch leaked coroutine warnings
# Native macOS tests are now in the standalone Sift repo at ~/dev/apps/util/sift
```

### Rules for new code

1. Add tests in `tests/test_<module>.py`
2. Mock all external I/O (IB connections via `MagicMock`, file paths via `patch`)
3. Use temp parquet roots for storage tests
4. Mark DB tests with `@pytest.mark.integration`
5. Run coverage and confirm the 95% gate passes before committing
6. Run `-W error::RuntimeWarning` at least once before committing when script tests mock async runners such as `ib.ib.run(...)`
7. `pyproject.toml` enforces `fail_under = 95`; `if __name__ == "__main__"` blocks are excluded
8. `clients/ib_client.py` is excluded from the coverage fail-under gate, but focused behavior tests now live in `tests/test_ib_client.py`

### Test deps

`pytest`, `pytest-cov`, `responses` (installed in `~/market-warehouse/.venv/`)

### Pre-commit hook

A secrets scanner runs on every commit, checking staged files for API keys,
passwords, private keys, tokens, and credentials. Install with:

```bash
ln -sf ../../tools/pre-commit-secrets-scan.sh .git/hooks/pre-commit
```

Catches: AWS keys, API key/secret/password assignments, private key headers,
GitHub/Slack tokens, Google API keys, connection strings with credentials,
hardcoded IB credentials, staged `.env` files. Allowlists test files,
placeholders, comments, `os.environ` reads, and error messages to avoid false
positives. Bypass with `git commit --no-verify` if needed.
