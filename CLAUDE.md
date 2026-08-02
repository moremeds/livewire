# Livewire

Livewire is a local-first market data warehouse for quantitative research. Parquet data lake as system of record, DuckDB as the analytical query layer over it, and ClickHouse for production benchmarking. Rebranded 2026-05-17 from "market-data-warehouse"; the repo dir is now `livewire/`, the on-disk data tree remains at `~/market-warehouse/` (descriptive, not project-named).

## Project Layout

Two directory trees: this **git repo** and the **data warehouse** at `~/market-warehouse/`.

```
livewire/                           # Git repo
├── clients/                        # ~30 client modules (bronze/IB/Massive/DuckDB/FRED/quality/telemetry/…)
│   └── __init__.py                 # Authoritative export list — check it rather than this tree
├── presets/
│   ├── volatility.json             # CBOE Volatility Indices (VIX, VVIX, etc.)
│   ├── volatility-intraday.json    # VIX/SPX/NDX/RUT/VXN/RVX IB-backed intraday (30m, 1h derived)
│   ├── futures-index.json          # CME/CBOT Index Futures (ES, NQ, RTY, YM)
│   ├── futures-energy.json         # NYMEX Energy Futures (CL, NG)
│   ├── futures-metals.json         # COMEX Metals Futures (GC, SI)
│   ├── futures-treasuries.json     # CBOT Treasury Futures (ZB, ZN, ZF)
│   └── ...                         # S&P 500, NDX-100, Russell 2000 sector presets
├── scripts/
│   ├── setup_market_warehouse.sh   # One-time system bootstrap
│   ├── livewire_ingest.py          # Ingest subcommands: daily, historical, robust, CBOE, intraday, universe
│   ├── livewire_quality.py         # Quality subcommands: health, coverage, report, weekly, watchdog
│   ├── livewire_ops.py             # Ops subcommands: scheduled job, alerts
│   └── livewire_store.py           # Storage subcommands: DuckDB catalog, Silver rebuild, R2 sync, parquet migration
├── livewire_scripts/               # Importable implementations behind the script entrypoints
├── livewire_node/                  # Nodemailer alert helpers (failure + nightly digest)
├── launchd/                        # macOS launchd templates
├── tools/                          # Developer hooks and helper shell tools
├── tests/
│   ├── conftest.py                 # Shared fixtures
│   ├── test_daily_bar_fallback.py  # Unit tests for fallback providers
│   ├── test_uw_client.py           # Unit tests — HTTP mocked via `responses`
│   ├── test_fetch_ib_historical.py # Tests for IB fetch script
│   ├── test_daily_update.py        # Tests for daily update script
│   ├── test_ib_client.py           # Focused tests for IB client connect fallback
│   └── test_historical_provider.py # Tests for HistoricalProvider, contract spec helpers
├── pyproject.toml                  # pytest config, coverage enforcement
├── .env.example
└── README.md

~/market-warehouse/                 # Data warehouse (created by setup script)
├── .venv/                          # Python 3.13 venv
├── data-lake/
│   ├── bronze/asset_class=equity/  # Per-ticker Hive-partitioned Parquet (symbol=AAPL/1d.parquet)
│   ├── bronze/asset_class=futures/ # Per-contract Hive-partitioned Parquet (symbol=ES_202506/1d.parquet)
│   ├── bronze/asset_class=rates/   # FRED Treasury yields (symbol=DGS10/1d.parquet)
│   ├── bronze-delisted/asset_class=equity/  # Archived delisted symbols excluded from future sync/backfill runs
│   ├── silver/                     # Cleaned / adjusted
│   └── gold/                       # Derived analytics / factor tables
├── logs/telemetry.jsonl            # Reliability telemetry artifact
├── logs/quality_audit.jsonl        # Central quality-flag artifact
├── clickhouse/                     # Optional ClickHouse data
├── scripts/                        # Bootstrap SQL, helper scripts
└── logs/
```

## Architecture

- **Parquet** is the system of record
- **Data lake tiers**: bronze (normalized Parquet) -> silver (cleaned) -> gold (derived)
- **DuckDB** is the analytical query layer: views over the parquet lake plus a small coverage table. It copies no bar data and is never a second system of record
- **ClickHouse** is optional, for production-style benchmarking and concurrency testing
- **Python env**: dev/test runs go through `uv` (`uv sync --dev`, `uv run pytest` — matches CI). The launchd runtime venv lives at `~/market-warehouse/.venv/`; the `source …/bin/activate` + `python …` invocations in the operator examples below run against it

## Native macOS Client (Extracted)

The native macOS client has been extracted to the standalone **Sift** app at `~/dev/apps/util/sift/`.

See the [Sift CLAUDE.md](~/dev/apps/util/sift/CLAUDE.md) for module layout, build instructions, and testing.

## Analytical Targets

DuckDB addresses the lake in place — see "DuckDB analytical catalog" below. ClickHouse mirrors the daily schema with MergeTree engines partitioned by `toYYYYMM(trade_date)` when production-style benchmarking is needed.

## IB Gateway / IBC

**IB Gateway + IBC run on the Mac mini — which is the host these sessions run ON.** Livewire is a consumer of that infrastructure and this repo does not install, configure, or restart the Gateway. (Earlier local-install notes here — `/opt/ibc/`, `~/ibc/`, local watchdog LaunchAgents — described a setup that no longer applies.)

⚠️ **Connect to `127.0.0.1:4001`, never the LAN IP.** The mini's LAN address is TCP-open, so `nc -z` succeeds against it — but `TrustedTwsApiClientIPs` is empty, so an API connection there silently times out after ~4 minutes with no error. A "hanging" IB run is almost always this. An earlier version of this file framed the Gateway as remote from the working host; it is not.

- **Connection**: `MDW_IB_HOST`/`MDW_IB_PORT` env vars or `--host`/`--port` flags. The code default, `127.0.0.1:4001`, is already correct — do not override it with the LAN IP.
- **Gateway version**: pinned to **10.45** (10.46 is incompatible)
- **Trading mode**: live; **2FA** is approved manually in IBKR Mobile on every fresh login — livewire cannot bypass this
- **Do NOT**: write order-management workflows, attempt to restart/manage the Gateway from this repo, or auto-retry on connection failure (failures usually mean 2FA, IBKR maintenance, session conflict, or market-data permission — not something livewire should recover)

## Data Ingestion

Primary data source: **Interactive Brokers** via `ib_async`. Requires the IB Gateway on the Mac mini (see "IB Gateway / IBC" above). IB-backed ingest commands run a preflight check before connecting; if the Gateway is unreachable they report status and exit cleanly rather than burning a 4-min IB timeout. `daily --source massive` and `historical --backfill --source massive` are explicit non-IB equity paths and bypass IB preflight.

- `IBClient` wraps `ib_async.IB` with connection management, historical data, and contract qualification
- `IBClient.connect()` defaults to `clientId=0` and automatically retries successive `clientId` values if IB reports error `326` (`client id already in use`)
- `IBClient.get_historical_data()` fetches daily bars via `reqHistoricalData`
- `BronzeClient` is the live service storage client: it discovers symbols from parquet, merges or replaces per-ticker snapshots, and publishes with `temp -> validate -> os.replace()`
- Equity daily Bronze rows include non-null `source` and `price_basis`. Canonical rows are raw; legacy rows migrate to `legacy/unknown`. IB `TRADES` rows are classified per applicable split event and normalized before publication, because IB treatment is not consistent across split history. Ambiguity blocks the whole symbol mutation.
- `DailyBarFallbackClient` is a narrow recovery client for unresolved target-day gaps in the current U.S. equity universe. Provider order: Nasdaq `assetclass=stocks`, Nasdaq `assetclass=etf`, then Stooq U.S. daily CSV.
- `MassiveClient` is the optional daily U.S. equity accelerator and validation reference. It uses `MASSIVE_API_KEY`, stores `adjusted=false` bars with `adj_close = close`, and is not used for equity intraday or broker-specific asset classes.
- `MassiveClient` also exposes paginated split and cash-dividend reference data. `scripts/livewire_ingest.py corporate-actions` reconciles those events into revision-aware per-symbol bronze histories; corrections and cancellations remain auditable, while only latest active revisions feed the Silver engine.
- `scripts/livewire_store.py rebuild-silver` derives fully back-adjusted daily bars and compact intraday factor intervals under `data-lake/silver/`, then advances `revisions/current.json` atomically after every artifact validates. Bronze is read-only; splits adjust price and volume, while dividends adjust price only.
- `MassiveFlatfileClient` is the only equity-intraday provider path. It uses Massive S3 credentials and downloads whole-market SIP minute files.
- `adj_close` is set to `close` (IB TRADES data doesn't provide adjusted prices)
- **CBOE volatility indices** are fetched directly from CBOE's public API (`cdn.cboe.com/api/global/delayed_quotes/charts/historical/`) via `scripts/livewire_ingest.py cboe-vol`, not IB. This is the authoritative source for VIX, VVIX, VXHYG, VXSMH, and all other CBOE volatility indices. For `VIX` and `SPX`, `cboe-vol` also appends newer rows from CBOE's official daily-price CSV backup when the chart JSON lags. The writer normalizes stale parquet schemas on merge (drops extra columns from older schema versions) and rewrites files to fix schema drift even when no new data is available.
- **Treasury yield rates** are fetched from FRED via `scripts/livewire_ingest.py fred-rates` using `FRED_API_KEY`. Default series are `DGS3`, `DGS5`, `DGS10`, and `DGS30`; they write to `data-lake/bronze/asset_class=rates/symbol=<series>/1d.parquet` with `trade_date`, `symbol_id`, `tenor_years`, `yield_pct`, and `source`.

### IB BarData → Bronze mapping

| IB BarData field | Bronze column | Transform |
|---|---|---|
| `bar.date` | `trade_date` | `str(bar.date)` |
| (from ticker) | `symbol_id` | Read existing parquet ID or derive stable ID |
| `bar.open` | `open` | Already float |
| `bar.high` | `high` | Already float |
| `bar.low` | `low` | Already float |
| `bar.close` | `close` | Already float |
| `bar.close` | `adj_close` | Same value |
| `bar.volume` | `volume` | `int(bar.volume)` |
| provider | `source` | Row-level `ib`, `massive`, `nasdaq`, `stooq`, or `legacy` |
| normalization gate | `price_basis` | Canonical `raw`; unresolved legacy/fallback rows use `unknown` |

Operational commands for this contract are
`scripts/livewire_quality.py calibrate-daily-basis`,
`scripts/livewire_store.py migrate-price-basis`,
`scripts/livewire_quality.py audit-split-basis`,
`scripts/livewire_quality.py resolve-split-basis`,
`scripts/livewire_store.py repair-split-basis`,
`scripts/livewire_quality.py audit-legacy-basis`,
`scripts/livewire_quality.py triage-breaks`,
`scripts/livewire_store.py repair-legacy-basis`, and
`scripts/livewire_store.py rollback-legacy-basis`. Audit manifests record their
resolved data-lake root; repair and rollback reject a different active root
before mutation, and reject a manifest with no root recorded rather than failing
open. Prehistory splits do not affect stored rows; post-history
splits stay pending until repeated provider evidence confirms the effective
post-event basis. Ambiguous in-history events may be resolved from resumable,
repeated IB evidence, which the audit replays against current Bronze and action
hashes. The resolver first determines whether each repeated IB boundary is raw
or adjusted; it does not assume a fixed IB history basis. Two overlapping
Massive adjusted ranges are a narrow fallback when IB evidence remains
ambiguous; repeated provider evidence may also repair
nonpositive OHLC fields in the row's existing basis. Silver applies split
factors only to rows marked raw and fails closed on split-affected unknown rows;
dividend adjustment remains independent.

⚠️ **~90% of the equity universe is `price_basis='unknown'`** (`source='legacy'`).
Those symbols stage today only because they have no splits — `build_factor_intervals`
raises `unknown price_basis for split-affected row` the moment a split touches one,
and the symbol is quarantined and evicted. INTC is exactly this shape (`unknown` ×
11,676 rows). Any new split against that population converts a clean symbol into a
quarantined one, so this is the standing threat to "newly added data is always
silver grade" — not a hypothetical.

### IB BarData → Futures Bronze mapping

| IB BarData field | Bronze column | Transform |
|---|---|---|
| `bar.date` | `trade_date` | `str(bar.date)` |
| (from composite ticker) | `contract_id` | Stable hash of composite ticker (e.g. `ES_202506`) |
| (from composite ticker) | `root_symbol` | Parsed from `ticker.rsplit("_", 1)[0]` |
| (from composite ticker) | `expiry_date` | `YYYY-MM-01` derived from expiry code |
| `bar.open` | `open` | Already float |
| `bar.high` | `high` | Already float |
| `bar.low` | `low` | Already float |
| `bar.close` | `close` | Already float |
| `bar.close` | `settlement` | Same value (IB doesn't provide settlement) |
| `bar.volume` | `volume` | `int(bar.volume)` |
| (default) | `open_interest` | `0` (IB BarData doesn't include OI) |

### Running the pipeline

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py historical                                  # Mag 7 default
python scripts/livewire_ingest.py historical --tickers AAPL NVDA              # Custom tickers
python scripts/livewire_ingest.py historical --preset presets/sp500.json      # From preset with cursor resume
python scripts/livewire_ingest.py historical --years 0 --skip-existing        # Inception, skip existing
python scripts/livewire_ingest.py historical --preset presets/sp500.json --backfill --source auto  # Backfill older equity data; auto keeps deep history on IB
python scripts/livewire_ingest.py historical --preset presets/volatility.json --asset-class volatility  # CBOE vol indices (IB backfill)
python scripts/livewire_ingest.py cboe-vol                                                        # CBOE vol indices (daily sync, preferred)
python scripts/livewire_ingest.py fred-rates                                                      # FRED Treasury yields (DGS3/DGS5/DGS10/DGS30)
python scripts/livewire_ingest.py corporate-actions --tickers NVDA AAPL SPY                       # Targeted Massive split/dividend reconciliation
python scripts/livewire_ingest.py corporate-actions --full-reconcile                             # Whole equity-bronze universe; may infer cancellations
python scripts/livewire_ingest.py corporate-actions --workers 4 --resume --full-reconcile        # Resume incomplete whole-universe reconciliation
python scripts/livewire_ingest.py corporate-actions --dry-run                                    # Compare without publishing
python scripts/livewire_store.py rebuild-silver --tickers NVDA AAPL SPY                          # Targeted adjusted daily/factor rebuild
python scripts/livewire_store.py rebuild-silver --full --dry-run                                 # Full comparison without publishing
python scripts/livewire_quality.py audit-legacy-basis --full --output <lake>/repairs/.../audit.json  # Read-only basis audit (both detectors)
python scripts/livewire_quality.py triage-breaks --audit-manifest <.../audit.json> --output <lake>/repairs/triage/current.json --resume
python scripts/livewire_store.py repair-legacy-basis --audit-manifest <.../audit.json> --output-dir <.../batch1> --priority-only --resume
python scripts/livewire_store.py rollback-legacy-basis --output-dir <.../batch1>                 # Undo a repair batch from its backups
python scripts/livewire_store.py resolve-yahoo-basis --failure-manifest <.../rev-dry.json> --output <lake>/repairs/unknown-basis/<stamp>/manifest.json   # dry-run: Yahoo true-raw reconstruct + self-gate the split-affected unknown-basis failures
python scripts/livewire_store.py resolve-yahoo-basis --failure-manifest <.../rev-dry.json> --output <.../manifest.json> --apply --output-dir <.../batch1> --allow-rewrite --ib-verify --priority-order --resume   # apply: publish only IB-anchor-verified reconstructions (2FA-gated)
python livewire_scripts/validate_silver_canary.py --tickers NVDA AAPL SPY --control SYMBOL       # Read-only factor/OHLCV/bronze-integrity canary
python scripts/livewire_ingest.py historical --preset presets/futures-index.json --asset-class futures  # CME/CBOT index futures
python scripts/livewire_ingest.py historical --preset presets/futures-energy.json --asset-class futures  # NYMEX energy futures
python scripts/livewire_ingest.py historical --host 192.168.1.50 --port 4001 --tickers AAPL            # Remote IB Gateway
```

IB connection defaults to `127.0.0.1:4001`, configurable via `--host`/`--port` flags or `MDW_IB_HOST`/`MDW_IB_PORT` environment variables.

Corporate-action artifacts live at
`data-lake/bronze/asset_class=corporate_action/symbol=<encoded_symbol>/events.parquet`.
Provider corrections increment `event_revision` and link through
`supersedes_action_id`; full reconciliations may append cancellation revisions,
while targeted runs never infer disappearance by default. The scheduled daily
job reconciles actions first (full provider reconciliation on Sunday), runs all
market-data lanes, and rebuilds Silver when **its own inputs** succeeded.

### IB is not a single point of failure

`rebuild-silver` reads equity bronze and the corporate-action store — both
Massive-backed. It never reads IB. So the Silver gate depends on exactly those
two lanes, **not** on futures/cmdty (IB daily), CBOE, or fx. Gating on every lane
meant one stale FX contract blocked the adjusted rebuild for the whole ~13K
equity universe.

IB legitimately owns futures/cmdty daily and volatility intraday
(VIX/SPX/NDX/RUT/VXN/RVX). It no longer owns fx — see "FX and DXY" below. The
rule is that IB *failure* must not cascade:

- An unreachable Gateway exits `GATEWAY_DOWN_EXIT_CODE` (86, distinct from 1
  and argparse's 2). The lane is **skipped, not retried** — 2FA and IBKR
  maintenance are not something livewire recovers, and retrying burns
  3×`retry_delay_seconds` against a dead port. It logs `=== Skipped <scope> ===`
  and the run is DEGRADED, not failed.
- `fetch_batch` maps a raised fetch to the exception, never to `[]`. Collapsing
  both meant a total IB outage classified every ticker `no_trade`, held
  `errors` at 0, and `resolve_exit_code` reported success for a run that
  ingested nothing.

### FX and DXY — Yahoo owns the asset class, IB does not

`scripts/livewire_ingest.py fx` is the only writer of `asset_class=fx`. It is not an
IB lane and never was viable as one: `resolve_fx_pair()` accepts only the 36 hardcoded
`SUPPORTED_IB_FX_PAIRS`, which contains **no NDF currency** and cannot express a
non-six-letter symbol like `DXY`. `fx` was therefore removed from `ASSET_CLASSES`, and
`run_fx_sync` runs it beside the CBOE lane. `resolve_fx_pair()` itself is untouched —
`make_contract()` still uses it for anyone explicitly asking for an IB fx contract.

Source per (symbol, timeframe) — never mixed within one file:

| | Daily | 1m / 5m / 30m | 1h |
|---|---|---|---|
| Currency pairs | Yahoo `<PAIR>=X` | **Massive** `C:<PAIR>` | **Yahoo** |
| `DXY` | Yahoo `DX-Y.NYB` | Yahoo | Yahoo |

Measured 2026-07-27 — re-measure before trusting, the entitlement floors roll:

- **DXY exists only on Yahoo.** IB's `IND DX @NYBOT` returns error 162 (no
  permission); Massive returns 0 rows for `I:DXY`/`C:DXY`/`I:USDX`. Yahoo
  `DX-Y.NYB` daily reaches **1971-01-04**. Yahoo returns 17,219 timestamps but only
  **14,108** carry prices — the rest are null holiday padding and are skipped, never
  back-filled. 14,108 over 55.6 years is 253.9/year, i.e. the trading calendar.
- **Massive REST FX floor is 2 years rolling** (2024-07-24), identical for daily and
  for 1m/5m/30m/1h. Below it, requests 403 — an entitlement boundary, never a
  "no history" signal.
- **1h is Yahoo's even for pairs.** Yahoo's 1h reached 2023-10-09 (EURUSD), *past*
  Massive's floor, in one unthrottled request. Don't "unify" 1h onto Massive.
- **Massive REST allows 5 requests/minute** and sends no `Retry-After`, so reactive
  backoff (1s/2s/4s) cannot clear the window. The lane paces preemptively via
  `MassiveClient(min_interval_seconds=...)`. Nightly ≈12 min; the full 760-day seed
  ≈2 h, dominated by 1m.
- **Massive's S3 `global_forex/` prefix lists back to 2010 but GETs 403.** The
  flat-file entitlement covers `us_stocks_sip` only. Probe permission boundaries with
  GET, never with LIST — the listing alone promises 16 years that cannot be fetched.

Both intraday providers serve rolling windows, so history is **accumulated**:
`merge_ticker_rows` dedups on `bar_timestamp`, and the floor bounds only the initial
seed. Never replace an intraday fx file — a replace throws away everything that has
already rolled out of the provider's window.

```bash
python scripts/livewire_ingest.py fx                       # seed maximum depth (~2h)
python scripts/livewire_ingest.py fx --days 7              # nightly catch-up
python scripts/livewire_ingest.py fx --tickers DXY EURUSD --timeframes 1d 1h
```

`--days` bounds only Massive. Yahoo's chart API takes discrete `range=` values, so
Yahoo-sourced series always fetch their full window regardless.

### Immutable release artifacts — production does not run from the checkout

`scripts/livewire_ops.py release` builds the merged `origin/main` commit into
`<warehouse>/releases/<sha>/` (a `git archive` export plus its own
`uv sync --frozen --no-dev` virtualenv, then `chmod -R a-w`) and atomically
repoints `<warehouse>/current` at it. The scheduled jobs `cd` into `current`, so
editing, branching, or breaking the working tree cannot change what runs tonight.

```bash
python scripts/livewire_ops.py release promote            # build+serve origin/main
python scripts/livewire_ops.py release promote --dry-run  # decide without building
python scripts/livewire_ops.py release list               # `*` marks what is served
python scripts/livewire_ops.py release rollback           # serve the previous one
```

- **A `git worktree` export would not work.** It leaves a `.git` file pointing
  back at the dev repo, so the artifact stays tethered to the checkout it is
  supposed to be independent of. `git archive` has no such tether.
- ⚠️ **`promote` exports `origin/main` but RUNS the checkout's own builder.**
  The two come from different commits. A fix to `release.py` itself does not
  take effect until the checkout you run `promote` from contains it — exporting
  the fixed SHA is not enough. Measured 2026-07-29: promoting from a feature
  branch produced a release whose *source* had `build_node_modules` but whose
  *build* never ran it, so `node_modules/` was silently absent again.
  **`git checkout main && git pull` before promoting anything that changes the
  promoter.**
- ⚠️ **Never `rm -rf` the release `current` points at.** `promote` short-circuits
  on `current already at <sha> — nothing to promote`, checking the symlink and
  not the directory, so deleting the target leaves `current` **dangling** and
  `promote` refuses to rebuild it. Recover with `release rollback` (restores a
  real target), then `promote`. Jobs already running are unaffected —
  `os.getcwd()` is physical — but any new job would fail.
- **`ci.yml` runs on push to main for this reason.** A squash merge creates a
  commit no pull-request run ever covered; `promote` gates on a completed,
  successful run for that exact SHA and otherwise keeps serving the previous
  release. `--allow-unverified` bypasses the gate and is needed exactly once,
  to bootstrap the first release from a SHA predating the push trigger.
- **Flipping `current` mid-run is safe.** `os.getcwd()` is physical, so a job
  that already `cd`-ed into `current` finishes against the release it started
  on. `prune` never collects the release `current` points at.
- **A release carries no `.env`** (gitignored, so `git archive` omits it).
  Credentials must live in `~/market-warehouse/.env`, which
  `livewire_scripts/scheduled_env.py` already loads. `promote` warns when that
  file is absent — without it a scheduled job resolves every credential to
  nothing, the same failure the worktree note below describes.
- **The data lake is deliberately not isolated.** It is the single source of
  truth and both dev and production write it; concurrency there is handled where
  it always was, by the `fcntl.flock` serialization in `clients/parquet_io.py`.
  Containerizing instead would split that into two lock domains that do not see
  each other, and move IB's client source address off `127.0.0.1`.

### Scheduled-job invariants worth not re-breaking

- **The three job plists point at `<warehouse>/current`, never at a checkout.**
  They used to `cd` into the repo and run whatever was on disk at that moment —
  branch, uncommitted edits and all. Only `release-promote` still reads the
  repo, because building the artifact is its job. The older trap this replaced:
  pointing launchd at `.worktrees/<branch>/`, which has no `.env` (gitignored)
  and so resolved every credential to nothing, killing both ingestion and the
  failure alert that would have reported it. A release has no `.env` either —
  which is why credentials must live in `~/market-warehouse/.env`.
- **Alerts that fail to send are persisted** to `<log_dir>/alerts_undelivered/`
  and counted by the watchdog. A WARNING in the log the job just broke is not
  an alert.
- ⚠️ **The lane runner must never run the alert.** `_run_in_own_process_group`
  is keyword-only on `stdout/env/timeout` and returns a `CompletedProcess` with
  **no stdout** (a lane streams into the log file). Threading it into
  `_page_failure` made every page raise `TypeError` *out of `main()`* — so on
  2026-08-02 one failed symbol out of 14,577 in corporate-actions killed the
  whole nightly job, and equity, futures, cmdty, CBOE, FX and Silver never ran.
  No alert was sent either; only the watchdog noticed, 4.5h later. `_page_failure`
  therefore takes **no runner parameter**, and `send_failure_alert` defaults to
  `subprocess.run` *late* so the seam stays patchable. The reason 95% coverage
  missed this: every fake runner in the tests swallows `**kwargs` and every test
  reaching the alert patches `send_failure_alert` itself, so the real pairing was
  never executed. `TestTheLaneRunnerNeverRunsTheAlert` uses the real signature.
- **Every lane pages, and the timeout pages too.** `send_failure_alert` sits at
  the *end* of `run_with_retries` and is reachable only by falling out of the
  retry loop — an early `return` for a new failure mode silently skips it, so
  the timeout branch `break`s. `_run_scheduled_lane` had **no alert path at
  all**, which is why the 2026-07-28 corporate-action wedge produced no alert
  from this job; corporate-actions, CBOE, FX and Silver all run through it.
  `run_cboe_volatility_sync` also carried a byte-identical private copy of the
  lane body, so it silently missed every fix made to the shared one — it now
  calls `_run_scheduled_lane` like the rest. A down Gateway stays silent:
  degraded is not failed.
- **A release carries no `node_modules`.** `git archive` exports only tracked
  files and `node_modules/` is gitignored, so releases shipped without
  `nodemailer` and every alert path was dead. `release promote` now runs
  `npm ci --omit=dev` between `build_venv` and `freeze` (it must precede the
  `chmod -R a-w`) and import-checks the result.
- **corporate-actions fails on a rate, not on one symbol.** `main()` gates the
  Silver rebuild on `action_code == 0`, and the lane returned `1 if failed` — so
  one flaky provider response blocked the adjusted rebuild for the whole ~13K
  equity universe (2026-08-02, `TGNA: Response ended prematurely`, 1 of 14,577).
  A symbol that fails simply keeps the actions already in the store. The rule is
  the rate alone (`FAILURE_RATE_TOLERANCE`, 5%) with no absolute floor, so a
  targeted 2-ticker run that loses one still fails; `resolve_exit_code`'s
  `max(50, …)` floor is calibrated for the equity universe and does not fit here.
  Exit 0 with failures still prints a WARNING naming the count.
- **The watchdog requires the `silver` scope** and reads the equity
  `SUMMARY_JSON`: `=== Done equity ===` with `updated=0` is not healthy.
- ⚠️ **Coverage's cost is per-file, and it outgrew its budget silently.**
  `compute_coverage` opens one parquet footer per symbol per timeframe. Measured
  2026-08-02: `1d` alone is 13,270 files at 11.8 ms each = **154s single-threaded**,
  and there are five timeframes. Against the 600s budget in
  `_spawn_post_success_quality` it timed out **every night from 2026-07-07** —
  coverage logs stop at 2026-06-17, so `weekly` (a pure parser over those logs)
  has produced nothing but 83-byte `No coverage logs found` stubs ever since.
  Nothing was wrong with the data; the detector was blind.
  `FOOTER_READ_WORKERS=16` takes 5.3x off it (154.0s → 29.2s; 32 threads only
  reaches 25.2s, so the curve is flat past 16), and the budget is now 1800s to
  absorb what threads cannot — a **cold glob measured at 281s for one
  timeframe**, against 0.6s warm for the next. Cold is the normal morning state,
  the same asymmetry the DuckDB catalog is built around.
- **A swallowed WARNING is how this hid for four weeks.** `_spawn_post_success_quality`
  must never flip a successful run to failure — that part is right — but nothing
  counted the warnings. `nightly_digest._quality_jobs_section` now reports them;
  keep it, it is the only thing standing between a dead detector and another
  month of silence.
- **coverage/weekly/digest run once, after Silver.** They used to fire inside
  each asset class's success branch — four digests a night, all before Silver,
  so `_silver_section` parsed a log that could not yet contain Silver's
  summary and the `window_regressions` warning was structurally unreachable.
- **A corrupt per-symbol parquet is quarantined, not fatal.** One truncated
  `1m.parquet` aborted the entire whole-market publish every night from
  2026-07-14; the file is now moved to `<lake>/quarantine/<stamp>/` and the
  symbol reported for targeted backfill while the rest of the market publishes.

Silver artifacts are published beneath `MDW_SILVER_DIR` (default
`data-lake/silver`). Daily files preserve Apex-required OHLCV names and add
`price_adjustment_factor`, `split_volume_factor`, and `adjustment_revision`;
factor files contain exhaustive date intervals. Immutable revision manifests are
written before `current.json`, which is the final cross-file commit record.

#### The silver-grade window

**The contract: every symbol publishes the longest suffix of its history that is
silver grade. Deep history is not a goal — a symbol may publish a short series;
what it publishes must be right. Data added at either end (backfilled history or a
new daily bar) is silver grade or it does not publish.**

The window is **derived on every publish and never persisted**, so backfilled
history extends it by itself once the data supports it.

`rebuild-silver` applies **two trims, in this order**. Neither subsumes the other:

1. **The seed floor** — `clients/seed_boundary.classify_seed_boundary`, applied to
   **raw bronze before adjustment**. Deterministic: it looks at a known location
   (the 2021-06-11→21 bulk-seed window) and compares the observed step against the
   fold *predicted* from the corporate-action store. No threshold to tune. This is
   the only detector that sees the **2×–5× class** — the blind heuristic missed 63
   such symbols (APH, TSLA, GE, WMT, CSX, SOXX…), which classified `clean` while
   their pre-seed history was double-adjusted. It **measures rather than assumes**:
   KLAC/COO have a predicted fold but a flat boundary and stay clean. A
   seed-corrupt symbol is **trimmed to its post-seed window, not quarantined** —
   its ~5 years of post-2021-06 history are perfectly good.
2. **The window scan** — `clients/silver_window.resolve_window`, a blind
   >`--continuity-threshold` (default `6.0`) scan over the **adjusted** series, for
   every other unexplained break. Exempt evidence-backed dates with
   `--continuity-allowlist <ISO_DATE>…` (global by date, not per-symbol).

Everything this design does happens **above 6.0**. A symbol whose only unexplained
break is 3×–5× publishes with that break intact — a 4× single-day move is ordinary
for a small cap, and trimming them all would amputate more real history than it
repairs. The honest claim is: *everything published is silver grade at the 6.0
definition.* Nothing assumes 6.0; lower the threshold and re-triage if that changes.

A symbol that cannot stage at all (e.g. `unknown price_basis` against a split) is
quarantined — and quarantine means its artifact is **moved to
`<silver>/evicted/<revision>/…`**, not merely dropped from the manifest. Apex
resolves symbols by path construction and never consults the manifest for
membership, so an un-manifested file keeps serving stale data forever; moving it is
the only eviction Apex can perceive (it then fails closed with HTTP 500).

Factor intervals are deliberately **wider** than the daily window: Apex LEFT JOINs
bronze intraday bars onto them and hard-fails on any uncovered bar, and bronze
intraday extends before a trimmed window. Never narrow factors to match the daily
file.

#### Two active splits on one ex-date

`latest_active()` dedupes on the **provider-scoped** `provider_event_id`, so one
logical event recorded under two ids survives twice — and `build_factor_intervals`
multiplied every active action, both into `splits_by_date` and in the per-bar
loop. The store has always assumed one active split per ex-date
(`corporate_action_store.py`, `# ponytail: one active split per ex-date is
assumed`); nothing enforced it.

Measured 2026-08-02: 18 such ex-dates across 16 symbols. They are not two events
— they are one event disagreeing with itself: exact inverses (LIME `300:1` and
`1:300`, TTSH `3000:1` and `1:3000`), ratios that migrated between dates across
revisions (TSM 2007 and 2009 swapped), or the same ratio restated at another
scale (PGC `10:11` and `100:110`, CZFS `1:1.01` and `100:101`).

- **Equal ratios collapse.** One event written twice applies once. Dropping the
  duplicate from `action_factors` is the half that matters — checking the ratio
  without removing the entry still double-adjusts in the per-bar loop.
- **Unequal ratios fail closed**, quarantining the symbol. Nothing in the store
  says which is right, so publishing either one would be a guess.

⚠️ **Count duplicate records, not affected symbols.** Only **5** of the 16 have
their duplicate ex-date *inside* stored bronze (FTLF, LADR, MDRR, OUT, SLG); for
the rest it is prehistory and touches no stored row, exactly as
`first_trade_date < action.ex_date` already required. All 5 were independently
absent from Silver, so the production impact at discovery was **zero** — the bug
was latent, not active. Reading blast radius off the action store alone
overstates it every time.

#### Cancellation inference is provider-scoped

`reconcile(..., full_reconcile=True)` infers a cancellation from an event's
*absence* in the provider response. `reconcile()` only ever speaks for
`RECONCILE_PROVIDER` (Massive) — `_from_provider` hardcodes it — but the sweep
used to cancel **every** active row regardless of provider. So the Sunday
`--full-reconcile` undid the yahoo splits `apply_repairs` had added, every week:
507 of 1,014 cancelled across 2026-07-19 (418) and 2026-07-26 (89). Absence from
a Massive response says nothing about an event Massive was never asked for.

#### Break triage — keeping real market moves

Not every discontinuity is corruption. `scripts/livewire_quality.py triage-breaks`
classifies each break the audit recorded against Massive as a *second source*
(`clients/break_triage.py`), using both bases:

| Signal | Verdict | Effect |
|---|---|---|
| Our jump present in Massive's **raw** series | `real_move` | keep — never trimmed |
| Our jump absent from Massive's raw series | `bad_data` | trim |
| Massive's **adjusted÷raw** factor steps across the date | `missing_action` | trim (the record is what's missing, not the price) |
| Provider cannot answer | `inconclusive` | trim |

```bash
python scripts/livewire_quality.py triage-breaks \
    --audit-manifest <.../audit.json> --output <lake>/repairs/triage/current.json --resume
```

- **`/v2/aggs` is entitled for a rolling ~5 years only** (floor measured
  **2021-07-12** on 2026-07-17). Every older break is `inconclusive` — always. A
  large `inconclusive` count is the expected shape, not a failure.
- **The floor rolls, so the verdict manifest is durable and default-loaded** from
  `<data-lake-root>/repairs/triage/current.json`. The nightly job passes no flags;
  without the verdicts at that path every confirmed `real_move` is re-read as an
  unexplained break and trimmed the next night. Never delete the verdict store to
  "force a re-triage" — a verdict obtained today may be unobtainable next year.
- Transient provider failures (rate-limit, 5xx, timeout, a wrapped connection
  failure) **abort the run and are never checkpointed**; `--resume` re-asks them.
- The run probes the credentials against an entitled date first: a bad key 401s on
  every request, which is indistinguishable from the entitlement floor and would
  otherwise trim the whole population silently.

#### Window regressions — the prevention invariant

A symbol whose window start moves **later** than the revision currently serving it
is **withheld from republication** and keeps serving its previous window. This is
the fail-closed half of the contract: the suffix rule trusts the newer side of a
break, which is right for the 2021-06 seed artifact and wrong for a bad new bar —
a corrupt close arriving tonight would otherwise collapse the window onto itself
and publish one garbage row. The nightly digest reports the count under
**Silver rebuild**; the run still exits 0, so the digest is the only alert.

`--allow-window-regression` overrides it. **Required exactly once, for the rev-3
bootstrap**, because rev-2 published untrimmed history and every intentional trim
looks like a regression on that first run.

#### Legacy-basis full repair → rev-3 (operator-reviewed, resumable)

Two commands feed the window: an offline audit that classifies the legacy population
`clean`/`mixed`/`error` with **both** detectors, and a resumable IB re-derivation
that rewrites `mixed` symbols to canonical true-raw. Then a single
`rebuild-silver --full` publishes rev-3. Run the phases in order and **review the
`mixed` count before touching IB**:

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

#### Unknown-basis reconstruction — IB-anchor-verified (`resolve-yahoo-basis`)

`scripts/livewire_store.py resolve-yahoo-basis` flips `unknown → raw` for the
unknown-basis population so split-affected symbols can stage in Silver. It
reconstructs true-raw from Yahoo (`raw = yahoo_adj × Π split`), reconciles Yahoo's
splits against the action store, classifies each bronze row (relabel/rewrite/
mismatch), and **fails closed** on split-mismatch or >5% row mismatch (ticker
reuse / wrong entity). Dry-run by default. Reconciliation is **bounded to
in-history splits** (`ex_date > first stored bronze date`): a split on/before the
first row touches no stored row — both `reconstruct_raw_closes` and
`build_factor_intervals` apply only `ex_date > bar_date` — so a Yahoo/store
disagreement there is immaterial and must not block the symbol.

`--apply` **requires `--ib-verify`** — no publish without IB confirmation. IB is a
**gate, never a data source**: the reconstruction is compared to IB only on the
**post-last-split window** (where IB is definitionally raw; IB's deep history is
basis-ambiguous and must not be compared). A symbol publishes only when its recent
window matches IB within tolerance. Every non-`published` verdict (`ib_mismatch`,
`ib_insufficient_overlap`, `high_mismatch`, `split_mismatch`, `stage_fail`) leaves
bronze **untouched** and lands in the review queue — fail-closed = no downgrade.
IB unreachable **aborts** the run (never a withhold); `--resume` re-asks. Writes go
through the same verbatim-backup + `rollback-legacy-basis` path as the other basis
repairs.

Rollout is batched and 2FA-gated: batch-1 = the split-affected unknown-basis
failures from `rebuild-silver --full --dry-run --failure-output`, then a STOP GATE
on the published-vs-review ratio before the ~12K tail. Full design + plan:
`docs/superpowers/specs/2026-07-19-unknown-basis-ib-verified-reconstruction-design.md`
and `docs/superpowers/plans/2026-07-19-unknown-basis-ib-verified-reconstruction.md`.

Reliability foundation environment variables:
- `MDW_TELEMETRY_PATH` (default `~/market-warehouse/logs/telemetry.jsonl`): telemetry JSONL append path; set to `none` to disable telemetry.
- `MDW_QUALITY_AUDIT_PATH` (default `~/market-warehouse/logs/quality_audit.jsonl`): central quality-flag audit JSONL append path.
- `MDW_ALERT_SEVERITY_THRESHOLD` (default `warning`): minimum quality-flag severity that triggers per-flag email.
- `MDW_ALERT_RATE_LIMIT_SECONDS` (default `300`): de-dup window for identical `(source, ticker, category)` alert emails.
- `MDW_ORCHESTRATOR_TIMEOUT_SECONDS` (default `300`): per-ticker hard timeout for `scripts/livewire_ingest.py robust`.
- `MDW_ORCHESTRATOR_MAX_ATTEMPTS` (default `3`): per-ticker retry budget for `scripts/livewire_ingest.py robust`.
- `MDW_ORCHESTRATOR_COOLDOWN_SECONDS` (default `60`): sleep between orchestrator retry attempts.
- `MDW_LOG_LEVEL` (default `INFO`): logger root level for reliability tooling.
- `MDW_UNDELIVERED_DIR` (default `~/market-warehouse/logs/quality_alerts_undelivered/`): where failed per-flag alert HTML bodies are preserved.
- `MDW_LOG_DIR` (default `~/market-warehouse/logs/`): where `scripts/livewire_quality.py report --email` writes `quality_summary_YYYY-MM-DD.marker`.

Massive S3 flat-file environment variables:
- `MASSIVE_S3_ACCESS_KEY`: required Massive S3 access key for equity intraday.
- `MASSIVE_S3_SECRET_KEY`: required Massive S3 secret key for equity intraday.
- `MDW_FLATFILE_LOOKBACK_DAYS` (default `7`): direct `flatfile-ingest catch-up` lookback.
- `MDW_FLATFILE_BUCKETS` (default `256`): raw ticker buckets per trading day.
- `MDW_FLATFILE_STORAGE_MULTIPLIER` (default `8`): capacity-planning multiplier for a full build.
- `MDW_FLATFILE_MIN_FREE_GB` (default `25`): required free-space reserve after a full build.
- `MDW_FLATFILE_MIN_PUBLISH_RATIO` (default `0.9`): minimum share of the raw
  file's ticker set a publish must cover before the run fails. Nothing checked
  this before — a raw file holding 12,000 symbols could publish 40 and exit 0.
  Skipped on a resumed run, where a low published count is legitimate.
- `MDW_SYNC_PHASE_TIMEOUT_SECONDS` (default `21600`, 6h): hard per-phase budget
  in `daily-backfill`. There was no timeout on this path at all, so a wedged IB
  call blocked its phase forever and launchd would not start another instance.
- `MDW_DAILY_JOB_DEADLINE_SECONDS` (default `14400`, 4h): **total** wall-clock
  budget for one `run-daily-job` run, shared across every lane. It is
  deliberately a total, not per-lane: `main()` runs seven lanes sequentially
  (corporate-actions, equity, futures, cmdty, CBOE, FX, Silver), so a per-lane
  budget of N hours would permit a 7N-hour job. Measured whole-job wall clock
  over 2026-07-01..28: healthy runs peak at **3.27h**, the watchdog checks at
  **+4.5h**, so the budget must sit in that narrow band. A lane that exhausts
  the budget is killed **by process group** (`subprocess.run`'s own timeout
  signals only the direct child, orphaning `--workers` pools that keep holding
  `fcntl.flock`), is never retried, and **pages**.

DuckDB analytical catalog environment variables:
- `MDW_DUCKDB_PATH` (default `~/market-warehouse/analytics.duckdb`): catalog database holding the coverage table. Views need no database at all.

Current fetch behavior:
- Normal mode atomically replaces the per-ticker bronze snapshot
- Backfill mode merges older bars into the same per-ticker bronze snapshot
- Daily and intraday mutations take a blocking advisory lock per exact parquet
  path before read/replace/merge/publish; persistent `*.parquet.lock` sidecars are
  coordination files and are excluded from symbol discovery.
- The live service path writes bronze parquet only
- If IB returns an empty head timestamp, the fetcher falls back to `IB_EARLIEST_DATE` instead of skipping the symbol
- `--asset-class volatility` uses `Index('SYMBOL', 'CBOE')` contracts instead of `Stock('SYMBOL', 'SMART')` and writes to `data-lake/bronze/asset_class=volatility/`
- `--asset-class futures` uses `Future(root, expiry, exchange)` contracts with composite tickers (`ES_202506`), writes to `data-lake/bronze/asset_class=futures/`, and uses the futures parquet schema (contract_id, root_symbol, expiry_date, settlement, open_interest)

### Backfill mode

`--backfill` fetches only missing older data for tickers already in bronze parquet:
- Queries each ticker's oldest existing `trade_date` from parquet
- Fetches the inception → oldest_date gap. For equity, `--source auto` keeps this deep older-history path on IB because live Massive validation showed long requests can return partial ranges. `--source massive` is explicit and does not complete the cursor if the returned range does not reach the requested start.
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

`backfill-all` (`livewire_scripts/backfill_runner.py`) is the default warehouse build. It runs equity daily seed/backfill for `sp500`, `ndx100`, and `r2k`; the older-history daily phase remains IB-backed through `--source auto`. It then syncs FRED Treasury rates and runs one maximum-entitled-history, full-market Massive flat-file equity-intraday build in parallel with the CBOE/IB volatility lane. It finishes by refreshing the DuckDB coverage table.

`daily-backfill` (`livewire_scripts/sync_runner.py`) is the routine catch-up runner. It uses Massive for recent equity daily gaps and one whole-market flat-file catch-up over the default 7 calendar days (`MDW_DAILY_BACKFILL_INTRADAY_DAYS`). It also runs the full-universe `flatfile-ingest-daily catch-up` day_aggs lane (`MDW_DAILY_BACKFILL_DAY_AGGS_DAYS`, default 7) before the intraday flatfile phase — this lane owns the ~20K SIP daily universe and heals symbols that have intraday but no `1d`. Side lanes stay on their existing sources: FRED rates, CBOE daily volatility, and IB volatility intraday. The DuckDB coverage refresh runs last, after every writer. At the end it prints one `SUMMARY_JSON` line (per-phase label/exit/duration + failed list) that the intraday-catchup wrapper and nightly digest consume.

Output: per-ticker bronze Parquet at `data-lake/bronze/asset_class=equity/symbol=<ticker>/{1d,1m,5m,30m,1h}.parquet` and volatility/index bronze Parquet under `data-lake/bronze/asset_class=volatility/symbol=<ticker>/`.

### Futures preset format

Futures presets use a `contracts` array instead of `tickers`:
```json
{
  "name": "futures-index",
  "asset_class": "futures",
  "contracts": [
    {"root": "ES", "exchange": "CME", "expiry": "202506"},
    {"root": "NQ", "exchange": "CME", "expiry": "202506"}
  ]
}
```
`load_preset()` flattens these into composite tickers (`ES_202506`) and returns an exchange map for contract construction.

Delisted symbols that should no longer participate in future syncs or backfills should be archived outside the canonical sync path under `data-lake/bronze-delisted/asset_class=equity/symbol=<ticker>/1d.parquet`.

### Daily updates

`scripts/livewire_ingest.py daily` is the lightweight command for daily scheduled runs (~2,500 tickers). It discovers tickers from bronze parquet, detects gaps vs the latest trading day, fetches only missing bars, validates OHLCV integrity, and atomically rewrites only the affected per-ticker snapshots. If IB leaves unresolved target trading days after validation, the command can recover those dates from the fallback chain before publishing parquet.

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

**Scheduling with launchd** (macOS):
```bash
# The three jobs run the immutable release, so they take the WAREHOUSE path.
# The promoter is the one job that reads the repo — it is what builds the release.
WAREHOUSE=~/market-warehouse
for L in daily-update daily-update-watchdog intraday-catchup; do
  sed "s|/path/to/warehouse|$WAREHOUSE|g" "launchd/com.livewire.$L.plist.example" \
    > ~/Library/LaunchAgents/com.livewire.$L.plist
done
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.release-promote.plist.example \
  > ~/Library/LaunchAgents/com.livewire.release-promote.plist
for L in daily-update daily-update-watchdog intraday-catchup release-promote; do
  launchctl load ~/Library/LaunchAgents/com.livewire.$L.plist
done
```
`scripts/livewire_ops.py run-daily-job` loads `~/.secrets`, repo `.env`, and `~/market-warehouse/.env` before invoking the retrying scheduled runner. The runner automatically syncs equities, futures, and cmdty via IB, then all volatility indices via CBOE's public API and DXY/FX via Yahoo+Massive, in a single invocation; pass `--asset-class <name>` to run only one IB asset class (skips both the CBOE volatility and fx syncs). After a successful run it also spawns coverage + weekly quality reports and sends the nightly digest email.

A second scheduled job, `com.livewire.intraday-catchup`, runs at 05:00 UTC daily (= 01:00 ET EDT / 00:00 ET EST) and invokes `scripts/livewire_ops.py run-intraday-catchup-job`. This calls the existing `daily-backfill` orchestrator so equity daily + intraday parquet (1m/5m/30m/1h), FRED Treasury rates, CBOE volatility daily, and IB volatility intraday (30m and 5m, with 1h derived locally from 30m) all refresh. Equity intraday requires `MASSIVE_S3_ACCESS_KEY` and `MASSIVE_S3_SECRET_KEY`; missing credentials fail the orchestrator before any phases run, and there is no REST or IB equity-intraday fallback. The wrapper is single-attempt because `daily-backfill` owns its phase execution and activity-based stall detection; on terminal failure the wrapper sends one alert through the same Nodemailer pipeline as the daily-update wrapper, tagged `--job-name intraday_catchup`. Logs land at `~/market-warehouse/logs/intraday_catchup_YYYY-MM-DD.log` (UTC date). The default 7-day `MDW_DAILY_BACKFILL_INTRADAY_DAYS` lookback absorbs a single missed run; widen via `~/market-warehouse/.env` if you need more headroom. 05:00 UTC is well after Massive's empirical whole-market SIP minute-aggregate publish (file for trade-date D appears shortly after midnight ET on D+1) and gives a 1h15m buffer after IBC's `AutoRestartTime=11:45` ET nightly restart (= 03:45 UTC), which requires 2FA approval before port 4001 is available.

The main sync runs at 06:00 UTC daily (= 02:00 ET EDT / 01:00 ET EST, ~12h after US RTH close). Shifted from 05:05 to give a 2h15m buffer after IBC's 03:45 UTC nightly restart and 2FA window. The watchdog runs at 10:30 UTC daily (= 06:30 ET) and alerts if the scheduled sync never started or never logged a completion marker. Non-trading days are harmless no-ops. All three plists use `StartCalendarInterval` with Mac-local `Hour`/`Minute` (launchd has no `TimeZone` key); on this Mac (`Asia/Hong_Kong`, UTC+8) the configured Hour values map to the UTC targets above. See each plist's header comment for the conversion table to other Mac timezones.

**Key design:**
- Discovers tickers from parquet via `BronzeClient.get_latest_dates()` — no hardcoded lists
- `--target-date YYYY-MM-DD` lets operators run a fixed-date catch-up and prevents bars later than the requested target from being published
- Live service writes avoid analytical database file-lock contention
- Bar validation: checks OHLCV relationships, positive prices, valid trading days, duplicate dates
- Atomically rewrites a per-ticker bronze snapshot after each successful merge
- The active sync universe is the canonical bronze tree only; archive delisted symbols outside that tree if they should stop participating in future syncs/backfills
- Recovery path for unresolved target-day gaps (equity only): `daily --source massive` fetches the missing target-date bars directly from Massive; the default IB daily path can still compare against Massive when configured and then falls back to Nasdaq historical quote API (`stocks`, then `etf`) and Stooq `symbol.us`; fallback is skipped for non-equity asset classes (volatility, futures, CMDTY, FX)
- Fallback bars use the same validation and bronze merge path as IB bars
- Per-ticker outcomes are classified as `updated` / `no_trade` (no bars returned — the instrument didn't trade, not a failure) / `partial` (target day filled, older gaps remain) / `error` (exception/HTTP failure). The run emits one machine-readable `SUMMARY_JSON` line (schema in `livewire_scripts/daily_outcomes.py`) plus the human table.
- Exit code is threshold-based (`resolve_exit_code`): a run fails only on systemic failure — `error` count over `max(50, 5% of processed)`, or zero updates with any error. `no_trade`/`partial` never fail a run, so an illiquid warrant no longer fails the whole job.
- Pure-Python NYSE trading calendar — no new dependencies
- Logs to `~/market-warehouse/logs/daily_update_YYYY-MM-DD.log`
- Terminal scheduled failures use the Nodemailer CLI at `scripts/livewire_ops.py send-alert`; the failure email uses a static, truthful incident report (parsed from `SUMMARY_JSON`) and writes a sibling `*.human.md` report. There is no AI enrichment — Cerebras was removed.

### Reliability tooling

`scripts/livewire_ingest.py robust` is the productized per-ticker IB orchestrator for bulk daily-bar seed/backfill runs. Use it instead of direct historical command loops for any IB bulk run larger than roughly five tickers:

```bash
python scripts/livewire_ingest.py robust --preset presets/sp500.json --mode seed
python scripts/livewire_ingest.py robust --preset presets/sp500.json --mode backfill
python scripts/livewire_ingest.py robust --tickers AAPL MSFT --mode seed --timeout 300 --max-attempts 3 --cooldown 60
```

Outcome categories:

| Category | Meaning |
| --- | --- |
| `ok` | Child exited cleanly and bronze exists; row delta is included when known. |
| `ok-noop` | Backfill exited cleanly with no row delta, treated as "no older history". |
| `skip` | Seed skipped because bronze already exists, or backfill skipped because no seed parquet exists. |
| `fail` | Child exited non-zero, exhausted retries, or exited zero without producing seed bronze. |
| `timeout` | All attempts hit the hard timeout. |

`scripts/livewire_quality.py report` reads telemetry and quality-audit JSONL:

```bash
python scripts/livewire_quality.py report --view summary --since 24h
python scripts/livewire_quality.py report --view flap --since 24h --source ib
python scripts/livewire_quality.py report --view quality --since 24h --severity critical
python scripts/livewire_quality.py report --view summary --since 24h --email
```

Views are `summary`, `flap`, and `quality`; `--source` accepts `all`, `ib`, `uw`, or `massive`. `report --email` sends the daily-summary Nodemailer mode. The routine post-daily email is now the **nightly digest** (`scripts/livewire_quality.py digest --run-date YYYY-MM-DD --email`), which the daily-job wrapper spawns on success — it assembles one plain-text report from the per-job `SUMMARY_JSON` lines (outcome table per asset class, intraday phase table, coverage line, disk headroom) and writes `quality_summary_YYYY-MM-DD.marker` for the watchdog. Quality flags are emitted beside parquet as `<parquet>.meta.json`; the sidecar schema and central audit JSONL schema are specified in `docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md`.

### IB intraday backfill (non-equity)

`scripts/livewire_ingest.py intraday-backfill` is IB-only and remains available
for non-equity asset classes. Equity intraday uses `flatfile-ingest`.

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py intraday-backfill --timeframe 1m --asset-class futures --source ib --preset presets/futures-index.json
python scripts/livewire_ingest.py intraday-backfill --timeframe 5m --asset-class volatility --source ib --preset presets/volatility-intraday.json
```

- Per-timeframe cursor: `~/market-warehouse/cursors/cursor_intraday_{1m,5m,30m,1h}_{preset}.json`. Resumes after interrupt.
- IB error 162/200 ("HMDS no data" / ambiguous contract) marks the ticker complete and moves on — no infinite retry loop.
- Default depth: 5 years for 1m, 2 years for 1h, 1 year for 5m (matches `INTRADAY_MAX_DEPTH`).
- `--days N` fetches only a recent calendar-day window and is intended for daily catch-up jobs.
- `--skip-existing` consults `min(bar_timestamp)` in the existing per-ticker parquet and skips if it already covers the requested depth.
- IB BarData with `formatDate=1` returns naive ET datetimes; the script attaches `America/New_York` and converts to UTC before validation/merge.
- Logs to `~/market-warehouse/logs/backfill_intraday_{1m,5m,30m,1h}_YYYY-MM-DD.log`.

### Massive flat-file ingestion (equity intraday)

`scripts/livewire_ingest.py flatfile-ingest` is the only equity-intraday path.
It discovers the maximum entitled range, stages bucketed raw daily Parquet,
publishes every exact provider ticker, and derives all intraday timeframes.

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py flatfile-ingest discover
python scripts/livewire_ingest.py flatfile-ingest backfill
python scripts/livewire_ingest.py flatfile-ingest catch-up --days 7
python scripts/livewire_ingest.py flatfile-ingest repair --dates 2026-06-05
```

Requires `MASSIVE_S3_ACCESS_KEY` and `MASSIVE_S3_SECRET_KEY`. `backfill`
discovers the provider-entitled range and refuses to download unless projected
storage leaves the configured reserve. Raw partitions live under
`data-lake/raw/massive/us_stocks_sip/minute_aggs_v1/date=YYYY-MM-DD/`; resume
state lives under `~/market-warehouse/cursors/`. Modes operate on every symbol
in each selected whole-market file; ticker and preset filters are unsupported.

### Massive day_aggs flat-file ingestion (full-universe equity daily)

`scripts/livewire_ingest.py flatfile-ingest-daily` widens the daily ingest
universe from the ~2.5K preset-driven `daily` command to the full SIP universe
(~20K tickers) by reading Massive's `day_aggs_v1` whole-market daily flat files
back to the provider's rolling GET floor (**2021-07-28** as of 2026-07-29 — see
the warning below; the LIST-advertised 2003 start is not fetchable). Same
operational model as `flatfile-ingest` (discover / backfill
/ catch-up / repair, durable cursor under `~/market-warehouse/cursors/`,
capacity guard on full backfill), but writes per-ticker `1d.parquet` bronze
via `BronzeClient` — no intraday derivation. Raw partitions live under
`data-lake/raw/massive/us_stocks_sip/day_aggs_v1/date=YYYY-MM-DD/`; the publish
phase uses a process pool by default (`--workers`, env
`MDW_FLATFILE_DAILY_WORKERS`, default 4) with 32 ticker buckets per day
(`--buckets`, env `MDW_FLATFILE_DAILY_BUCKETS`).

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire_ingest.py flatfile-ingest-daily discover
python scripts/livewire_ingest.py flatfile-ingest-daily backfill --workers 4
python scripts/livewire_ingest.py flatfile-ingest-daily catch-up --days 14
python scripts/livewire_ingest.py flatfile-ingest-daily repair --dates 2026-06-11
```

Requires `MASSIVE_S3_ACCESS_KEY` and `MASSIVE_S3_SECRET_KEY`. Both
`flatfile-ingest` and `flatfile-ingest-daily` can run side-by-side — they use
separate raw paths and separate cursors.

⚠️ **The flat-file GET floor is a rolling 5 years. LIST lies.** Measured
2026-07-29 with one GET per calendar year against both prefixes, then a binary
search: **2021-07-27 → 403, 2021-07-28 → OK**, i.e. exactly 1827 days = 5.00
years before the probe date, identical for `day_aggs` and `minute_aggs`. Every
year 2003–2021 returns `403 Forbidden`. The LIST-derived `discovery.earliest`
of `2003-09-10` (5755 days) is the same trap already documented for
`global_forex/` — *probe permission boundaries with GET, never with LIST*.
An earlier version of this file claimed day_aggs reaches "back to 2003"; it
does not, and never did.

Two consequences:

- **`backfill` is not a deep-history tool.** It re-fetches inside the rolling
  window only. As of 2026-07-29 the warehouse already holds the entire entitled
  range (`raw_completed` starts 2021-06-11, *earlier* than the current floor,
  because those files were fetched when the window reached further back).
- **Never delete raw partitions to reclaim space.** Anything older than the
  current floor cannot be re-downloaded, ever — the same standing as the triage
  verdict store. Re-measure before trusting any of this; the floor rolls forward
  one day per day. Result: `logs/probes/2026-07-29-flatfile-get-floor.json`.

### Coverage tracking + auto-recovery

`scripts/livewire_quality.py coverage` runs after each successful daily job (spawned by `run-daily-job`). For each tracked timeframe (`1d`, `1m`, `1h`, `5m`, `30m`) it counts how many symbols in the **active bronze universe for that timeframe** have bars current as-of the target trading day, using parquet footer statistics (not a full column read). A symbol counts as present if it is current OR absent from the day's raw traded set (no-trade is not missing). It writes a one-line summary to `~/market-warehouse/logs/coverage_YYYY-MM-DD.log`, and — when coverage drops below `MDW_COVERAGE_ALERT_THRESHOLD` (default `0.95`) — triggers a targeted backfill subprocess and re-checks.

```bash
python scripts/livewire_quality.py coverage                                # Today's coverage + auto-recovery
python scripts/livewire_quality.py coverage --no-recover                   # Report only
python scripts/livewire_quality.py coverage --target-date 2026-04-06       # Specific trading day
python scripts/livewire_quality.py coverage --threshold 0.99               # Stricter threshold
python scripts/livewire_quality.py coverage --force                        # Run on a non-trading day
```

- 1d recovery uses Massive daily REST. Intraday recovery downloads and republishes the whole target-day flat file with `flatfile-ingest repair --dates <date>`. Coverage uses that day's raw `_symbols.parquet` set.
- **Safety cap (default 100):** if more than N symbols are missing for any single timeframe, the script aborts the auto-recovery and emails immediately. This prevents a runaway IB rate-limit hit when an entire daily run failed for some other reason.
- Email goes out only when post-recovery gaps remain. A fully successful recovery downgrades to an INFO log — no false-positive email storms.
- Reuses the existing Nodemailer alert path at `scripts/livewire_ops.py send-alert`.

### Weekly quality summary

`scripts/livewire_quality.py weekly` is a pure parser over the seven daily coverage logs from the previous ISO week. Self-skips on non-Sunday so the entrypoint can call it unconditionally every day.

```bash
python scripts/livewire_quality.py weekly            # Sunday: write the report; other days: noop
python scripts/livewire_quality.py weekly --force    # Render anyway
python scripts/livewire_quality.py weekly --week 2026-14
```

Output: `~/market-warehouse/logs/quality_weekly_YYYY-WW.md` with a coverage trend table, symbol churn (added/removed), and persistent gaps (≥3 consecutive missing days at any timeframe).

### Health check (intraday)

`scripts/livewire_quality.py health --intraday --timeframe {1h,5m}` performs interior gap detection for the intraday parquet, with optional suspected-halt annotation (contiguous gap < 30 min surrounded by normal bars). Default behaviour is **report-only**. When `--symbol`, `--since`, and `--timeframe` are all set, the command implicitly repairs that narrow window by shelling out to `scripts/livewire_ingest.py intraday-backfill` (no separate `--repair` flag — full scope means repair).

```bash
python scripts/livewire_quality.py health --intraday --timeframe 5m                       # Scan all symbols
python scripts/livewire_quality.py health --intraday --timeframe 5m --symbol AAPL         # Scan one symbol
python scripts/livewire_quality.py health --intraday --timeframe 5m --symbol AAPL --since 2026-04-01  # Repair window
```

### DuckDB analytical catalog

DuckDB addresses the parquet lake with SQL. It copies no bar data: the only
durable artifact is a coverage table of per-symbol file statistics (~536 KB for
26,382 symbol-rows across seven views).

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

⚠️ **Glob enumeration is the dominant cost of the whole lake, and it dwarfs
reading data.** Measured 2026-08-02 against 13,270 equity 1d files / 19.75M rows:

| Operation | Time |
|---|---|
| Open one known parquet file | 0.04s |
| `CREATE VIEW` over the equity `1h` glob | **221.04s** |
| Whole-universe `count(*)`, filesystem cache warm | 0.86s |
| The same query after the cache was evicted | **283.84s** |
| `parquet_metadata()` over equity 1d (the "footer-only shortcut") | 471s |

Three rules follow, and breaking any of them makes the catalog unusable:

- **Views are registered on demand, never eagerly.** `CREATE VIEW` binds the
  schema, and binding enumerates the glob — so registering all 13 views costs 13
  full enumerations before a single query runs. `connect()` registers nothing by
  default; `duckdb sql` registers only the views its query text names.
- **Symbol-scoped reads bypass views entirely.** `read_symbols()` /
  `duckdb bars` construct `symbol=<TICKER>/<tf>.parquet` paths directly. A
  two-symbol query took 0.53s that way against >5 min through the glob.
- **The coverage table is durable because the cache is not.** The nightly job
  writes 23.57 GB of intraday, which is exactly what evicted the cache between
  the 0.86s and 283.84s readings. Cold is the normal morning state, so freshness
  questions get a table rather than being re-derived from 13,270 footers.

Coverage is **daily-only**. A pass over the intraday tier would enumerate and
scan 23.57 GB, and intraday cannot be materialised at all — equity `1m` alone is
23.57 GB against ~20 GiB of free disk.

`build` publishes by writing a staging database and `os.replace()`-ing it into
place. This is required, not stylistic: DuckDB is single-writer, so an in-place
rebuild fails outright whenever a reader is connected. Concurrent `read_only`
readers are fine — four simultaneous readers measured 0.00s each.

**Postgres was removed** (2026-08-02). It had been dead in production for
months: `MDW_POSTGRES_DSN` was unset in `~/market-warehouse/.env`, so both
orchestrators skipped the lane every night and 14 days of nightly logs contain
no `postgres` line at all. `tests/test_duckdb_containment.py` now holds the
line that got DuckDB retired in 2026-05 — DuckDB may be imported only by the
catalog modules, and no command may materialise bars out of bronze.

## Testing

**All new code in `clients/` and `scripts/` must have tests. Coverage is enforced at 95% (`fail_under = 95` in `pyproject.toml`, `--cov-fail-under=95` in CI) for the source currently included by `pyproject.toml`; `clients/ib_client.py` is still omitted from the fail-under gate and covered by focused tests separately.**

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

## Pre-commit Hook

A secrets scanner runs on every commit, checking staged files for API keys, passwords, private keys, tokens, and credentials. Install with:

```bash
ln -sf ../../tools/pre-commit-secrets-scan.sh .git/hooks/pre-commit
```

Catches: AWS keys, API key/secret/password assignments, private key headers, GitHub/Slack tokens, Google API keys, connection strings with credentials, hardcoded IB credentials, staged `.env` files. Allowlists test files, placeholders, comments, `os.environ` reads, and error messages to avoid false positives. Bypass with `git commit --no-verify` if needed.

## Key Implementation Details

- IB BarData provides native float/int types — no string parsing needed
- `symbol_id` is now a stable 53-bit hash from `blake2b(symbol)` for new symbols
- Live ingestion writes bronze parquet directly; DuckDB reads that parquet in place when SQL access is needed
- Empty IB head timestamps now fall back to the earliest supported IB historical date instead of skipping the symbol
- Bronze Parquet uses per-ticker Hive-partitioned layout: `data-lake/bronze/asset_class=equity/symbol=AAPL/1d.parquet` (futures: `asset_class=futures/symbol=ES_202506/1d.parquet`)
- Bronze publication is atomic at the file level: write temp parquet, validate it, then `os.replace()` into place
- Bronze mutation is serialized per exact parquet path with `fcntl.flock`; this prevents concurrent writers from silently losing each other's updates while allowing different symbols and timeframes to proceed independently.
- `BronzeClient` accepts `asset_class` constructor param (`"equity"`, `"volatility"`, or `"futures"`) to select the appropriate parquet schema. Default `"equity"` preserves all existing behavior.
- `IBClient.connect()` auto-retries successive `clientId` values after IB error `326`, then records the actual connected ID

## Known Environment Gotchas

Common traps that derail debugging sessions — check these before investigating further:

- **IB Gateway availability**: the Gateway runs on the mini, which is the host you are on — check with `nc -z 127.0.0.1 "${MDW_IB_PORT:-4001}"` before assuming IB is up. **A `nc -z` against the LAN IP also succeeds and is a trap**: the port is open but `TrustedTwsApiClientIPs` is empty, so the API connection silently times out. Do NOT attempt restarts: failures usually mean 2FA, IBKR maintenance, or session conflict, not something livewire should recover.
- **Empty IB head timestamps**: IB returns empty head timestamps for some symbols. The fallback to `IB_EARLIEST_DATE` is intentional — don't treat it as an error.
- **IB error 326 (client ID in use)**: Handled by auto-retry in `IBClient.connect()`. Don't manually reassign client IDs.
- **Weekend/holiday runs**: IB returns no data on non-trading days. These are harmless no-ops — don't debug "no data returned" on weekends or holidays.
- **CBOE volatility fetch**: Volatility indices use CBOE's public API, not IB. If VIX or SPX data looks stale, check `scripts/livewire_ingest.py cboe-vol` and the official daily-price CSV backup behavior, not IB connectivity.

## Provider Interface

`clients/historical_provider.py` defines:
- `HistoricalProvider` — abstract interface for fetching IB historical data
- `IBProvider` — direct IB Gateway connection via ib_async
- `BarRecord` — OHLCV bar dataclass
- `ib_contract_to_spec()` / `spec_to_ib_contract()` — contract serialization helpers

### Date Formats

All dates are ISO format:
- Bar dates: `YYYY-MM-DD` (e.g., `2025-01-02`)
- Head timestamps: ISO 8601 datetime (e.g., `2010-01-04T09:30:00`)
