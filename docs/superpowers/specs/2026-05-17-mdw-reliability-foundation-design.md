# Livewire Reliability Foundation — Design (Sub-project A)

**Date:** 2026-05-17
**Status:** Approved, awaiting implementation plan
**Project:** Livewire (a market data warehouse). Rebranded 2026-05-17; still serves the same role.
**Scope:** Sub-project A of the larger Livewire system redesign (decomposition: A reliability, B postgres, C massive.io, D intraday timeframes, E options chains, F gold tables)
**Supersedes:** [`2026-05-17-ib-connection-telemetry-design.md`](./2026-05-17-ib-connection-telemetry-design.md) — that smaller spec is folded in here as Component 1 (`clients/telemetry.py`)

## Naming convention (post-rebrand)

The repo path is now `/Users/chenxi/projects/livewire/` but several internal conventions still reference the prior name:

| Surface | Status | Recommended decision |
|---|---|---|
| Data directory `~/market-warehouse/` | Unchanged on disk | **Keep as-is** — renaming the data tree is a separate operation (lots of data to move, launchd plists to update); the directory name is descriptive ("market warehouse"), not a project brand |
| Env-var prefix `MDW_*` | Existing config uses this | **Keep `MDW_*`** — it refers to the descriptive domain ("market data warehouse"), not the project name. Avoids touching launchd plists, `.env` files, secrets, watchdog scripts |
| Logger prefix `mdw.*` | Used in proposed new modules | **Keep `mdw.*`** — same reasoning |
| User-facing docs (titles, README, spec headers) | Said "market-data-warehouse" | **Update to "Livewire"** — this spec already uses "Livewire" in its title |
| Python module / package names | None today (flat `clients/` + `scripts/`) | N/A — no package rename needed |

If you'd prefer to rename `MDW_*` → `LIVEWIRE_*` or `LW_*` later, it's a separate sweep: spec it, do it once, support both prefixes for one release cycle. Not blocking this work.

## Why this exists

On 2026-05-17 we hit a sustained instability while attempting bulk historical backfill via Interactive Brokers. The pain pattern surfaced multiple failure modes that share a single root: the pipeline is observation-blind. Specifically:

- **Connection-state opacity.** HMDS farm flapped between `2106 OK` and `2105 broken` 11 times in a 12-minute span, then went silent for 28 minutes mid-fetch. Farm-state events were buried in unstructured text alongside thousands of unrelated lines. We could not compute "HMDS uptime over the last 24 hours" without manual grep.
- **Silent partial fetches.** SMH's bronze parquet was written with 1,758 of an expected ~6,000 rows. From the outside it looked identical to a clean fetch — no flag, no sidecar, no alert.
- **Single long-lived sessions degrading.** A 3-ETF run completed cleanly in 2 minutes; a 95-ticker run on the same Gateway started timing out within 8 minutes. Different connection-lifetime characteristics, identical CLI surface.
- **No per-ticker timeout or retry budget.** One hung head-timestamp call (XLE) stalled the entire batch for 28 minutes.
- **No structured cross-source perspective.** Today only IB is wired up for ingestion; tomorrow UW and massive.io join, and the pipeline will fail in different ways from each. No unified observation layer.

The user's directive was clear: **"we need to ensure the stability here ... cant tolerate for the instability on and off"** and **"I want to strengthen this project, from bottom up."** The brainstorming-phase answers established:

- **Goal:** *zero silent data loss* — failures are tolerable but invisible failures are forbidden. The SMH case must become impossible.
- **Partial-fetch detection scope:** all four categories — range shortfall, interior gaps, fetch-error tainting, row-count anomaly (the last is a stub in Sub-A, activated in Sub-C when a second source exists).
- **Flag storage:** sidecar JSON per parquet + central JSONL audit log. No new database dependency in Sub-A. Clean migration path to Postgres in Sub-B.
- **Orchestrator productization:** new `scripts/run_ib_fetch_robust.py` entry point; ALL IB-talking jobs (bulk backfills, daily update, intraday backfills, single-ticker debug) route through it.
- **Alerting strength:** email on flag + daily summary, via the existing Nodemailer path.
- **Approach:** *integrated platform* — multi-source-ready from day 1 with `source` tagging everywhere. Sub-C/E plug in by adding source clients; no Sub-A retrofits.

## Non-goals

- **Not** self-healing connection management. This is observation + bounded retry + alerts. Humans decide MTTR.
- **Not** per-request telemetry beyond connection events. UW/massive `record_request` interfaces are sketched but implemented in Sub-C.
- **Not** Postgres migration. Bronze stays parquet; telemetry and audit stay JSONL. Sub-B handles the analytical-layer migration.
- **Not** options data, intraday timeframe expansion, gold tables, or massive.io ingestion. Those are sub-projects C/D/E/F.
- **Not** a real-time dashboard. CLI report + email summaries only.
- **Not** modifying the existing bronze schema or write semantics. Quality flags are observational metadata, not bronze content.

## Architecture

```
┌───────────────────────────────────────────────────────────────────────────┐
│            scripts/run_ib_fetch_robust.py  (NEW — orchestrator)           │
│   • Single entry point for ALL multi-ticker IB fetches                    │
│   • Per-ticker subprocess spawn (process isolation)                       │
│   • Hard timeout (--timeout, default 300s)                                │
│   • Retry budget (--max-attempts, default 3)                              │
│   • Cooldown between attempts (--cooldown, default 60s)                   │
│   • Cursor-aware: skips already-done tickers                              │
│   • Backfill mode: distinguishes "no older data" from "failed"            │
└───────────────────────────┬───────────────────────────────────────────────┘
                            │ spawns per-ticker subprocess
                            ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  scripts/fetch_ib_historical.py | daily_update.py | backfill_intraday.py  │
│  (existing single-ticker workers, ~+50 LOC each):                         │
│   • Build fetch_metadata (start, end, ib_head_ts, errors, bars_received)  │
│   • On success, before atomic-publish:                                    │
│       flags = quality_detector.detect_all(bars, metadata)                 │
│       if flags: quality_flags.write_sidecar + append_audit + alert        │
│   • Atomic-publish proceeds regardless (partial gets saved + flagged)     │
└───────────────────────────┬───────────────────────────────────────────────┘
                            │ uses
                            ▼
┌───────────────────────────────────────────────────────────────────────────┐
│  clients/ib_client.py (transport)  →  clients/telemetry.py (observer)     │
│                                       ConnectionTelemetry(source="ib",…)  │
│                                       emits per-event JSONL               │
│                                       Stubs: UWTelemetry, MassiveTelemetry│
│                                              (interfaces ready for Sub-C) │
└───────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────┐
│  clients/quality_detector.py — pure detection (no I/O)                    │
│    • detect_range_shortfall(expected_start, actual_start, head_ts)        │
│    • detect_interior_gaps(bars, nyse_calendar)                            │
│    • detect_fetch_tainting(errors_during_fetch)                           │
│    • detect_row_count_anomaly(...)  ← STUB in Sub-A, activated in Sub-C   │
│                                                                           │
│  clients/quality_flags.py — emit (3 independent paths)                    │
│    • write_sidecar(parquet_path, flags, metadata) → <parquet>.meta.json   │
│    • append_audit(flag, source, ticker, tf, path) → quality_audit.jsonl   │
│    • alert_on_flag(flag, severity) → Nodemailer email (severity-gated)    │
└───────────────────────────────────────────────────────────────────────────┘

      Storage (single-process invariant; no locking needed):
        ~/market-warehouse/logs/telemetry.jsonl       (one combined file, source-tagged)
        ~/market-warehouse/logs/quality_audit.jsonl   (one combined file)
        ~/market-warehouse/data-lake/bronze/.../1d.parquet.meta.json   (sidecar per parquet)

┌───────────────────────────────────────────────────────────────────────────┐
│  scripts/data_quality_report.py — unified CLI (NEW)                       │
│    --since 7d --source ib|uw|massive|all --view summary|flap|quality      │
│    --email   (renders HTML, sends via Nodemailer)                         │
└───────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────┐
│  scripts/run_daily_update_job.py (existing, +10 LOC at end):              │
│    after the daily sync completes, invoke                                 │
│    data_quality_report.py --view summary --since 24h --email              │
└───────────────────────────────────────────────────────────────────────────┘
```

### Key boundary decisions

- **One combined telemetry JSONL** with `source` field tagging every event (not per-source files). CLI report filters with `--source`. Makes cross-source rollups trivial.
- **Closed-set `source` enum** validated at every write boundary: `{"ib", "uw", "massive"}`. Prevents JSONL pollution from typos or rogue callers.
- **Sidecar JSON is a convenience for per-parquet inspection**; the central audit JSONL is the source of truth for queries and alerts. Independent emit paths — any one failing doesn't sink the others.
- **Orchestrator is the single execution model** for all IB-talking jobs. `fetch_ib_historical.py` becomes a single-ticker worker that the orchestrator invokes. `daily_update.py` and `backfill_intraday.py` get the same quality-detection hook but keep their own multi-ticker loops (they don't strictly need subprocess isolation since their per-ticker calls are already short-lived).
- **Row-count anomaly is a stub in Sub-A** — interface defined, returns `None` until a second source exists. Activated in Sub-C without changing call sites.
- **Partial bronze still gets written** — but with a sidecar flag that makes the partial state queryable and alerted. The operator can decide whether to delete and re-fetch.
- **Telemetry default-on for production, default-off in tests** via `MDW_TELEMETRY_PATH` env var.

## Components

### `clients/telemetry.py` (new, ~250 LOC)

Source-agnostic. Stub classes for UW and Massive defined with their full interface so Sub-C/E plug in by implementing the methods.

```python
class BaseTelemetry:
    def __init__(self, source: str, jsonl_path: Path): ...
    def _emit(self, record: dict) -> None: ...    # rate-limited warnings on failure
    def start(self) -> None: ...
    def stop(self) -> None: ...    # idempotent

class ConnectionTelemetry(BaseTelemetry):
    """IB-specific. Subscribes to ib_async events."""
    def __init__(self, source: str = "ib", *, ib: IB, jsonl_path: Path): ...
    def _on_error(self, reqId, errorCode, errorString, contract): ...
        # 2104/2105/2106/2107/2158 → farm_state; else → ib_error

class UWTelemetry(BaseTelemetry):
    def __init__(self, source: str = "uw", *, jsonl_path: Path): ...
    def record_request(self, endpoint: str, status: int, dt_ms: int): ...    # stub in Sub-A
    def record_rate_limit(self, remaining: int, reset_at: int): ...          # stub in Sub-A

class MassiveTelemetry(BaseTelemetry):
    def __init__(self, source: str = "massive", *, jsonl_path: Path): ...
    def record_request(self, endpoint: str, status: int, dt_ms: int): ...    # stub in Sub-A
```

Codes parsed by `ConnectionTelemetry`:

| Code | Meaning | `event` | `state` |
|---:|---|---|---|
| 2104 | Market data farm OK | `farm_state` | `ok` |
| 2105 | HMDS farm broken | `farm_state` | `broken` |
| 2106 | HMDS farm OK | `farm_state` | `ok` |
| 2107 | HMDS farm inactive | `farm_state` | `inactive` |
| 2158 | Sec-def farm OK | `farm_state` | `ok` |

Farm name (`usfarm`, `ushmds`, `secdefnj`, …) extracted from trailing `:farmname` suffix. Unknown codes emit `{"event":"ib_error","code":N,...}` so visibility is preserved on unexpected codes.

### `clients/quality_detector.py` (new, ~200 LOC)

Pure functions. No I/O. Easy to test exhaustively.

```python
@dataclass(frozen=True)
class QualityFlag:
    category: str           # "range_shortfall" | "interior_gaps" | "fetch_tainted" | "row_count_anomaly"
    severity: str           # "critical" | "warning" | "info"
    detail: dict            # category-specific structured detail
    ts: str                 # ISO 8601 UTC

def detect_range_shortfall(
    expected_start: date,
    actual_start: date,
    ib_head_timestamp: date | None,
) -> QualityFlag | None: ...

def detect_interior_gaps(
    bars: list[BarRecord],
    trading_calendar: TradingCalendar,
) -> QualityFlag | None: ...

def detect_fetch_tainting(
    errors_during_fetch: list[FetchError],
) -> QualityFlag | None: ...

def detect_row_count_anomaly(
    bars: list[BarRecord],
    reference_source: "SourceComparison | None" = None,
) -> QualityFlag | None:
    if reference_source is None:
        return None    # STUB in Sub-A

def detect_all(bars, metadata, trading_calendar) -> list[QualityFlag]: ...
```

Thresholds (initial values, tunable):

| Detector | Warning threshold | Critical threshold |
|---|---|---|
| `range_shortfall` | > 5 trading days | > 30 trading days OR shortfall against `ib_head_timestamp` |
| `interior_gaps` | ≥ 1 missing trading day | ≥ 10 consecutive OR ≥ 30 total missing |
| `fetch_tainted` | ≥ 1 error (162 / 2105) | ≥ 5 errors |
| `row_count_anomaly` | abs(delta) > 1% of reference | abs(delta) > 5% of reference |

### `clients/quality_flags.py` (new, ~150 LOC)

Three independent emit paths. Any failing alone doesn't sink the others.

```python
_VALID_SOURCES = {"ib", "uw", "massive"}

def write_sidecar(parquet_path: Path, flags: list[QualityFlag], metadata: dict) -> bool:
    """Atomic temp → os.replace. Returns False on OSError (logged warning)."""

def append_audit(
    flag: QualityFlag, source: str, ticker: str, timeframe: str, parquet_path: Path
) -> bool:
    """Append one JSON line. Raises ValueError on invalid source. Returns False on OSError."""

def alert_on_flag(
    flag: QualityFlag, source: str, ticker: str, severity_threshold: str = "warning"
) -> bool:
    """Spawn Nodemailer email if severity meets threshold AND not rate-limited.
    On SMTP failure, write rendered HTML to ~/market-warehouse/logs/quality_alerts_undelivered/."""
```

Sidecar JSON schema (example):

```json
{
  "parquet_path": "asset_class=equity/symbol=SMH/1d.parquet",
  "fetch_started_at": "2026-05-17T19:11:56Z",
  "fetch_completed_at": "2026-05-17T19:43:33Z",
  "source": "ib",
  "asset_class": "equity",
  "ticker": "SMH",
  "timeframe": "1d",
  "expected_start": "1993-01-29",
  "actual_start": "2019-05-20",
  "actual_end": "2026-05-15",
  "ib_head_timestamp": "1993-01-29",
  "bars_received": 1758,
  "errors_during_fetch": [{"code": 162, "count": 12}, {"code": 2105, "count": 3}],
  "retry_attempts_used": 3,
  "flags": [
    {"category": "range_shortfall", "severity": "critical", "detail": {"shortfall_years": 26.3}},
    {"category": "fetch_tainted", "severity": "warning", "detail": {"error_count": 15}}
  ]
}
```

Audit JSONL line schema:

```json
{"ts":"2026-05-17T19:43:33Z","source":"ib","ticker":"SMH","timeframe":"1d","parquet_path":"...","category":"range_shortfall","severity":"critical","detail":{"shortfall_years":26.3}}
```

### `scripts/run_ib_fetch_robust.py` (new, ~300 LOC)

Productized orchestrator. The `/tmp/orchestrate_ib_fetch.sh` script from 2026-05-17 rewritten in Python for testability.

```bash
python scripts/run_ib_fetch_robust.py \
    --preset presets/sp500.json --mode seed \
    --asset-class equity \
    --timeout 300 --max-attempts 3 --cooldown 60
```

Outcome categories emitted per ticker:

| Code | Meaning |
|---|---|
| `[ok]` | Bronze written with mtime > start. Success. |
| `[ok-noop]` | Backfill mode, exit 0, no rows added. Recognized as "no older data" — NOT failure. |
| `[skip]` | Seed mode + bronze exists, OR backfill mode + bronze missing. |
| `[fail]` | All `--max-attempts` exited non-zero or wrote no bronze. |
| `[timeout]` | Subprocess SIGKILLed after `--timeout`. Counts as failed attempt; retry applies. |

Final summary line:

```
=== orch done mode=seed ok=61 ok-noop=8 skip=0 fail=1 timeout=0 elapsed=192m ===
```

### `scripts/fetch_ib_historical.py` (existing, ~+80 LOC)

After IB fetch returns bars, **before** atomic publish:

```python
fetch_metadata = {
    "fetch_started_at": ..., "fetch_completed_at": ...,
    "source": "ib", "ticker": ticker, "timeframe": "1d",
    "expected_start": expected_start, "ib_head_timestamp": head_ts,
    "errors_during_fetch": collected_errors,
    "bars_received": len(bars), "retry_attempts_used": ...,
}
flags = quality_detector.detect_all(bars, fetch_metadata, trading_calendar)
if flags:
    quality_flags.write_sidecar(parquet_path, flags, fetch_metadata)
    for f in flags:
        quality_flags.append_audit(f, "ib", ticker, "1d", parquet_path)
        quality_flags.alert_on_flag(f, "ib", ticker)
# atomic publish proceeds regardless
```

New flag: `--no-quality` (debug only) — disables the quality-detector hook.

Same integration in `scripts/daily_update.py` and `scripts/backfill_intraday.py` (~+50 LOC each).

### `scripts/data_quality_report.py` (new, ~250 LOC)

```bash
python scripts/data_quality_report.py --view summary --since 24h
python scripts/data_quality_report.py --view flap --since 7d --source ib
python scripts/data_quality_report.py --view quality --since 24h --severity warning
python scripts/data_quality_report.py --view summary --since 24h --email
```

`--view summary` (per source) prints:
- Connection event count, total session time
- Farm uptime % per farm
- Flap count
- Quality flag counts by category × severity
- Top 10 tickers by flag count

Definitions used throughout the report:
- **Drop** = a state transition from `ok` to any non-`ok` state (`broken`, `inactive`, or any future code).
- **Flap** = a contiguous burst of ≥3 state transitions where every consecutive pair is < 10 min apart. The burst ends when the gap to the next transition exceeds 10 min. Each burst counts as one flap.
- **MTBD (mean-time-between-drops)** = average wall-clock interval between successive *drops* within the `--since` window.
- **Uptime %** = fraction of `--since` window during which the farm's most recent observed state was `ok`. Periods before the first observed state for a farm in the window are excluded from the denominator.

`--view flap` prints chronological flap windows.

`--view quality` prints chronological flag list filtered by severity.

`--email` flag renders HTML body and invokes the Nodemailer `--mode daily-summary` entry point. Writes a marker file `~/market-warehouse/logs/quality_summary_YYYY-MM-DD.marker`.

### `scripts/send_daily_update_failure_email.mjs` (existing, ~+60 LOC)

Two new entry points:
- `--mode flag-alert --payload <json>` — single quality-flag email (called from `quality_flags.alert_on_flag`)
- `--mode daily-summary --payload <json>` — daily roll-up email (called from `data_quality_report.py --email`)

Both render HTML using the existing template family.

### `scripts/run_daily_update_job.py` (existing, ~+15 LOC)

At the end of the day's sync, after the existing watchdog completion marker:

```python
subprocess.run([
    sys.executable, "scripts/data_quality_report.py",
    "--view", "summary", "--since", "24h", "--email",
])
```

Failure of this step is logged as ERROR but does not block the watchdog completion marker — the sync itself already succeeded.

### `scripts/check_daily_update_watchdog.py` (existing, ~+10 LOC)

Extended to verify both markers exist:
- `daily_update_YYYY-MM-DD.marker` (existing)
- `quality_summary_YYYY-MM-DD.marker` (new)

Missing summary marker → existing alert path.

## Data flow

### Write path: one orchestrated ticker

```
1. CLI: python scripts/run_ib_fetch_robust.py --preset presets/sp500.json --mode seed

2. Orchestrator outer loop reads tickers, for each T:
       ├─ pre-check: bronze exists? (skip if seed/exists, skip if backfill/missing)
       ├─ record bronze_state_before (mtime, rows_before)
       └─ for attempt in 1..max_attempts:
           ├─ start_ts = now()
           ├─ spawn: fetch_ib_historical.py --tickers T --years 0 [--backfill] ...
           │
           │  ┌──────── inside the subprocess ────────────────────────┐
           │  │ a. IBClient.connect()                                  │
           │  │     ConnectionTelemetry("ib", ib, telemetry.jsonl)     │
           │  │     emits: {source:"ib", event:"connected"}            │
           │  │                                                        │
           │  │ b. IB Gateway warns 2104/2106/2107 etc.                │
           │  │     telemetry emits: {event:"farm_state",code:N,...}   │
           │  │                                                        │
           │  │ c. head-timestamp request                              │
           │  │     metadata["ib_head_timestamp"] = result             │
           │  │                                                        │
           │  │ d. window loop: reqHistoricalData per chunk            │
           │  │     metadata["errors_during_fetch"].append(...)        │
           │  │     bars accumulated                                   │
           │  │                                                        │
           │  │ e. AFTER bars collected, BEFORE atomic publish:        │
           │  │     flags = quality_detector.detect_all(bars, meta)    │
           │  │     for f in flags:                                    │
           │  │         quality_flags.write_sidecar(path, flags, meta) │
           │  │         quality_flags.append_audit(f, "ib", T, ...)    │
           │  │         quality_flags.alert_on_flag(f, "ib", T)        │
           │  │                                                        │
           │  │ f. BronzeClient atomic publish (temp → os.replace)     │
           │  │     — partial data STILL writes; sidecar tags it       │
           │  │                                                        │
           │  │ g. IBClient.disconnect()                               │
           │  │     telemetry: "disconnected", "telemetry_stopped"     │
           │  └────────────────────────────────────────────────────────┘
           │
           ├─ wait up to --timeout seconds; SIGKILL on overrun
           ├─ success_check based on mode (seed: mtime>start, backfill: rows↑ OR exit==0)
           ├─ if success: record outcome and break
           └─ else: sleep --cooldown, continue retry loop
       │
       └─ per-ticker line written to orch_runs/<stamp>/_summary.log:
              [N/M ok|ok-noop|skip|fail|timeout] T attempt=A dt=Ds rows=R (Δ+R)

3. Orchestrator final summary:
       === orch done mode=seed ok=61 ok-noop=8 skip=0 fail=1 timeout=0 ===
```

### Read paths (three independent inspection paths)

**Path 1 — Per-parquet sidecar.** `cat ~/market-warehouse/.../symbol=X/1d.parquet.meta.json` shows fetch metadata, flags, errors, retry attempts. Use case: "did this specific bronze file fetch cleanly?"

**Path 2 — Central audit JSONL via CLI.** `data_quality_report.py --view quality --since 24h` reads `quality_audit.jsonl` via DuckDB; prints table grouped by source/ticker.

**Path 3 — Cross-source telemetry.** `data_quality_report.py --view summary --since 7d` reads `telemetry.jsonl` via DuckDB; computes uptime / flap count / MTBD per (source, farm).

### Alerting flow

```
quality_detector returns N flags
  │
  ▼
quality_flags.alert_on_flag(flag, source, ticker)
  │
  ├─ severity < threshold ? log + skip
  ├─ rate-limit check (de-dup identical flags within 5 min) ? skip
  └─ spawn: node send_daily_update_failure_email.mjs --mode flag-alert --payload '{...}'
       │
       ▼
     Nodemailer SMTP send
       │
       ├─ success: log
       └─ fail: log ERROR (alerting failure IS critical),
                preserve HTML body to quality_alerts_undelivered/,
                drop event, do NOT block parquet publish
```

### Daily summary roll-up

```
run_daily_update_job.py final step (after watchdog completion marker):
  │
  ▼
subprocess: python scripts/data_quality_report.py --view summary --since 24h --email
  │
  ├─ render HTML body: per-source uptime, flap counts,
  │  quality-flag rollup by category, top-10 affected tickers,
  │  links to today's orchestrator summary file
  │
  └─ spawn: node send_daily_update_failure_email.mjs --mode daily-summary
```

### Cross-source extensibility (for Sub-projects B/C/E)

When `clients/uw_client.py` extension lands in Sub-project C:

```python
# Inside UWClient.__init__:
self._telemetry = UWTelemetry(jsonl_path=telemetry_path)
# Inside each request method:
self._telemetry.record_request(endpoint, status_code, dt_ms)
```

Same `telemetry.jsonl` file gets entries with `"source":"uw"`. CLI report's `--source uw` filter just works — no code change in the report.

### Concurrency

Single-process invariant: at most one IB-talking subprocess at a time (the orchestrator spawns ticker N+1 only after ticker N exits). Telemetry and `quality_audit.jsonl` writers are therefore single-writer. `os.write(O_APPEND)` is atomic for line sizes < ~4 KB; we accept the rare interleaved-but-valid-JSONL edge case if the invariant ever breaks (reader skips corrupt lines via DuckDB's `ignore_errors=true`).

## Error handling

**Principle: every observation/alerting path is allowed to fail; none of them blocks the data path.**

### Failure matrix

| Failure | Behavior | Why |
|---|---|---|
| `telemetry.jsonl` write fails (any source) | Logged warning rate-limited to 1/min, dropped, IB/UW/massive request continues | Telemetry is observational |
| `quality_audit.jsonl` write fails | Logged warning, **sidecar JSON still written**, email-on-flag still attempted | Audit is one of three independent emit paths |
| Sidecar JSON write fails (e.g., dir missing) | Logged warning, **audit still written**, alert still sent | Sidecar is a convenience |
| Email send fails (SMTP down / Nodemailer crash) | Logged ERROR (alerting failure is critical), HTML body preserved at `quality_alerts_undelivered/<ts>_<ticker>.html`, does NOT retry or block bronze publish | Bronze still has sidecar + audit; daily summary surfaces gaps |
| Quality detector raises | Caught in caller; ticker marked `[ok-detect-error]` in orchestrator summary; audit JSONL gets one `detector_error` event tagging the ticker | Detection bug must not hide data |
| `source` value not in `{ib, uw, massive}` | `quality_flags.append_audit` raises `ValueError`; caller catches and logs ERROR; flag dropped from audit but still written to sidecar | Closed-set enum prevents pollution |
| Orchestrator subprocess hangs past `--timeout` | SIGKILL, `pkill -P` to clean orphans, line logged as `[N/M timeout] T attempt=A dt=Ds (>Ts, killed)` | Hard cap on every attempt |
| Orchestrator subprocess exits non-zero | Counted as failure; retry up to `--max-attempts` with `--cooldown` sleep; if all exhausted → `[N/M fail]` | Bounded retry budget |
| Orchestrator: backfill exits 0, no rows added | `[N/M ok-noop] T (no older history)` — NOT counted as failure | Fixes today's orchestrator bug |
| Orchestrator: parent crashes / SIGTERM | Explicit `pkill -P` cleanup in `atexit` handler; in-flight ticker has no bronze update so next run picks it up | Cursor-free safe-resume |
| `telemetry.jsonl` parent dir missing | `BaseTelemetry.start()` catches `FileNotFoundError`, logs warning, sets `self._disabled = True`, `_emit()` becomes no-op | Disabled is safer than broken |
| Stub `UWTelemetry` / `MassiveTelemetry` instantiated before Sub-C ships | Classes exist with valid `__init__`/`start`/`stop`; methods are no-ops; logged INFO line on instantiation | Interface contract from day 1 |
| Concurrent JSONL writers race | Lines remain valid JSONL (atomic `os.write(O_APPEND)` for <4 KB lines); reader's `read_json_auto(..., ignore_errors=true)` skips corrupt residue | Single-process invariant; contingency only |
| Trading calendar lookup fails | Local NYSE calendar (no network dep); if local lookup raises → `gap_detection_unavailable` info-flag, no exception propagated | Fail-open for one detector |
| IB returns more rows than expected | Not flagged in Sub-A; row-count-anomaly stub returns None; logged INFO | Anomaly direction matters in Sub-C |

### Alerting-failure paradox

The thing we worry about most is alerts not delivering, because that breaks "impossible to miss." Mitigation:

1. **Per-flag email failures are logged as ERROR** with the rendered HTML preserved at `~/market-warehouse/logs/quality_alerts_undelivered/<ts>_<ticker>.html`.
2. **Daily summary email runs `data_quality_report.py` against the audit JSONL**, so it shows flags-fired in the last 24h regardless of whether per-flag emails delivered.
3. **The daily summary email itself failing is detectable** via the extended watchdog (`quality_summary_YYYY-MM-DD.marker` check).

This isn't 100% — SMTP outage lasting more than a few days would eventually go undetected. We accept that as outer-edge risk; manual fallback documented as `grep ERROR ~/market-warehouse/logs/*.log | grep alert`.

### Logging conventions

- `mdw.telemetry` for telemetry-layer warnings
- `mdw.quality` for quality-detector / quality-flag warnings
- `mdw.orchestrator` for `run_ib_fetch_robust.py`
- All observational warnings use `WARNING`; only alerting-path failures use `ERROR`
- Respects `MDW_LOG_LEVEL` env var

### Backward compatibility

- `fetch_ib_historical.py` retains its existing CLI surface; quality-detector hook activates automatically; disable with `--no-quality`.
- `daily_update.py` and `backfill_intraday.py` — no CLI changes, internal hook.
- Existing tests in `tests/test_fetch_ib_historical.py`, `tests/test_daily_update.py`, etc. continue to pass; new tests cover the new code paths.

## Testing

Per `AGENTS.md`: 100% coverage on `clients/` and `scripts/`. New modules must hit that bar from day one.

### Test files

#### `tests/test_telemetry.py` (new, replaces `test_ib_telemetry.py`)

14 tests covering: BaseTelemetry emit semantics + rate limiting + disabled-state, ConnectionTelemetry handler attach/detach/parsing for codes 2104/2105/2106/2107/2158/162, UWTelemetry/MassiveTelemetry stub no-op behavior, source-tagging, env-var default path resolution.

#### `tests/test_quality_detector.py` (new)

16 tests, table-driven per category: range_shortfall (clean, SMH-case, head matches actual, tolerance), interior_gaps (no gaps, weekend, half-day, single gap, critical threshold, calendar unavailable), fetch_tainting (zero / one / above threshold), row_count_anomaly stub returns None, detect_all returns multiple flags or empty.

#### `tests/test_quality_flags.py` (new)

10 tests: atomic temp→replace sidecar, complete schema, OSError graceful, audit one-line-per-flag, invalid-source ValueError, OSError graceful, alert below threshold skipped, alert above threshold spawns Nodemailer, rate-limited duplicate within 5 min, SMTP-failure HTML preservation.

#### `tests/test_run_ib_fetch_robust.py` (new)

12 tests: skip-existing in seed mode, skip-missing in backfill mode, success detection per mode, retry on non-zero, max-attempts exhausted, timeout SIGKILL + pkill, cooldown sleep timing, backfill ok-noop, backfill real success, summary log format, atexit cleanup kills orphans, SIGTERM handler clean shutdown.

#### `tests/test_data_quality_report.py` (new)

12 tests: summary view uptime per farm, per-source breakdown, quality counts by category, top-10 affected tickers, flap view chronological order, severity filter, source filter, since-window filter, malformed lines skipped with count, missing files exit 0, email mode renders HTML and invokes Nodemailer, email marker write.

#### `tests/test_fetch_ib_historical.py` (existing, extend)

4 new tests: detector invoked on success path, `--no-quality` skips detector, clean fetch produces no sidecar/audit/email, partial fetch writes sidecar + audit + invokes alert.

Same shape added to `tests/test_daily_update.py` and `tests/test_backfill_intraday.py`.

#### `tests/test_run_daily_update_job.py` (existing, extend)

3 new tests: end-of-day quality report invoked, report subprocess failure logged not blocking, watchdog extended to check both markers.

### Coverage gate

- `pyproject.toml` `fail_under = 100` already enforced.
- All new modules (`clients/telemetry.py`, `clients/quality_detector.py`, `clients/quality_flags.py`, `scripts/run_ib_fetch_robust.py`, `scripts/data_quality_report.py`) **included in the gate from day one**.
- `clients/ib_client.py` keeps its existing exclusion (already covered by focused tests).
- Tests must pass `-W error::RuntimeWarning` per project rule.

### Manual verification

1. **Telemetry sanity**: Any IB fetch for ~30 s → `telemetry.jsonl` contains `connected` + `farm_state` + `disconnected` with `"source":"ib"`.
2. **Quality detection on a real partial**: Re-run SMH fetch (known partial via IB) → bronze gets sidecar JSON with `range_shortfall` critical flag; audit JSONL gains entry; email arrives.
3. **Clean fetch produces no sidecar**: Any clean fetch (e.g., recent NVDA) → no sidecar, no audit entry, no email.
4. **CLI report**: `data_quality_report.py --view summary --since 24h` prints rollup with farm uptime + flag counts.
5. **Orchestrator timeout**: Force a long-hanging fetch (during HMDS down) → SIGKILL fires at 300 s, summary shows `[timeout]`, retry attempted.
6. **Daily summary email**: `data_quality_report.py --view summary --since 24h --email` → HTML email arrives.
7. **End-to-end daily**: Daily-update job runs a full cycle → both `daily_update.marker` AND `quality_summary.marker` exist; summary email arrived.

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `MDW_TELEMETRY_PATH` | `~/market-warehouse/logs/telemetry.jsonl` | Where telemetry JSONL is appended. Set to `none` (case-insensitive) to disable. |
| `MDW_QUALITY_AUDIT_PATH` | `~/market-warehouse/logs/quality_audit.jsonl` | Where quality-flag audit JSONL is appended. |
| `MDW_ALERT_SEVERITY_THRESHOLD` | `warning` | Minimum severity for per-flag email. Set to `critical` to suppress warning-level emails. |
| `MDW_ALERT_RATE_LIMIT_SECONDS` | `300` | De-dup window for identical (source, ticker, category) flags. |
| `MDW_ORCHESTRATOR_TIMEOUT_SECONDS` | `300` | Per-ticker hard timeout. |
| `MDW_ORCHESTRATOR_MAX_ATTEMPTS` | `3` | Per-ticker retry budget. |
| `MDW_ORCHESTRATOR_COOLDOWN_SECONDS` | `60` | Sleep between attempts. |
| `MDW_LOG_LEVEL` | `INFO` | Logger root level. |

No new dependencies. DuckDB and Nodemailer are already in the project.

## File checklist

**New:**
- `clients/telemetry.py`
- `clients/quality_detector.py`
- `clients/quality_flags.py`
- `scripts/run_ib_fetch_robust.py`
- `scripts/data_quality_report.py`
- `tests/test_telemetry.py`
- `tests/test_quality_detector.py`
- `tests/test_quality_flags.py`
- `tests/test_run_ib_fetch_robust.py`
- `tests/test_data_quality_report.py`

**Modified:**
- `clients/ib_client.py` (add `ConnectionTelemetry` hook in `connect`/`disconnect`)
- `scripts/fetch_ib_historical.py` (quality-detector hook, `--no-quality` flag)
- `scripts/daily_update.py` (quality-detector hook)
- `scripts/backfill_intraday.py` (quality-detector hook)
- `scripts/run_daily_update_job.py` (end-of-day quality report invocation)
- `scripts/check_daily_update_watchdog.py` (verify both markers)
- `scripts/send_daily_update_failure_email.mjs` (add `flag-alert` + `daily-summary` modes)
- `tests/test_ib_client.py` (telemetry integration tests)
- `tests/test_fetch_ib_historical.py` (quality-detector integration tests)
- `tests/test_daily_update.py` (same)
- `tests/test_backfill_intraday.py` (same)
- `tests/test_run_daily_update_job.py` (end-of-day quality report tests)

**Documentation:**
- `CLAUDE.md` and `README.md` — document the new env vars, the `run_ib_fetch_robust.py` entry point, the `data_quality_report.py` CLI, the sidecar/audit JSONL layout
- `.codex/project-memory.md` — record durable facts: reliability foundation in place; orchestrator is the canonical multi-ticker IB execution model; telemetry/audit JSONL are the canonical observation surfaces; quality-flag categories and severity thresholds

**Out of scope (deferred to future sub-projects):**
- `clients/uw_client.py` extension (Sub-C)
- `clients/massive_client.py` (Sub-C)
- Postgres analytical layer (Sub-B)
- `detect_row_count_anomaly` real implementation (Sub-C, once a second source exists)
- 4h / 15m intraday timeframes (Sub-D)
- Options chain capture (Sub-E)
- Gold tables (Sub-F)
