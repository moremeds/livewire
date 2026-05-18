# IB Connection Telemetry — Design

**Date:** 2026-05-17
**Status:** SUPERSEDED — folded into [`2026-05-17-mdw-reliability-foundation-design.md`](./2026-05-17-mdw-reliability-foundation-design.md) as Component 1 (`clients/telemetry.py`). The broader spec generalizes the design to multi-source (`ib` + `uw` + `massive`), adds quality-flag detection, orchestrator productization, and alerting. Retained here for design-history reference; do not implement against this doc.
**Scope:** L1 of the market data warehouse stack — the IB Gateway connection layer
**Author:** Drafted via brainstorming session 2026-05-17

## Why this exists

On 2026-05-17 we hit a sustained instability while attempting bulk historical backfill via Interactive Brokers. Symptoms:

- Historical Market Data Service (HMDS) farm flapped between `2106 OK` and `2105 broken` 11 times in a 12-minute span.
- Long-lived IB sessions that began healthy degraded mid-run: a session that fetched 3 ETFs cleanly in 2 minutes hung 28 minutes later on a head-timestamp call.
- A 95-ticker bulk sweep produced only 4 partially-seeded tickers in 62 minutes; one ticker (SMH) silently received a partial fetch (1,758 of an expected ~6,000 rows) with the bronze parquet looking outwardly identical to a clean fetch.
- We could not determine root cause from existing logs because farm-state events were buried in unstructured text alongside thousands of unrelated lines, with no way to compute "HMDS uptime over the last 24 hours" without manual grep.

The user's directive was clear: **"we need to ensure the stability here ... we can let it running for longer time, but cant tolerate for the instability on and off"** and **"do we have a means to ensure the stability ... I want to strengthen this project, from bottom up"**.

The clarifying-question phase established:
1. **Visibility first, then targeted fix** — building resilience patterns before understanding the root cause risks designing for a phantom failure mode.
2. **Scope: connection-lifecycle + farm-state events only** — not a full firehose; this is the layer where today's pain originates.
3. **Consumption: CLI report on demand** — no dashboards, no auto-summaries; analyzable when curious.
4. **Approach: sidecar observer (composition)** — keep `IBClient` focused on transport; put telemetry in its own client module.

This spec describes the visibility layer. A follow-up spec (after ≥1 week of telemetry data) will propose the targeted resilience fix based on what the data actually shows.

## Non-goals

- **Not** a self-healing connection layer. This is observation only.
- **Not** per-request telemetry (head-timestamp duration, reqHistoricalData success rate). Deferred to the follow-up resilience spec.
- **Not** account/portfolio/sec-def event capture. Out of scope.
- **Not** a dashboard, alerting pipeline, or external metrics exporter (Prometheus, Datadog, etc.). CLI-only.
- **Not** modifying the existing data path (`BronzeClient`, `DBClient`, fallback chain). Telemetry is observational only.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                  Application code                        │
│  (fetch_ib_historical.py, daily_update.py, etc.)         │
└─────────────────────────┬────────────────────────────────┘
                          │ uses
                          ▼
┌──────────────────────────────────────────────────────────┐
│            clients/ib_client.py  (transport)             │
│  • opens/closes IB connection                            │
│  • on connect: instantiates + starts ConnectionTelemetry │
│  • on disconnect: stops it                               │
└─────────────────────────┬────────────────────────────────┘
                          │ wires events
                          ▼
┌──────────────────────────────────────────────────────────┐
│      clients/ib_telemetry.py  (sidecar observer)         │
│  • subscribes to ib.errorEvent, connected/disconnected   │
│  • parses 2104/2105/2106/2107/2158 farm codes            │
│  • emits one JSONL line per event                        │
└─────────────────────────┬────────────────────────────────┘
                          │ appends
                          ▼
┌──────────────────────────────────────────────────────────┐
│  ~/market-warehouse/logs/ib_connection_telemetry.jsonl   │
│  (append-only, one JSON object per line)                 │
└─────────────────────────┬────────────────────────────────┘
                          │ reads via DuckDB
                          ▼
┌──────────────────────────────────────────────────────────┐
│       scripts/ib_telemetry_report.py  (CLI analyzer)     │
│  --since 7d --farm ushmds|all --detail summary|flap|...  │
└──────────────────────────────────────────────────────────┘
```

### Key boundary decisions

- `IBClient` remains a transport. Telemetry hook is ~30 LOC delta, the rest of the surface unchanged.
- Telemetry is **default-on for production, default-off in tests** via `MDW_IB_TELEMETRY_PATH` env var (empty/unset string → disabled).
- JSONL on the filesystem — same idiom as the existing log files. Read with DuckDB which is already a project dependency.
- Reader is a separate script — keeps the import graph clean (no analysis code in the live write path).

## Components

### `clients/ib_telemetry.py` (new, ~150 LOC)

```python
class ConnectionTelemetry:
    """Subscribes to ib_async connection events and emits structured JSONL."""

    def __init__(self, ib: IB, jsonl_path: Path): ...
    def start(self) -> None: ...
    def stop(self) -> None: ...   # idempotent

    def _on_connected(self) -> None: ...
    def _on_disconnected(self) -> None: ...
    def _on_error(self, reqId, errorCode, errorString, contract) -> None: ...

    def _emit(self, record: dict) -> None: ...   # never raises
```

Codes parsed:

| Code | Meaning | JSONL `event` | Extracted `state` |
|---:|---|---|---|
| 2104 | Market data farm OK | `farm_state` | `ok` |
| 2105 | HMDS farm broken | `farm_state` | `broken` |
| 2106 | HMDS farm OK | `farm_state` | `ok` |
| 2107 | HMDS farm inactive | `farm_state` | `inactive` |
| 2158 | Sec-def farm OK | `farm_state` | `ok` |

Farm name (`usfarm`, `ushmds`, `secdefnj`, …) is extracted from the trailing `:farmname` text the IB warning message carries.

Any IB error code not in the table above is emitted as `{"event":"ib_error","code":N,...}` so we don't lose visibility on unexpected codes, but we don't try to classify them.

### `clients/ib_client.py` (existing, ~30 LOC delta)

```python
class IBClient:
    def __init__(self, host=..., port=..., client_id=0,
                 telemetry_path: Path | None = _default_telemetry_path()):
        ...
        self._telemetry_path = telemetry_path
        self._telemetry: ConnectionTelemetry | None = None

    def connect(self):
        # existing handshake / clientId-retry logic unchanged
        if self._telemetry_path is not None:
            self._telemetry = ConnectionTelemetry(self.ib, self._telemetry_path)
            self._telemetry.start()

    def disconnect(self):
        if self._telemetry is not None:
            self._telemetry.stop()
            self._telemetry = None
        # existing disconnect
```

`_default_telemetry_path()` reads env `MDW_IB_TELEMETRY_PATH`. Empty/unset → `~/market-warehouse/logs/ib_connection_telemetry.jsonl`. Set to the literal string `"none"` (case-insensitive) to disable explicitly.

### `scripts/ib_telemetry_report.py` (new, ~100 LOC)

```bash
python scripts/ib_telemetry_report.py --since 7d
python scripts/ib_telemetry_report.py --since 24h --farm ushmds
python scripts/ib_telemetry_report.py --detail timeline --since 24h
```

Output modes:

- **`--detail summary`** (default): per-farm uptime %, total connect/disconnect events, flap count, mean-time-between-drops, total events.

  Definitions used throughout the reader:
  - **Drop** = a state transition from `ok` to any non-`ok` state (`broken`, `inactive`, or any future code).
  - **Flap** = a contiguous burst of ≥3 state transitions where every consecutive pair is < 10 min apart. The burst ends when the gap to the next transition exceeds 10 min. Each burst counts as one flap, regardless of length.
  - **Mean-time-between-drops** = average wall-clock interval between successive *drops* (as defined above) within the `--since` window.
  - **Uptime %** = fraction of `--since` window during which the farm's most recent observed state was `ok`. Periods before the first observed state for a farm in the window are excluded from the denominator.
- **`--detail flap`**: chronological list of flap windows `[start, end, duration, farm, sequence_of_states]`.
- **`--detail timeline`**: hour-of-day heatmap (24 cells × 7 days) showing % of time each farm was `ok` — visually surfaces maintenance windows.

DuckDB used internally: `SELECT ... FROM read_json_auto(<path>, ignore_errors=true)`.

### JSONL schema (one example per event type)

```json
{"ts":"2026-05-17T18:38:31Z","event":"telemetry_started"}
{"ts":"2026-05-17T18:38:31Z","event":"connected","host":"127.0.0.1","port":4001,"client_id":0}
{"ts":"2026-05-17T18:38:31Z","event":"farm_state","code":2104,"farm":"usfarm","state":"ok","raw":"Market data farm connection is OK:usfarm"}
{"ts":"2026-05-17T18:38:31Z","event":"farm_state","code":2106,"farm":"ushmds","state":"ok","raw":"HMDS data farm connection is OK:ushmds"}
{"ts":"2026-05-17T18:46:01Z","event":"farm_state","code":2105,"farm":"ushmds","state":"broken","raw":"HMDS data farm connection is broken:ushmds"}
{"ts":"2026-05-17T19:11:56Z","event":"disconnected","reason":"client_initiated"}
{"ts":"2026-05-17T19:11:56Z","event":"telemetry_stopped"}
{"ts":"2026-05-17T...Z","event":"ib_error","code":162,"req_id":7,"msg":"Request Timed Out"}
```

All timestamps are ISO 8601 UTC with `Z` suffix.

## Data flow

### Write path — one IB-talking process lifecycle

```
1. process start
   └─> IBClient(host, port, telemetry_path=<default>)
       └─> reads MDW_IB_TELEMETRY_PATH; "none"/empty → telemetry_path = None

2. IBClient.connect()
   ├─> existing ib_async.connectAsync() handshake + clientId-retry
   ├─> {"event":"connected", ...}  ← first JSONL line
   └─> ConnectionTelemetry.start()
       ├─> ib.errorEvent      += _on_error
       ├─> ib.connectedEvent  += _on_connected
       └─> ib.disconnectedEvent += _on_disconnected

3. IB Gateway emits warnings 2104/2106/2107 right after handshake
   └─> _on_error parses code → {"event":"farm_state", ...}

4. Main work: head-timestamp + window fetches (no telemetry overhead in this scope)

5. Mid-run: HMDS flips broken
   └─> ib_async raises errorEvent(2105, "HMDS data farm connection is broken:ushmds")
       └─> _on_error → {"event":"farm_state","code":2105,"farm":"ushmds","state":"broken"}

6. process exit
   └─> IBClient.disconnect()
       ├─> ConnectionTelemetry.stop()
       └─> ib.disconnect() → emits disconnectedEvent → {"event":"disconnected"}
```

### Read path — `scripts/ib_telemetry_report.py`

```
1. argparse → cutoff_ts (from --since), --farm filter, --detail mode
2. duckdb.connect()
3. SELECT * FROM read_json_auto(<path>, ignore_errors=true)
   WHERE ts >= cutoff_ts AND (<farm_filter>)
4. Compute metrics in DuckDB SQL (uptime %, flaps, MTBD)
5. Print as text (no third-party rendering deps)
```

### Concurrency

Today the warehouse runs at most one IB-talking process at a time (`fetch_ib_historical.py` *or* `daily_update.py`; they would collide on the IB session anyway). We expect one writer per JSONL file. `os.write()` with `O_APPEND` is atomic for the typical per-line size (<1 KB), so we do not need locking. In the rare case of two writers, lines remain valid JSONL (each `_emit` writes one `data\n` string in one syscall), they just interleave chronologically.

## Error handling

**Principle: telemetry must NEVER break the live IB write path.** Telemetry is observational. If it fails, we log a warning and continue with real work.

| Failure mode | Behavior |
|---|---|
| `telemetry_path` parent dir doesn't exist | `ConnectionTelemetry.start()` catches `FileNotFoundError`, logs `WARNING: telemetry disabled (path X unwritable)`, sets internal disabled flag, never raises. Subscriptions not attached. |
| JSONL append fails (disk full, permission denied) | `_emit` catches `OSError`, logs `WARNING: telemetry write failed: <err>` **at most once per minute** (rate-limited via timestamp tracking), drops the event. Live IB work continues. |
| Unexpected event shape (IB adds a new field) | `_emit` catches `TypeError` / `ValueError` from `json.dumps`, logs warning, drops the event. |
| Telemetry handler raises during event dispatch | Each handler wrapped in `try/except Exception` at the outer call. Exception logged and swallowed — does **not** propagate into `ib_async`'s event loop. |
| `ConnectionTelemetry.stop()` called twice | First call detaches handlers and sets `self._stopped = True`; second call is a no-op. Prevents `ib_async` from raising `ValueError: handler not found` if `IBClient.disconnect()` runs twice. |
| Telemetry path is `None` / `"none"` / empty | `ConnectionTelemetry` never instantiated. Zero overhead. Used by unit tests. |
| Reader hits a malformed line | `read_json_auto(..., ignore_errors=true)` skips bad lines; reader prints `<N skipped>` footer. |
| Reader runs with no JSONL file present | Print `no telemetry data found at <path>` and exit 0. Not an error condition. |

### Explicit non-handling

- *Concurrent writers interleaving bytes within a single line.* Low-risk given single-IB-process invariant; if it ever happens, reader skips the corrupt line. We will not preemptively add `fcntl.flock` complexity.

### Logging conventions

- All telemetry warnings use logger name `mdw.telemetry` so users can `logging.getLogger("mdw.telemetry").setLevel(logging.ERROR)` to silence.
- No telemetry warning ever uses `logger.error()` — telemetry failures are degraded-operation, not errors. Reserve `error` for actual IB failures.

## Testing

Per `AGENTS.md`: all `clients/` and `scripts/` code needs tests; the repo enforces 100% coverage for the configured source set. Telemetry must hit that bar.

### Unit tests — `tests/test_ib_telemetry.py` (new)

ib_async events mocked; no live Gateway.

| Test | Verifies |
|---|---|
| `test_start_attaches_three_handlers` | After `start()`, each of `errorEvent`, `connectedEvent`, `disconnectedEvent` has exactly one new handler. |
| `test_stop_detaches_handlers_and_is_idempotent` | After `stop()`, handler counts return to baseline. Calling `stop()` twice does not raise. |
| `test_farm_state_parsing_table_driven` | Each of `(2104,"...:usfarm")`, `(2105,"...:ushmds")`, `(2106,"...:ushmds")`, `(2107,"...:ushmds")`, `(2158,"...:secdefnj")` emits JSONL with correct `code`, `farm`, `state`. |
| `test_unknown_error_code_emits_ib_error` | Code 162 emits `{"event":"ib_error","code":162,...}` not `farm_state`. |
| `test_emit_failure_does_not_raise` | Patch `open()` to raise `OSError` → `_emit` logs warning, returns normally. |
| `test_emit_warning_rate_limited` | 100 emit failures in a row → exactly 1 warning logged in the first minute. |
| `test_handler_exception_swallowed` | `_on_error` patched to raise → caller doesn't see the exception. |
| `test_serialize_failure_does_not_raise` | Event containing a non-JSON-serializable object → caught, dropped, warning logged. |
| `test_telemetry_disabled_when_path_none` | `IBClient(telemetry_path=None).connect()` does not instantiate `ConnectionTelemetry`. |
| `test_telemetry_disabled_when_parent_dir_missing` | `telemetry_path=/no/such/dir/x.jsonl` → start() logs warning, returns disabled, no exception. |
| `test_default_path_respects_env` | Setting `MDW_IB_TELEMETRY_PATH=/tmp/x.jsonl` makes `_default_telemetry_path()` return that path. |
| `test_default_path_disabled_when_env_is_none` | `MDW_IB_TELEMETRY_PATH=none` → `_default_telemetry_path()` returns `None`. |

### Integration test — `tests/test_ib_client.py` (extend existing)

| Test | Verifies |
|---|---|
| `test_connect_writes_connected_event_to_jsonl` | `IBClient(telemetry_path=tmp_path/"t.jsonl").connect()` produces JSONL containing `{"event":"connected",...}`. Uses existing `MagicMock` IB pattern. |
| `test_disconnect_writes_telemetry_stopped` | Connect → disconnect → JSONL contains both `telemetry_started` and `telemetry_stopped`. |

### Reader tests — `tests/test_ib_telemetry_report.py` (new)

Synthetic JSONL fixtures.

| Test | Verifies |
|---|---|
| `test_summary_uptime_calculation` | Fixture with `ushmds` ok→broken→ok → computed uptime % matches expected ratio ±0.1%. |
| `test_flap_detection` | 4 state transitions, each < 10 min apart → one contiguous burst → flap count = 1. Two separated bursts (gap > 10 min between them) → flap count = 2. |
| `test_since_window_filter` | `--since 1h` excludes events older than 1h. |
| `test_farm_filter` | `--farm ushmds` excludes `usfarm` and `secdefnj` events. |
| `test_no_file_exits_zero` | Missing JSONL path → exit code 0, prints "no telemetry data". |
| `test_malformed_lines_skipped_with_count` | 2 bad + 5 good lines → output reports "2 skipped". |
| `test_timeline_heatmap_24x7_shape` | `--detail timeline --since 7d` → 7 rows × 24 cols of percentages. |

### Coverage requirements

- `pyproject.toml` enforces `fail_under = 100` for the configured source set.
- Both new files (`clients/ib_telemetry.py`, `scripts/ib_telemetry_report.py`) **will** be included in the coverage gate from day one — no exclusion.
- Tests must pass `-W error::RuntimeWarning` (per project rule for script tests that mock async runners).

### Manual verification (post-merge, before declaring done)

1. Run any IB-touching command for ~30 seconds.
2. `cat ~/market-warehouse/logs/ib_connection_telemetry.jsonl` shows at least one `connected` + one `farm_state` + one `disconnected` line.
3. `python scripts/ib_telemetry_report.py --since 1h` prints a summary block (not an error).
4. After ≥1 hour of any backfill: re-run the report and verify it reflects the activity.

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `MDW_IB_TELEMETRY_PATH` | `~/market-warehouse/logs/ib_connection_telemetry.jsonl` | Where the JSONL is appended. Set to `none` (case-insensitive) to disable. Empty string also treated as the default. |

No new config files. No new dependencies (DuckDB is already in the project).

## Out-of-scope follow-ups

Once we have ≥1 week of telemetry data, a follow-up spec will use it to design the targeted resilience layer. Candidate L2/L3 patterns to evaluate (informed by what the data shows):

- Per-request hard timeout (port the orchestrator's 5-min cap into `IBClient.get_historical_data`).
- Per-ticker process isolation as the default for bulk-backfill jobs (productize the orchestrator built ad-hoc on 2026-05-17).
- HMDS-state preflight before starting a long batch.
- Connection-age awareness — disconnect+reconnect after N minutes of session lifetime.
- Partial-fetch detection in `BronzeClient` (distinguish "fetched 1758 rows because that's all there is" from "fetched 1758 rows because windows timed out").

These are NOT in this spec. This spec is the observation layer that makes those decisions data-driven.

## File checklist

- New: `clients/ib_telemetry.py`
- New: `scripts/ib_telemetry_report.py`
- Modified: `clients/ib_client.py` (add telemetry hook in `connect`/`disconnect`)
- New: `tests/test_ib_telemetry.py`
- New: `tests/test_ib_telemetry_report.py`
- Modified: `tests/test_ib_client.py` (add 2 telemetry-integration tests)
- Modified: `CLAUDE.md` and/or `README.md` (document `MDW_IB_TELEMETRY_PATH` env var and the report command)
- Modified: `.codex/project-memory.md` (record durable fact: connection-state telemetry is now baked into `IBClient`; analysis via `scripts/ib_telemetry_report.py`)

No changes to: `BronzeClient`, `DBClient`, `DailyBarFallbackClient`, fallback chain, daily-update job, coverage report, watchdog, presets, schema.
