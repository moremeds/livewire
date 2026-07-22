# Sub-C Massive Daily Validation Plan

## Dependency Graph

- T1 -> T2 -> T3
- T2 -> T4
- T3, T4 -> T5
- T5 -> T6
- T3, T5 -> T7
- T6, T7 -> T8

## T1 Massive Client Tests

depends_on: []

- Add `tests/test_massive_client.py`.
- Red tests for:
  - missing `MASSIVE_API_KEY` raises auth error,
  - bearer auth/session headers are configured,
  - per-ticker daily endpoint uses `/v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}`,
  - grouped daily endpoint uses `/v2/aggs/grouped/locale/us/market/stocks/{date}`,
  - HTTP 429 respects `Retry-After`,
  - malformed bars are rejected,
  - fractional volume rounds with metadata,
  - ET date normalization handles per-ticker midnight and grouped close timestamps.

Verification:

```bash
python -m pytest tests/test_massive_client.py -q
```

Expected first result: fails before `clients/massive_client.py` exists.

## T2 Massive Client Implementation

depends_on: [T1]

- Add `clients/massive_client.py`.
- Implement:
  - typed exception hierarchy,
  - env token loading from `MASSIVE_API_KEY`,
  - `requests.Session` with bearer auth,
  - retry for 429/5xx/timeouts,
  - `get_daily_bars(ticker, start, end)`,
  - `get_grouped_daily(date)`,
  - `normalize_daily_bar(...)`.
- Keep implementation daily-stock-only.

Verification:

```bash
python -m pytest tests/test_massive_client.py -q
```

## T3 Telemetry Tests and Implementation

depends_on: [T2]

- Extend `tests/test_telemetry.py` for Massive rate-limit records.
- Extend `tests/test_uw_client.py` so `_get()` records request telemetry and rate-limit telemetry.
- Add telemetry injection to `UWClient`.
- Add telemetry injection to `MassiveClient`.

Verification:

```bash
python -m pytest tests/test_telemetry.py tests/test_uw_client.py tests/test_massive_client.py -q
```

## T4 Ingest Preflight Routing

depends_on: [T2]

- Extend `tests/test_livewire_entrypoints.py` for:
  - `daily --source massive` bypasses IB preflight,
  - default `daily` still preflights,
  - `daily --source ib` still preflights,
  - help paths never preflight.
- Update `scripts/livewire_ingest.py` with source-aware daily preflight routing.

Verification:

```bash
python -m pytest tests/test_livewire_entrypoints.py -q
```

## T5 Daily Massive Source and Recovery

depends_on: [T3, T4]

- Extend `tests/test_daily_update.py` for:
  - parser accepts `--source ib|massive`,
  - `--source massive` is equity-only and does not instantiate `IBClient`,
  - explicit Massive source publishes rows,
  - fallback recovery uses Massive before existing fallback client,
  - source counters mention Massive.
- Update `livewire_scripts/daily_update.py`:
  - add `--source` with default `ib`,
  - add `MassiveClient` seam,
  - add `fetch_massive_bars`,
  - use Massive before public fallback for equity gaps,
  - preserve non-equity IB behavior.

Verification:

```bash
python -m pytest tests/test_daily_update.py tests/test_livewire_entrypoints.py -q
```

## T6 Row Count Anomaly

depends_on: [T5]

- Extend `tests/test_quality_detector.py` for direct and `detect_all()` metadata-driven row-count anomaly.
- Implement `detect_row_count_anomaly` in `clients/quality_detector.py`.
- Pass optional `reference_source` through `_run_quality_detection()`.
- Build Massive reference metadata from fetched Massive bars when available.

Verification:

```bash
python -m pytest tests/test_quality_detector.py tests/test_daily_update.py -q
```

## T7 Docs and Environment

depends_on: [T3, T5]

- Update `.env.example` with `MASSIVE_API_KEY`.
- Update `README.md` and `CLAUDE.md` for:
  - `daily --source massive`,
  - Massive as near-term daily equity accelerator,
  - IB remaining primary for historical and non-equity,
  - intraday staying in Sub-D.

Verification:

```bash
rg -n "MASSIVE_API_KEY|--source massive|Massive" .env.example README.md CLAUDE.md
```

## T8 Final Gate and Milestone Commit

depends_on: [T6, T7]

- Run focused tests after each slice.
- Run full configured gate:

```bash
python -m pytest tests -q --cov=clients --cov=livewire_scripts --cov=scripts --cov-report=term-missing -W error::RuntimeWarning
```

- Optional live smoke only if `MASSIVE_API_KEY` is already exported in the shell.
- Commit each milestone:
  - docs/spec plan,
  - Massive client/telemetry,
  - ingest and daily wiring,
  - row-count quality/docs/final verification.
