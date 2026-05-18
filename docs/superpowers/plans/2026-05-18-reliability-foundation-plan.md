# Livewire Reliability Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make silent data loss in the Livewire ingestion pipeline impossible by adding source-agnostic telemetry, partial-fetch detection, a productized per-ticker orchestrator, and a unified data-quality CLI — without changing the parquet bronze write semantics.

**Architecture:** Three new client modules (`clients/telemetry.py`, `clients/quality_detector.py`, `clients/quality_flags.py`) provide observation primitives. The existing single-ticker workers (`fetch_ib_historical.py`, `daily_update.py`, `backfill_intraday.py`) get a small quality-hook before atomic publish. A new orchestrator (`scripts/run_ib_fetch_robust.py`) replaces the `/tmp/orchestrate_ib_fetch.sh` workaround with a process-isolation, timeout, retry-budget runner. A new CLI (`scripts/data_quality_report.py`) aggregates the telemetry + audit JSONL feeds and produces email rollups. Two new Nodemailer modes (`flag-alert`, `daily-summary`) close the alerting loop.

**Tech Stack:** Python 3.13 (uv venv at `~/market-warehouse/.venv/`), `ib_async`, `pyarrow` / `duckdb` (read JSONL), `pytest` + `pytest-cov`, Node + Nodemailer for SMTP send, JSONL files as append-only telemetry/audit transport, atomic parquet writes via temp + `os.replace`.

**Spec:** [`docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md`](../specs/2026-05-17-mdw-reliability-foundation-design.md) — read before starting.

---

## Dependency graph

```
Phase 0 — Foundations
   T1 BaseTelemetry   ──►  T2 ConnectionTelemetry ──►  T3 UW/MassiveTelemetry stubs
   T4 QualityFlag + range_shortfall ──►  T5 interior_gaps ──►  T6 fetch_tainting ──►  T7 anomaly stub + detect_all
   T8 write_sidecar  ──►  T9 append_audit  ──►  T10 alert_on_flag
                                              (T10 depends on T11/T12 for spawn target)

Phase 1 — Email modes
   T11 flag-alert mode      ──►  T10 (loops back to wire alert_on_flag)
   T12 daily-summary mode

Phase 2 — Integration into existing workers (each depends on Phase 0 + Phase 1)
   T13 IBClient telemetry hook  (uses T2)
   T14 fetch_ib_historical hook (uses T7, T8-T10)
   T15 daily_update hook        (uses T7, T8-T10)
   T16 backfill_intraday hook   (uses T7, T8-T10)

Phase 3 — Orchestrator + Report
   T17 orchestrator skeleton    ──►  T18 timeout/retry  ──►  T19 ok-noop + atexit
   T20 report summary view      ──►  T21 flap + quality views  ──►  T22 --email mode

Phase 4 — End-to-end wiring
   T23 run_daily_update_job end-of-day report (uses T22)
   T24 check_daily_update_watchdog both-markers (uses T22 marker)
   T25 Documentation sweep
   T26 Final coverage + manual verification
```

**Ordering note:** Tasks within a phase are not strictly serial; the engineer can parallelize T2/T3 if comfortable, and T11/T12 can be tackled in either order. But Phase 1 (email modes) MUST land before Phase 2 (integration) starts wiring `alert_on_flag`, because T10's tests will spawn the Node script.

## Pre-implementation verifications (done 2026-05-18)

These were checked against the live codebase before this plan was finalized; the corresponding tasks use the verified anchors below:

| What | Verified | Result |
|---|---|---|
| `ib_async` event API | `ib_async==2.1.0`; `errorEvent` / `connectedEvent` / `disconnectedEvent` are `eventkit.Event` instances exposing `.connect(handler)` + `.disconnect(handler)` | ✅ T2 code as written is correct |
| Trading-calendar code location | `scripts/daily_update.py` already defines `is_trading_day(d: date) -> bool` (line 222), `previous_trading_day(d: date) -> date` (line 270), `trading_days_between(start, end)` (line 278). Imported by `tests/test_daily_update.py`. | ✅ T5 = extraction (not new code); also updates `tests/test_daily_update.py` import |
| `fetch_ib_historical.py` flow | `main()` (line 574) → `_run_backfill` or `_run_normal`. Each ticker passes through `backfill_ticker(ticker, bars, bronze, asset_class)` (line 550). IB errors are NOT collected per-ticker today. | ✅ T14 anchors `_run_quality_detection` inside `backfill_ticker` (single call site for both modes). `errors_during_fetch=[]` for Sub-A; full collection is a follow-up. |
| `daily_update.py` merge call site | Line 869: `inserted = bronze.merge_ticker_rows(ticker, rows)`. Quality hook inserts BEFORE this line; `valid_bars` is the live list at that point. | ✅ T15 anchors exactly here |
| `backfill_intraday.py` merge call site | `backfill_ticker` (line 149) collects `outcome.errors: list[str]` during the chunk loop (line 175). Merge call at line 199. | ✅ T16 reuses `outcome.errors` directly as `errors_during_fetch` — cleanest integration |
| `send_daily_update_failure_email.mjs` transport | Line 448: `export async function sendFailureAlert({ transportOptions, message })` — already takes a pre-built message `{subject, html, text}`. | ✅ T11/T12 simplify — just add `buildFlagAlertMessage` + `buildDailySummaryMessage` and dispatch via the existing `sendFailureAlert` |
| `run_daily_update_job.py` success path | `=== Done … ===` log line followed by `return 0` (line 258). End-of-day report invocation goes between them. | ✅ T23 anchor confirmed |

**Open follow-up flagged:** the `errors_during_fetch` list for the daily-bar fetch path stays empty in Sub-A. Wiring a second IB `errorEvent` handler alongside `ConnectionTelemetry` to collect per-ticker errors is a small but separate task — left as a TODO marker in T14 rather than expanding Sub-A scope. `backfill_intraday.py` already collects them and uses them in T16.

## Contract: bar normalization for detectors

`detect_all` and the per-category detectors accept a list of objects with a `.trade_date` attribute (string or `date`). The three integration call sites in T14/T15/T16 receive bars in different shapes:

| Caller | Native shape | Date field |
|---|---|---|
| T14 `fetch_ib_historical.py` | `ib_async.BarData` | `.date` (string or `date`) |
| T15 `daily_update.py` | `ib_async.BarData` | `.date` (string or `date`) |
| T16 `backfill_intraday.py` | `dict` (intraday bronze row) | `["bar_timestamp"]` (timezone-aware `datetime`) |

**Each `_run_quality_detection` helper MUST normalize its input before calling `detect_all`.** Use this canonical adapter:

```python
from types import SimpleNamespace

def _normalize_bars_for_detection(bars: list) -> list:
    """Produce a list of objects exposing `.trade_date` (str ISO date).

    Accepts: IB BarData (with .date), intraday row dicts (with bar_timestamp), or
    objects already exposing .trade_date.
    """
    out = []
    for b in bars:
        if hasattr(b, "trade_date"):
            out.append(b)
        elif hasattr(b, "date"):
            d = b.date
            out.append(SimpleNamespace(trade_date=str(d)[:10]))
        elif isinstance(b, dict):
            ts = b.get("trade_date") or b.get("bar_timestamp")
            out.append(SimpleNamespace(trade_date=str(ts)[:10]))
        else:    # pragma: no cover - defensive
            continue
    return out
```

Add this once in `clients/quality_detector.py` (export it), and have each `_run_quality_detection` call it before `detect_all(bars=normalized, ...)`. Update the T4-T7 test fixtures to keep using the `_Bar(trade_date=...)` helper — the detector's input contract is unchanged.

**Pre-flight (do once before Task 1):**

```bash
cd /Users/chenxi/projects/livewire
source ~/market-warehouse/.venv/bin/activate
git checkout -b feat/reliability-foundation
python -m pytest tests/ -q --cov=clients --cov=scripts 2>&1 | tail -5    # baseline; expect 100%
git status                                                                 # expect clean
```

If the baseline coverage isn't already at 100%, stop and ask — the gate is invariant.

**Cross-cutting rules the executor MUST observe:**

1. **Coverage omit list is frozen.** New files (`clients/telemetry.py`, `clients/quality_detector.py`, `clients/quality_flags.py`, `clients/trading_calendar.py`, `scripts/run_ib_fetch_robust.py`, `scripts/data_quality_report.py`) MUST NOT be added to `pyproject.toml`'s `[tool.coverage.run].omit`. They are subject to the 100% gate from day one.

2. **`_RATE_LIMIT_CACHE` module state must be cleared between tests.** Add this autouse fixture to `tests/conftest.py` once T10 lands:
   ```python
   @pytest.fixture(autouse=True)
   def _clear_alert_rate_limit():
       try:
           from clients import quality_flags
           quality_flags._RATE_LIMIT_CACHE.clear()
       except (ImportError, AttributeError):
           pass
       yield
   ```

3. **T10 vs T11/T12 ordering.** T10's unit tests mock `subprocess.run`, so T10 can land before T11/T12 exist. But Phase 2 (T14-T16) integration tests will spawn the real Node script. **Land T11 and T12 before starting T14**, or stub them with a `--mode flag-alert` no-op while T14-T16 land. Recommended: do T10 → T11 → T12 → T13, then move into Phase 2.

4. **One commit per task** unless a task explicitly chains steps that must land atomically (T4-T7 all extending `quality_detector.py` can each commit individually). Each commit gets the test + implementation together so coverage stays at 100% on every revision.

5. **If you hit ambiguity or a verification gap not covered by the plan, STOP and report.** Do not invent. The plan has been hardened against 14 review findings; remaining ambiguity is genuinely new information.

---

## Task 1: `BaseTelemetry` — skeleton + emit + disabled state

**Files:**
- Create: `clients/telemetry.py`
- Test: `tests/test_telemetry.py`

**Spec reference:** Components → `clients/telemetry.py` (BaseTelemetry section), Configuration table (`MDW_TELEMETRY_PATH`), Error handling matrix (`telemetry.jsonl write fails`, `parent dir missing`).

- [ ] **Step 1: Write the failing tests for `BaseTelemetry` core behavior**

```python
# tests/test_telemetry.py
import json
import os
from pathlib import Path

import pytest

from clients.telemetry import BaseTelemetry


def test_base_telemetry_emits_jsonl_line(tmp_path):
    path = tmp_path / "telemetry.jsonl"
    t = BaseTelemetry(source="ib", jsonl_path=path)
    t.start()
    t._emit({"event": "connected", "client_id": 0})
    t.stop()
    lines = path.read_text().splitlines()
    assert len(lines) == 3  # start, event, stop (start/stop framing is OK either way)
    record = json.loads(lines[1])
    assert record["source"] == "ib"
    assert record["event"] == "connected"
    assert "ts" in record
    assert record["client_id"] == 0


def test_base_telemetry_disabled_when_path_is_none(tmp_path, caplog):
    t = BaseTelemetry(source="ib", jsonl_path=None)
    t.start()
    t._emit({"event": "x"})
    t.stop()
    # No file written, no exception
    assert t._disabled is True


def test_base_telemetry_disabled_when_parent_dir_missing(tmp_path, caplog):
    path = tmp_path / "nope" / "missing" / "telemetry.jsonl"
    t = BaseTelemetry(source="ib", jsonl_path=path)
    t.start()
    assert t._disabled is True


def test_base_telemetry_emit_failure_rate_limited(tmp_path, caplog, monkeypatch):
    path = tmp_path / "telemetry.jsonl"
    t = BaseTelemetry(source="ib", jsonl_path=path)
    t.start()

    # Simulate intermittent write failure: monkeypatch _do_write
    fails = [0]
    def boom(line):
        fails[0] += 1
        raise OSError("disk full")
    monkeypatch.setattr(t, "_do_write", boom)

    for _ in range(10):
        t._emit({"event": "x"})
    # Should NOT raise; warning rate-limited to 1/min — we don't assert log count strictly
    assert fails[0] == 10
    t.stop()


def test_stop_is_idempotent(tmp_path):
    path = tmp_path / "telemetry.jsonl"
    t = BaseTelemetry(source="ib", jsonl_path=path)
    t.start()
    t.stop()
    t.stop()    # second call must not raise
```

- [ ] **Step 2: Run to confirm failure**

```bash
python -m pytest tests/test_telemetry.py -v
```
Expected: `ModuleNotFoundError: No module named 'clients.telemetry'` or `ImportError: cannot import name 'BaseTelemetry'`.

- [ ] **Step 3: Implement `BaseTelemetry`**

```python
# clients/telemetry.py
"""Source-agnostic JSONL telemetry primitives for the Livewire pipeline.

See: docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_VALID_SOURCES = {"ib", "uw", "massive"}

_logger = logging.getLogger("mdw.telemetry")


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_default_path() -> Optional[Path]:
    raw = os.environ.get(
        "MDW_TELEMETRY_PATH",
        str(Path.home() / "market-warehouse" / "logs" / "telemetry.jsonl"),
    )
    if raw.strip().lower() in {"none", "off", "disabled", ""}:
        return None
    return Path(raw).expanduser()


class BaseTelemetry:
    """Append-only JSONL emitter with disabled-when-broken fallback."""

    _WARN_RATE_LIMIT_SECONDS = 60

    def __init__(self, source: str, jsonl_path: Optional[Path]):
        if source not in _VALID_SOURCES:
            raise ValueError(f"source must be one of {_VALID_SOURCES}, got {source!r}")
        self.source = source
        self.jsonl_path = jsonl_path
        self._disabled = False
        self._started = False
        self._last_warn_at = 0.0

    def start(self) -> None:
        if self._started:
            return
        if self.jsonl_path is None:
            self._disabled = True
            self._started = True
            return
        try:
            self.jsonl_path.parent.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            pass    # parent dir exists, fine
        except OSError as exc:
            _logger.warning(
                "telemetry path %s unusable (%s); disabling", self.jsonl_path, exc
            )
            self._disabled = True
            self._started = True
            return
        if not self.jsonl_path.parent.is_dir():
            self._disabled = True
        self._started = True
        self._emit({"event": "telemetry_started"})

    def stop(self) -> None:
        if not self._started or self._disabled:
            self._started = False
            return
        self._emit({"event": "telemetry_stopped"})
        self._started = False

    def _emit(self, record: dict) -> None:
        if self._disabled or not self._started:
            return
        record = dict(record)
        record.setdefault("ts", _utc_iso())
        record.setdefault("source", self.source)
        line = json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
        try:
            self._do_write(line)
        except OSError as exc:
            now = time.monotonic()
            if now - self._last_warn_at > self._WARN_RATE_LIMIT_SECONDS:
                _logger.warning("telemetry write failed: %s (rate-limited)", exc)
                self._last_warn_at = now

    def _do_write(self, line: str) -> None:
        # Separate method so tests can patch it.
        with self.jsonl_path.open("a", encoding="utf-8") as fh:
            fh.write(line)
```

- [ ] **Step 4: Run tests to confirm pass**

```bash
python -m pytest tests/test_telemetry.py -v
```
Expected: 5/5 PASS.

- [ ] **Step 5: Coverage check (this file only)**

```bash
python -m pytest tests/test_telemetry.py --cov=clients.telemetry --cov-report=term-missing
```
Expected: 100% coverage of `clients/telemetry.py` lines covered so far. If `_resolve_default_path` is uncovered, add a test:

```python
def test_resolve_default_path_disabled_via_env(monkeypatch):
    from clients.telemetry import _resolve_default_path
    monkeypatch.setenv("MDW_TELEMETRY_PATH", "none")
    assert _resolve_default_path() is None


def test_resolve_default_path_uses_explicit_path(monkeypatch, tmp_path):
    from clients.telemetry import _resolve_default_path
    monkeypatch.setenv("MDW_TELEMETRY_PATH", str(tmp_path / "x.jsonl"))
    assert _resolve_default_path() == tmp_path / "x.jsonl"


def test_invalid_source_rejected(tmp_path):
    with pytest.raises(ValueError, match="source must be one of"):
        BaseTelemetry(source="bogus", jsonl_path=tmp_path / "t.jsonl")
```

- [ ] **Step 6: Commit**

```bash
git add clients/telemetry.py tests/test_telemetry.py
git commit -m "feat(telemetry): add BaseTelemetry JSONL emitter

Source-tagged append-only telemetry with disabled-when-broken fallback,
parent-dir auto-create, and rate-limited warnings on write failure.
First component of Sub-A reliability foundation."
```

---

## Task 2: `ConnectionTelemetry` — IB farm-state parsing

**Files:**
- Modify: `clients/telemetry.py`
- Test: `tests/test_telemetry.py`

**Spec reference:** Components → `ConnectionTelemetry`, farm-state code table (2104/2105/2106/2107/2158).

- [ ] **Step 1: Write the failing tests**

```python
# Append to tests/test_telemetry.py
import types
from unittest.mock import MagicMock

from clients.telemetry import ConnectionTelemetry


def _fake_ib():
    ib = MagicMock()
    ib.errorEvent = MagicMock()
    ib.connectedEvent = MagicMock()
    ib.disconnectedEvent = MagicMock()
    return ib


def test_connection_telemetry_attaches_handlers(tmp_path):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    ib.errorEvent.connect.assert_called_once()
    ib.connectedEvent.connect.assert_called_once()
    ib.disconnectedEvent.connect.assert_called_once()
    t.stop()
    ib.errorEvent.disconnect.assert_called_once()


@pytest.mark.parametrize("code,state", [
    (2104, "ok"),
    (2105, "broken"),
    (2106, "ok"),
    (2107, "inactive"),
    (2158, "ok"),
])
def test_connection_telemetry_parses_farm_codes(tmp_path, code, state):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t._on_error(reqId=-1, errorCode=code, errorString=f"All connections OK:usfarm", contract=None)
    t.stop()
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    farm_records = [r for r in records if r["event"] == "farm_state"]
    assert len(farm_records) == 1
    assert farm_records[0]["code"] == code
    assert farm_records[0]["state"] == state
    assert farm_records[0]["farm"] == "usfarm"
    assert farm_records[0]["source"] == "ib"


def test_connection_telemetry_unknown_code_emits_ib_error(tmp_path):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t._on_error(reqId=42, errorCode=162, errorString="HMDS query returned no data", contract=None)
    t.stop()
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    error_records = [r for r in records if r["event"] == "ib_error"]
    assert len(error_records) == 1
    assert error_records[0]["code"] == 162
    assert error_records[0]["req_id"] == 42


def test_connection_telemetry_no_farm_suffix(tmp_path):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t._on_error(reqId=-1, errorCode=2106, errorString="HMDS data farm connection is OK", contract=None)
    t.stop()
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    farm_records = [r for r in records if r["event"] == "farm_state"]
    assert farm_records[0]["farm"] is None  # no :farmname suffix


def test_connection_telemetry_connected_disconnected_events(tmp_path):
    ib = _fake_ib()
    t = ConnectionTelemetry(ib=ib, jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t._on_connected()
    t._on_disconnected()
    t.stop()
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    events = [r["event"] for r in records]
    assert "connected" in events
    assert "disconnected" in events
```

- [ ] **Step 2: Run to confirm failure**

```bash
python -m pytest tests/test_telemetry.py -v -k connection
```
Expected: `ImportError: cannot import name 'ConnectionTelemetry'`.

- [ ] **Step 3: Implement `ConnectionTelemetry`**

```python
# Append to clients/telemetry.py
_FARM_STATE_BY_CODE = {
    2104: "ok",
    2105: "broken",
    2106: "ok",
    2107: "inactive",
    2158: "ok",
}


def _parse_farm_name(error_string: str) -> Optional[str]:
    """Extract trailing ':farmname' suffix if present."""
    if not error_string or ":" not in error_string:
        return None
    tail = error_string.rsplit(":", 1)[-1].strip()
    if not tail or " " in tail:
        return None
    return tail


class ConnectionTelemetry(BaseTelemetry):
    """IB-specific telemetry: subscribes to ib_async error/connected/disconnected events."""

    def __init__(self, *, ib, jsonl_path: Optional[Path], source: str = "ib"):
        super().__init__(source=source, jsonl_path=jsonl_path)
        self._ib = ib
        self._attached = False

    def start(self) -> None:
        super().start()
        if self._disabled or self._attached:
            return
        self._ib.errorEvent.connect(self._on_error)
        self._ib.connectedEvent.connect(self._on_connected)
        self._ib.disconnectedEvent.connect(self._on_disconnected)
        self._attached = True

    def stop(self) -> None:
        if self._attached:
            try:
                self._ib.errorEvent.disconnect(self._on_error)
                self._ib.connectedEvent.disconnect(self._on_connected)
                self._ib.disconnectedEvent.disconnect(self._on_disconnected)
            except Exception as exc:  # pragma: no cover - defensive
                _logger.warning("telemetry detach failed: %s", exc)
            self._attached = False
        super().stop()

    def _on_error(self, reqId, errorCode, errorString, contract):
        if errorCode in _FARM_STATE_BY_CODE:
            self._emit({
                "event": "farm_state",
                "code": int(errorCode),
                "state": _FARM_STATE_BY_CODE[errorCode],
                "farm": _parse_farm_name(errorString),
                "message": str(errorString),
            })
        else:
            self._emit({
                "event": "ib_error",
                "code": int(errorCode),
                "req_id": int(reqId),
                "message": str(errorString),
            })

    def _on_connected(self):
        self._emit({"event": "connected"})

    def _on_disconnected(self):
        self._emit({"event": "disconnected"})
```

- [ ] **Step 4: Run tests, confirm pass**

```bash
python -m pytest tests/test_telemetry.py -v
```
Expected: all green.

- [ ] **Step 5: Coverage check**

```bash
python -m pytest tests/test_telemetry.py --cov=clients.telemetry --cov-report=term-missing
```
Expected: 100% on `clients/telemetry.py`. If `_parse_farm_name` has a branch uncovered (empty string, no colon, whitespace tail), add small parametrized test.

- [ ] **Step 6: Commit**

```bash
git add clients/telemetry.py tests/test_telemetry.py
git commit -m "feat(telemetry): add ConnectionTelemetry for IB farm-state events

Parses IB errorCodes 2104/2105/2106/2107/2158 into farm_state records
with state + farm name extracted from ':farmname' suffix. Unknown codes
emit ib_error so visibility is preserved."
```

---

## Task 3: `UWTelemetry` + `MassiveTelemetry` stubs

**Files:**
- Modify: `clients/telemetry.py`
- Test: `tests/test_telemetry.py`

**Spec reference:** Components → `clients/telemetry.py` UW/Massive stubs. "Stub `UWTelemetry` / `MassiveTelemetry` instantiated before Sub-C ships" row in Error handling matrix.

- [ ] **Step 1: Write the failing tests**

```python
# Append to tests/test_telemetry.py
from clients.telemetry import UWTelemetry, MassiveTelemetry


def test_uw_telemetry_stub_no_op(tmp_path, caplog):
    t = UWTelemetry(jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t.record_request(endpoint="/options/AAPL", status=200, dt_ms=42)
    t.record_rate_limit(remaining=100, reset_at=1700000000)
    t.stop()
    # Stub: methods exist, do not raise, emit JSONL records tagged uw
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    assert any(r["source"] == "uw" for r in records)


def test_massive_telemetry_stub_no_op(tmp_path):
    t = MassiveTelemetry(jsonl_path=tmp_path / "t.jsonl")
    t.start()
    t.record_request(endpoint="/v2/bars/AAPL", status=200, dt_ms=15)
    t.stop()
    records = [json.loads(l) for l in (tmp_path / "t.jsonl").read_text().splitlines()]
    assert any(r["source"] == "massive" for r in records)


def test_uw_telemetry_source_locked_to_uw():
    t = UWTelemetry(jsonl_path=None)
    assert t.source == "uw"


def test_massive_telemetry_source_locked_to_massive():
    t = MassiveTelemetry(jsonl_path=None)
    assert t.source == "massive"
```

- [ ] **Step 2: Confirm failure** — `python -m pytest tests/test_telemetry.py -v -k "uw or massive"` should fail with import error.

- [ ] **Step 3: Implement stubs**

```python
# Append to clients/telemetry.py
class UWTelemetry(BaseTelemetry):
    """Unusual Whales telemetry. Stub interface for Sub-A — activated in Sub-C."""

    def __init__(self, *, jsonl_path: Optional[Path], source: str = "uw"):
        super().__init__(source=source, jsonl_path=jsonl_path)

    def start(self) -> None:
        super().start()
        if not self._disabled:
            _logger.info("UWTelemetry started (stub; Sub-C activates record_request)")

    def record_request(self, endpoint: str, status: int, dt_ms: int) -> None:
        self._emit({
            "event": "uw_request",
            "endpoint": endpoint,
            "status": int(status),
            "dt_ms": int(dt_ms),
        })

    def record_rate_limit(self, remaining: int, reset_at: int) -> None:
        self._emit({
            "event": "uw_rate_limit",
            "remaining": int(remaining),
            "reset_at": int(reset_at),
        })


class MassiveTelemetry(BaseTelemetry):
    """Massive.io telemetry. Stub interface for Sub-A — activated in Sub-C."""

    def __init__(self, *, jsonl_path: Optional[Path], source: str = "massive"):
        super().__init__(source=source, jsonl_path=jsonl_path)

    def start(self) -> None:
        super().start()
        if not self._disabled:
            _logger.info("MassiveTelemetry started (stub; Sub-C activates record_request)")

    def record_request(self, endpoint: str, status: int, dt_ms: int) -> None:
        self._emit({
            "event": "massive_request",
            "endpoint": endpoint,
            "status": int(status),
            "dt_ms": int(dt_ms),
        })
```

- [ ] **Step 4: Run + cover** — `python -m pytest tests/test_telemetry.py --cov=clients.telemetry --cov-report=term-missing` → 100%.

- [ ] **Step 5: Commit**

```bash
git add clients/telemetry.py tests/test_telemetry.py
git commit -m "feat(telemetry): add UWTelemetry and MassiveTelemetry stubs

Stub classes ship the full interface (record_request, record_rate_limit)
so Sub-C plugs in by implementing call sites. Source values locked to
'uw' and 'massive' respectively."
```

---

## Task 4: `QualityFlag` dataclass + `detect_range_shortfall`

**Files:**
- Create: `clients/quality_detector.py`
- Test: `tests/test_quality_detector.py`

**Spec reference:** Components → `clients/quality_detector.py`, threshold table (`range_shortfall` row).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_quality_detector.py
from datetime import date
import pytest

from clients.quality_detector import QualityFlag, detect_range_shortfall


def test_quality_flag_is_frozen():
    f = QualityFlag(category="range_shortfall", severity="critical", detail={"x": 1}, ts="2026-05-17T00:00:00Z")
    with pytest.raises(Exception):
        f.severity = "warning"


def test_range_shortfall_clean_returns_none():
    flag = detect_range_shortfall(
        expected_start=date(2020, 1, 1),
        actual_start=date(2020, 1, 1),
        ib_head_timestamp=date(2020, 1, 1),
    )
    assert flag is None


def test_range_shortfall_within_tolerance_returns_none():
    # 3 trading days short — under warning threshold of >5
    flag = detect_range_shortfall(
        expected_start=date(2020, 1, 1),
        actual_start=date(2020, 1, 6),
        ib_head_timestamp=date(2020, 1, 1),
    )
    assert flag is None


def test_range_shortfall_warning_threshold():
    # 10 trading days short of expected, but head_ts matches expected (i.e., data IS available)
    flag = detect_range_shortfall(
        expected_start=date(2020, 1, 1),
        actual_start=date(2020, 1, 15),
        ib_head_timestamp=date(2020, 1, 1),
    )
    assert flag is not None
    assert flag.severity == "warning"
    assert flag.category == "range_shortfall"


def test_range_shortfall_critical_against_head_ts():
    # SMH case: expected 1993, got 2019, head_ts says 1993 (huge gap)
    flag = detect_range_shortfall(
        expected_start=date(1993, 1, 29),
        actual_start=date(2019, 5, 20),
        ib_head_timestamp=date(1993, 1, 29),
    )
    assert flag is not None
    assert flag.severity == "critical"
    assert "shortfall_days" in flag.detail


def test_range_shortfall_head_ts_matches_actual_returns_none():
    # IB legitimately has no older data — head_ts == actual_start, so not a fault
    flag = detect_range_shortfall(
        expected_start=date(1993, 1, 29),
        actual_start=date(2019, 5, 20),
        ib_head_timestamp=date(2019, 5, 20),
    )
    assert flag is None


def test_range_shortfall_no_head_ts_uses_expected_diff_only():
    flag = detect_range_shortfall(
        expected_start=date(2020, 1, 1),
        actual_start=date(2020, 2, 15),
        ib_head_timestamp=None,
    )
    assert flag is not None
    assert flag.severity in {"warning", "critical"}
```

- [ ] **Step 2: Confirm failure** — `python -m pytest tests/test_quality_detector.py -v` → ImportError.

- [ ] **Step 3: Implement `QualityFlag` + `detect_range_shortfall`**

```python
# clients/quality_detector.py
"""Pure quality-flag detection. No I/O.

See: docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Optional

_RANGE_SHORTFALL_WARNING_DAYS = 5
_RANGE_SHORTFALL_CRITICAL_DAYS = 30


def _utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class QualityFlag:
    category: str    # "range_shortfall" | "interior_gaps" | "fetch_tainted" | "row_count_anomaly"
    severity: str    # "critical" | "warning" | "info"
    detail: dict[str, Any] = field(default_factory=dict)
    ts: str = field(default_factory=_utc_iso)


def detect_range_shortfall(
    expected_start: date,
    actual_start: date,
    ib_head_timestamp: Optional[date],
) -> Optional[QualityFlag]:
    """Flag when actual_start is materially later than expected_start.

    If ib_head_timestamp equals actual_start, treat as 'IB has no older data' (clean).
    If ib_head_timestamp is earlier than actual_start, the shortfall is real.
    """
    if actual_start <= expected_start:
        return None
    shortfall_days = (actual_start - expected_start).days
    head_indicates_loss = ib_head_timestamp is not None and ib_head_timestamp < actual_start
    if ib_head_timestamp is not None and ib_head_timestamp >= actual_start:
        return None    # IB legitimately doesn't have older data
    if head_indicates_loss:
        severity = "critical"
    elif shortfall_days > _RANGE_SHORTFALL_CRITICAL_DAYS:
        severity = "critical"
    elif shortfall_days > _RANGE_SHORTFALL_WARNING_DAYS:
        severity = "warning"
    else:
        return None
    return QualityFlag(
        category="range_shortfall",
        severity=severity,
        detail={
            "expected_start": expected_start.isoformat(),
            "actual_start": actual_start.isoformat(),
            "shortfall_days": shortfall_days,
            "ib_head_timestamp": ib_head_timestamp.isoformat() if ib_head_timestamp else None,
        },
    )
```

- [ ] **Step 4: Run + cover** — 7/7 pass, 100% coverage of the lines so far.

- [ ] **Step 5: Commit**

```bash
git add clients/quality_detector.py tests/test_quality_detector.py
git commit -m "feat(quality): add QualityFlag and detect_range_shortfall

Pure-function detector for range shortfall vs IB head_timestamp. SMH-case
(head says 1993, actual 2019) is critical; legitimate 'IB has no older
data' returns None."
```

---

## Task 5: `detect_interior_gaps` + trading-calendar integration

**Files:**
- Modify: `clients/quality_detector.py`
- Test: `tests/test_quality_detector.py`

**Spec reference:** Components → threshold table (`interior_gaps`), Error handling matrix (`Trading calendar lookup fails`).

**Note:** The repo already ships a pure-Python NYSE trading calendar inside `scripts/daily_update.py`. **Verified** against the live source: `is_trading_day(d: date) -> bool` (line 222), `previous_trading_day(d: date) -> date` (line 270), `trading_days_between(start, end)` (line 278), plus the supporting `_easter(year)` and `get_nyse_holidays(year)` helpers. This task **extracts** them to `clients/trading_calendar.py` without behavior changes.

- [ ] **Step 1: Extract calendar helpers to `clients/trading_calendar.py`**

Move these functions from `scripts/daily_update.py` (around lines 200-285) to a new `clients/trading_calendar.py`, preserving their bodies exactly:
- `_easter(year: int) -> date`
- `get_nyse_holidays(year: int) -> set[date]`
- `is_trading_day(d: date) -> bool`
- `previous_trading_day(d: date) -> date`
- `trading_days_between(start: date, end: date) -> int`

Then in `scripts/daily_update.py`, replace those definitions with:

```python
from clients.trading_calendar import (
    _easter,
    get_nyse_holidays,
    is_trading_day,
    previous_trading_day,
    trading_days_between,
)
```

**Update the test import**: `tests/test_daily_update.py` currently imports these names directly from `scripts.daily_update`. Re-exporting them via the import above keeps those tests passing. If you prefer to switch test imports to `clients.trading_calendar`, do that as a second edit and confirm `python -m pytest tests/test_daily_update.py -q` is green.

- [ ] **Step 1b: Verify the existing daily_update tests still pass**

```bash
python -m pytest tests/test_daily_update.py::TestEaster tests/test_daily_update.py -q -k "easter or trading_day or trading_days_between" 2>&1 | tail -10
```
Expected: all green. If any fail, the extraction changed behavior — re-check the moves.

- [ ] **Step 2: Write the failing tests**

```python
# Append to tests/test_quality_detector.py
from clients.quality_detector import detect_interior_gaps

# Use a simple BarRecord stub — match clients.historical_provider's BarRecord shape
class _Bar:
    def __init__(self, d, c=100.0):
        self.trade_date = d if isinstance(d, str) else d.isoformat()


def test_interior_gaps_no_gap():
    bars = [_Bar(f"2026-04-{day:02d}") for day in [1, 2, 3, 6, 7, 8]]  # Apr 4,5 = weekend
    assert detect_interior_gaps(bars, trading_calendar=None) is None


def test_interior_gaps_single_missing_trading_day():
    # Apr 1, 2, [missing Apr 3], 6, 7
    bars = [_Bar(f"2026-04-{day:02d}") for day in [1, 2, 6, 7]]
    flag = detect_interior_gaps(bars, trading_calendar=None)
    assert flag is not None
    assert flag.category == "interior_gaps"
    assert flag.severity in {"warning", "critical"}
    assert flag.detail["missing_days_count"] >= 1


def test_interior_gaps_consecutive_critical():
    # 10 consecutive trading days missing — critical
    bars = [_Bar("2026-04-01")] + [_Bar(f"2026-04-{day:02d}") for day in [17, 20]]
    flag = detect_interior_gaps(bars, trading_calendar=None)
    assert flag is not None
    assert flag.severity == "critical"


def test_interior_gaps_empty_bars_returns_none():
    assert detect_interior_gaps([], trading_calendar=None) is None


def test_interior_gaps_single_bar_returns_none():
    assert detect_interior_gaps([_Bar("2026-04-01")], trading_calendar=None) is None


def test_interior_gaps_calendar_failure_emits_info_flag(monkeypatch):
    # Simulate trading-calendar import/raise
    from clients import quality_detector
    def boom(d):
        raise RuntimeError("calendar broken")
    monkeypatch.setattr(quality_detector, "_default_is_trading_day", boom)
    bars = [_Bar("2026-04-01"), _Bar("2026-04-10")]
    flag = detect_interior_gaps(bars, trading_calendar=None)
    assert flag is not None
    assert flag.category == "interior_gaps"
    assert flag.severity == "info"
    assert flag.detail.get("status") == "gap_detection_unavailable"
```

- [ ] **Step 3: Implement `detect_interior_gaps`**

```python
# Append to clients/quality_detector.py
from datetime import timedelta

try:
    from clients.trading_calendar import is_trading_day as _default_is_trading_day
except ImportError:    # pragma: no cover - exercised only before T5 helper extraction
    _default_is_trading_day = None

_INTERIOR_GAPS_WARNING_DAYS = 1
_INTERIOR_GAPS_CRITICAL_CONSECUTIVE = 10
_INTERIOR_GAPS_CRITICAL_TOTAL = 30


def _coerce_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value)[:10])


def detect_interior_gaps(
    bars: list,
    trading_calendar=None,
) -> Optional[QualityFlag]:
    """Find missing trading days inside the bar range."""
    if not bars or len(bars) < 2:
        return None
    is_trading_day = trading_calendar or _default_is_trading_day
    if is_trading_day is None:
        return QualityFlag(
            category="interior_gaps",
            severity="info",
            detail={"status": "gap_detection_unavailable", "reason": "no_calendar"},
        )
    try:
        dates = sorted({_coerce_date(b.trade_date) for b in bars})
        start, end = dates[0], dates[-1]
        present = set(dates)
        cursor = start + timedelta(days=1)
        missing: list[date] = []
        max_consecutive = 0
        current_run = 0
        while cursor < end:
            if is_trading_day(cursor):
                if cursor in present:
                    current_run = 0
                else:
                    missing.append(cursor)
                    current_run += 1
                    max_consecutive = max(max_consecutive, current_run)
            cursor += timedelta(days=1)
    except Exception as exc:    # calendar lookup blew up
        return QualityFlag(
            category="interior_gaps",
            severity="info",
            detail={"status": "gap_detection_unavailable", "reason": str(exc)},
        )
    if not missing:
        return None
    if max_consecutive >= _INTERIOR_GAPS_CRITICAL_CONSECUTIVE or len(missing) >= _INTERIOR_GAPS_CRITICAL_TOTAL:
        severity = "critical"
    else:
        severity = "warning"
    return QualityFlag(
        category="interior_gaps",
        severity=severity,
        detail={
            "missing_days_count": len(missing),
            "max_consecutive_missing": max_consecutive,
            "first_missing": missing[0].isoformat(),
            "last_missing": missing[-1].isoformat(),
        },
    )
```

- [ ] **Step 4: Run + cover** — 6/6 pass, 100% coverage of quality_detector lines so far.

- [ ] **Step 5: Commit**

```bash
git add clients/quality_detector.py clients/trading_calendar.py tests/test_quality_detector.py
git commit -m "feat(quality): add detect_interior_gaps with calendar-aware scan

Counts missing trading days inside the bar range; flags >=10 consecutive
or >=30 total as critical. Calendar failure fails open with an info
flag rather than propagating the exception."
```

---

## Task 6: `detect_fetch_tainting`

**Files:**
- Modify: `clients/quality_detector.py`
- Test: `tests/test_quality_detector.py`

**Spec reference:** Components → threshold table (`fetch_tainted` row), Error handling matrix (`errors_during_fetch`).

- [ ] **Step 1: Write the failing tests**

```python
# Append to tests/test_quality_detector.py
from clients.quality_detector import detect_fetch_tainting


def test_fetch_tainting_no_errors_returns_none():
    assert detect_fetch_tainting([]) is None


def test_fetch_tainting_one_error_warning():
    flag = detect_fetch_tainting([{"code": 162, "count": 1, "message": "no data"}])
    assert flag is not None
    assert flag.severity == "warning"
    assert flag.category == "fetch_tainted"


def test_fetch_tainting_aggregated_count_critical():
    flag = detect_fetch_tainting([
        {"code": 162, "count": 4},
        {"code": 2105, "count": 2},
    ])
    assert flag.severity == "critical"
    assert flag.detail["error_count"] == 6


def test_fetch_tainting_codes_recorded():
    flag = detect_fetch_tainting([
        {"code": 162, "count": 2},
        {"code": 2105, "count": 1},
    ])
    assert set(flag.detail["codes"]) == {162, 2105}
```

- [ ] **Step 2: Confirm failure** — `python -m pytest tests/test_quality_detector.py -v -k tainting` → ImportError.

- [ ] **Step 3: Implement**

```python
# Append to clients/quality_detector.py
_FETCH_TAINT_WARNING_COUNT = 1
_FETCH_TAINT_CRITICAL_COUNT = 5


def detect_fetch_tainting(errors_during_fetch: list[dict]) -> Optional[QualityFlag]:
    if not errors_during_fetch:
        return None
    total = sum(int(e.get("count", 1)) for e in errors_during_fetch)
    codes = sorted({int(e["code"]) for e in errors_during_fetch if "code" in e})
    if total >= _FETCH_TAINT_CRITICAL_COUNT:
        severity = "critical"
    elif total >= _FETCH_TAINT_WARNING_COUNT:
        severity = "warning"
    else:    # pragma: no cover - unreachable given total >=1 entry
        return None
    return QualityFlag(
        category="fetch_tainted",
        severity=severity,
        detail={"error_count": total, "codes": codes},
    )
```

- [ ] **Step 4: Run + cover.**

- [ ] **Step 5: Commit**

```bash
git add clients/quality_detector.py tests/test_quality_detector.py
git commit -m "feat(quality): add detect_fetch_tainting

Aggregates errors collected during an IB fetch; >=5 → critical, >=1 →
warning. Records affected error codes so the operator can grep audit
JSONL by code."
```

---

## Task 7: `detect_row_count_anomaly` stub + `detect_all` aggregator

**Files:**
- Modify: `clients/quality_detector.py`
- Test: `tests/test_quality_detector.py`

**Spec reference:** Components → `detect_row_count_anomaly` (STUB in Sub-A) and `detect_all`.

- [ ] **Step 1: Write the failing tests**

```python
# Append to tests/test_quality_detector.py
from clients.quality_detector import detect_row_count_anomaly, detect_all


def test_row_count_anomaly_stub_returns_none():
    assert detect_row_count_anomaly([], reference_source=None) is None


def test_detect_all_clean_returns_empty():
    bars = [_Bar("2026-04-01"), _Bar("2026-04-02")]
    flags = detect_all(
        bars=bars,
        metadata={
            "expected_start": date(2026, 4, 1),
            "ib_head_timestamp": date(2026, 4, 1),
            "errors_during_fetch": [],
        },
        trading_calendar=lambda d: True,
    )
    assert flags == []


def test_detect_all_returns_multiple_flags():
    bars = [_Bar("2020-01-10"), _Bar("2020-01-11")]    # actual_start 2020-01-10
    flags = detect_all(
        bars=bars,
        metadata={
            "expected_start": date(1993, 1, 1),
            "ib_head_timestamp": date(1993, 1, 1),
            "errors_during_fetch": [{"code": 2105, "count": 6}],
        },
        trading_calendar=lambda d: True,
    )
    categories = {f.category for f in flags}
    assert "range_shortfall" in categories
    assert "fetch_tainted" in categories


def test_detect_all_handles_missing_metadata_keys():
    flags = detect_all(
        bars=[_Bar("2026-04-01")],
        metadata={},
        trading_calendar=lambda d: True,
    )
    assert flags == []    # No expected_start → can't compute range_shortfall


def test_detect_all_isolates_detector_failures(monkeypatch):
    from clients import quality_detector
    monkeypatch.setattr(quality_detector, "detect_range_shortfall",
                        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")))
    bars = [_Bar("2020-01-10")]
    # Should NOT raise; failed detector logs and continues
    flags = detect_all(
        bars=bars,
        metadata={"expected_start": date(2020, 1, 1), "ib_head_timestamp": None, "errors_during_fetch": []},
        trading_calendar=lambda d: True,
    )
    # detector_error is recorded as a flag itself
    assert any(f.category == "detector_error" for f in flags)
```

- [ ] **Step 2: Confirm failure.**

- [ ] **Step 3: Implement**

```python
# Append to clients/quality_detector.py
import logging
_logger = logging.getLogger("mdw.quality")


def detect_row_count_anomaly(
    bars: list,
    reference_source=None,
) -> Optional[QualityFlag]:
    """STUB in Sub-A. Activated in Sub-C when a second source exists."""
    if reference_source is None:
        return None
    raise NotImplementedError("row-count-anomaly activation deferred to Sub-C")    # pragma: no cover


def detect_all(
    bars: list,
    metadata: dict,
    trading_calendar=None,
) -> list[QualityFlag]:
    flags: list[QualityFlag] = []

    def _safe(name: str, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            _logger.warning("detector %s raised: %s", name, exc)
            flags.append(QualityFlag(
                category="detector_error",
                severity="warning",
                detail={"detector": name, "error": str(exc)},
            ))
            return None

    expected_start = metadata.get("expected_start")
    if expected_start is not None and bars:
        actual_start = _coerce_date(bars[0].trade_date)
        f = _safe("range_shortfall",
                  detect_range_shortfall,
                  expected_start, actual_start, metadata.get("ib_head_timestamp"))
        if f:
            flags.append(f)

    if len(bars) >= 2:
        f = _safe("interior_gaps", detect_interior_gaps, bars, trading_calendar)
        if f:
            flags.append(f)

    errors = metadata.get("errors_during_fetch") or []
    f = _safe("fetch_tainted", detect_fetch_tainting, errors)
    if f:
        flags.append(f)

    f = _safe("row_count_anomaly",
              detect_row_count_anomaly,
              bars, metadata.get("reference_source"))
    if f:
        flags.append(f)

    return flags
```

- [ ] **Step 4: Run + cover** — 100% on `clients/quality_detector.py`. The `pragma: no cover` lines (`NotImplementedError`) are expected.

- [ ] **Step 5: Commit**

```bash
git add clients/quality_detector.py tests/test_quality_detector.py
git commit -m "feat(quality): add row_count_anomaly stub and detect_all aggregator

detect_all wraps each detector in a try/except — a buggy detector emits
a detector_error flag rather than hiding data. row_count_anomaly stub
returns None until Sub-C provides a reference source."
```

---

## Task 8: `quality_flags.write_sidecar`

**Files:**
- Create: `clients/quality_flags.py`
- Test: `tests/test_quality_flags.py`

**Spec reference:** Components → `clients/quality_flags.py` → `write_sidecar`, sidecar JSON schema example.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_quality_flags.py
import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from clients.quality_detector import QualityFlag
from clients.quality_flags import write_sidecar


def _flag(category="range_shortfall", severity="critical"):
    return QualityFlag(
        category=category,
        severity=severity,
        detail={"k": "v"},
        ts="2026-05-17T00:00:00Z",
    )


def test_write_sidecar_atomic_temp_then_replace(tmp_path):
    parquet = tmp_path / "1d.parquet"
    parquet.write_bytes(b"")  # placeholder
    metadata = {
        "ticker": "SMH",
        "timeframe": "1d",
        "source": "ib",
        "bars_received": 1758,
    }
    ok = write_sidecar(parquet, [_flag()], metadata)
    assert ok is True
    sidecar = parquet.with_suffix(".parquet.meta.json")
    assert sidecar.exists()
    payload = json.loads(sidecar.read_text())
    assert payload["ticker"] == "SMH"
    assert payload["flags"][0]["category"] == "range_shortfall"
    assert payload["bars_received"] == 1758


def test_write_sidecar_includes_parquet_path_relative(tmp_path):
    parquet = tmp_path / "symbol=SMH" / "1d.parquet"
    parquet.parent.mkdir()
    parquet.write_bytes(b"")
    write_sidecar(parquet, [_flag()], {"ticker": "SMH", "timeframe": "1d", "source": "ib"})
    payload = json.loads(parquet.with_suffix(".parquet.meta.json").read_text())
    assert payload["parquet_path"].endswith("symbol=SMH/1d.parquet")


def test_write_sidecar_oserror_returns_false(tmp_path, monkeypatch, caplog):
    parquet = tmp_path / "1d.parquet"
    parquet.write_bytes(b"")

    def boom(*a, **kw):
        raise OSError("disk full")
    monkeypatch.setattr("os.replace", boom)
    ok = write_sidecar(parquet, [_flag()], {"ticker": "X", "timeframe": "1d", "source": "ib"})
    assert ok is False
```

- [ ] **Step 2: Confirm failure.**

- [ ] **Step 3: Implement**

```python
# clients/quality_flags.py
"""Quality-flag emit paths: sidecar JSON, audit JSONL, alert email.

Three independent emit paths — any failing alone doesn't sink the others.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import time
from dataclasses import asdict
from pathlib import Path
from typing import Optional

from clients.quality_detector import QualityFlag

_logger = logging.getLogger("mdw.quality")

_VALID_SOURCES = {"ib", "uw", "massive"}


def _sidecar_path(parquet_path: Path) -> Path:
    return parquet_path.with_suffix(parquet_path.suffix + ".meta.json")


def write_sidecar(parquet_path: Path, flags: list[QualityFlag], metadata: dict) -> bool:
    """Write <parquet>.meta.json atomically. Returns True on success."""
    sidecar = _sidecar_path(parquet_path)
    payload = dict(metadata)
    payload["parquet_path"] = str(parquet_path)
    payload["flags"] = [asdict(f) for f in flags]
    try:
        fd, tmp_path = tempfile.mkstemp(
            prefix=".sidecar_", suffix=".tmp", dir=str(sidecar.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True, default=str)
            os.replace(tmp_path, sidecar)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:    # pragma: no cover - best-effort cleanup
                pass
            raise
    except OSError as exc:
        _logger.warning("sidecar write failed for %s: %s", parquet_path, exc)
        return False
    return True
```

- [ ] **Step 4: Run + cover** — 100% on the lines added so far.

- [ ] **Step 5: Commit**

```bash
git add clients/quality_flags.py tests/test_quality_flags.py
git commit -m "feat(quality): add write_sidecar atomic emit path

Writes <parquet>.meta.json via temp+os.replace. Returns False on OSError
rather than raising — sidecar is one of three independent emit paths
and must not block the data path."
```

---

## Task 9: `quality_flags.append_audit` + source validation

**Files:**
- Modify: `clients/quality_flags.py`
- Test: `tests/test_quality_flags.py`

**Spec reference:** Components → `append_audit`, audit JSONL schema example, error handling row (`source value not in {ib, uw, massive}`).

- [ ] **Step 1: Write the failing tests**

```python
# Append to tests/test_quality_flags.py
from clients.quality_flags import append_audit, _resolve_audit_path


def test_append_audit_writes_one_jsonl_line(tmp_path, monkeypatch):
    audit = tmp_path / "audit.jsonl"
    monkeypatch.setenv("MDW_QUALITY_AUDIT_PATH", str(audit))
    ok = append_audit(_flag(), source="ib", ticker="SMH", timeframe="1d", parquet_path=tmp_path / "1d.parquet")
    assert ok is True
    lines = audit.read_text().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["source"] == "ib"
    assert record["ticker"] == "SMH"
    assert record["category"] == "range_shortfall"


def test_append_audit_rejects_invalid_source(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_QUALITY_AUDIT_PATH", str(tmp_path / "audit.jsonl"))
    with pytest.raises(ValueError, match="source must be one of"):
        append_audit(_flag(), source="bogus", ticker="SMH", timeframe="1d", parquet_path=tmp_path / "1d.parquet")


def test_append_audit_oserror_returns_false(tmp_path, monkeypatch):
    audit = tmp_path / "nope" / "audit.jsonl"
    monkeypatch.setenv("MDW_QUALITY_AUDIT_PATH", str(audit))

    def boom(*a, **kw):
        raise OSError("readonly fs")
    monkeypatch.setattr("pathlib.Path.open", boom)
    ok = append_audit(_flag(), source="ib", ticker="SMH", timeframe="1d", parquet_path=tmp_path / "1d.parquet")
    assert ok is False
```

- [ ] **Step 2: Confirm failure** — `ImportError: cannot import name 'append_audit'`.

- [ ] **Step 3: Implement**

```python
# Append to clients/quality_flags.py
def _utc_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_audit_path() -> Path:
    raw = os.environ.get(
        "MDW_QUALITY_AUDIT_PATH",
        str(Path.home() / "market-warehouse" / "logs" / "quality_audit.jsonl"),
    )
    return Path(raw).expanduser()


def append_audit(
    flag: QualityFlag,
    *,
    source: str,
    ticker: str,
    timeframe: str,
    parquet_path: Path,
) -> bool:
    """Append one JSON line to the central audit JSONL. Raises on invalid source."""
    if source not in _VALID_SOURCES:
        raise ValueError(f"source must be one of {_VALID_SOURCES}, got {source!r}")
    record = {
        "ts": _utc_iso(),
        "source": source,
        "ticker": ticker,
        "timeframe": timeframe,
        "parquet_path": str(parquet_path),
        "category": flag.category,
        "severity": flag.severity,
        "detail": flag.detail,
    }
    line = json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
    audit = _resolve_audit_path()
    try:
        audit.parent.mkdir(parents=True, exist_ok=True)
        with audit.open("a", encoding="utf-8") as fh:
            fh.write(line)
    except OSError as exc:
        _logger.warning("audit append failed: %s", exc)
        return False
    return True
```

- [ ] **Step 4: Run + cover.**

- [ ] **Step 5: Commit**

```bash
git add clients/quality_flags.py tests/test_quality_flags.py
git commit -m "feat(quality): add append_audit JSONL emit path

Single line per (flag × ticker) appended to MDW_QUALITY_AUDIT_PATH.
Closed-set source enum validation prevents JSONL pollution from typos."
```

---

## Task 10: `quality_flags.alert_on_flag` + Nodemailer spawn + rate-limit + SMTP-fail preservation

**Files:**
- Modify: `clients/quality_flags.py`
- Test: `tests/test_quality_flags.py`

**Spec reference:** Components → `alert_on_flag`, Configuration table (`MDW_ALERT_SEVERITY_THRESHOLD`, `MDW_ALERT_RATE_LIMIT_SECONDS`), Error handling matrix (`Email send fails`).

**Dependency:** Spawns the Node script with mode `flag-alert`. That mode is added in **Task 11** — make sure you've also completed Task 11 before the integration tasks (T14-T16) start using `alert_on_flag` against a live SMTP. The unit tests below mock `subprocess.run`, so Task 10 can land independently.

- [ ] **Step 1: Write the failing tests**

```python
# Append to tests/test_quality_flags.py
from clients.quality_flags import alert_on_flag


def test_alert_below_threshold_skipped(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_ALERT_SEVERITY_THRESHOLD", "critical")
    called = []
    monkeypatch.setattr("subprocess.run", lambda *a, **kw: called.append(a) or _ok())
    ok = alert_on_flag(_flag(severity="warning"), source="ib", ticker="SMH")
    assert ok is False
    assert called == []    # below threshold → never spawned


def test_alert_above_threshold_spawns(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_ALERT_SEVERITY_THRESHOLD", "warning")
    called = []
    def fake_run(*a, **kw):
        called.append(a)
        return _ok()
    monkeypatch.setattr("subprocess.run", fake_run)
    ok = alert_on_flag(_flag(severity="critical"), source="ib", ticker="SMH")
    assert ok is True
    assert called, "subprocess.run should have been invoked"
    cmd = called[0][0]
    assert "send_daily_update_failure_email.mjs" in " ".join(cmd)
    assert "flag-alert" in cmd


def test_alert_rate_limit_dedupes_within_window(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_ALERT_SEVERITY_THRESHOLD", "warning")
    monkeypatch.setenv("MDW_ALERT_RATE_LIMIT_SECONDS", "300")
    counts = [0]
    def fake_run(*a, **kw):
        counts[0] += 1
        return _ok()
    monkeypatch.setattr("subprocess.run", fake_run)
    from clients import quality_flags
    quality_flags._RATE_LIMIT_CACHE.clear()    # ensure clean state
    alert_on_flag(_flag(severity="critical"), source="ib", ticker="SMH")
    alert_on_flag(_flag(severity="critical"), source="ib", ticker="SMH")    # duplicate
    assert counts[0] == 1


def test_alert_smtp_failure_preserves_html(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_ALERT_SEVERITY_THRESHOLD", "warning")
    monkeypatch.setenv("MDW_UNDELIVERED_DIR", str(tmp_path / "undelivered"))
    def fake_run(*a, **kw):
        return _fail("SMTP timeout")
    monkeypatch.setattr("subprocess.run", fake_run)
    from clients import quality_flags
    quality_flags._RATE_LIMIT_CACHE.clear()
    ok = alert_on_flag(_flag(severity="critical"), source="ib", ticker="HOOD")
    assert ok is False
    saved = list((tmp_path / "undelivered").glob("*HOOD*"))
    assert saved, "undelivered HTML should be preserved"


def _ok():
    from subprocess import CompletedProcess
    return CompletedProcess(args=[], returncode=0, stdout=b"", stderr=b"")


def _fail(msg):
    from subprocess import CompletedProcess
    return CompletedProcess(args=[], returncode=1, stdout=b"", stderr=msg.encode())
```

- [ ] **Step 2: Confirm failure** — `ImportError: cannot import name 'alert_on_flag'`.

- [ ] **Step 3: Implement**

```python
# Append to clients/quality_flags.py
_SEVERITY_ORDER = {"info": 0, "warning": 1, "critical": 2}
_RATE_LIMIT_CACHE: dict[tuple[str, str, str], float] = {}

_REPO_ROOT = Path(__file__).resolve().parent.parent
_EMAIL_SCRIPT = _REPO_ROOT / "scripts" / "send_daily_update_failure_email.mjs"


def _resolve_threshold() -> str:
    return os.environ.get("MDW_ALERT_SEVERITY_THRESHOLD", "warning").lower()


def _resolve_rate_limit_seconds() -> int:
    raw = os.environ.get("MDW_ALERT_RATE_LIMIT_SECONDS", "300")
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 300


def _resolve_undelivered_dir() -> Path:
    raw = os.environ.get(
        "MDW_UNDELIVERED_DIR",
        str(Path.home() / "market-warehouse" / "logs" / "quality_alerts_undelivered"),
    )
    p = Path(raw).expanduser()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _render_alert_html(flag: QualityFlag, source: str, ticker: str) -> str:
    return (
        f"<html><body>"
        f"<h2>[Livewire] {flag.severity.upper()} quality flag</h2>"
        f"<p><b>Source:</b> {source} &nbsp; <b>Ticker:</b> {ticker}</p>"
        f"<p><b>Category:</b> {flag.category}</p>"
        f"<pre>{json.dumps(flag.detail, indent=2)}</pre>"
        f"</body></html>"
    )


def alert_on_flag(
    flag: QualityFlag,
    *,
    source: str,
    ticker: str,
    severity_threshold: Optional[str] = None,
) -> bool:
    """Spawn Nodemailer email if severity meets threshold. Returns True if email sent."""
    threshold = (severity_threshold or _resolve_threshold()).lower()
    if _SEVERITY_ORDER.get(flag.severity, 0) < _SEVERITY_ORDER.get(threshold, 1):
        return False

    key = (source, ticker, flag.category)
    now = time.monotonic()
    rl = _resolve_rate_limit_seconds()
    last = _RATE_LIMIT_CACHE.get(key, 0.0)
    if rl > 0 and (now - last) < rl:
        _logger.info("alert rate-limited: %s/%s/%s", source, ticker, flag.category)
        return False
    _RATE_LIMIT_CACHE[key] = now

    payload = {
        "source": source,
        "ticker": ticker,
        "category": flag.category,
        "severity": flag.severity,
        "detail": flag.detail,
        "ts": flag.ts,
    }
    cmd = [
        "node", str(_EMAIL_SCRIPT),
        "--mode", "flag-alert",
        "--payload", json.dumps(payload),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30)
    except (subprocess.SubprocessError, OSError) as exc:
        _logger.error("alert spawn failed: %s", exc)
        _preserve_undelivered(flag, source, ticker)
        return False
    if result.returncode != 0:
        _logger.error(
            "alert send returned %s: %s", result.returncode, (result.stderr or b"").decode("utf-8", "replace")
        )
        _preserve_undelivered(flag, source, ticker)
        return False
    return True


def _preserve_undelivered(flag: QualityFlag, source: str, ticker: str) -> None:
    try:
        out_dir = _resolve_undelivered_dir()
        ts = _utc_iso().replace(":", "-")
        path = out_dir / f"{ts}_{source}_{ticker}.html"
        path.write_text(_render_alert_html(flag, source, ticker), encoding="utf-8")
    except OSError as exc:    # pragma: no cover - last-resort logging only
        _logger.error("could not preserve undelivered alert: %s", exc)
```

- [ ] **Step 4: Run + cover.**

- [ ] **Step 5: Commit**

```bash
git add clients/quality_flags.py tests/test_quality_flags.py
git commit -m "feat(quality): add alert_on_flag email path

Severity-gated, rate-limited, spawns Nodemailer flag-alert mode.
SMTP failure preserves the rendered HTML at MDW_UNDELIVERED_DIR
so a manual sweep can recover lost alerts."
```

---

## Task 11: `send_daily_update_failure_email.mjs` — `--mode flag-alert`

**Files:**
- Modify: `scripts/send_daily_update_failure_email.mjs`
- Test: `scripts/send_daily_update_failure_email.test.mjs`

**Spec reference:** Components → `scripts/send_daily_update_failure_email.mjs` (Two new entry points).

- [ ] **Step 1: Confirm the boundary**

**Verified:** `scripts/send_daily_update_failure_email.mjs:448` already exports `sendFailureAlert({ transportOptions, message })` — a thin wrapper over `nodemailer.createTransport(opts).sendMail(message)`. The cleanest extension is: add a `buildFlagAlertMessage(payload)` builder that returns `{subject, html, text}`, then have `main()` dispatch to `sendFailureAlert` with the right message. **No refactor of the transport path is needed.**

- [ ] **Step 2: Write the failing test**

```javascript
// Append to scripts/send_daily_update_failure_email.test.mjs
test("flag-alert mode renders HTML body with ticker and category", async () => {
  const { parseArgs, buildFlagAlertMessage } = await import("./send_daily_update_failure_email.mjs");
  const args = parseArgs([
    "--mode", "flag-alert",
    "--payload", JSON.stringify({
      source: "ib",
      ticker: "SMH",
      category: "range_shortfall",
      severity: "critical",
      detail: { shortfall_days: 9601 },
      ts: "2026-05-17T19:43:33Z",
    }),
  ]);
  assert.equal(args.mode, "flag-alert");
  const message = buildFlagAlertMessage(args.payload);
  assert.match(message.subject, /\[Livewire\].*SMH.*range_shortfall/);
  assert.match(message.html, /shortfall_days/);
  assert.match(message.html, /critical/i);
});

test("flag-alert mode rejects missing payload", async () => {
  const { parseArgs } = await import("./send_daily_update_failure_email.mjs");
  assert.throws(() => parseArgs(["--mode", "flag-alert"]),
    /payload.*required/i);
});
```

- [ ] **Step 3: Confirm failure**

```bash
cd /Users/chenxi/projects/livewire
node --test scripts/send_daily_update_failure_email.test.mjs 2>&1 | head -50
```

- [ ] **Step 4: Implement** — in `scripts/send_daily_update_failure_email.mjs`, extend `parseArgs` to accept `--mode flag-alert --payload <json>`, and add `buildFlagAlertMessage` plus the dispatch in `main`:

```javascript
// near top, after existing constants:
const MODES = new Set(["failure", "flag-alert", "daily-summary"]);

// in parseArgs:
//   case "--mode": result.mode = next(); break;
//   case "--payload": result.payloadRaw = next(); break;
// after parsing loop:
if (result.mode === "flag-alert" || result.mode === "daily-summary") {
  if (!result.payloadRaw) {
    throw new Error("--payload is required for mode " + result.mode);
  }
  result.payload = JSON.parse(result.payloadRaw);
}

export function buildFlagAlertMessage(payload) {
  const { source, ticker, category, severity, detail, ts } = payload;
  const subject = `${DEFAULT_SUBJECT_PREFIX.replace("Market Data Warehouse", "Livewire")} ` +
    `${severity.toUpperCase()} ${ticker} ${category}`;
  const detailHtml = `<pre>${escapeHtml(JSON.stringify(detail, null, 2))}</pre>`;
  const html = `<html><body>
    <h2>[Livewire] ${escapeHtml(severity)} quality flag</h2>
    <p><b>Source:</b> ${escapeHtml(source)} &nbsp; <b>Ticker:</b> ${escapeHtml(ticker)}</p>
    <p><b>Category:</b> ${escapeHtml(category)}</p>
    <p><b>Detected at:</b> ${escapeHtml(ts || "")}</p>
    ${detailHtml}
  </body></html>`;
  const text = `[Livewire] ${severity.toUpperCase()} ${ticker} ${category}\n` +
    JSON.stringify(detail, null, 2);
  return { subject, html, text };
}

// in main(), after parseArgs:
if (options.mode === "flag-alert") {
  const msg = buildFlagAlertMessage(options.payload);
  // reuse the existing sendFailureAlert / nodemailer transport with msg.{subject,html,text}
  // (the existing function probably builds its own message — refactor to accept a pre-built
  // message OR add a new sendMessage helper. Pattern: extract common sendMessage and have
  // both modes call it with their respective builders.)
  return sendMessage(options, msg);
}
```

(Implementation note: in `main()`, after `parseArgs`, switch on `options.mode`. For `flag-alert`, build the message via `buildFlagAlertMessage(options.payload)`, then resolve transport via the existing `resolveAlertConfig(env)` path and call `sendFailureAlert({ transportOptions, message })` — same boundary the failure mode already uses. No new helper extraction.)

- [ ] **Step 5: Run tests**

```bash
node --test scripts/send_daily_update_failure_email.test.mjs
```
Expected: existing tests still pass + new tests pass.

- [ ] **Step 6: Commit**

```bash
git add scripts/send_daily_update_failure_email.mjs scripts/send_daily_update_failure_email.test.mjs
git commit -m "feat(email): add flag-alert mode for quality-flag emails

New --mode flag-alert --payload <json> entry point spawned by
clients/quality_flags.alert_on_flag. Subject and body branded
Livewire; payload schema matches QualityFlag asdict()."
```

---

## Task 12: `send_daily_update_failure_email.mjs` — `--mode daily-summary`

**Files:**
- Modify: `scripts/send_daily_update_failure_email.mjs`
- Test: `scripts/send_daily_update_failure_email.test.mjs`

**Spec reference:** Components → `--mode daily-summary --payload <json>`. The payload is built by `data_quality_report.py --view summary --since 24h --email` (Task 22).

- [ ] **Step 1: Write the failing test**

```javascript
// Append to scripts/send_daily_update_failure_email.test.mjs
test("daily-summary mode renders rollup HTML", async () => {
  const { parseArgs, buildDailySummaryMessage } = await import("./send_daily_update_failure_email.mjs");
  const payload = {
    window: "24h",
    sources: [
      { source: "ib", connection_events: 142, uptime_pct: 97.2, flap_count: 3 },
    ],
    flag_counts_by_category: { range_shortfall: 1, fetch_tainted: 2 },
    top_tickers: [{ ticker: "SMH", flag_count: 2 }],
  };
  const args = parseArgs(["--mode", "daily-summary", "--payload", JSON.stringify(payload)]);
  const msg = buildDailySummaryMessage(args.payload);
  assert.match(msg.subject, /\[Livewire\].*daily summary/i);
  assert.match(msg.html, /97\.2/);
  assert.match(msg.html, /SMH/);
});
```

- [ ] **Step 2: Confirm failure** + **Step 3: Implement `buildDailySummaryMessage`** following the same pattern as Task 11. Wire it into `main()` similarly.

- [ ] **Step 4: Run tests + Step 5: Commit**

```bash
git add scripts/send_daily_update_failure_email.mjs scripts/send_daily_update_failure_email.test.mjs
git commit -m "feat(email): add daily-summary mode for end-of-day rollup

Payload contains per-source uptime, flap count, flag-counts-by-category,
top tickers. Spawned by data_quality_report.py --email."
```

---

## Task 13: `IBClient` — attach `ConnectionTelemetry` in connect/disconnect

**Files:**
- Modify: `clients/ib_client.py`
- Test: `tests/test_ib_client.py`

**Spec reference:** Architecture diagram (IBClient → ConnectionTelemetry), Data flow → step `a` (`ConnectionTelemetry("ib", ib, telemetry.jsonl)`).

**Note:** `clients/ib_client.py` is on the coverage **exclusion list** (per `pyproject.toml:omit`), so 100% coverage of the new hook is not auto-enforced. The repo policy still requires focused tests in `test_ib_client.py` — do that here.

- [ ] **Step 1: Locate disconnect/close method**

```bash
grep -n "^    def \(connect\|disconnect\|close\|__aexit__\|__exit__\)" clients/ib_client.py
```

- [ ] **Step 2: Write the failing tests**

```python
# Append to tests/test_ib_client.py
from unittest.mock import MagicMock, patch
from clients.ib_client import IBClient


def test_connect_attaches_telemetry(monkeypatch, tmp_path):
    monkeypatch.setenv("MDW_TELEMETRY_PATH", str(tmp_path / "t.jsonl"))
    fake_ib = MagicMock()
    fake_ib.connect.return_value = None
    fake_ib.errorEvent = MagicMock()
    fake_ib.connectedEvent = MagicMock()
    fake_ib.disconnectedEvent = MagicMock()
    with patch("clients.ib_client.IB", return_value=fake_ib):
        client = IBClient()
        client.connect(host="127.0.0.1", port=4001, client_id=99)
    fake_ib.errorEvent.connect.assert_called_once()
    fake_ib.connectedEvent.connect.assert_called_once()


def test_disconnect_detaches_telemetry(monkeypatch, tmp_path):
    monkeypatch.setenv("MDW_TELEMETRY_PATH", str(tmp_path / "t.jsonl"))
    fake_ib = MagicMock()
    fake_ib.errorEvent = MagicMock()
    fake_ib.connectedEvent = MagicMock()
    fake_ib.disconnectedEvent = MagicMock()
    with patch("clients.ib_client.IB", return_value=fake_ib):
        client = IBClient()
        client.connect(host="127.0.0.1", port=4001, client_id=99)
        client.disconnect()
    fake_ib.errorEvent.disconnect.assert_called_once()
```

- [ ] **Step 3: Implement the hook**

In `clients/ib_client.py`:

```python
# top of file, near other imports:
from clients.telemetry import ConnectionTelemetry, _resolve_default_path as _resolve_telemetry_path

# inside IBClient.__init__ (or wherever self._ib is constructed), add:
self._telemetry: ConnectionTelemetry | None = None

# at the successful return inside connect() — between
#     self._last_client_id = current_id
# and
#     self.logger.info(...)
#     return
# add:
self._telemetry = ConnectionTelemetry(
    ib=self._ib,
    jsonl_path=_resolve_telemetry_path(),
    source="ib",
)
self._telemetry.start()

# inside disconnect() (or close()), BEFORE self._ib.disconnect():
if self._telemetry is not None:
    self._telemetry.stop()
    self._telemetry = None
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_ib_client.py -v
```

- [ ] **Step 5: Smoke-check coverage gate is unaffected**

```bash
python -m pytest tests/ -q --cov=clients --cov=scripts --cov-report=term-missing 2>&1 | tail -10
```
Expected: still 100%.

- [ ] **Step 6: Commit**

```bash
git add clients/ib_client.py tests/test_ib_client.py
git commit -m "feat(ib_client): attach ConnectionTelemetry on connect/disconnect

IB error/connected/disconnected events now flow into telemetry.jsonl
tagged source=ib. Farm-state codes 2104/2105/2106/2107/2158 become
structured records the data_quality_report CLI can roll up."
```

---

## Task 14: `fetch_ib_historical.py` — quality-detector hook + `--no-quality`

**Files:**
- Modify: `scripts/fetch_ib_historical.py`
- Test: `tests/test_fetch_ib_historical.py`

**Spec reference:** Components → `scripts/fetch_ib_historical.py` integration snippet, Data flow → step `e`.

- [ ] **Step 1: Verified anchor**

**Single integration point:** `backfill_ticker(ticker, bars, bronze, asset_class)` at `scripts/fetch_ib_historical.py:550`. Both `_run_backfill` (line ~795) and `_run_normal` (line ~889) route through `backfill_ticker` after IB returns bars. Adding the hook inside `backfill_ticker` covers both paths with one edit.

Current shape (line 550-572):
```python
def backfill_ticker(ticker: str, bars: list, bronze: BronzeClient, asset_class: str = "equity") -> int:
    if not bars:
        console.print(f"  [yellow]No backfill data for {ticker}[/yellow]")
        return 0
    symbol_id = bronze.get_symbol_id(ticker)
    if asset_class == "futures":
        ...
    rows = bars_to_rows(bars, symbol_id)
    inserted = bronze.merge_ticker_rows(ticker, rows)
    if hasattr(bronze, "write_ticker_parquet"):
        bronze.write_ticker_parquet(ticker, symbol_id, BRONZE_DIR)
    return inserted
```

Hook fires immediately **after** `if not bars: return 0` and **before** `bronze.merge_ticker_rows` — so detection runs on the actual bars before publish, but partial bronze still gets written and flagged.

- [ ] **Step 2: Write the failing tests** — using the existing fixture style (`_make_bar`, `SimpleNamespace`):

```python
# Append to tests/test_fetch_ib_historical.py
from unittest.mock import MagicMock, patch
from datetime import date


class TestQualityHookIntegration:
    def test_quality_hook_invoked_on_success(self, tmp_path):
        """detect_all → write_sidecar + append_audit + alert_on_flag all fire when flags exist."""
        from clients.quality_detector import QualityFlag
        fake_flag = QualityFlag(
            category="range_shortfall", severity="warning",
            detail={"x": 1}, ts="2026-05-17T00:00:00Z",
        )
        bars = [_make_bar(date="2025-01-02")]
        bronze = MagicMock()
        bronze.get_symbol_id.return_value = 42

        with patch("scripts.fetch_ib_historical.detect_all", return_value=[fake_flag]) as m_detect, \
             patch("scripts.fetch_ib_historical.write_sidecar", return_value=True) as m_sidecar, \
             patch("scripts.fetch_ib_historical.append_audit", return_value=True) as m_audit, \
             patch("scripts.fetch_ib_historical.alert_on_flag", return_value=True) as m_alert:
            inserted = backfill_ticker("AAPL", bars, bronze, asset_class="equity")
        assert m_detect.call_count == 1
        assert m_sidecar.call_count == 1
        assert m_audit.call_count == 1
        assert m_alert.call_count == 1

    def test_clean_fetch_produces_no_sidecar(self, tmp_path):
        """detect_all returns [] → no emit calls."""
        bars = [_make_bar(date="2025-01-02")]
        bronze = MagicMock()
        bronze.get_symbol_id.return_value = 42

        with patch("scripts.fetch_ib_historical.detect_all", return_value=[]) as m_detect, \
             patch("scripts.fetch_ib_historical.write_sidecar") as m_sidecar, \
             patch("scripts.fetch_ib_historical.append_audit") as m_audit, \
             patch("scripts.fetch_ib_historical.alert_on_flag") as m_alert:
            backfill_ticker("AAPL", bars, bronze, asset_class="equity")
        m_detect.assert_called_once()
        m_sidecar.assert_not_called()
        m_audit.assert_not_called()
        m_alert.assert_not_called()

    def test_empty_bars_skip_detection_entirely(self):
        """Existing 'if not bars: return 0' path is unchanged — detector not called."""
        bronze = MagicMock()
        with patch("scripts.fetch_ib_historical.detect_all") as m_detect:
            inserted = backfill_ticker("AAPL", [], bronze)
        assert inserted == 0
        m_detect.assert_not_called()

    def test_no_quality_flag_disables_hook(self, monkeypatch):
        """When MDW_NO_QUALITY=1 is set (or via module flag), detect_all is skipped."""
        monkeypatch.setattr("scripts.fetch_ib_historical._QUALITY_ENABLED", False)
        bars = [_make_bar(date="2025-01-02")]
        bronze = MagicMock()
        bronze.get_symbol_id.return_value = 42
        with patch("scripts.fetch_ib_historical.detect_all") as m_detect:
            backfill_ticker("AAPL", bars, bronze)
        m_detect.assert_not_called()
```

- [ ] **Step 3: Confirm failure.**

- [ ] **Step 4: Implement** — add a `_run_quality_detection` helper near the top of `scripts/fetch_ib_historical.py`:

```python
# scripts/fetch_ib_historical.py — add near other helpers, after BronzeClient import
from clients.quality_detector import detect_all
from clients.quality_flags import write_sidecar, append_audit, alert_on_flag

# Module-level flag toggled by --no-quality. Default ON.
_QUALITY_ENABLED = True


def _run_quality_detection(
    *,
    ticker: str,
    timeframe: str,
    asset_class: str,
    bars: list,
    parquet_path: "Path",
    expected_start: "date | None" = None,
    ib_head_timestamp: "date | None" = None,
    source: str = "ib",
) -> None:
    """Run detect_all and emit sidecar/audit/alert if any flags fire.

    Best-effort: any internal failure logs a warning but does not raise.
    """
    if not _QUALITY_ENABLED or not bars:
        return
    normalized = _normalize_bars_for_detection(bars)
    metadata = {
        "asset_class": asset_class,
        "ticker": ticker,
        "timeframe": timeframe,
        "source": source,
        "bars_received": len(bars),
        # Sub-A scope: errors_during_fetch left empty for daily-bar paths.
        # backfill_intraday.py wires it from outcome.errors. Full collection
        # for daily-bar paths is a follow-up task.
        "errors_during_fetch": [],
        "expected_start": expected_start,
        "ib_head_timestamp": ib_head_timestamp,
    }
    try:
        flags = detect_all(bars=normalized, metadata=metadata, trading_calendar=None)
    except Exception as exc:    # pragma: no cover - detect_all itself wraps detectors
        console.print(f"  [yellow]quality detection raised: {exc}[/yellow]")
        return
    if not flags:
        return
    write_sidecar(parquet_path, flags, metadata)
    for f in flags:
        append_audit(f, source=source, ticker=ticker, timeframe=timeframe, parquet_path=parquet_path)
        alert_on_flag(f, source=source, ticker=ticker)
```

Import `_normalize_bars_for_detection` from `clients.quality_detector` alongside `detect_all`.

**Wiring `expected_start` and `ib_head_timestamp`:** In `backfill_ticker`, derive these from the surrounding context. For `--backfill` mode, the operator is asking IB for everything older than the current bronze head — so `expected_start = IB_EARLIEST_DATE` is the right canonical value (already imported at line ~84 of `fetch_ib_historical.py`). `ib_head_timestamp` should be passed by the caller (`_run_backfill` already calls `ib.get_head_timestamp_async(contract)` at line 401 — surface that result through to `backfill_ticker`). For Sub-A:

- If wiring `ib_head_timestamp` end-to-end is too invasive, set `ib_head_timestamp=None` and let `expected_start` alone drive the heuristic. The detector handles None gracefully (treats as "unknown, compare actual vs expected only").
- **Minimum viable: wire `expected_start=IB_EARLIEST_DATE` in `backfill_ticker`. That alone makes the SMH case fire as a `warning` flag.** With `ib_head_timestamp` also wired through, SMH escalates to `critical`. The latter is preferred but not blocking.

In the argparse setup of `main()`, add:

```python
parser.add_argument("--no-quality", action="store_true",
                    help="Disable the post-fetch quality detection hook (debug only)")
```

In `main()`, set the module flag after `args = parser.parse_args(...)`:

```python
global _QUALITY_ENABLED
_QUALITY_ENABLED = not args.no_quality
```

Modify `backfill_ticker` at `scripts/fetch_ib_historical.py:550` — insert the hook between `if not bars: return 0` and `symbol_id = bronze.get_symbol_id(ticker)`:

```python
def backfill_ticker(ticker: str, bars: list, bronze: BronzeClient, asset_class: str = "equity") -> int:
    if not bars:
        console.print(f"  [yellow]No backfill data for {ticker}[/yellow]")
        return 0

    # NEW: quality detection before merge
    parquet_path = BRONZE_DIR / f"asset_class={asset_class}" / f"symbol={ticker}" / "1d.parquet"
    _run_quality_detection(
        ticker=ticker, timeframe="1d", asset_class=asset_class,
        bars=bars, parquet_path=parquet_path,
        # expected_start = IB earliest fires range_shortfall for SMH-case
        # (operator asked for inception data, IB returned a partial range).
        expected_start=IB_EARLIEST_DATE,
        ib_head_timestamp=None,    # follow-up: thread the get_head_timestamp result through
    )

    symbol_id = bronze.get_symbol_id(ticker)
    # ... existing body unchanged ...
```

**Why a single-site edit covers both paths:** Both `_run_backfill` (called by `--backfill` mode) and `_run_normal` (default mode) route their bars through `backfill_ticker(ticker, bars, bronze, asset_class)` after IB returns. The single-site edit is correct.

**Follow-up tracked (NOT in this plan):** Daily-bar IB fetches do not collect per-ticker IB error codes today. `errors_during_fetch` is therefore `[]` for the daily-bar paths in Sub-A, which means `detect_fetch_tainting` cannot fire from this surface. `backfill_intraday.py` already collects them and uses them in T16. Adding error collection for daily-bar fetches is a small follow-up — install a second `errorEvent` handler alongside `ConnectionTelemetry` that buckets errors by `reqId` and joins with the per-ticker `reqHistoricalData` call. Document this gap in the T25 docs sweep and leave for a future iteration.

- [ ] **Step 5: Run tests + the rest of the test_fetch_ib_historical suite** (don't regress)

```bash
python -m pytest tests/test_fetch_ib_historical.py -v
python -m pytest tests/test_fetch_ib_historical.py --cov=scripts.fetch_ib_historical --cov-report=term-missing
```

- [ ] **Step 6: Commit**

```bash
git add scripts/fetch_ib_historical.py tests/test_fetch_ib_historical.py
git commit -m "feat(fetch): add quality-detector hook with --no-quality opt-out

After fetch returns bars, before atomic publish: detect_all → write_sidecar
+ append_audit + alert_on_flag. Partial bronze still writes (with sidecar
flagging it) so SMH-case is queryable, not invisible."
```

---

## Task 15: `daily_update.py` — quality-detector hook

**Files:**
- Modify: `scripts/daily_update.py`
- Test: `tests/test_daily_update.py`

**Spec reference:** Components → "Same integration in `scripts/daily_update.py`".

- [ ] **Step 1: Verified anchor**

`scripts/daily_update.py:869` — `inserted = bronze.merge_ticker_rows(ticker, rows)`. The new hook fires immediately **before** this line, on `valid_bars` (which is the live `list[BarData]` already past validation at this point). `expected_start_date = latest + 1 trading day` is available from `latest = date.fromisoformat(latest_dates[ticker])` upstream in the loop.

The integration sits inside the per-ticker block under `with IBClient() as ib, _fallback_client() as fallback:` — verify by reading lines 800-880.

- [ ] **Step 2: Write the failing test**

```python
# Append to tests/test_daily_update.py, alongside the existing test classes
class TestQualityHookIntegration:
    def test_quality_hook_fires_before_merge(self, tmp_path):
        """The new helper is invoked with valid_bars before merge_ticker_rows."""
        # We test the helper in isolation (the per-ticker block is large and
        # already integration-tested by the existing tests). The helper itself
        # is what we add — and its contract is: given non-empty bars, call
        # detect_all; emit sidecar/audit/alert iff flags exist.
        from clients.quality_detector import QualityFlag
        from scripts.daily_update import _run_quality_detection

        bars = [_make_bar(date="2026-05-15")]
        parquet_path = tmp_path / "x.parquet"
        parquet_path.write_bytes(b"")
        fake_flag = QualityFlag(category="fetch_tainted", severity="warning", detail={}, ts="2026-05-17T00:00:00Z")

        with patch("scripts.daily_update.detect_all", return_value=[fake_flag]) as m_detect, \
             patch("scripts.daily_update.write_sidecar", return_value=True) as m_sidecar, \
             patch("scripts.daily_update.append_audit", return_value=True) as m_audit, \
             patch("scripts.daily_update.alert_on_flag", return_value=True) as m_alert:
            _run_quality_detection(
                ticker="AAPL", asset_class="equity",
                bars=bars, parquet_path=parquet_path,
                expected_start=date(2026, 5, 14),
            )
        assert m_detect.called
        assert m_sidecar.call_count == 1
        assert m_audit.call_count == 1
        assert m_alert.call_count == 1

    def test_quality_hook_skips_when_no_bars(self, tmp_path):
        from scripts.daily_update import _run_quality_detection
        with patch("scripts.daily_update.detect_all") as m_detect:
            _run_quality_detection(
                ticker="AAPL", asset_class="equity",
                bars=[], parquet_path=tmp_path / "x.parquet",
                expected_start=date(2026, 5, 14),
            )
        m_detect.assert_not_called()
```

- [ ] **Step 3: Implement** — daily_update has its own merge path. Add the helper near the other module-level helpers (around the same place `bars_to_rows` lives):

```python
# scripts/daily_update.py — alongside other clients imports
from clients.quality_detector import detect_all
from clients.quality_flags import write_sidecar, append_audit, alert_on_flag


def _run_quality_detection(
    *,
    ticker: str,
    asset_class: str,
    bars: list,
    parquet_path: Path,
    expected_start: date | None = None,
    source: str = "ib",
) -> None:
    if not bars:
        return
    metadata = {
        "asset_class": asset_class,
        "ticker": ticker,
        "timeframe": "1d",
        "source": source,
        "bars_received": len(bars),
        "expected_start": expected_start,
        "ib_head_timestamp": None,
        "errors_during_fetch": [],
    }
    try:
        flags = detect_all(bars=bars, metadata=metadata, trading_calendar=None)
    except Exception:    # pragma: no cover
        return
    if not flags:
        return
    write_sidecar(parquet_path, flags, metadata)
    for f in flags:
        append_audit(f, source=source, ticker=ticker, timeframe="1d", parquet_path=parquet_path)
        alert_on_flag(f, source=source, ticker=ticker)
```

At line 868, just before `inserted = bronze.merge_ticker_rows(ticker, rows)`:

```python
parquet_path = bronze_dir / f"symbol={ticker}" / "1d.parquet"
_run_quality_detection(
    ticker=ticker,
    asset_class=asset_class,
    bars=valid_bars,
    parquet_path=parquet_path,
    expected_start=latest + timedelta(days=1) if latest else None,
)
inserted = bronze.merge_ticker_rows(ticker, rows)    # existing line, unchanged
```

- [ ] **Step 4: Run tests + Step 5: Commit**

```bash
git add scripts/daily_update.py tests/test_daily_update.py
git commit -m "feat(daily): add quality-detector hook to scheduled sync

Per-ticker daily merge now emits sidecar + audit + alert on any
detected flag. Existing fallback recovery path is unaffected."
```

---

## Task 16: `backfill_intraday.py` — quality-detector hook

**Files:**
- Modify: `scripts/backfill_intraday.py`
- Test: `tests/test_backfill_intraday.py`

**Spec reference:** Components → "Same integration in `scripts/backfill_intraday.py`".

- [ ] **Step 1: Verified anchor**

**This is the cleanest integration of the three.** `backfill_ticker` (line 149) already builds a `TickerOutcome` with `outcome.errors: list[str]` populated during the chunk loop (line 175). The merge call is at line 199: `outcome.bars_inserted = bronze.merge_ticker_rows(ticker, all_rows)`. The hook fires **between** `if all_rows:` and `bronze.merge_ticker_rows`, using `outcome.errors` directly as the `errors_during_fetch` feed.

- [ ] **Step 2: Write the failing test**

```python
# Append to tests/test_backfill_intraday.py
class TestQualityHookIntegration:
    def test_quality_hook_fires_with_outcome_errors(self, tmp_path):
        from clients.quality_detector import QualityFlag
        from scripts.backfill_intraday import _run_quality_detection

        bars = [_make_ib_bar(datetime(2026, 4, 6, 9, 30))]
        outcome = TickerOutcome(ticker="AAPL", errors=["2026-04-06: error 162"])
        parquet_path = tmp_path / "5m.parquet"
        parquet_path.write_bytes(b"")
        fake_flag = QualityFlag(category="fetch_tainted", severity="critical", detail={}, ts="2026-05-17T00:00:00Z")

        with patch("scripts.backfill_intraday.detect_all", return_value=[fake_flag]) as m_detect, \
             patch("scripts.backfill_intraday.write_sidecar", return_value=True) as m_sidecar, \
             patch("scripts.backfill_intraday.append_audit", return_value=True) as m_audit, \
             patch("scripts.backfill_intraday.alert_on_flag", return_value=True) as m_alert:
            _run_quality_detection(
                ticker="AAPL", timeframe="5m",
                bars=bars, parquet_path=parquet_path,
                outcome=outcome,
            )
        # detect_all should have been called with errors_during_fetch populated
        kwargs = m_detect.call_args.kwargs
        assert kwargs["metadata"]["errors_during_fetch"]
        assert m_sidecar.called

    def test_quality_hook_skips_empty_bars(self, tmp_path):
        from scripts.backfill_intraday import _run_quality_detection
        outcome = TickerOutcome(ticker="AAPL")
        with patch("scripts.backfill_intraday.detect_all") as m_detect:
            _run_quality_detection(
                ticker="AAPL", timeframe="5m", bars=[],
                parquet_path=tmp_path / "x.parquet", outcome=outcome,
            )
        m_detect.assert_not_called()
```

- [ ] **Step 3: Implement** — add the helper and the inline call:

```python
# scripts/backfill_intraday.py — near other helpers
from clients.quality_detector import detect_all
from clients.quality_flags import write_sidecar, append_audit, alert_on_flag


def _run_quality_detection(
    *,
    ticker: str,
    timeframe: str,
    bars: list,
    parquet_path: Path,
    outcome: TickerOutcome,
    source: str = "ib",
) -> None:
    if not bars:
        return
    # Feed outcome.errors as errors_during_fetch (already collected during chunk loop)
    errors = [{"code": 0, "count": 1, "message": e} for e in (outcome.errors or [])]
    metadata = {
        "asset_class": "equity",
        "ticker": ticker,
        "timeframe": timeframe,
        "source": source,
        "bars_received": len(bars),
        "errors_during_fetch": errors,
        "expected_start": None,
        "ib_head_timestamp": None,
    }
    try:
        flags = detect_all(bars=bars, metadata=metadata, trading_calendar=None)
    except Exception:    # pragma: no cover
        return
    if not flags:
        return
    write_sidecar(parquet_path, flags, metadata)
    for f in flags:
        append_audit(f, source=source, ticker=ticker, timeframe=timeframe, parquet_path=parquet_path)
        alert_on_flag(f, source=source, ticker=ticker)
```

In `backfill_ticker`, just before `bronze.merge_ticker_rows` (line 199):

```python
if all_rows:
    # NEW: quality detection
    parquet_path = bronze.bronze_dir / f"asset_class=equity" / f"symbol={ticker}" / f"{timeframe}.parquet"
    _run_quality_detection(
        ticker=ticker, timeframe=timeframe,
        bars=all_rows, parquet_path=parquet_path, outcome=outcome,
    )
    outcome.bars_inserted = bronze.merge_ticker_rows(ticker, all_rows)
```

Commit:

```bash
git add scripts/backfill_intraday.py tests/test_backfill_intraday.py
git commit -m "feat(intraday): add quality-detector hook to intraday backfill

1h and 5m bronze gets the same sidecar+audit+alert treatment when a
fetch comes back with gaps or fetch errors."
```

---

## Task 17: `run_ib_fetch_robust.py` — orchestrator skeleton (CLI + ticker iteration)

**Files:**
- Create: `scripts/run_ib_fetch_robust.py`
- Test: `tests/test_run_ib_fetch_robust.py`

**Spec reference:** Components → `scripts/run_ib_fetch_robust.py`, Data flow → orchestrator outer loop.

- [ ] **Step 1: Write the failing tests for CLI parse + ticker source**

```python
# tests/test_run_ib_fetch_robust.py
import json
from pathlib import Path
import pytest
from unittest.mock import MagicMock, patch

from scripts.run_ib_fetch_robust import parse_args, load_tickers, _bronze_path_for, _is_already_done


def test_parse_args_defaults():
    args = parse_args(["--preset", "presets/sp500.json", "--mode", "seed"])
    assert args.timeout == 300
    assert args.max_attempts == 3
    assert args.cooldown == 60
    assert args.asset_class == "equity"
    assert args.mode == "seed"


def test_parse_args_env_overrides(monkeypatch):
    monkeypatch.setenv("MDW_ORCHESTRATOR_TIMEOUT_SECONDS", "120")
    monkeypatch.setenv("MDW_ORCHESTRATOR_MAX_ATTEMPTS", "5")
    monkeypatch.setenv("MDW_ORCHESTRATOR_COOLDOWN_SECONDS", "30")
    args = parse_args(["--preset", "presets/sp500.json", "--mode", "seed"])
    assert args.timeout == 120
    assert args.max_attempts == 5
    assert args.cooldown == 30


def test_load_tickers_from_preset(tmp_path):
    preset = tmp_path / "p.json"
    preset.write_text(json.dumps({"name": "test", "tickers": ["AAPL", "MSFT"]}))
    assert load_tickers(preset_path=preset, explicit=None) == ["AAPL", "MSFT"]


def test_load_tickers_explicit_wins(tmp_path):
    assert load_tickers(preset_path=None, explicit=["HOOD"]) == ["HOOD"]


def test_is_already_done_seed(tmp_path):
    p = tmp_path / "asset_class=equity" / "symbol=AAPL" / "1d.parquet"
    p.parent.mkdir(parents=True)
    p.write_bytes(b"x")
    assert _is_already_done(p, mode="seed") is True
    assert _is_already_done(p, mode="backfill") is False


def test_is_already_done_backfill_missing(tmp_path):
    p = tmp_path / "asset_class=equity" / "symbol=GHOST" / "1d.parquet"
    assert _is_already_done(p, mode="seed") is False
    assert _is_already_done(p, mode="backfill") is True   # backfill skip if missing
```

- [ ] **Step 2: Confirm failure** — `ModuleNotFoundError`.

- [ ] **Step 3: Implement the skeleton**

```python
#!/usr/bin/env python
# scripts/run_ib_fetch_robust.py
"""Productized orchestrator: per-ticker process isolation for IB fetches.

Replaces /tmp/orchestrate_ib_fetch.sh. Spawns one subprocess per ticker,
enforces hard timeout + retry budget + cooldown. Recognizes ok-noop for
backfill mode (no older history available, exit 0 = success).

See: docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BRONZE_DIR = Path.home() / "market-warehouse" / "data-lake" / "bronze"
DEFAULT_LOG_DIR = Path.home() / "market-warehouse" / "logs"

_logger = logging.getLogger("mdw.orchestrator")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Robust per-ticker IB fetch orchestrator")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--preset", type=Path, help="Preset JSON file with .tickers array")
    src.add_argument("--tickers", nargs="+", help="Explicit ticker list")
    p.add_argument("--mode", choices=["seed", "backfill"], required=True)
    p.add_argument("--asset-class", default="equity", choices=["equity", "volatility", "futures", "cmdty", "fx"])
    p.add_argument("--timeout", type=int, default=_env_int("MDW_ORCHESTRATOR_TIMEOUT_SECONDS", 300))
    p.add_argument("--max-attempts", type=int, default=_env_int("MDW_ORCHESTRATOR_MAX_ATTEMPTS", 3))
    p.add_argument("--cooldown", type=int, default=_env_int("MDW_ORCHESTRATOR_COOLDOWN_SECONDS", 60))
    p.add_argument("--bronze-dir", type=Path, default=DEFAULT_BRONZE_DIR)
    p.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    return p.parse_args(argv)


def load_tickers(*, preset_path: Optional[Path], explicit: Optional[list[str]]) -> list[str]:
    if explicit:
        return list(explicit)
    if preset_path is None:
        return []
    payload = json.loads(preset_path.read_text())
    return list(payload.get("tickers") or [])


def _bronze_path_for(bronze_dir: Path, asset_class: str, ticker: str, timeframe: str = "1d") -> Path:
    return bronze_dir / f"asset_class={asset_class}" / f"symbol={ticker}" / f"{timeframe}.parquet"


def _is_already_done(parquet_path: Path, mode: str) -> bool:
    """seed: skip if exists. backfill: skip if missing (no prior bronze to extend)."""
    if mode == "seed":
        return parquet_path.exists()
    if mode == "backfill":
        return not parquet_path.exists()
    return False    # pragma: no cover - argparse choices prevent other values
```

- [ ] **Step 4: Run + cover** — `python -m pytest tests/test_run_ib_fetch_robust.py -v --cov=scripts.run_ib_fetch_robust --cov-report=term-missing` → 100% on lines added.

- [ ] **Step 5: Commit**

```bash
git add scripts/run_ib_fetch_robust.py tests/test_run_ib_fetch_robust.py
git commit -m "feat(orchestrator): add run_ib_fetch_robust skeleton

CLI parse with env-var overrides, preset/explicit ticker source,
seed/backfill skip predicates. Subprocess spawn and timeout handling
come in next task."
```

---

## Task 18: Orchestrator — subprocess timeout + SIGKILL + retry budget

**Files:**
- Modify: `scripts/run_ib_fetch_robust.py`
- Test: `tests/test_run_ib_fetch_robust.py`

**Spec reference:** Outcome categories table, Error handling matrix (`subprocess hangs past --timeout`, `subprocess exits non-zero`).

- [ ] **Step 1: Write the failing tests**

```python
# Append to tests/test_run_ib_fetch_robust.py
from scripts.run_ib_fetch_robust import run_one_ticker, OutcomeCategory


def test_success_first_attempt(tmp_path, monkeypatch):
    parquet = tmp_path / "asset_class=equity" / "symbol=AAPL" / "1d.parquet"
    parquet.parent.mkdir(parents=True)

    def fake_run(*a, **kw):
        # Simulate child writing the parquet
        parquet.write_bytes(b"data")
        return MagicMock(returncode=0)
    monkeypatch.setattr("subprocess.run", fake_run)
    outcome = run_one_ticker(
        ticker="AAPL", mode="seed", asset_class="equity",
        bronze_dir=tmp_path, timeout=10, max_attempts=3, cooldown=0,
    )
    assert outcome.code == OutcomeCategory.OK
    assert outcome.attempts_used == 1


def test_timeout_then_success(tmp_path, monkeypatch):
    parquet = tmp_path / "asset_class=equity" / "symbol=MSFT" / "1d.parquet"
    parquet.parent.mkdir(parents=True)
    calls = [0]

    def fake_run(*a, **kw):
        calls[0] += 1
        if calls[0] == 1:
            raise subprocess.TimeoutExpired(cmd=a[0], timeout=kw.get("timeout", 10))
        parquet.write_bytes(b"data")
        return MagicMock(returncode=0)
    monkeypatch.setattr("subprocess.run", fake_run)
    outcome = run_one_ticker(
        ticker="MSFT", mode="seed", asset_class="equity",
        bronze_dir=tmp_path, timeout=10, max_attempts=3, cooldown=0,
    )
    assert outcome.code == OutcomeCategory.OK
    assert outcome.attempts_used == 2


def test_all_attempts_timeout_is_fail(tmp_path, monkeypatch):
    def fake_run(*a, **kw):
        raise subprocess.TimeoutExpired(cmd=a[0], timeout=kw.get("timeout", 10))
    monkeypatch.setattr("subprocess.run", fake_run)
    outcome = run_one_ticker(
        ticker="HOOD", mode="seed", asset_class="equity",
        bronze_dir=tmp_path, timeout=1, max_attempts=2, cooldown=0,
    )
    assert outcome.code == OutcomeCategory.TIMEOUT
    assert outcome.attempts_used == 2


def test_non_zero_exit_retried(tmp_path, monkeypatch):
    parquet = tmp_path / "asset_class=equity" / "symbol=X" / "1d.parquet"
    parquet.parent.mkdir(parents=True)
    calls = [0]

    def fake_run(*a, **kw):
        calls[0] += 1
        if calls[0] < 3:
            return MagicMock(returncode=1)
        parquet.write_bytes(b"data")
        return MagicMock(returncode=0)
    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setattr("time.sleep", lambda s: None)    # no real cooldown
    outcome = run_one_ticker(
        ticker="X", mode="seed", asset_class="equity",
        bronze_dir=tmp_path, timeout=10, max_attempts=3, cooldown=0,
    )
    assert outcome.code == OutcomeCategory.OK


def test_cooldown_sleeps_between_attempts(tmp_path, monkeypatch):
    sleeps = []

    def fake_sleep(s):
        sleeps.append(s)
    monkeypatch.setattr("time.sleep", fake_sleep)

    def fake_run(*a, **kw):
        return MagicMock(returncode=1)
    monkeypatch.setattr("subprocess.run", fake_run)
    run_one_ticker(
        ticker="X", mode="seed", asset_class="equity",
        bronze_dir=tmp_path, timeout=10, max_attempts=3, cooldown=30,
    )
    assert sleeps == [30, 30]    # 2 cooldowns between 3 attempts
```

- [ ] **Step 2: Confirm failure** — `ImportError`.

- [ ] **Step 3: Implement `run_one_ticker`**

```python
# Append to scripts/run_ib_fetch_robust.py
from dataclasses import dataclass
from enum import Enum


class OutcomeCategory(str, Enum):
    OK = "ok"
    OK_NOOP = "ok-noop"
    SKIP = "skip"
    FAIL = "fail"
    TIMEOUT = "timeout"


@dataclass
class TickerOutcome:
    ticker: str
    code: OutcomeCategory
    attempts_used: int
    elapsed_seconds: float
    rows_before: int
    rows_after: int
    note: str = ""


def _build_worker_cmd(ticker: str, mode: str, asset_class: str) -> list[str]:
    """Construct the subprocess args for fetch_ib_historical."""
    cmd = [
        sys.executable, str(REPO_ROOT / "scripts" / "fetch_ib_historical.py"),
        "--tickers", ticker,
        "--asset-class", asset_class,
        "--batch-size", "1",
        "--max-concurrent", "1",
    ]
    if mode == "seed":
        cmd += ["--years", "0"]
    elif mode == "backfill":
        cmd += ["--backfill"]
    return cmd


def _count_rows(parquet_path: Path) -> int:
    if not parquet_path.exists():
        return 0
    try:
        import duckdb
        return duckdb.connect().execute(
            f"select count(*) from read_parquet('{parquet_path}')"
        ).fetchone()[0]
    except Exception as exc:    # pragma: no cover - duckdb missing handled by venv
        _logger.warning("row count failed for %s: %s", parquet_path, exc)
        return 0


def run_one_ticker(
    *,
    ticker: str,
    mode: str,
    asset_class: str,
    bronze_dir: Path,
    timeout: int,
    max_attempts: int,
    cooldown: int,
) -> TickerOutcome:
    parquet = _bronze_path_for(bronze_dir, asset_class, ticker)
    if _is_already_done(parquet, mode):
        return TickerOutcome(ticker, OutcomeCategory.SKIP, 0, 0.0, 0, 0)

    cmd = _build_worker_cmd(ticker, mode, asset_class)
    rows_before = _count_rows(parquet)
    start = time.monotonic()

    attempts = 0
    last_was_timeout = False
    while attempts < max_attempts:
        attempts += 1
        try:
            result = subprocess.run(cmd, timeout=timeout, capture_output=True)
            last_was_timeout = False
        except subprocess.TimeoutExpired:
            last_was_timeout = True
            _logger.warning("[%s] attempt %d timeout after %ss", ticker, attempts, timeout)
            if attempts < max_attempts:
                time.sleep(cooldown)
            continue

        if result.returncode == 0:
            rows_after = _count_rows(parquet)
            elapsed = time.monotonic() - start
            if parquet.exists() and rows_after > rows_before:
                return TickerOutcome(
                    ticker, OutcomeCategory.OK, attempts, elapsed, rows_before, rows_after,
                    note=f"rows +{rows_after - rows_before}",
                )
            # backfill ok-noop handled in Task 19
            return TickerOutcome(
                ticker, OutcomeCategory.OK, attempts, elapsed, rows_before, rows_after,
            )
        _logger.warning("[%s] attempt %d exit=%d", ticker, attempts, result.returncode)
        if attempts < max_attempts:
            time.sleep(cooldown)

    elapsed = time.monotonic() - start
    code = OutcomeCategory.TIMEOUT if last_was_timeout else OutcomeCategory.FAIL
    return TickerOutcome(ticker, code, attempts, elapsed, rows_before, rows_before)
```

- [ ] **Step 4: Run + cover.**

- [ ] **Step 5: Commit**

```bash
git add scripts/run_ib_fetch_robust.py tests/test_run_ib_fetch_robust.py
git commit -m "feat(orchestrator): add per-ticker subprocess timeout + retry

Hard timeout via subprocess.run(timeout=...) — child SIGKILLed on overrun
(subprocess.run handles SIGKILL internally on Timeout). Retry budget
honored, cooldown sleeps between attempts."
```

---

## Task 19: Orchestrator — ok-noop, final summary, main entry point

**Files:**
- Modify: `scripts/run_ib_fetch_robust.py`
- Test: `tests/test_run_ib_fetch_robust.py`

**Spec reference:** Outcome `[ok-noop]`, final summary line format, Error handling matrix (`backfill exits 0, no rows added`, `parent crashes / SIGTERM`).

- [ ] **Step 1: Write the failing tests**

```python
def test_backfill_ok_noop_when_no_rows_added(tmp_path, monkeypatch):
    parquet = tmp_path / "asset_class=equity" / "symbol=COIN" / "1d.parquet"
    parquet.parent.mkdir(parents=True)
    parquet.write_bytes(b"existing")

    def fake_run(*a, **kw):
        # Backfill ran clean but no older history → exit 0 + no row delta
        return MagicMock(returncode=0)
    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setattr("scripts.run_ib_fetch_robust._count_rows", lambda p: 100)
    outcome = run_one_ticker(
        ticker="COIN", mode="backfill", asset_class="equity",
        bronze_dir=tmp_path, timeout=10, max_attempts=3, cooldown=0,
    )
    assert outcome.code == OutcomeCategory.OK_NOOP
    assert outcome.attempts_used == 1


def test_summary_line_format():
    from scripts.run_ib_fetch_robust import format_summary
    outcomes = [
        TickerOutcome("AAPL", OutcomeCategory.OK, 1, 12.0, 0, 6000),
        TickerOutcome("HOOD", OutcomeCategory.FAIL, 3, 900.0, 0, 0),
        TickerOutcome("COIN", OutcomeCategory.OK_NOOP, 1, 8.0, 100, 100),
    ]
    line = format_summary(outcomes, mode="seed", elapsed_minutes=15)
    assert "ok=1" in line
    assert "ok-noop=1" in line
    assert "fail=1" in line
    assert "elapsed=15m" in line


```

**Note:** the spec calls out an `atexit` handler that `pkill -P`s in-flight children if the parent crashes. With `subprocess.run(timeout=...)`, the parent never has a long-lived child handle outside the `subprocess.run` call — Python's subprocess module already SIGKILLs the child on TimeoutExpired, and a parent SIGTERM/SIGKILL propagates SIGHUP to the in-flight child via the kernel. **We intentionally do NOT add a bespoke orphan-cleanup atexit hook in Sub-A** — it would have nothing to clean. If the threat model later expands to long-lived parallel workers, revisit.

- [ ] **Step 2: Confirm failure.**

- [ ] **Step 3: Implement ok-noop, summary, and cleanup**

```python
# Modify run_one_ticker — replace the "parquet.exists() and rows_after > rows_before" block:
if result.returncode == 0:
    rows_after = _count_rows(parquet)
    elapsed = time.monotonic() - start
    if mode == "backfill" and parquet.exists() and rows_after == rows_before:
        return TickerOutcome(
            ticker, OutcomeCategory.OK_NOOP, attempts, elapsed, rows_before, rows_after,
            note="no older history",
        )
    if parquet.exists() and (rows_after > rows_before or mode == "seed"):
        return TickerOutcome(
            ticker, OutcomeCategory.OK, attempts, elapsed, rows_before, rows_after,
            note=f"rows +{rows_after - rows_before}",
        )
    # subprocess exited 0 but produced no bronze → treat as fail
    return TickerOutcome(
        ticker, OutcomeCategory.FAIL, attempts, elapsed, rows_before, rows_after,
        note="exit 0 but no bronze written",
    )


# Append:
def format_summary(outcomes: list[TickerOutcome], *, mode: str, elapsed_minutes: int) -> str:
    counts = {c: 0 for c in OutcomeCategory}
    for o in outcomes:
        counts[o.code] += 1
    return (
        f"=== orch done mode={mode} "
        f"ok={counts[OutcomeCategory.OK]} "
        f"ok-noop={counts[OutcomeCategory.OK_NOOP]} "
        f"skip={counts[OutcomeCategory.SKIP]} "
        f"fail={counts[OutcomeCategory.FAIL]} "
        f"timeout={counts[OutcomeCategory.TIMEOUT]} "
        f"elapsed={elapsed_minutes}m ==="
    )


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    tickers = load_tickers(preset_path=args.preset, explicit=args.tickers)
    if not tickers:
        _logger.error("no tickers to process")
        return 2

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    log_dir = args.log_dir / f"orch_{args.mode}_{stamp}"
    log_dir.mkdir(parents=True, exist_ok=True)
    summary_log = log_dir / "_summary.log"

    overall_start = time.monotonic()
    outcomes: list[TickerOutcome] = []
    for i, ticker in enumerate(tickers, 1):
        outcome = run_one_ticker(
            ticker=ticker,
            mode=args.mode,
            asset_class=args.asset_class,
            bronze_dir=args.bronze_dir,
            timeout=args.timeout,
            max_attempts=args.max_attempts,
            cooldown=args.cooldown,
        )
        outcomes.append(outcome)
        line = f"[{i}/{len(tickers)} {outcome.code.value}] {ticker} attempts={outcome.attempts_used} dt={outcome.elapsed_seconds:.0f}s {outcome.note}"
        print(line, flush=True)
        with summary_log.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    elapsed_min = int((time.monotonic() - overall_start) / 60)
    summary = format_summary(outcomes, mode=args.mode, elapsed_minutes=elapsed_min)
    print(summary, flush=True)
    with summary_log.open("a", encoding="utf-8") as fh:
        fh.write(summary + "\n")
    return 0 if all(o.code != OutcomeCategory.FAIL for o in outcomes) else 1


if __name__ == "__main__":    # pragma: no cover
    sys.exit(main())
```

- [ ] **Step 4: Run + cover.**

- [ ] **Step 5: Make it executable + smoke test (no live IB)**

```bash
chmod +x scripts/run_ib_fetch_robust.py
python scripts/run_ib_fetch_robust.py --tickers FAKE --mode seed --timeout 5 --max-attempts 1 --cooldown 0 2>&1 | tail -5
```
Expected: a `[1/1 fail]` line + final summary. No crash.

- [ ] **Step 6: Commit**

```bash
git add scripts/run_ib_fetch_robust.py tests/test_run_ib_fetch_robust.py
git commit -m "feat(orchestrator): add ok-noop, summary line, main entry point

Backfill exit-0-with-zero-rows is recognized as 'no older history' and
counted as success. Final summary line matches spec format. main()
iterates tickers, dispatches to run_one_ticker, writes per-ticker
lines to orch_runs/<stamp>/_summary.log."
```

---

## Task 20: `data_quality_report.py` — `--view summary`

**Files:**
- Create: `scripts/data_quality_report.py`
- Test: `tests/test_data_quality_report.py`

**Spec reference:** Components → `scripts/data_quality_report.py`, definitions (drop / flap / MTBD / Uptime %).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_data_quality_report.py
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.data_quality_report import (
    parse_args,
    load_telemetry,
    load_audit,
    compute_summary,
    render_summary_text,
)


def _utc(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def test_load_telemetry_skips_malformed_lines(tmp_path):
    f = tmp_path / "t.jsonl"
    f.write_text(
        '{"ts":"2026-05-17T00:00:00Z","source":"ib","event":"connected"}\n'
        'NOT JSON LINE\n'
        '{"ts":"2026-05-17T00:01:00Z","source":"ib","event":"farm_state","code":2106,"state":"ok","farm":"ushmds"}\n'
    )
    rows = load_telemetry(f, since=_utc("2026-05-16T00:00:00Z"))
    assert len(rows) == 2


def test_compute_summary_uptime(tmp_path):
    rows = [
        {"ts": "2026-05-17T00:00:00Z", "source": "ib", "event": "farm_state", "code": 2106, "state": "ok", "farm": "ushmds"},
        {"ts": "2026-05-17T01:00:00Z", "source": "ib", "event": "farm_state", "code": 2105, "state": "broken", "farm": "ushmds"},
        {"ts": "2026-05-17T01:30:00Z", "source": "ib", "event": "farm_state", "code": 2106, "state": "ok", "farm": "ushmds"},
    ]
    audit_rows = []
    window_start = _utc("2026-05-17T00:00:00Z")
    window_end = _utc("2026-05-17T02:00:00Z")
    summary = compute_summary(rows, audit_rows, window_start=window_start, window_end=window_end)
    ib = next(s for s in summary["sources"] if s["source"] == "ib")
    farm = next(f for f in ib["farms"] if f["farm"] == "ushmds")
    # 1h ok, 30m broken, 30m ok → uptime = (60+30) / 120 = 75%
    assert abs(farm["uptime_pct"] - 75.0) < 0.5


def test_compute_summary_flap_count():
    rows = []
    base = _utc("2026-05-17T00:00:00Z")
    for i, state in enumerate(["ok", "broken", "ok", "broken", "ok"]):
        code = 2106 if state == "ok" else 2105
        rows.append({
            "ts": (base + timedelta(minutes=i * 2)).isoformat().replace("+00:00", "Z"),
            "source": "ib",
            "event": "farm_state",
            "code": code,
            "state": state,
            "farm": "ushmds",
        })
    summary = compute_summary(rows, [], window_start=base, window_end=base + timedelta(hours=1))
    ib = next(s for s in summary["sources"] if s["source"] == "ib")
    farm = next(f for f in ib["farms"] if f["farm"] == "ushmds")
    assert farm["flap_count"] == 1    # 5 transitions <10min apart = 1 contiguous flap burst


def test_render_summary_text_includes_uptime(tmp_path):
    summary = {
        "window": "24h",
        "sources": [
            {
                "source": "ib",
                "connection_events": 142,
                "farms": [{"farm": "ushmds", "uptime_pct": 97.2, "flap_count": 3, "mtbd_seconds": 1800}],
            },
        ],
        "flag_counts_by_category": {"range_shortfall": 1},
        "top_tickers": [{"ticker": "SMH", "flag_count": 2}],
    }
    text = render_summary_text(summary)
    assert "97.2" in text
    assert "SMH" in text
```

- [ ] **Step 2: Confirm failure** — `ModuleNotFoundError`.

- [ ] **Step 3: Implement**

```python
#!/usr/bin/env python
# scripts/data_quality_report.py
"""Unified CLI for telemetry + quality-audit aggregation.

Views: summary | flap | quality
Sources: ib | uw | massive | all
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TELEMETRY = Path.home() / "market-warehouse" / "logs" / "telemetry.jsonl"
DEFAULT_AUDIT = Path.home() / "market-warehouse" / "logs" / "quality_audit.jsonl"

_SINCE_RE = re.compile(r"^(\d+)\s*([smhd])$")


def _parse_since(raw: str) -> timedelta:
    m = _SINCE_RE.match(raw.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"invalid --since: {raw!r}")
    n, unit = int(m.group(1)), m.group(2)
    return {"s": timedelta(seconds=n), "m": timedelta(minutes=n),
            "h": timedelta(hours=n), "d": timedelta(days=n)}[unit]


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Livewire data quality report")
    p.add_argument("--view", choices=["summary", "flap", "quality"], required=True)
    p.add_argument("--since", default="24h", type=_parse_since)
    p.add_argument("--source", default="all", choices=["all", "ib", "uw", "massive"])
    p.add_argument("--severity", default=None, choices=[None, "info", "warning", "critical"])
    p.add_argument("--telemetry-path", type=Path, default=DEFAULT_TELEMETRY)
    p.add_argument("--audit-path", type=Path, default=DEFAULT_AUDIT)
    p.add_argument("--email", action="store_true", help="Render HTML and spawn Nodemailer daily-summary")
    return p.parse_args(argv)


def _iter_jsonl(path: Path) -> Iterable[dict]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue    # skip corrupt residue


def load_telemetry(path: Path, *, since: datetime) -> list[dict]:
    out = []
    for r in _iter_jsonl(path):
        ts = r.get("ts")
        if not ts:
            continue
        try:
            t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if t >= since:
            r["_ts"] = t
            out.append(r)
    return out


def load_audit(path: Path, *, since: datetime) -> list[dict]:
    out = []
    for r in _iter_jsonl(path):
        ts = r.get("ts")
        if not ts:
            continue
        try:
            t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if t >= since:
            r["_ts"] = t
            out.append(r)
    return out


def _compute_farm_uptime(transitions: list[tuple[datetime, str]], window_start: datetime, window_end: datetime) -> float:
    if not transitions:
        return 0.0
    transitions = sorted(transitions, key=lambda x: x[0])
    window_seconds = max(1, (window_end - window_start).total_seconds())
    ok_seconds = 0.0
    prev_t, prev_state = transitions[0]
    if prev_t > window_start:
        # We don't know state before first transition; conservative: exclude from denominator
        denom_start = prev_t
    else:
        denom_start = window_start
    for t, state in transitions[1:]:
        if prev_state == "ok":
            ok_seconds += (t - max(prev_t, denom_start)).total_seconds()
        prev_t, prev_state = t, state
    if prev_state == "ok":
        ok_seconds += (window_end - max(prev_t, denom_start)).total_seconds()
    denom = max(1.0, (window_end - denom_start).total_seconds())
    return 100.0 * ok_seconds / denom


def _compute_flap_count(transitions: list[tuple[datetime, str]]) -> int:
    """A flap burst = ≥3 transitions where every consecutive pair is <10 min apart."""
    if len(transitions) < 3:
        return 0
    transitions = sorted(transitions)
    bursts = 0
    burst_len = 1
    for i in range(1, len(transitions)):
        if (transitions[i][0] - transitions[i - 1][0]).total_seconds() < 600:
            burst_len += 1
        else:
            if burst_len >= 3:
                bursts += 1
            burst_len = 1
    if burst_len >= 3:
        bursts += 1
    return bursts


def compute_summary(telemetry: list[dict], audit: list[dict], *, window_start: datetime, window_end: datetime) -> dict:
    by_source_farm: dict[tuple[str, Optional[str]], list[tuple[datetime, str]]] = defaultdict(list)
    by_source_events: Counter = Counter()
    for r in telemetry:
        src = r.get("source", "?")
        by_source_events[src] += 1
        if r.get("event") == "farm_state":
            by_source_farm[(src, r.get("farm"))].append((r["_ts"], r.get("state", "?")))

    sources = []
    for src in sorted({s for s, _ in by_source_farm} | set(by_source_events)):
        farms = []
        for (s, farm), transitions in by_source_farm.items():
            if s != src:
                continue
            farms.append({
                "farm": farm or "(unknown)",
                "uptime_pct": round(_compute_farm_uptime(transitions, window_start, window_end), 1),
                "flap_count": _compute_flap_count(transitions),
                "mtbd_seconds": None,    # populated in Task 21 if needed
            })
        sources.append({
            "source": src,
            "connection_events": by_source_events[src],
            "farms": farms,
        })

    flag_counts: Counter = Counter()
    ticker_counts: Counter = Counter()
    for r in audit:
        flag_counts[r.get("category", "?")] += 1
        ticker_counts[r.get("ticker", "?")] += 1

    return {
        "window": f"{window_start.isoformat()} → {window_end.isoformat()}",
        "sources": sources,
        "flag_counts_by_category": dict(flag_counts),
        "top_tickers": [{"ticker": t, "flag_count": c} for t, c in ticker_counts.most_common(10)],
    }


def render_summary_text(summary: dict) -> str:
    lines = ["=== Livewire Data Quality Summary ===", f"Window: {summary['window']}", ""]
    for s in summary["sources"]:
        lines.append(f"[{s['source']}] events={s['connection_events']}")
        for f in s["farms"]:
            lines.append(f"  farm={f['farm']} uptime={f['uptime_pct']}% flaps={f['flap_count']}")
    lines.append("")
    lines.append("Quality flags by category:")
    for cat, n in summary["flag_counts_by_category"].items():
        lines.append(f"  {cat}: {n}")
    lines.append("")
    lines.append("Top affected tickers:")
    for t in summary["top_tickers"]:
        lines.append(f"  {t['ticker']}: {t['flag_count']} flag(s)")
    return "\n".join(lines)
```

- [ ] **Step 4: Run + cover** — 4/4 tests pass + 100% on lines added.

- [ ] **Step 5: Commit**

```bash
git add scripts/data_quality_report.py tests/test_data_quality_report.py
git commit -m "feat(report): add data_quality_report --view summary

Loads telemetry + audit JSONL, computes per-(source,farm) uptime, flap
count, flag counts by category, and top-10 affected tickers. Renders
plain-text rollup."
```

---

## Task 21: `data_quality_report.py` — `--view flap` + `--view quality`

**Files:**
- Modify: `scripts/data_quality_report.py`
- Test: `tests/test_data_quality_report.py`

**Spec reference:** `--view flap` (chronological windows), `--view quality` (severity filter, source filter).

- [ ] **Step 1: Write the failing tests**

```python
def test_render_flap_view_chronological():
    from scripts.data_quality_report import render_flap_view
    rows = [
        {"_ts": _utc(f"2026-05-17T00:0{i}:00Z"), "source": "ib", "event": "farm_state",
         "state": "ok" if i % 2 == 0 else "broken", "farm": "ushmds", "code": 2106}
        for i in range(6)
    ]
    text = render_flap_view(rows)
    assert "ushmds" in text
    assert text.count("00:") >= 1


def test_render_quality_view_severity_filter():
    from scripts.data_quality_report import render_quality_view
    audit = [
        {"_ts": _utc("2026-05-17T00:00:00Z"), "source": "ib", "ticker": "SMH",
         "category": "range_shortfall", "severity": "critical", "detail": {}},
        {"_ts": _utc("2026-05-17T00:01:00Z"), "source": "ib", "ticker": "NVDA",
         "category": "interior_gaps", "severity": "warning", "detail": {}},
    ]
    text = render_quality_view(audit, severity_filter="critical")
    assert "SMH" in text
    assert "NVDA" not in text


def test_main_dispatch_summary(tmp_path, monkeypatch, capsys):
    from scripts.data_quality_report import main
    t = tmp_path / "telemetry.jsonl"
    t.write_text(json.dumps({
        "ts": "2026-05-17T00:00:00Z", "source": "ib", "event": "connected",
    }) + "\n")
    a = tmp_path / "audit.jsonl"
    a.write_text("")
    rc = main(["--view", "summary", "--since", "30d",
               "--telemetry-path", str(t), "--audit-path", str(a)])
    captured = capsys.readouterr()
    assert rc == 0
    assert "Livewire Data Quality Summary" in captured.out
```

- [ ] **Step 2: Confirm failure** + **Step 3: Implement**

```python
# Append to scripts/data_quality_report.py
def render_flap_view(telemetry: list[dict]) -> str:
    farm_events: dict[str, list[dict]] = defaultdict(list)
    for r in telemetry:
        if r.get("event") != "farm_state":
            continue
        farm_events[r.get("farm") or "(unknown)"].append(r)
    lines = ["=== Flap windows (chronological) ==="]
    for farm in sorted(farm_events):
        events = sorted(farm_events[farm], key=lambda x: x["_ts"])
        lines.append(f"\n[{farm}] {len(events)} transitions")
        for e in events:
            lines.append(f"  {e['_ts'].strftime('%Y-%m-%dT%H:%M:%SZ')} state={e.get('state')} code={e.get('code')}")
    return "\n".join(lines)


def render_quality_view(audit: list[dict], *, severity_filter: Optional[str] = None) -> str:
    rows = sorted(audit, key=lambda x: x["_ts"])
    if severity_filter:
        order = {"info": 0, "warning": 1, "critical": 2}
        rows = [r for r in rows if order.get(r.get("severity"), 0) >= order.get(severity_filter, 0)]
    lines = ["=== Quality flags ==="]
    for r in rows:
        lines.append(
            f"{r['_ts'].strftime('%Y-%m-%dT%H:%M:%SZ')} "
            f"[{r.get('severity', '?'):>8}] {r.get('source')}/{r.get('ticker')}/"
            f"{r.get('timeframe', '1d')} {r.get('category')}"
        )
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    now = datetime.now(timezone.utc)
    window_start = now - args.since
    telemetry = load_telemetry(args.telemetry_path, since=window_start)
    audit = load_audit(args.audit_path, since=window_start)

    if args.source != "all":
        telemetry = [r for r in telemetry if r.get("source") == args.source]
        audit = [r for r in audit if r.get("source") == args.source]

    if args.view == "summary":
        summary = compute_summary(telemetry, audit, window_start=window_start, window_end=now)
        text = render_summary_text(summary)
        print(text)
        if args.email:
            _send_email(summary)    # Task 22
    elif args.view == "flap":
        print(render_flap_view(telemetry))
    elif args.view == "quality":
        print(render_quality_view(audit, severity_filter=args.severity))
    return 0


if __name__ == "__main__":    # pragma: no cover
    sys.exit(main())
```

- [ ] **Step 4: Run + cover + Step 5: Commit**

```bash
git add scripts/data_quality_report.py tests/test_data_quality_report.py
git commit -m "feat(report): add --view flap and --view quality

Chronological flap-burst listing and severity-filtered quality view.
Source filter applied uniformly across all views."
```

---

## Task 22: `data_quality_report.py` — `--email` mode + marker file

**Files:**
- Modify: `scripts/data_quality_report.py`
- Test: `tests/test_data_quality_report.py`

**Spec reference:** `--email` flag renders HTML body and invokes Nodemailer `--mode daily-summary`. Marker file `~/market-warehouse/logs/quality_summary_YYYY-MM-DD.marker`.

- [ ] **Step 1: Write the failing test**

```python
def test_email_mode_spawns_nodemailer_and_writes_marker(tmp_path, monkeypatch):
    from scripts.data_quality_report import main
    t = tmp_path / "t.jsonl"; t.write_text("")
    a = tmp_path / "a.jsonl"; a.write_text("")
    marker_dir = tmp_path / "markers"
    monkeypatch.setenv("MDW_LOG_DIR", str(marker_dir))

    spawned = []
    def fake_run(*a, **kw):
        spawned.append(a)
        from subprocess import CompletedProcess
        return CompletedProcess(args=[], returncode=0, stdout=b"", stderr=b"")
    monkeypatch.setattr("subprocess.run", fake_run)

    rc = main(["--view", "summary", "--since", "1h", "--email",
               "--telemetry-path", str(t), "--audit-path", str(a)])
    assert rc == 0
    assert spawned, "Nodemailer should be invoked"
    cmd = spawned[0][0]
    assert "daily-summary" in cmd
    markers = list(marker_dir.glob("quality_summary_*.marker"))
    assert markers, "marker file should be written"
```

- [ ] **Step 2: Confirm failure.**

- [ ] **Step 3: Implement**

```python
# Append to scripts/data_quality_report.py
import subprocess


def _resolve_log_dir() -> Path:
    raw = os.environ.get(
        "MDW_LOG_DIR",
        str(Path.home() / "market-warehouse" / "logs"),
    )
    return Path(raw).expanduser()


_EMAIL_SCRIPT = REPO_ROOT / "scripts" / "send_daily_update_failure_email.mjs"


def _send_email(summary: dict) -> bool:
    payload = json.dumps(summary, default=str)
    cmd = ["node", str(_EMAIL_SCRIPT), "--mode", "daily-summary", "--payload", payload]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=60)
    except (subprocess.SubprocessError, OSError) as exc:
        print(f"daily-summary email spawn failed: {exc}", file=sys.stderr)
        return False
    if result.returncode != 0:
        print(f"daily-summary email returned {result.returncode}: {result.stderr!r}", file=sys.stderr)
        return False
    # Write marker
    log_dir = _resolve_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    (log_dir / f"quality_summary_{date_str}.marker").write_text("ok\n", encoding="utf-8")
    return True
```

- [ ] **Step 4: Run + cover** — 100% on data_quality_report.

- [ ] **Step 5: Commit**

```bash
git add scripts/data_quality_report.py tests/test_data_quality_report.py
git commit -m "feat(report): add --email mode + daily-summary marker

--email renders summary, spawns Nodemailer daily-summary mode, writes
quality_summary_YYYY-MM-DD.marker so the watchdog (T24) can verify
the rollup actually ran."
```

---

## Task 23: `run_daily_update_job.py` — end-of-day quality report

**Files:**
- Modify: `scripts/run_daily_update_job.py`
- Test: `tests/test_run_daily_update_job.py`

**Spec reference:** Components → `scripts/run_daily_update_job.py` (+15 LOC at end).

- [ ] **Step 1: Verified anchor**

`scripts/run_daily_update_job.py:258` — `return 0` inside `run_with_retries`, immediately after `append_log(log_file, "=== Done ... ===")`. The end-of-day report subprocess fires **between** those two lines.

- [ ] **Step 2: Write the failing tests**

```python
# Append to tests/test_run_daily_update_job.py (use existing _config helper)
from subprocess import CompletedProcess


class TestEndOfDayQualityReport:
    def test_report_invoked_after_successful_daily(self, tmp_path):
        config = _config(tmp_path)
        # First runner call = daily_update subprocess → success.
        # Second runner call = data_quality_report subprocess → success.
        calls = []

        def fake_runner(cmd, **kwargs):
            calls.append(list(cmd))
            return CompletedProcess(args=cmd, returncode=0, stdout=b"", stderr=b"")

        from scripts.run_daily_update_job import run_with_retries
        rc = run_with_retries(
            config, daily_update_args=[],
            runner=fake_runner, sleep_fn=lambda s: None,
            now_fn=lambda: datetime(2026, 5, 18, 20, 0, tzinfo=timezone.utc),
        )
        assert rc == 0
        # Verify the second subprocess invocation was data_quality_report --email
        assert len(calls) >= 2
        report_cmd = calls[1]
        assert any("data_quality_report.py" in str(c) for c in report_cmd)
        assert "--email" in report_cmd

    def test_report_failure_does_not_fail_daily(self, tmp_path):
        config = _config(tmp_path)
        calls = []

        def fake_runner(cmd, **kwargs):
            calls.append(list(cmd))
            # Daily succeeds, report fails
            rc = 0 if "daily_update.py" in " ".join(str(c) for c in cmd) else 2
            return CompletedProcess(args=cmd, returncode=rc, stdout=b"", stderr=b"report failed")

        from scripts.run_daily_update_job import run_with_retries
        rc = run_with_retries(
            config, daily_update_args=[],
            runner=fake_runner, sleep_fn=lambda s: None,
            now_fn=lambda: datetime(2026, 5, 18, 20, 0, tzinfo=timezone.utc),
        )
        assert rc == 0    # daily success is unaffected by report failure
```

- [ ] **Step 3: Implement**

In `scripts/run_daily_update_job.py`, inside `run_with_retries`, after the existing success block:

```python
if result.returncode == 0:
    append_log(
        log_file,
        (
            "=== Done "
            f"{now_fn():%Y-%m-%dT%H:%M:%SZ} "
            f"(attempt {attempt}/{config.max_attempts}) ==="
        ),
    )
    # NEW: end-of-day quality report (best-effort; failure does NOT block sync success)
    try:
        runner(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "data_quality_report.py"),
                "--view", "summary", "--since", "24h", "--email",
            ],
            timeout=120,
            check=False,
            capture_output=True,
        )
    except Exception as exc:    # pragma: no cover - logged but tolerated
        append_log(log_file, f"WARNING: end-of-day quality report failed: {exc}")
    return 0
```

- [ ] **Step 4: Run tests + Step 5: Commit**

```bash
git add scripts/run_daily_update_job.py tests/test_run_daily_update_job.py
git commit -m "feat(daily): invoke data_quality_report at end of day

Best-effort end-of-day rollup email. Subprocess failure is logged but
does not affect the sync's own success status; the existing completion
marker still gets written."
```

---

## Task 24: `check_daily_update_watchdog.py` — verify both markers

**Files:**
- Modify: `scripts/check_daily_update_watchdog.py`
- Test: `tests/test_check_daily_update_watchdog.py`

**Spec reference:** Components → `scripts/check_daily_update_watchdog.py` (+10 LOC).

- [ ] **Step 1: Write the failing tests**

```python
# Append to tests/test_check_daily_update_watchdog.py
from subprocess import CompletedProcess


class TestQualitySummaryMarker:
    def test_passes_when_both_markers_present(self, tmp_path):
        config = _config(tmp_path)
        config.log_dir.mkdir(parents=True, exist_ok=True)
        # Daily log with completion marker
        daily_log = build_daily_log_file(config.log_dir, "2026-05-18")
        daily_log.write_text("=== Done 2026-05-18T20:00:00Z (attempt 1/3) ===\n")
        # Quality summary marker present
        (config.log_dir / "quality_summary_2026-05-18.marker").write_text("ok\n")

        rc = run_watchdog(config, run_date="2026-05-18")
        assert rc == 0

    def test_alerts_when_quality_marker_missing(self, tmp_path):
        config = _config(tmp_path)
        config.log_dir.mkdir(parents=True, exist_ok=True)
        daily_log = build_daily_log_file(config.log_dir, "2026-05-18")
        daily_log.write_text("=== Done 2026-05-18T20:00:00Z (attempt 1/3) ===\n")
        # No quality_summary marker

        calls = []
        def fake_runner(cmd, **kwargs):
            calls.append(list(cmd))
            return CompletedProcess(args=cmd, returncode=0, stdout=b"sent", stderr=b"")

        rc = run_watchdog(config, run_date="2026-05-18", runner=fake_runner)
        assert rc == WATCHDOG_ALERT_SENT_EXIT_CODE
        # An alert was attempted
        assert calls, "watchdog should have spawned the alert subprocess"
```

- [ ] **Step 2: Implement**

In `scripts/check_daily_update_watchdog.py`, extend `run_watchdog`:

```python
def run_watchdog(config, *, run_date, env=None, runner=subprocess.run) -> int:
    daily_log_file = build_daily_log_file(config.log_dir, run_date)
    watchdog_log_file = build_watchdog_log_file(config.log_dir, run_date)
    marker_file = build_watchdog_marker_file(config.warehouse_dir, run_date)

    daily_complete = log_has_completion_marker(daily_log_file)
    quality_marker = config.log_dir / f"quality_summary_{run_date}.marker"
    quality_complete = quality_marker.exists()

    if daily_complete and quality_complete:
        return 0

    if not daily_complete:
        reason = determine_watchdog_error(daily_log_file, run_date)
    else:
        reason = (
            f"Daily sync completed on {run_date} but the end-of-day quality "
            f"summary marker is missing at {quality_marker}."
        )
    # ... existing alert path stays the same
```

- [ ] **Step 3: Run tests + Step 4: Commit**

```bash
git add scripts/check_daily_update_watchdog.py tests/test_check_daily_update_watchdog.py
git commit -m "feat(watchdog): verify both daily and quality-summary markers

Watchdog now alerts when the daily sync completed but the end-of-day
quality summary email never ran. Closes the alerting-failure gap from
the spec's failure matrix."
```

---

## Task 25: Documentation sweep

**Files:**
- Modify: `CLAUDE.md`, `README.md`, `AGENTS.md`, `.codex/project-memory.md`

**Spec reference:** Components → Documentation section.

- [ ] **Step 1: Add new env vars to CLAUDE.md**

Append to the existing env-var documentation (find the relevant section by grepping `MDW_IB_HOST` in CLAUDE.md). Document all 8 vars from the spec Configuration table **plus**:
- `MDW_UNDELIVERED_DIR` — where `alert_on_flag` preserves SMTP-failed HTML bodies (default: `~/market-warehouse/logs/quality_alerts_undelivered/`). Introduced in T10; not in the original spec table.
- `MDW_LOG_DIR` — where `data_quality_report.py --email` writes the `quality_summary_YYYY-MM-DD.marker`. Defaults to `~/market-warehouse/logs/`.

- [ ] **Step 2: Add `run_ib_fetch_robust.py` and `data_quality_report.py` usage**

Add a new section titled "Reliability tooling" near the existing "Daily updates" section, covering:
- `run_ib_fetch_robust.py` CLI surface, outcome categories table
- `data_quality_report.py` CLI surface, view names, `--email`
- Sidecar JSON schema (link to spec)
- Audit JSONL schema (link to spec)

- [ ] **Step 3: Update README.md**

Add one-paragraph summary in the "Reliability" / "Data Quality" section with pointers to the spec + plan + CLI commands.

- [ ] **Step 4: Update .codex/project-memory.md**

Add these durable facts under the existing "Durable Facts" section:

```markdown
- The canonical multi-ticker IB execution model is `scripts/run_ib_fetch_robust.py`. Use it instead of bare `fetch_ib_historical.py` for any bulk run >5 tickers.
- Telemetry events (IB farm states, connection lifecycle) land in `~/market-warehouse/logs/telemetry.jsonl`. Schema is source-tagged JSONL with `{ts, source, event, ...}`.
- Quality flags (range_shortfall, interior_gaps, fetch_tainted, row_count_anomaly) are emitted to three independent paths: sidecar `<parquet>.meta.json`, central `quality_audit.jsonl`, and Nodemailer email via `--mode flag-alert`.
- `scripts/data_quality_report.py --view summary --since 24h --email` is the daily rollup; it runs end-of-day from `run_daily_update_job.py` and writes a `quality_summary_YYYY-MM-DD.marker`.
- Source enum is closed-set `{"ib", "uw", "massive"}` validated at every JSONL emit boundary.
```

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md README.md AGENTS.md .codex/project-memory.md
git commit -m "docs: document reliability foundation (Sub-A)

New env vars (MDW_TELEMETRY_PATH, MDW_QUALITY_AUDIT_PATH, …),
orchestrator entry point, data_quality_report CLI, sidecar+audit
JSONL surfaces. Memory file records the durable workflow facts."
```

---

## Task 26: Final coverage + manual verification

**Files:** none (verification only)

- [ ] **Step 1: Full test suite with coverage**

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/ -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning 2>&1 | tail -30
```
Expected: all green, `TOTAL` line shows `100%`. If anything below 100%, add the missing tests before declaring done.

- [ ] **Step 2: Walk the spec manual-verification checklist**

From spec § Testing → Manual verification (1-7). For each, run the command and confirm the expected artifact:

1. **Telemetry sanity** — Run any ~30s IB fetch. Verify `tail telemetry.jsonl` contains `connected`, `farm_state`, `disconnected` events with `"source":"ib"`.
2. **Quality detection on a real partial** — Re-run SMH fetch with a known-incomplete history. Verify sidecar `1d.parquet.meta.json` exists with `range_shortfall` critical, audit JSONL gains entry, email arrives.
3. **Clean fetch produces no sidecar** — Run any clean recent ticker (NVDA, AAPL). Verify no sidecar, no audit, no email.
4. **CLI report** — `python scripts/data_quality_report.py --view summary --since 24h`. Verify per-farm uptime + flag counts print.
5. **Orchestrator timeout** — Force a long-hang fetch (e.g., during HMDS down). Verify SIGKILL fires at 300s, summary line shows `[timeout]`, retry attempted.
6. **Daily summary email** — `python scripts/data_quality_report.py --view summary --since 24h --email`. Verify HTML email arrives + marker file written.
7. **End-to-end daily** — Run full daily cycle (cron or manual). Verify BOTH `daily_update.marker` AND `quality_summary.marker` exist; summary email arrived.

- [ ] **Step 3: Open the PR**

```bash
git push -u origin feat/reliability-foundation
gh pr create --title "Sub-A: Reliability Foundation" --body "$(cat <<'EOF'
## Summary
Implements Sub-project A of the Livewire reliability roadmap: source-agnostic telemetry, four-category quality detection, productized per-ticker orchestrator, unified data-quality CLI, and end-of-day email rollup.

## Spec
docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md

## Plan
docs/superpowers/plans/2026-05-18-reliability-foundation-plan.md

## Coverage
100% on clients/ and scripts/ (existing gate maintained).

## Test plan
- [ ] Full pytest with coverage clean
- [ ] Spec § Testing → Manual verification (1-7) walked
- [ ] Run `run_ib_fetch_robust.py --tickers SMH --mode backfill` against live IB Gateway and confirm range_shortfall flag fires
- [ ] Daily-summary email landed
EOF
)"
```

(Per project rules: do NOT push to master directly; PR is mandatory.)

---

## Self-review

After writing this plan, I checked it against the spec section by section:

| Spec section | Plan coverage |
|---|---|
| Naming convention table | T13/T14/T25 keep MDW_* and mdw.* prefixes; T25 updates user-facing strings to "Livewire" |
| Components → telemetry.py | T1-T3 |
| Components → quality_detector.py | T4-T7 |
| Components → quality_flags.py | T8-T10 |
| Components → run_ib_fetch_robust.py | T17-T19 |
| Components → fetch_ib_historical.py | T14 (with --no-quality flag explicit) |
| Components → daily_update.py | T15 |
| Components → backfill_intraday.py | T16 |
| Components → data_quality_report.py | T20-T22 |
| Components → send_daily_update_failure_email.mjs | T11-T12 |
| Components → run_daily_update_job.py | T23 |
| Components → check_daily_update_watchdog.py | T24 |
| Data flow: write path | T14-T19 collectively reproduce steps a-g |
| Data flow: read paths | T20-T22 |
| Error handling matrix | T1 (disabled state), T8/T9/T10 (OSError graceful), T7 (detector_error), T19 (atexit), T9 (invalid source), T18 (timeout) |
| Configuration env vars | T17/T18 use them; T25 documents them |
| Testing coverage gate | T26 enforces |

**Placeholder scan:** All `pass`-body placeholder tests in T14/T15/T16/T23/T24 replaced with concrete bodies that mirror the existing test-file fixture styles (`_make_bar`, `_make_ib_bar`, `_config(tmp_path)`, `SimpleNamespace`-based mocks). One legitimate "Sub-A scope" TODO documented in T14 (daily-bar `errors_during_fetch` collection — deferred to a follow-up task with explicit rationale).

**Type consistency check:** `QualityFlag` dataclass shape consistent across T4-T22. `TickerOutcome` / `OutcomeCategory` consistent across T17-T19. `source` enum closed-set `{"ib","uw","massive"}` referenced consistently in T1, T3, T9, T10, T20.

**Scope check:** This is a single sub-project (Sub-A) — appropriately scoped for one plan. Sub-B through Sub-F are deferred per the spec.

## Hardening pass (2026-05-18)

After the initial draft, five concerns from a calibrated self-review were verified against the live codebase and the plan was updated:

1. **`ib_async` event API** — confirmed `eventkit.Event` with `.connect()/.disconnect()` (T2 unchanged).
2. **Trading calendar** — confirmed `is_trading_day`/`previous_trading_day`/`trading_days_between` exist at `scripts/daily_update.py:222-280`; T5 became an extraction task with concrete function-by-function instructions.
3. **Integration anchors (T14/T15/T16)** — verified line numbers (`backfill_ticker:550`, `daily_update.py:869`, `backfill_intraday.py:199`); replaced placeholder `pass` test bodies with concrete ones using the existing fixture conventions. T16 simplified to reuse the existing `outcome.errors` list directly.
4. **Email transport (T11/T12)** — verified `sendFailureAlert({transportOptions, message})` at line 448 is already the correct boundary; removed the "extract a sendMessage helper" refactor language.
5. **Orchestrator orphan cleanup (T18/T19)** — removed the broken `_cleanup_orphans` / `_CHILD_PIDS` / `atexit` code because `subprocess.run(timeout=...)` already SIGKILLs its child and there's no long-lived Popen handle to track. Documented the intentional non-decision.

Plus: `MDW_UNDELIVERED_DIR` (introduced in T10) and `MDW_LOG_DIR` (used in T22) added to the T25 documentation sweep. Verification block added near the top of this plan so the engineer can see at a glance what was confirmed vs. what's still open.

**Open follow-up explicitly tracked (not in Sub-A):** Daily-bar IB fetches do not collect per-ticker IB error codes today, so `errors_during_fetch` is `[]` for T14/T15 paths. This means `detect_fetch_tainting` can fire from T16 (intraday) but not from T14/T15 (daily). Wiring a second `errorEvent` handler alongside `ConnectionTelemetry` to bucket errors by `reqId` and join with the per-ticker `reqHistoricalData` call is a small follow-up task — flagged in T14's implementation block.
