# Apex Silver Revision Watcher Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make continuously running Apex detect Livewire Silver revisions and atomically reseed affected active subscriptions without restarting Docker.

**Architecture:** Add a strict manifest reader and polling service at the Livewire adapter boundary. Add true replace-history APIs through IndicatorEngine, TASignalService, and SubscriptionManager; buffer affected-symbol ticks during replacement and replay them after an atomic state swap. Wire watcher lifecycle and health into FastAPI.

**Tech Stack:** Python 3.13, asyncio, dataclasses, pathlib, FastAPI, pytest, DuckDB fixtures.

## Global Constraints

- Work in `/Users/moremeds/projects/apex` on branch `feat/silver-revision-watcher`; do not include existing `.serena/project.yml` or `uv.lock` changes.
- Depends on revision contract R0 from `docs/superpowers/specs/2026-07-12-silver-adjusted-bars-apex-revisions-design.md`.
- Apex remains available while an affected symbol refreshes; unrelated symbols never block.
- Missing/corrupt Silver artifacts never silently fall back to raw.
- `APEX_LIVEWIRE_REVISION_POLL_SECONDS` defaults to `30`.
- Each task uses TDD and commits only its listed files.

---

## File Structure

- Create `src/infrastructure/adapters/livewire/revisions.py`: strict manifest models, parsing, checksum validation.
- Create `src/application/subscriptions/revision_watcher.py`: polling lifecycle and retry state.
- Modify `src/domain/signals/indicator_engine.py`: atomic replacement of one `(symbol, timeframe)` history deque.
- Modify `src/application/services/ta_signal_service.py`: refresh/tick-buffer boundary and replay.
- Modify `src/application/subscriptions/manager.py`: targeted multi-timeframe reseed and applied revisions.
- Modify `src/api/server.py`: construct/start/stop watcher.
- Modify `src/api/routes/health.py`: expose revision health.
- Create matching tests under `tests/unit/infrastructure/livewire/`, `tests/unit/application/subscriptions/`, and `tests/unit/api/`.

### Task 1: Revision manifest contract reader

**Files:**
- Create: `src/infrastructure/adapters/livewire/revisions.py`
- Create: `tests/unit/infrastructure/livewire/test_revisions.py`
- Create: `tests/fixtures/livewire/silver/revisions/current.json`

**Interfaces:**
- Produces: `AffectedSymbol`, `SilverRevision`, and `RevisionManifestReader.read_current() -> SilverRevision`.

- [ ] **Step 1: Write failing parser and checksum tests**

```python
def test_reader_parses_current_manifest(tmp_path: Path) -> None:
    artifact = tmp_path / "asset_class=equity/symbol=NVDA/1d.parquet"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"silver")
    _write_manifest(tmp_path, artifact, revision=42)
    revision = RevisionManifestReader(tmp_path).read_current()
    assert revision.revision == 42
    assert revision.affected[0].symbol == "NVDA"

def test_reader_rejects_path_escape(tmp_path: Path) -> None:
    _write_raw_manifest(tmp_path, artifact_path="../bronze/secret")
    with pytest.raises(RevisionManifestError, match="outside Silver root"):
        RevisionManifestReader(tmp_path).read_current()
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/unit/infrastructure/livewire/test_revisions.py -q --no-cov`
Expected: FAIL because `revisions.py` does not exist.

- [ ] **Step 3: Implement strict frozen models and reader**

```python
@dataclass(frozen=True)
class AffectedSymbol:
    symbol: str
    earliest_date: date
    timeframes: tuple[str, ...]

@dataclass(frozen=True)
class SilverRevision:
    schema_version: int
    revision: int
    generation_id: str
    published_at: datetime
    affected: tuple[AffectedSymbol, ...]

class RevisionManifestReader:
    def __init__(self, silver_root: Path) -> None:
        self._root = silver_root.resolve()

    def read_current(self) -> SilverRevision:
        payload = json.loads((self._root / "revisions/current.json").read_text())
        if payload["schema_version"] != 1 or payload["revision"] < 1:
            raise RevisionManifestError("unsupported or invalid revision manifest")
        self._verify_artifacts(payload["artifacts"])
        return _parse_revision(payload)
```

Validation must reject unknown schema versions, duplicate affected symbols, unsupported timeframes, non-UTC timestamps, malformed SHA-256 values, missing artifacts, checksum mismatches, and paths resolving outside Silver root.

- [ ] **Step 4: Run focused tests**

Run: `uv run pytest tests/unit/infrastructure/livewire/test_revisions.py -q --no-cov`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/infrastructure/adapters/livewire/revisions.py tests/unit/infrastructure/livewire/test_revisions.py tests/fixtures/livewire/silver/revisions/current.json
git commit -m "feat: validate Livewire Silver revisions"
```

### Task 2: Atomic history replacement

**Files:**
- Modify: `src/domain/signals/indicator_engine.py`
- Modify: `src/application/services/ta_signal_service.py`
- Test: `tests/unit/signals/test_indicator_engine.py`
- Create: `tests/unit/application/test_ta_signal_service_revisions.py`

**Interfaces:**
- Produces: `IndicatorEngine.replace_symbol_histories(symbol, histories) -> dict[str, int]`.
- Produces: `TASignalService.replace_symbol_histories(symbol, histories) -> dict[str, int]`.

- [ ] **Step 1: Write tests proving replacement is not append-only**

```python
def test_replace_history_overwrites_same_timestamp_values(engine) -> None:
    engine.inject_historical_bars("NVDA", "1d", [_bar("2024-06-07", 1208.88)])
    count = engine.replace_symbol_histories(
        "NVDA", {"1d": [_bar("2024-06-07", 120.888)]}
    )
    assert count == {"1d": 1}
    assert list(engine._history[("NVDA", "1d")])[0]["close"] == 120.888
```

- [ ] **Step 2: Verify failure**

Run: `uv run pytest tests/unit/signals/test_indicator_engine.py -q --no-cov -k replace_history`
Expected: FAIL with missing method.

- [ ] **Step 3: Implement locked deque replacement and async service delegation**

```python
def replace_symbol_histories(
    self, symbol: str, histories: dict[str, list[dict[str, Any]]]
) -> dict[str, int]:
    replacements = {
        timeframe: deque(sorted(bars, key=lambda row: row["timestamp"]), maxlen=self._max_history)
        for timeframe, bars in histories.items()
    }
    with self._get_symbol_lock(symbol):
        for timeframe, replacement in replacements.items():
            self._history[(symbol, timeframe)] = replacement
    return {timeframe: len(rows) for timeframe, rows in replacements.items()}
```

Validate duplicate timestamps before constructing replacements. Add a symbol-level
lock used by both replacement and live-bar mutation so every configured timeframe
becomes visible together. The TASignalService wrapper raises when the engine is
unavailable; unlike initial warmup, a revision replacement must not report false
success.

- [ ] **Step 4: Run focused tests**

Run: `uv run pytest tests/unit/signals/test_indicator_engine.py tests/unit/application/test_ta_signal_service_revisions.py -q --no-cov`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/domain/signals/indicator_engine.py src/application/services/ta_signal_service.py tests/unit/signals/test_indicator_engine.py tests/unit/application/test_ta_signal_service_revisions.py
git commit -m "feat: atomically replace indicator history"
```

### Task 3: Targeted subscription reseed with tick buffering

**Files:**
- Modify: `src/application/services/ta_signal_service.py`
- Modify: `src/application/subscriptions/manager.py`
- Test: `tests/unit/application/subscriptions/test_manager.py`
- Test: `tests/unit/application/test_ta_signal_service_revisions.py`

**Interfaces:**
- Produces: `SubscriptionManager.refresh_revision(revision: SilverRevision) -> RefreshResult`.
- Produces: `TASignalService.begin_symbol_refresh(symbol)`, `commit_symbol_refresh(symbol)`, and `abort_symbol_refresh(symbol)`.

- [ ] **Step 1: Write failure-first tests**

```python
@pytest.mark.asyncio
async def test_refresh_only_active_affected_symbols() -> None:
    await manager.subscribe("NVDA")
    result = await manager.refresh_revision(_revision(42, ["NVDA", "AAPL"]))
    assert result.applied == {"NVDA": 42}
    assert provider.calls.count(("NVDA", "1d")) == 2
    assert ("AAPL", "1d") not in provider.calls

def test_ticks_buffer_and_replay_in_event_time_order(service) -> None:
    service.begin_symbol_refresh("NVDA")
    service._on_market_data_tick(_tick("NVDA", 2))
    service._on_market_data_tick(_tick("NVDA", 1))
    service.commit_symbol_refresh("NVDA")
    assert service.replayed_sequences == [1, 2]
```

- [ ] **Step 2: Verify failure**

Run: `uv run pytest tests/unit/application/subscriptions/test_manager.py tests/unit/application/test_ta_signal_service_revisions.py -q --no-cov -k 'refresh or buffer'`
Expected: FAIL with missing refresh APIs.

- [ ] **Step 3: Implement per-symbol refresh state**

Use one asyncio lock per symbol in SubscriptionManager. Fetch every configured
timeframe first; only then call `replace_symbol_histories` once. TASignalService
buffers matching-symbol ticks in a bounded deque while passing unrelated ticks
through normally. Default ceilings are 10,000 ticks or 120 seconds; exceeding
either aborts refresh and marks the symbol unavailable. `abort_symbol_refresh`
leaves the old histories intact and replays buffered ticks into the unchanged
state.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/unit/application/subscriptions/test_manager.py tests/unit/application/test_ta_signal_service_revisions.py -q --no-cov`
Expected: PASS, including failure, overflow, and unrelated-symbol cases.

- [ ] **Step 5: Commit**

```bash
git add src/application/services/ta_signal_service.py src/application/subscriptions/manager.py tests/unit/application/subscriptions/test_manager.py tests/unit/application/test_ta_signal_service_revisions.py
git commit -m "feat: reseed revised subscriptions safely"
```

### Task 4: Poller lifecycle and health

**Files:**
- Create: `src/application/subscriptions/revision_watcher.py`
- Modify: `src/api/server.py`
- Modify: `src/api/routes/health.py`
- Test: `tests/unit/application/subscriptions/test_revision_watcher.py`
- Test: `tests/unit/api/test_server_lifespan.py`
- Test: `tests/unit/api/test_health.py`

**Interfaces:**
- Consumes: `RevisionManifestReader.read_current()` and `SubscriptionManager.refresh_revision()`.
- Produces: `RevisionWatcher.start()`, `stop()`, and `health() -> dict[str, Any]`.

- [ ] **Step 1: Write polling/idempotency/lifecycle tests**

Test initial observation, duplicate revision no-op, skipped revision, malformed manifest retention, bounded retry, startup, shutdown, and health payload fields.

- [ ] **Step 2: Verify failure**

Run: `uv run pytest tests/unit/application/subscriptions/test_revision_watcher.py tests/unit/api/test_server_lifespan.py tests/unit/api/test_health.py -q --no-cov`
Expected: FAIL because watcher and health fields are absent.

- [ ] **Step 3: Implement watcher**

```python
class RevisionWatcher:
    async def _poll_once(self) -> None:
        revision = await asyncio.to_thread(self._reader.read_current)
        if revision.revision <= self.observed_revision:
            return
        self.observed_revision = revision.revision
        result = await self._manager.refresh_revision(revision)
        self.per_symbol_revision.update(result.applied)
        if not result.failed:
            self.last_fully_applied_revision = revision.revision
```

Server wiring requires both `APEX_LIVEWIRE_SILVER_ROOT` and a subscription manager. Start the watcher after pipeline construction, store it as `app.state.revision_watcher`, and stop it before Xenon/event-bus teardown.

- [ ] **Step 4: Run focused and full Apex gates**

Run: `uv run pytest tests/unit/application/subscriptions tests/unit/api tests/unit/infrastructure/livewire -q --no-cov`
Expected: PASS.

Run: `uv run pytest tests -q`
Expected: PASS with coverage at or above 40%.

Run: `uv run mypy src/ --ignore-missing-imports`
Expected: exit 0.

- [ ] **Step 5: Update docs and commit**

Update `README.md` and `docs/livewire-apex-integration.md` with the future Silver
read-only mount and environment variables, but do not edit the live
`/Users/moremeds/apex-deploy/compose.yml`, deploy, or change adjusted defaults in
this PR.

```bash
git add src/application/subscriptions/revision_watcher.py src/api/server.py src/api/routes/health.py tests README.md docs/livewire-apex-integration.md
git commit -m "feat: watch Silver revisions in Apex"
```
