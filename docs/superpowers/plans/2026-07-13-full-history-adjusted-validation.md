# Full-History Adjusted Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only, resumable, full-equity-history validator that uses Massive adjusted history first, fresh IB history as fallback, and pointwise plus 20/50/200-session moving-average checks to validate Bronze and Silver.

**Architecture:** Put deterministic coverage, reconstruction, and comparison logic in a focused client module. Extend the Massive adapter for paginated adjusted aggregates and SMA values, then add a read-only orchestration command that acquires provider evidence, checkpoints per symbol, writes JSON/Markdown reports outside the data lake, and never invokes a Bronze writer. Provider clients remain injectable so all failure and coverage states are tested without external services.

**Tech Stack:** Python 3.13, PyArrow/Parquet, Decimal, `requests`, `ib_async`, pytest, Ruff, Pyright.

## Global Constraints

- Bronze and Silver are read-only inputs; validation must not publish, replace, or repair canonical artifacts.
- Massive `adjusted=true` validates split-only reconstruction; dividend-adjusted Silver is validated separately.
- Fresh IB fills Massive bar-coverage gaps and is labelled same-provider replay evidence where appropriate.
- Every local date must be covered; intersection-only comparison cannot pass.
- Pointwise OHLC checks remain authoritative even when rolling moving averages pass.
- Default warning threshold is 1 basis point and failure threshold is 5 basis points.
- API credentials come only from environment variables and never enter reports or caches.
- The configured source set must retain at least 95 percent test coverage.

---

### Task 1: Deterministic Validation Core

**Files:**
- Create: `clients/adjusted_history_validation.py`
- Test: `tests/test_adjusted_history_validation.py`

**Interfaces:**
- Consumes: Bronze/Silver/reference rows as `list[dict[str, object]]`, `CorporateAction` values, and one `as_of_date`.
- Produces: `CoverageMap`, `SeriesComparison`, `build_split_only_rows(...)`, `build_total_return_rows(...)`, `merge_reference_rows(...)`, `rolling_sma(...)`, and `compare_series(...)`.

- [x] **Step 1: Write failing coverage and moving-average tests**

  Add tests proving Massive precedence, IB fallback, unresolved local dates, duplicate-date rejection, session-based 20/50/200 SMA eligibility, a point error that cancels in an SMA, threshold-edge behavior, and mechanical split-jump detection. Use compact synthetic rows such as:

  ```python
  coverage = merge_reference_rows(
      [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)],
      massive=[bar(date(2024, 1, 3), 20)],
      ib=[bar(date(2024, 1, 2), 10)],
  )
  assert coverage.sources == {date(2024, 1, 2): "ib", date(2024, 1, 3): "massive"}
  assert coverage.unresolved == (date(2024, 1, 4),)
  ```

- [x] **Step 2: Run the tests and verify RED**

  Run: `uv run pytest tests/test_adjusted_history_validation.py -q`

  Expected: collection fails because `clients.adjusted_history_validation` does not exist.

- [x] **Step 3: Implement the pure validation types and functions**

  Use immutable dataclasses. Reject duplicate dates and invalid OHLC before comparison. Compute relative error as `abs(local-reference) / abs(reference) * 10_000`; classify `warning` above 1 bp and `failure` above 5 bp. Moving averages use exactly the preceding N ordered sessions and become unavailable across missing reference dates. Reconstruct split-only rows using split actions only and an independent call to pure factor arithmetic; reconstruct total return from all active effective actions without reading persisted factor rows.

- [x] **Step 4: Run focused tests and refactor while green**

  Run: `uv run pytest tests/test_adjusted_history_validation.py -q`

  Expected: all new tests pass.

- [x] **Step 5: Commit**

  ```bash
  git add clients/adjusted_history_validation.py tests/test_adjusted_history_validation.py
  git commit -m "feat: add adjusted history validation core"
  ```

### Task 2: Massive Adjusted Aggregates and SMA Evidence

**Files:**
- Modify: `clients/massive_client.py`
- Modify: `clients/__init__.py`
- Modify: `tests/test_massive_client.py`

**Interfaces:**
- Consumes: existing authenticated `MassiveClient` session and bounded ticker/date/window requests.
- Produces: `MassiveSMAValue`, `get_daily_bars(..., adjusted=True)` with same-origin pagination, `get_sma(ticker, window, start, end)`, and normalized optional `MassiveDividend.historical_adjustment_factor` evidence.

- [x] **Step 1: Write failing provider-adapter tests**

  Add responses tests proving aggregate `next_url` pagination preserves authentication without accepting cross-origin URLs, SMA nested `results.values` pagination works, timestamps normalize to trade dates, malformed/non-finite SMA values fail, `historical_adjustment_factor` is retained and validated when present, and `adjusted=true` is present on both price endpoints.

- [x] **Step 2: Run tests and verify RED**

  Run: `uv run pytest tests/test_massive_client.py -q -k 'daily_bars or sma'`

  Expected: failures for missing `get_sma`/`MassiveSMAValue` and incomplete aggregate pagination.

- [x] **Step 3: Implement bounded same-origin pagination**

  Add a shared private page iterator that validates HTTPS, `api.massive.com`, strips any `apiKey`, and caps at 100 pages. Normalize SMA rows into:

  ```python
  @dataclass(frozen=True)
  class MassiveSMAValue:
      trade_date: date
      value: float
  ```

  Request `/v1/indicators/sma/{ticker}` with `adjusted=true`, `window`, `series_type=close`, ascending order, timestamp bounds, and `limit=5000`.

- [x] **Step 4: Run the full Massive client tests**

  Run: `uv run pytest tests/test_massive_client.py -q`

  Expected: all tests pass.

- [x] **Step 5: Commit**

  ```bash
  git add clients/massive_client.py clients/__init__.py tests/test_massive_client.py
  git commit -m "feat: fetch Massive adjusted SMA evidence"
  ```

### Task 3: Read-Only Provider Acquisition and Checkpointing

**Files:**
- Create: `livewire_scripts/adjusted_history_sources.py`
- Test: `tests/test_adjusted_history_sources.py`

**Interfaces:**
- Consumes: `MassiveClient`, `IBClient`, local date bounds, corporate actions, host/port, and a cache root.
- Produces: `SourceEvidence`, `ActionEvidence`, `fetch_massive_evidence(...)`, `fetch_massive_action_evidence(...)`, `fetch_ib_evidence(...)`, `load_cached_evidence(...)`, and `write_cached_evidence(...)`.

- [ ] **Step 1: Write failing acquisition/cache tests**

  Prove Massive partial ranges remain partial, fresh split/dividend events are compared with active local revisions, historical dividend factors are checked when present, unavailable/partial action evidence remains explicit, IB requests expand around split boundaries, IB results are filtered back to local bounds, retries/timeouts are isolated per symbol, cache identity includes request/as-of/provider/version, credentials are absent from serialized data, corrupt caches are rejected, and writes are atomic.

- [ ] **Step 2: Run tests and verify RED**

  Run: `uv run pytest tests/test_adjusted_history_sources.py -q`

  Expected: collection fails because the source module does not exist.

- [ ] **Step 3: Implement injectable read-only acquisition**

  Convert provider bars to the validation row contract without importing `BronzeClient`. For IB, qualify `Stock(symbol, "SMART", "USD")`, request one-year daily `TRADES` chunks, deduplicate/sort, stage rows as `source=ib, price_basis=split_adjusted`, classify split events with expanded context, normalize them, and rebuild the split-only comparison series. Implement per-symbol terminal acquisition states `ok`, `empty`, `timeout`, and `error`.

- [ ] **Step 4: Implement content-addressed JSON caches**

  Write to `.<name>.<pid>.tmp`, `fsync`, then `os.replace`. Store provider, symbol, request bounds, actual bounds, retrieved-at, as-of date, validator version, and payload SHA-256. Reject any identity mismatch.

- [ ] **Step 5: Run focused tests**

  Run: `uv run pytest tests/test_adjusted_history_sources.py tests/test_price_basis.py -q -W error::RuntimeWarning`

  Expected: all tests pass with no leaked coroutine warnings.

- [ ] **Step 6: Commit**

  ```bash
  git add livewire_scripts/adjusted_history_sources.py tests/test_adjusted_history_sources.py
  git commit -m "feat: acquire read-only adjusted history evidence"
  ```

### Task 4: Full-Universe Validator, Cursor, and Reports

**Files:**
- Create: `livewire_scripts/validate_adjusted_history.py`
- Create: `tests/test_validate_adjusted_history.py`
- Modify: `scripts/livewire_quality.py`
- Modify: `tests/test_livewire_entrypoints.py`
- Modify: `tasks/todo.md`

**Interfaces:**
- Consumes: Tasks 1-3 APIs, canonical Bronze/Silver/corporate-action Parquet, explicit tickers or full equity discovery, and CLI thresholds.
- Produces: `run(argv, *, massive_factory=..., ib_factory=...) -> int`, per-symbol JSON, run manifest JSON, Markdown summary, and atomic versioned cursor.

- [ ] **Step 1: Write failing end-to-end command tests**

  Build disposable Bronze/Silver/action fixtures and injected provider fakes. Prove Massive-first/IB-fallback coverage, unresolved failure, pointwise failure despite a passing SMA, dividend-factor corruption, input-change detection, checkpoint reuse/invalidation, output-root rejection, JSON/Markdown content, aggregate exit status, and no input hash changes.

- [ ] **Step 2: Run tests and verify RED**

  Run: `uv run pytest tests/test_validate_adjusted_history.py tests/test_livewire_entrypoints.py -q`

  Expected: missing command/module failures.

- [ ] **Step 3: Implement CLI and symbol orchestration**

  Support mutually exclusive `--tickers ...` and `--all-equities`, plus `--data-lake-root`, `--silver-root`, `--output-dir`, `--as-of-date`, `--host`, `--port`, `--workers`, `--warning-bps`, `--failure-bps`, `--resume`, and `--no-ib-fallback`. Resolve and bind all roots before provider calls. Process one symbol atomically and checkpoint after its detail JSON is durable.

- [ ] **Step 4: Implement reports and evidence dimensions**

  Emit ticker outcomes `pass`, `fail`, `unresolved`, `provider-error`, `input-changed`, and `resume-pending`, plus `price_evidence`, `action_reference_status`, `transformation_check`, and `independent_action_check`. Exit zero only when every requested ticker passes. Include exact coverage ranges, missing dates, pointwise/SMA statistics, worst dates, action-boundary evidence, and input/output hashes.

- [ ] **Step 5: Add quality CLI dispatch and active todo tracking**

  Map `validate-adjusted-history` to the new module and add the dependency graph `V1 -> V2 -> V3 -> V4 -> V5` with `depends_on` annotations to `tasks/todo.md`, checking off only completed and verified tasks.

- [ ] **Step 6: Run focused command tests**

  Run: `uv run pytest tests/test_adjusted_history_validation.py tests/test_adjusted_history_sources.py tests/test_validate_adjusted_history.py tests/test_massive_client.py tests/test_livewire_entrypoints.py -q -W error::RuntimeWarning`

  Expected: all tests pass.

- [ ] **Step 7: Commit**

  ```bash
  git add livewire_scripts/validate_adjusted_history.py tests/test_validate_adjusted_history.py scripts/livewire_quality.py tests/test_livewire_entrypoints.py tasks/todo.md
  git commit -m "feat: validate full adjusted equity history"
  ```

### Task 5: Operator Documentation and Complete Verification

**Files:**
- Modify: `README.md`
- Modify: `.codex/project-memory.md`
- Modify: `tasks/todo.md`

**Interfaces:**
- Consumes: completed validator CLI and verification evidence.
- Produces: operator command documentation, durable architecture note, completed task record, and live smoke artifacts outside canonical data.

- [ ] **Step 1: Document operation and evidence limits**

  Add the command, output location, strict full-coverage gate, Massive-first/IB-fallback behavior, resume usage, evidence grades, and explicit statement that IB replay is not independent vendor validation.

- [ ] **Step 2: Run focused and static verification**

  ```bash
  uv run pytest tests/test_adjusted_history_validation.py tests/test_adjusted_history_sources.py tests/test_validate_adjusted_history.py tests/test_massive_client.py tests/test_livewire_entrypoints.py -q -W error::RuntimeWarning
  uv run ruff check clients livewire_scripts scripts tests
  uv run ruff format --check clients livewire_scripts scripts tests
  uv run pyright
  ```

  Expected: zero failures and zero static-analysis errors.

- [ ] **Step 3: Run the CI-equivalent suite**

  Run: `uv run pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning`

  Expected: all tests pass and configured coverage is at least 95 percent.

- [ ] **Step 4: Run a live read-only smoke**

  Hash AAPL/MSFT/NVDA/SPY/PLTR Bronze and current Silver inputs, source `MASSIVE_API_KEY` without printing it, verify IB connectivity at the configured host/port, run the validator into a disposable output directory, then compare input hashes. If entitlement or IB connectivity blocks a source, record the exact `provider-error`/`unresolved` result rather than weakening the gate.

- [ ] **Step 5: Self-review the complete diff and verification artifacts**

  Check the implementation line-by-line against the approved specification, scan for credential leakage and canonical writes, inspect `git diff --check`, and confirm every task has evidence before marking it complete.

- [ ] **Step 6: Commit documentation and verification record**

  ```bash
  git add README.md .codex/project-memory.md tasks/todo.md
  git commit -m "docs: operate adjusted history validation"
  ```
