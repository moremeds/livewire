# Resumable Corporate-Action Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make whole-universe corporate-action reconciliation four-worker, resumable, scope-safe, and auditable before the production Silver split-basis audit.

**Architecture:** Keep the canonical Parquet write path in `CorporateActionStore.reconcile()` and serialize every store/cursor mutation in the command's main thread. Add a small cursor state unit for scope identity and atomic persistence, while long-lived worker loops own independent `MassiveClient` sessions and return fetched events through a bounded queue.

**Tech Stack:** Python 3.13, `argparse`, `dataclasses`, `concurrent.futures.ThreadPoolExecutor`, `queue.Queue`, `threading.Event`, pytest.

## Global Constraints

- Corporate-action Parquet remains the only canonical action write path.
- A symbol is checkpointed only after canonical reconciliation succeeds.
- CLI worker range is `1..16`; ordinary CLI default is `4`.
- Provider retries remain exclusively inside `MassiveClient`.
- Authentication/authorization failures stop new work and leave the cursor incomplete.
- Never advance `silver/revisions/current.json` in this implementation.
- Full configured verification must retain at least 95% coverage.

Dependency graph: `T1 -> T2 -> T3 -> T4`

---

### Task 1: Scope-Safe Atomic Cursor State

**Files:**
- Create: `livewire_scripts/corporate_action_cursor.py`
- Test: `tests/test_corporate_action_cursor.py`

**Interfaces:**
- Consumes: resolved data-lake `Path`, normalized `list[str]`, `full_reconcile: bool`, `dry_run: bool`, and `resume: bool`.
- Produces: `CursorIdentity`, `CorporateActionCursor`, `build_identity()`, `default_cursor_path()`, and `open_cursor()` for Task 2.

- [x] **Step 1: Write failing scope and lifecycle tests**

```python
def test_default_paths_isolate_modes_and_ticker_sets(tmp_path):
    base = build_identity(tmp_path, ["AAPL"], full_reconcile=False, dry_run=False)
    dry = build_identity(tmp_path, ["AAPL"], full_reconcile=False, dry_run=True)
    full = build_identity(tmp_path, ["AAPL"], full_reconcile=True, dry_run=False)
    other = build_identity(tmp_path, ["MSFT"], full_reconcile=False, dry_run=False)
    assert len({default_cursor_path(tmp_path, item) for item in (base, dry, full, other)}) == 4

def test_resume_missing_starts_and_incomplete_cross_date_resumes(tmp_path):
    identity = build_identity(tmp_path, ["AAPL", "MSFT"], full_reconcile=True, dry_run=False)
    path = tmp_path / "cursor.json"
    cursor = open_cursor(path, identity, resume=True, now=_utc(2026, 7, 13))
    cursor.mark_completed("AAPL", now=_utc(2026, 7, 13))
    resumed = open_cursor(path, identity, resume=True, now=_utc(2026, 7, 14))
    assert resumed.completed == {"AAPL"}

def test_completed_or_incompatible_cursor_rejects_resume(tmp_path):
    identity = build_identity(tmp_path, ["AAPL"], full_reconcile=True, dry_run=False)
    path = tmp_path / "cursor.json"
    cursor = open_cursor(path, identity, resume=False, now=_utc(2026, 7, 13))
    cursor.mark_completed("AAPL", now=_utc(2026, 7, 13))
    cursor.mark_run_completed(now=_utc(2026, 7, 13))
    with pytest.raises(ValueError, match="already complete"):
        open_cursor(path, identity, resume=True, now=_utc(2026, 7, 14))
```

- [x] **Step 2: Run the cursor tests and verify RED**

Run: `uv run pytest tests/test_corporate_action_cursor.py -q`

Expected: collection fails because `livewire_scripts.corporate_action_cursor` does not exist.

- [x] **Step 3: Implement identity, validation, and atomic persistence**

```python
@dataclass(frozen=True)
class CursorIdentity:
    schema_version: int
    data_lake_root: str
    ticker_sha256: str
    ticker_count: int
    full_reconcile: bool
    dry_run: bool

@dataclass
class CorporateActionCursor:
    path: Path
    identity: CursorIdentity
    started_at: datetime
    started_on_ny: date
    completed: set[str]
    run_completed_at: datetime | None = None

    def mark_completed(self, ticker: str, *, now: datetime) -> None:
        self.completed.add(ticker)
        self._save()

    def mark_run_completed(self, *, now: datetime) -> None:
        self.run_completed_at = now
        self._save()

def build_identity(root: Path, tickers: list[str], *, full_reconcile: bool,
                   dry_run: bool) -> CursorIdentity:
    normalized = sorted(set(ticker.upper() for ticker in tickers))
    ticker_hash = hashlib.sha256("\n".join(normalized).encode()).hexdigest()
    return CursorIdentity(1, str(root.resolve()), ticker_hash, len(normalized),
                          full_reconcile, dry_run)

def default_cursor_path(root: Path, identity: CursorIdentity) -> Path:
    scope = f"{identity.ticker_sha256}|{int(identity.full_reconcile)}|{int(identity.dry_run)}"
    scope_id = hashlib.sha256(scope.encode()).hexdigest()[:20]
    return root / "cursors" / "corporate_actions" / f"{scope_id}.json"

def open_cursor(path: Path, identity: CursorIdentity, *, resume: bool,
                now: datetime) -> CorporateActionCursor:
    if resume and path.exists():
        cursor = CorporateActionCursor.from_json(path)
        if cursor.identity != identity:
            raise ValueError("corporate-action cursor is incompatible with this run")
        if cursor.run_completed_at is not None:
            raise ValueError("corporate-action cursor is already complete")
        return cursor
    cursor = CorporateActionCursor(
        path=path,
        identity=identity,
        started_at=now,
        started_on_ny=now.astimezone(ZoneInfo("America/New_York")).date(),
        completed=set(),
    )
    cursor._save()
    return cursor
```

`_save()` must write JSON through a same-directory temporary file, flush and `os.fsync()` the file, publish with `os.replace()`, and remove abandoned temporary files. Serialized fields are exactly identity fields, timestamps, sorted `completed`, and `run_completed_at`; credentials are never accepted by this unit.

- [x] **Step 4: Run cursor tests and verify GREEN**

Run: `uv run pytest tests/test_corporate_action_cursor.py -q`

Expected: all cursor tests pass.

- [x] **Step 5: Commit cursor unit**

```bash
git add livewire_scripts/corporate_action_cursor.py tests/test_corporate_action_cursor.py
git commit -m "feat: add corporate action resume cursor"
```

### Task 2: CLI Integration and Sequential Compatibility

**Files:**
- Modify: `livewire_scripts/sync_corporate_actions.py`
- Modify: `tests/test_sync_corporate_actions.py`

**Interfaces:**
- Consumes: Task 1 cursor APIs.
- Produces: CLI flags `--workers`, `--resume`, `--cursor`; effective worker resolution; expanded deterministic summary counters.

- [x] **Step 1: Write failing CLI/counter tests**

```python
def test_worker_range_is_validated():
    with pytest.raises(SystemExit):
        sync_corporate_actions.parse_args(["--tickers", "AAPL", "--workers", "0"])
    with pytest.raises(SystemExit):
        sync_corporate_actions.parse_args(["--tickers", "AAPL", "--workers", "17"])

def test_injected_client_defaults_to_sequential_and_reports_cursor_counts(tmp_path, capsys):
    assert sync_corporate_actions.run(
        ["--tickers", "AAPL"], client=_Client(), store=_Store(), data_lake_root=tmp_path
    ) == 0
    summary = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert summary["requested"] == 1
    assert summary["attempted"] == 1
    assert summary["pending"] == 0
    assert summary["resumed"] == 0
    assert summary["completed"] == 1

def test_injected_client_rejects_explicit_parallel_workers(tmp_path):
    with pytest.raises(ValueError, match="supplied client"):
        sync_corporate_actions.run(
            ["--tickers", "AAPL", "--workers", "2"],
            client=_Client(), store=_Store(), data_lake_root=tmp_path,
        )
```

- [x] **Step 2: Run focused tests and verify RED**

Run: `uv run pytest tests/test_sync_corporate_actions.py -q`

Expected: failures for unknown CLI flags and missing summary keys.

- [x] **Step 3: Implement CLI flags and sequential cursor flow**

```python
def _worker_count(args: argparse.Namespace, *, injected_client: bool) -> int:
    if injected_client and args.workers is None:
        return 1
    workers = 4 if args.workers is None else args.workers
    if injected_client and workers > 1:
        raise ValueError("a supplied client requires --workers 1")
    return workers

def _summary(*, requested: int, attempted: int, resumed: int, completed: int,
             failed: int, counters: dict[str, int]) -> dict[str, int]:
    return {
        "requested": requested,
        "attempted": attempted,
        "pending": requested - resumed - attempted,
        "resumed": resumed,
        "completed": completed,
        "failed": failed,
        **counters,
    }
```

Open/reset the scope cursor before fetching; skip its completed symbols on resume; mark a symbol only after `reconcile()` returns; mark the run complete only when `completed == requested` and `failed == 0`. Preserve the existing injected-client call order and existing action-counter meanings.

- [x] **Step 4: Run command tests and verify GREEN**

Run: `uv run pytest tests/test_sync_corporate_actions.py tests/test_livewire_entrypoints.py -q`

Expected: all tests pass with no warning output.

- [x] **Step 5: Commit sequential integration**

```bash
git add livewire_scripts/sync_corporate_actions.py tests/test_sync_corporate_actions.py
git commit -m "feat: resume corporate action reconciliation"
```

### Task 3: Four-Worker Fetch Engine and Fatal-Error Shutdown

**Files:**
- Modify: `livewire_scripts/sync_corporate_actions.py`
- Modify: `tests/test_sync_corporate_actions.py`

**Interfaces:**
- Consumes: sequential reconciliation/cursor flow from Task 2 and `MassiveAuthError` from `clients.massive_client`.
- Produces: `_fetch_parallel()` iterator and optional `client_factory` injection in `run()`.

- [x] **Step 1: Write failing parallel behavior tests**

```python
def test_four_workers_use_distinct_clients_and_close_them(tmp_path):
    factory = _ClientFactory()
    result = sync_corporate_actions.run(
        ["--tickers", "A", "B", "C", "D", "--workers", "4"],
        client_factory=factory, store=_Store(), data_lake_root=tmp_path,
    )
    assert result == 0
    assert len(factory.clients) == 4
    assert all(client.closed for client in factory.clients)

def test_failed_symbol_is_not_checkpointed_and_resume_retries_it(tmp_path):
    cursor = tmp_path / "cursor.json"
    first = _ClientFactory(fail={"MSFT"})
    assert sync_corporate_actions.run(
        ["--tickers", "AAPL", "MSFT", "--workers", "2", "--cursor", str(cursor)],
        client_factory=first, store=_Store(), data_lake_root=tmp_path,
    ) == 1
    second = _ClientFactory()
    assert sync_corporate_actions.run(
        ["--tickers", "AAPL", "MSFT", "--workers", "2", "--cursor", str(cursor), "--resume"],
        client_factory=second, store=_Store(), data_lake_root=tmp_path,
    ) == 0
    assert second.fetched_symbols == {"MSFT"}

def test_auth_failure_stops_new_work_and_reports_pending(tmp_path, capsys):
    factory = _ClientFactory(auth_fail=True)
    assert sync_corporate_actions.run(
        ["--tickers", *[f"T{i}" for i in range(20)], "--workers", "2"],
        client_factory=factory, store=_Store(), data_lake_root=tmp_path,
    ) == 1
    summary = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert summary["pending"] > 0
    assert summary["requested"] == summary["resumed"] + summary["attempted"] + summary["pending"]
```

- [x] **Step 2: Run parallel tests and verify RED**

Run: `uv run pytest tests/test_sync_corporate_actions.py -q`

Expected: failures because `client_factory` and parallel execution do not exist.

- [x] **Step 3: Implement bounded worker loops and main-thread reconciliation**

```python
@dataclass(frozen=True)
class _FetchResult:
    ticker: str
    events: list[ProviderEvent] | None = None
    error: Exception | None = None

def _fetch_parallel(
    tickers: list[str], *, workers: int, client_factory: Callable[[], MassiveClient]
) -> Iterator[_FetchResult]:
    symbol_queue: Queue[str | None] = Queue()
    result_queue: Queue[_FetchResult | _WorkerDone] = Queue(maxsize=workers)
    stop = Event()
    # Submit exactly `workers` long-lived loops. Each creates/closes one client,
    # fetches until the queue is empty or auth sets `stop`, and emits one done sentinel.
    # The caller drains results until every worker emitted done.
```

Catch ordinary per-symbol failures into `_FetchResult.error`. On `MassiveAuthError`, set the stop event before emitting the error. The main thread increments `attempted` for every emitted symbol result, reconciles successes, checkpoints only after reconcile success, and leaves unstarted symbols as `pending`. Do not wrap Massive calls in another retry loop.

- [x] **Step 4: Run parallel and cursor tests and verify GREEN**

Run: `uv run pytest tests/test_sync_corporate_actions.py tests/test_corporate_action_cursor.py -q -W error::RuntimeWarning`

Expected: all tests pass and no RuntimeWarning is emitted.

- [x] **Step 5: Commit worker engine**

```bash
git add livewire_scripts/sync_corporate_actions.py tests/test_sync_corporate_actions.py
git commit -m "feat: parallelize corporate action fetches"
```

### Task 4: Operator Documentation and Verification

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `.codex/project-memory.md`
- Modify: `tasks/todo.md`

**Interfaces:**
- Consumes: final CLI behavior from Tasks 1-3.
- Produces: operator command and recorded durable runtime behavior.

- [x] **Step 1: Update operator examples and durable facts**

```markdown
python scripts/livewire_ingest.py corporate-actions --workers 4 --resume --full-reconcile
```

Document that default cursors are scope-isolated, only successful reconciliations checkpoint, a completed cursor requires a fresh run, and full reconciliation may infer cancellations.

- [x] **Step 2: Run focused quality gates**

Run: `uv run pytest tests/test_corporate_action_cursor.py tests/test_sync_corporate_actions.py tests/test_corporate_action_store.py tests/test_massive_client.py tests/test_livewire_entrypoints.py -q -W error::RuntimeWarning`

Expected: all focused tests pass.

Run: `uv run ruff check livewire_scripts/corporate_action_cursor.py livewire_scripts/sync_corporate_actions.py tests/test_corporate_action_cursor.py tests/test_sync_corporate_actions.py`

Expected: no lint errors.

Run: `uv run ruff format --check livewire_scripts/corporate_action_cursor.py livewire_scripts/sync_corporate_actions.py tests/test_corporate_action_cursor.py tests/test_sync_corporate_actions.py`

Expected: all files already formatted.

- [x] **Step 3: Run full CI-equivalent verification**

Run: `uv run pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing`

Expected: all tests pass and configured coverage remains at least 95%.

Run: `uv run pytest tests -q -W error::RuntimeWarning`

Expected: all tests pass without RuntimeWarning.

- [x] **Step 4: Self-review the implementation and operational boundary**

Run: `git diff --check && git status --short`

Verify from the final diff that all store writes remain in the main thread, all clients close, cursor publication follows successful reconciliation, no credentials enter output/state, and no production Silver pointer path is changed.

- [x] **Step 5: Commit documentation and verification record**

```bash
git add README.md CLAUDE.md .codex/project-memory.md tasks/todo.md docs/superpowers/specs/2026-07-13-resumable-corporate-action-reconciliation-design.md docs/superpowers/plans/2026-07-13-resumable-corporate-action-reconciliation.md
git commit -m "docs: record resumable action reconciliation"
```
