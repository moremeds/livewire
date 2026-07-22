# IB-verified Yahoo true-raw reconstruction — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an IB cross-verification gate and resumable priority batching to the existing `resolve_yahoo_basis` resolver, so the unknown-basis population can be flipped `unknown → raw` only after the reconstruction is confirmed against IB.

**Architecture:** `resolve_yahoo_basis.py` already reconstructs true-raw from Yahoo, reconciles splits against our action store, classifies each bronze row, and fails closed. This plan adds (1) a pure IB anchor-verdict function in `clients/yahoo_basis.py` that compares the reconstructed series to IB only on the post-last-split window (where IB is definitionally raw), (2) resumable priority batching in `run()`, and (3) IB-connection wiring that mirrors `repair_legacy_basis` exactly (lazy-connect once, connection failure aborts without checkpoint, disconnect in `finally`). `--apply` becomes impossible without `--ib-verify`.

**Tech Stack:** Python 3.13, pyarrow, `ib_async` (via `IBClient`/`IBHistoryFetcher`), pytest + pytest-cov.

## Global Constraints

- **No fabricated market values.** Test fixtures use REAL tickers at REAL frozen prices with an as-of date. Reuse the existing real AMC fixtures in `tests/test_resolve_yahoo_basis.py` (1:10 reverse split 2023-08-24; raw pre-split 1.96, adjusted 19.60). Mocking the IB/Yahoo *client* is allowed; the *values* are real.
- **IB host default `127.0.0.1:4001`.** Never the LAN IP. Never auto-retry an IB connection failure — it aborts the run.
- **Coverage gate ≥95%** (`fail_under = 95`). Run `uv run pytest` (matches CI). `clients/ib_client.py` is exempt but the new code is not.
- **No writer freeze in the apply phase** — bronze writes take `symbol_lock`; only the later `rebuild-silver --full` publish freezes the three writers (operator step, not code).
- **Fail closed = no bronze bytes change.** Every non-verified verdict leaves bronze untouched.
- Commit messages: no `Co-Authored-By`/AI-attribution trailers. Stage explicit paths, never `git add -A`.
- Work stays on branch `feat/ib-verified-basis-reconstruction` (worktree `.worktrees/ib-verified-basis-reconstruction/`).

---

### Task 1: Pure IB anchor-verdict function

Compares a reconstructed true-raw series against IB rows on the post-last-split window. Pure — takes `ib_rows` as an argument, does no fetching, no connection. This is the whole IB-verification logic, isolated for exhaustive unit testing.

**Files:**
- Modify: `clients/yahoo_basis.py` (append; it already holds the pure basis helpers `reconstruct_raw_closes`, `classify_existing_basis`, `_close_match`)
- Test: `tests/test_yahoo_basis.py`

**Interfaces:**
- Consumes: `_close_match(a, b, tol, abs_floor)` (already in this file).
- Produces:
  - `last_split_ex_date(store_splits: list[tuple[date, float]]) -> date | None`
  - `@dataclass(frozen=True) class AnchorVerdict: verified: bool; reason: str; overlap: int; window_start: date | None; mismatches: list[tuple[date, float, float]]`
    (`reason` ∈ `{"verified", "ib_insufficient_overlap", "ib_mismatch"}`)
  - `ib_anchor_verdict(corrected_rows: list[dict], ib_rows: list[dict], *, last_split_ex: date | None, tol: float = 0.02, abs_floor: float = 0.01, min_overlap: int = 5, window_cap: int = 250) -> AnchorVerdict`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_yahoo_basis.py  (append)
from datetime import date
from clients.yahoo_basis import AnchorVerdict, ib_anchor_verdict, last_split_ex_date

# Real frozen AMC raw closes across the 2023-08-24 1:10 reverse split.
_CORRECTED = [
    {"trade_date": date(2023, 8, 23), "close": 1.96},   # raw (pre-split, folded)
    {"trade_date": date(2023, 8, 24), "close": 14.37},  # ex-date
    {"trade_date": date(2023, 8, 25), "close": 12.43},  # post-split
    {"trade_date": date(2023, 8, 28), "close": 11.90},  # post-split
]
_LAST_SPLIT = date(2023, 8, 24)


def test_last_split_ex_date_picks_the_max():
    assert last_split_ex_date([(date(2020, 1, 2), 2.0), (date(2023, 8, 24), 0.1)]) == date(2023, 8, 24)
    assert last_split_ex_date([]) is None


def test_anchor_verified_when_ib_matches_post_split_window():
    ib = [
        {"trade_date": date(2023, 8, 25), "close": 12.43},
        {"trade_date": date(2023, 8, 28), "close": 11.90},
    ]
    v = ib_anchor_verdict(_CORRECTED, ib, last_split_ex=_LAST_SPLIT, min_overlap=2)
    assert v.verified and v.reason == "verified" and v.overlap == 2


def test_anchor_ignores_pre_split_rows_entirely():
    # IB carries a DIFFERENT (adjusted) pre-split value; the anchor must not look before the split.
    ib = [
        {"trade_date": date(2023, 8, 23), "close": 19.60},  # would mismatch if compared — but it is pre-split
        {"trade_date": date(2023, 8, 25), "close": 12.43},
        {"trade_date": date(2023, 8, 28), "close": 11.90},
    ]
    v = ib_anchor_verdict(_CORRECTED, ib, last_split_ex=_LAST_SPLIT, min_overlap=2)
    assert v.verified


def test_anchor_mismatch_when_ib_recent_close_disagrees():
    ib = [
        {"trade_date": date(2023, 8, 25), "close": 12.43},
        {"trade_date": date(2023, 8, 28), "close": 8.00},  # wrong entity / broken reconstruction
    ]
    v = ib_anchor_verdict(_CORRECTED, ib, last_split_ex=_LAST_SPLIT, min_overlap=2)
    assert not v.verified and v.reason == "ib_mismatch"
    assert v.mismatches == [(date(2023, 8, 28), 11.90, 8.00)]


def test_anchor_insufficient_overlap_fails_closed():
    ib = [{"trade_date": date(2023, 8, 25), "close": 12.43}]  # 1 < min_overlap
    v = ib_anchor_verdict(_CORRECTED, ib, last_split_ex=_LAST_SPLIT, min_overlap=5)
    assert not v.verified and v.reason == "ib_insufficient_overlap" and v.overlap == 1


def test_anchor_no_split_uses_full_overlap():
    corrected = [{"trade_date": date(2026, 7, d), "close": c} for d, c in [(13, 100.0), (14, 101.0), (15, 102.0)]]
    ib = [{"trade_date": date(2026, 7, d), "close": c} for d, c in [(13, 100.0), (14, 101.0), (15, 102.0)]]
    v = ib_anchor_verdict(corrected, ib, last_split_ex=None, min_overlap=3)
    assert v.verified and v.overlap == 3
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_yahoo_basis.py -k "anchor or last_split" -v`
Expected: FAIL with `ImportError: cannot import name 'ib_anchor_verdict'`.

- [ ] **Step 3: Write the implementation**

```python
# clients/yahoo_basis.py  (append)

def last_split_ex_date(store_splits: list[tuple[date, float]]) -> date | None:
    """Most recent split ex-date; None when the symbol has no splits."""
    return max((ex for ex, _ in store_splits), default=None)


@dataclass(frozen=True)
class AnchorVerdict:
    """Verdict of comparing a reconstructed true-raw series against IB on the window
    AFTER the last split, where IB is definitionally raw. ``mismatches`` is a small
    sample of ``(date, corrected_close, ib_close)`` for the manifest."""

    verified: bool
    reason: str  # "verified" | "ib_insufficient_overlap" | "ib_mismatch"
    overlap: int
    window_start: date | None
    mismatches: list[tuple[date, float, float]] = field(default_factory=list)


def _as_day(value) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def ib_anchor_verdict(
    corrected_rows: list[dict],
    ib_rows: list[dict],
    *,
    last_split_ex: date | None,
    tol: float = 0.02,
    abs_floor: float = 0.01,
    min_overlap: int = 5,
    window_cap: int = 250,
) -> AnchorVerdict:
    """Confirm the reconstruction against IB on the post-last-split window only.

    IB's basis is unreliable across split boundaries, so anything on or before the
    last split ex-date is ignored. The window is the most recent ``window_cap`` rows
    strictly after the last split (or the whole series when there is no split). The
    verdict is verified only when at least ``min_overlap`` dates overlap IB AND every
    overlapping close matches within tolerance.
    """
    window = sorted(
        (r for r in corrected_rows if last_split_ex is None or _as_day(r["trade_date"]) > last_split_ex),
        key=lambda r: _as_day(r["trade_date"]),
    )[-window_cap:]
    window_start = _as_day(window[0]["trade_date"]) if window else None
    ib_by_day = {_as_day(r["trade_date"]): float(r["close"]) for r in ib_rows}
    overlap_days = [_as_day(r["trade_date"]) for r in window if _as_day(r["trade_date"]) in ib_by_day]
    if len(overlap_days) < min_overlap:
        return AnchorVerdict(False, "ib_insufficient_overlap", len(overlap_days), window_start)
    corrected_by_day = {_as_day(r["trade_date"]): float(r["close"]) for r in window}
    mismatches = [
        (day, corrected_by_day[day], ib_by_day[day])
        for day in overlap_days
        if not _close_match(corrected_by_day[day], ib_by_day[day], tol, abs_floor)
    ]
    if mismatches:
        return AnchorVerdict(False, "ib_mismatch", len(overlap_days), window_start, mismatches[:20])
    return AnchorVerdict(True, "verified", len(overlap_days), window_start)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_yahoo_basis.py -k "anchor or last_split" -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add clients/yahoo_basis.py tests/test_yahoo_basis.py
git commit -m "feat(silver): pure IB anchor-verdict for basis reconstruction"
```

---

### Task 2: Resumable priority batching + failure-manifest input

Adds the batch plumbing to `resolve_yahoo_basis.run` with no IB involvement yet: skip completed symbols on `--resume`, cap work with `--limit`, order by preset priority, and accept a `rebuild-silver` failure manifest as the symbol source.

**Files:**
- Modify: `livewire_scripts/resolve_yahoo_basis.py` (`parse_args`, `_symbols`, `run`)
- Test: `tests/test_resolve_yahoo_basis.py`

**Interfaces:**
- Consumes: `repair_legacy_basis._priority_rank(presets_dir)`, `repair_legacy_basis._order_symbols(symbols, rank)` (import them).
- Produces (new `parse_args` flags): `--resume` (bool), `--limit` (int|None), `--priority-order` (bool), `--presets-dir` (Path, default `Path("presets")`), `--failure-manifest` (Path|None). New helper `_ordered_symbols(args, symbols) -> list[str]`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_resolve_yahoo_basis.py  (append)
import json

def test_failure_manifest_filters_to_split_affected_unknown(tmp_path):
    manifest = tmp_path / "fail.json"
    manifest.write_text(json.dumps({"failed": [
        {"symbol": "AMC", "reason": "unknown price_basis for split-affected row"},
        {"symbol": "KO", "reason": "dividend currency does not match bronze currency"},
    ]}))
    from livewire_scripts.resolve_yahoo_basis import _symbols, parse_args
    args = parse_args(["--failure-manifest", str(manifest), "--output", str(tmp_path / "o.json")])
    assert _symbols(args) == ["AMC"]  # KO's dividend reason is out of scope


def test_limit_caps_processed_symbols(tmp_path):
    for sym, close in [("AMC", 1.96), ("BBW", 20.0)]:
        _seed_bronze(tmp_path, sym, [("2026-07-14", close)], source="legacy", price_basis="unknown")
    out = tmp_path / "m.json"
    resolve_yahoo_basis.run(
        ["--tickers", "AMC", "BBW", "--limit", "1", "--output", str(out)],
        data_lake_root=tmp_path, yahoo_factory=_FakeYahoo, as_of_date=AS_OF,
    )
    assert len(json.loads(out.read_text())["symbols"]) == 1


def test_resume_skips_completed_symbols(tmp_path):
    _seed_bronze(tmp_path, "AMC", [("2023-08-23", 1.96), ("2023-08-24", 14.37), ("2023-08-25", 12.43)],
                 source="legacy", price_basis="unknown")
    _seed_split(tmp_path, "AMC", "2023-08-24", 10, 1)
    out, output_dir = tmp_path / "m.json", tmp_path / "apply"
    common = ["--tickers", "AMC", "--output", str(out), "--apply", "--output-dir", str(output_dir), "--relabel-only"]
    resolve_yahoo_basis.run(common, data_lake_root=tmp_path, yahoo_factory=_FakeYahoo, as_of_date=AS_OF)
    # second run with --resume must skip AMC (already in cursor.completed) and not re-apply
    calls = {"n": 0}
    class _CountingYahoo(_FakeYahoo):
        def get_daily(self, *a, **k):
            calls["n"] += 1
            return super().get_daily(*a, **k)
    resolve_yahoo_basis.run(common + ["--resume"], data_lake_root=tmp_path,
                            yahoo_factory=_CountingYahoo, as_of_date=AS_OF)
    assert calls["n"] == 0  # skipped, never re-fetched


def test_priority_order_orders_by_preset(tmp_path, monkeypatch):
    from livewire_scripts import resolve_yahoo_basis as R
    monkeypatch.setattr(R, "_priority_rank", lambda d: {"BBW": 0, "AMC": 1})
    from livewire_scripts.resolve_yahoo_basis import _ordered_symbols, parse_args
    args = parse_args(["--tickers", "AMC", "BBW", "--priority-order", "--output", str(tmp_path / "o.json")])
    assert _ordered_symbols(args, ["AMC", "BBW"]) == ["BBW", "AMC"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_resolve_yahoo_basis.py -k "failure_manifest or limit or resume_skips or priority_order" -v`
Expected: FAIL (`_symbols`/`_ordered_symbols`/flags not defined).

- [ ] **Step 3: Write the implementation**

Add to `parse_args` (after the existing arguments):

```python
    parser.add_argument("--failure-manifest", type=Path, help="rebuild-silver --failure-output JSON; uses its split-affected unknown-basis failures as the symbol source")
    parser.add_argument("--resume", action="store_true", help="skip symbols already recorded done in cursor.json")
    parser.add_argument("--limit", type=int, help="process at most N not-yet-completed symbols this session")
    parser.add_argument("--priority-order", action="store_true", help="order sp500 -> ndx100 -> r2k -> tail")
    parser.add_argument("--presets-dir", type=Path, default=Path("presets"), help="preset dir for --priority-order")
```

Import the ordering helpers at module top:

```python
from livewire_scripts.repair_legacy_basis import _order_symbols, _priority_rank
```

Extend `_symbols` to read a failure manifest (the reason string Silver raises is `unknown price_basis for split-affected row`):

```python
_SPLIT_UNKNOWN_REASON = "unknown price_basis for split-affected row"

def _symbols(args: argparse.Namespace) -> list[str]:
    if args.tickers:
        return [t.upper() for t in args.tickers]
    if getattr(args, "failure_manifest", None):
        payload = json.loads(args.failure_manifest.read_text())
        return [
            str(f["symbol"]).upper()
            for f in payload.get("failed", [])
            if _SPLIT_UNKNOWN_REASON in str(f.get("reason", ""))
        ]
    if args.symbols_file:
        payload = json.loads(args.symbols_file.read_text())
        raw = payload[args.symbols_key] if isinstance(payload, dict) else payload
        return [str(t).upper() for t in raw]
    raise ValueError("provide --tickers, --symbols-file, or --failure-manifest")


def _ordered_symbols(args: argparse.Namespace, symbols: list[str]) -> list[str]:
    if not args.priority_order:
        return symbols
    rank = _priority_rank(args.presets_dir)  # raises if no preset found (never a silent zero-symbol run)
    return _order_symbols(symbols, rank)
```

In `run`, replace the loop header `for symbol in _symbols(args):` with resume-skip + ordering + limit:

```python
    ordered = _ordered_symbols(args, _symbols(args))
    processed = 0
    for symbol in ordered:
        if args.resume and cursor["completed"].get(symbol, {}).get("status") == "done":
            continue
        if args.limit is not None and processed >= args.limit:
            break
        processed += 1
        # ... existing body (resolve_symbol, apply, cursor write) unchanged ...
```

(The existing `cursor` dict is already initialized in `run`; if a prior `cursor.json` exists under `--output-dir`, load it when `--resume` is set — mirror the load in the existing apply path.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_resolve_yahoo_basis.py -k "failure_manifest or limit or resume_skips or priority_order" -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the full resolver test file (no regressions)**

Run: `uv run pytest tests/test_resolve_yahoo_basis.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add livewire_scripts/resolve_yahoo_basis.py tests/test_resolve_yahoo_basis.py
git commit -m "feat(silver): resumable priority batching + failure-manifest input for basis resolver"
```

---

### Task 3: IB anchor gate wired into apply

Threads a live IB session through `run()` exactly like `repair_legacy_basis`: lazy-connect once, connection failure aborts the run with no checkpoint, disconnect in `finally`. The anchor gate runs for every `would_resolve` symbol before the write; `--apply` is refused without `--ib-verify`.

**Files:**
- Modify: `livewire_scripts/resolve_yahoo_basis.py` (`parse_args`, `run` signature + loop, apply branch)
- Test: `tests/test_resolve_yahoo_basis.py`

**Interfaces:**
- Consumes: `clients.ib_client.IBClient`, `clients.ib_client.IBConnectionError`, `livewire_scripts.adjusted_history_sources.IBHistoryFetcher`, `clients.yahoo_basis.ib_anchor_verdict`, `clients.yahoo_basis.last_split_ex_date`, `_store_split_ratios` (already in this module), `_Resolution.corrected` / `.actions` (already produced by `_resolve`).
- Produces: `run(..., ib_factory: Callable[[], Any] = IBClient, ib_fetcher_factory: Callable[[Any], Callable[[str, date, date], list[dict]]] = IBHistoryFetcher)`. New flags `--ib-verify`, `--ib-host` (default `MDW_IB_HOST` or `127.0.0.1`), `--ib-port` (default `MDW_IB_PORT` or `4001`), `--ib-tolerance` (float, default `0.02`), `--ib-window-cap` (int, default `250`), `--ib-min-overlap` (int, default `5`). Return code: `1` on abort, else `0`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_resolve_yahoo_basis.py  (append)

# Real frozen AMC IB closes on the post-split window (raw by construction there).
_AMC_IB_MATCH = [
    {"trade_date": date(2023, 8, 25), "close": 12.43},
    {"trade_date": date(2023, 8, 28), "close": 11.90},
    {"trade_date": date(2023, 8, 29), "close": 11.50},
    {"trade_date": date(2023, 8, 30), "close": 11.20},
    {"trade_date": date(2023, 8, 31), "close": 11.00},
]

def _fetcher(rows):
    return lambda client: (lambda symbol, start, end: [r for r in rows if start <= r["trade_date"] <= end])

class _FakeIB:
    def connect(self, **k): pass
    def disconnect(self): pass

def _seed_amc_multi(tmp_path):
    closes = [("2023-08-23", 19.60), ("2023-08-24", 14.37), ("2023-08-25", 12.43), ("2023-08-28", 11.90),
              ("2023-08-29", 11.50), ("2023-08-30", 11.20), ("2023-08-31", 11.00)]
    _seed_bronze(tmp_path, "AMC", closes, source="legacy", price_basis="unknown")
    _seed_split(tmp_path, "AMC", "2023-08-24", 10, 1)


def test_apply_requires_ib_verify(tmp_path):
    _seed_bronze(tmp_path, "AMC", [("2023-08-23", 1.96)], source="legacy", price_basis="unknown")
    with pytest.raises(ValueError, match="ib-verify"):
        resolve_yahoo_basis.run(
            ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply",
             "--output-dir", str(tmp_path / "a"), "--allow-rewrite"],
            data_lake_root=tmp_path, yahoo_factory=_FakeYahoo, as_of_date=AS_OF,
        )


def test_ib_verified_symbol_is_written(tmp_path):
    _seed_amc_multi(tmp_path)
    yahoo = _FakeYahoo(
        bars=[YahooBar(date.fromisoformat(d), c, c) for d, c in
              [("2023-08-23", 19.60), ("2023-08-25", 12.43), ("2023-08-28", 11.90),
               ("2023-08-29", 11.50), ("2023-08-30", 11.20), ("2023-08-31", 11.00)]],
        splits=_AMC_SPLIT)
    rc = resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply",
         "--output-dir", str(tmp_path / "a"), "--allow-rewrite", "--ib-verify", "--ib-min-overlap", "5"],
        data_lake_root=tmp_path, yahoo_factory=lambda: yahoo,
        ib_factory=_FakeIB, ib_fetcher_factory=_fetcher(_AMC_IB_MATCH), as_of_date=AS_OF,
    )
    assert rc == 0
    assert _bronze_basis(tmp_path, "AMC") == {"raw"}  # published


def test_ib_mismatch_leaves_bronze_untouched(tmp_path):
    _seed_amc_multi(tmp_path)
    bad_ib = [{**r, "close": r["close"] * 0.5} for r in _AMC_IB_MATCH]  # wrong entity
    entry_out = tmp_path / "m.json"
    rc = resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(entry_out), "--apply",
         "--output-dir", str(tmp_path / "a"), "--allow-rewrite", "--ib-verify", "--ib-min-overlap", "5"],
        data_lake_root=tmp_path, yahoo_factory=_FakeYahoo,
        ib_factory=_FakeIB, ib_fetcher_factory=_fetcher(bad_ib), as_of_date=AS_OF,
    )
    assert rc == 0
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}  # NOT written
    entry = next(s for s in json.loads(entry_out.read_text())["symbols"] if s["symbol"] == "AMC")
    assert entry["ib_verdict"] == "ib_mismatch"


def test_ib_connection_failure_aborts_without_checkpoint(tmp_path):
    _seed_amc_multi(tmp_path)
    class _DeadIB:
        def connect(self, **k):
            from clients.ib_client import IBConnectionError
            raise IBConnectionError("gateway down / 2FA")
        def disconnect(self): pass
    output_dir = tmp_path / "a"
    rc = resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply",
         "--output-dir", str(output_dir), "--allow-rewrite", "--ib-verify"],
        data_lake_root=tmp_path, yahoo_factory=_FakeYahoo,
        ib_factory=_DeadIB, ib_fetcher_factory=_fetcher(_AMC_IB_MATCH), as_of_date=AS_OF,
    )
    assert rc == 1  # aborted
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}  # untouched
    cursor = output_dir / "cursor.json"
    assert not cursor.is_file() or "AMC" not in json.loads(cursor.read_text()).get("completed", {})
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_resolve_yahoo_basis.py -k "ib_verify or ib_verified or ib_mismatch or ib_connection" -v`
Expected: FAIL (flags / `ib_factory` kwarg / abort return not implemented).

- [ ] **Step 3: Write the implementation**

Add imports at module top:

```python
from clients.ib_client import IBClient, IBConnectionError
from livewire_scripts.adjusted_history_sources import IBHistoryFetcher
from clients.yahoo_basis import ib_anchor_verdict, last_split_ex_date
```

Add the flags in `parse_args`:

```python
    parser.add_argument("--ib-verify", action="store_true", help="confirm each reconstruction against IB on the post-last-split window before writing")
    parser.add_argument("--ib-host", default=os.environ.get("MDW_IB_HOST", "127.0.0.1"))
    parser.add_argument("--ib-port", type=int, default=int(os.environ.get("MDW_IB_PORT", "4001")))
    parser.add_argument("--ib-tolerance", type=float, default=0.02)
    parser.add_argument("--ib-window-cap", type=int, default=250)
    parser.add_argument("--ib-min-overlap", type=int, default=5)
```

Enforce the requirement in `run` (AFTER the existing `--relabel-only`/`--allow-rewrite` check, so `test_apply_without_relabel_only_is_refused` still trips on "relabel-only" first):

```python
        if not (args.relabel_only or args.allow_rewrite):
            raise ValueError("--apply requires --relabel-only or --allow-rewrite")
        if not args.ib_verify:
            raise ValueError("--apply requires --ib-verify (no publish without IB confirmation)")
```

Change the `run` signature:

```python
def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    yahoo_factory: Callable[[], object] = YahooClient,
    ib_factory: Callable[[], object] = IBClient,
    ib_fetcher_factory: Callable[[object], Callable[[str, date, date], list[dict]]] = IBHistoryFetcher,
    as_of_date: date | None = None,
) -> int:
```

Add an anchor helper near `_resolve`:

```python
def _anchor_ok(resolution: _Resolution, *, fetcher, as_of, tol, window_cap, min_overlap) -> tuple[bool, str]:
    """Fetch IB for the post-last-split window and confirm the reconstruction. IB is a
    gate, never written into bronze."""
    corrected = resolution.corrected or []
    last_ex = last_split_ex_date(_store_split_ratios(resolution.actions))
    start = min(_as_date(r["trade_date"]) for r in corrected)
    ib_rows = fetcher(resolution.result["symbol"], start, as_of)
    verdict = ib_anchor_verdict(
        corrected, ib_rows, last_split_ex=last_ex,
        tol=tol, min_overlap=min_overlap, window_cap=window_cap,
    )
    return verdict.verified, verdict.reason
```

In the apply branch of the loop, gate the write. Mirror `repair_legacy_basis`'s lazy-connect/abort (`ib_client`, `fetcher`, `aborted` locals initialised before the loop; `disconnect` in a `finally`):

```python
        if args.apply and entry["status"] == "would_resolve":
            if args.ib_verify:
                if fetcher is None:
                    try:
                        ib_client = ib_factory()
                        ib_client.connect(host=args.ib_host, port=args.ib_port)
                        fetcher = ib_fetcher_factory(ib_client)
                    except Exception as exc:  # connection failure ABORTS — never a per-symbol verdict
                        print(f"IB connection failed, aborting run: {exc}", file=sys.stderr)
                        aborted = True
                        break
                try:
                    resolution = _resolve(symbol, bronze=bronze, store=store, yahoo=yahoo, as_of=as_of)
                    ok, reason = _anchor_ok(resolution, fetcher=fetcher, as_of=as_of,
                                            tol=args.ib_tolerance, window_cap=args.ib_window_cap,
                                            min_overlap=args.ib_min_overlap)
                except (IBConnectionError, ConnectionError, OSError, TimeoutError) as exc:
                    print(f"IB session lost mid-run, aborting run: {exc}", file=sys.stderr)
                    entry["ib_verdict"] = f"ib_error: {exc}"
                    aborted = True
                    break  # leave this symbol uncheckpointed for --resume
                entry["ib_verdict"] = reason
                if not ok:
                    entry["applied"] = "withheld_ib"  # NOT written; review queue
                    counts[entry["status"]] = counts.get(entry["status"], 0) + 1
                    results.append(entry)
                    continue
            # ... existing apply (apply_rewrite / apply_relabel_only) + cursor write unchanged ...
```

Wrap the loop in `try/finally` that disconnects `ib_client` if set, and change the final `return 0` to `return 1 if aborted else 0`.

- [ ] **Step 4: Update the existing apply tests to pass `--ib-verify`**

The three existing apply tests (`test_apply_relabel_only_flips_basis...`, `test_apply_rewrite_rewrites_full_ohlcv...`, `test_relabel_only_defers_rewrite_symbol`) now hit the new `--apply requires --ib-verify` guard. Add `"--ib-verify", "--ib-min-overlap", "1"` to each `run([...])` arg list and pass `ib_factory=_FakeIB, ib_fetcher_factory=_fetcher(_AMC_IB_MATCH)` so the anchor verifies. (`test_relabel_only_defers_rewrite_symbol` defers before any write, so its `unknown` assertion still holds.)

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_resolve_yahoo_basis.py -v`
Expected: all PASS (existing + 4 new IB tests).

- [ ] **Step 6: Run with the leaked-coroutine guard**

Run: `uv run pytest tests/test_resolve_yahoo_basis.py -W error::RuntimeWarning -v`
Expected: all PASS (CLAUDE.md rule 6 for async-runner mocks).

- [ ] **Step 7: Commit**

```bash
git add livewire_scripts/resolve_yahoo_basis.py tests/test_resolve_yahoo_basis.py
git commit -m "feat(silver): IB anchor gate for basis reconstruction, mandatory on --apply"
```

---

### Task 4: Register the subcommand + document it

Wire `resolve-yahoo-basis` into the `livewire_store.py` dispatcher and document the command + rollout in CLAUDE.md.

**Files:**
- Modify: `scripts/livewire_store.py` (the `COMMANDS` dict)
- Modify: `CLAUDE.md` (the operator command list under "Running the pipeline")
- Test: `tests/test_livewire_entrypoints.py` (holds the store dispatch tests, e.g. `test_store_dispatches_repair_legacy_basis` — mirror it)

**Interfaces:**
- Consumes: `livewire_scripts.resolve_yahoo_basis.main` (already exists).
- Produces: dispatchable `python scripts/livewire_store.py resolve-yahoo-basis …`.

- [ ] **Step 1: Write the failing test**

Mirror the existing `test_store_dispatches_repair_legacy_basis` convention (monkeypatch `import_module`, assert the sentinel return `7` and the module+argv the dispatcher forwarded):

```python
# tests/test_livewire_entrypoints.py  (append)
def test_store_dispatches_resolve_yahoo_basis(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_store.importlib, "import_module", lambda name: _fake_module(calls, name, accepts_argv=True)
    )
    assert livewire_store.main(["resolve-yahoo-basis", "--tickers", "AMC", "--output", "m.json"]) == 7
    assert calls == [("livewire_scripts.resolve_yahoo_basis", ["--tickers", "AMC", "--output", "m.json"])]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_livewire_entrypoints.py -k resolve_yahoo_basis -v`
Expected: FAIL (dispatcher does not know `resolve-yahoo-basis`).

- [ ] **Step 3: Register the subcommand**

In `scripts/livewire_store.py`, add to `COMMANDS` (next to `"repair-legacy-basis"`):

```python
    "resolve-yahoo-basis": "livewire_scripts.resolve_yahoo_basis",
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/test_livewire_entrypoints.py -k resolve_yahoo_basis -v`
Expected: PASS.

- [ ] **Step 5: Document in CLAUDE.md**

Under "Running the pipeline", after the `rebuild-silver` lines, add:

```markdown
python scripts/livewire_store.py resolve-yahoo-basis --failure-manifest <.../rev-dry.json> --output <lake>/repairs/unknown-basis/<stamp>/manifest.json   # dry-run: reconstruct + self-gate the split-affected unknown-basis failures
python scripts/livewire_store.py resolve-yahoo-basis --failure-manifest <.../rev-dry.json> --output <.../manifest.json> --apply --output-dir <.../batch1> --allow-rewrite --ib-verify --priority-order --resume   # apply: publish only IB-anchor-verified reconstructions (2FA-gated)
```

And add a short subsection describing: the IB anchor gate (post-last-split window only), `--apply` requiring `--ib-verify`, the review-queue verdicts, and the batch-1 → STOP-GATE → 12K rollout (reference the spec).

- [ ] **Step 6: Full suite + coverage gate**

Run: `uv run pytest tests/ -m "not integration" --cov=clients --cov=scripts -q`
Expected: all PASS, coverage ≥95%. (Deselect the two time-bomb integration tests per project memory if the full run hangs.)

- [ ] **Step 7: Commit**

```bash
git add scripts/livewire_store.py CLAUDE.md tests/test_livewire_entrypoints.py
git commit -m "feat(silver): register resolve-yahoo-basis subcommand + document IB-verified rollout"
```

---

## Rollout (operator, after the code lands and CI is green)

Not part of the coding tasks — the operator runs these once the PR merges. Per the spec §6:

1. Regenerate batch-1: `rebuild-silver --full --dry-run --failure-output /tmp/rev-dry.json` → the split-affected unknown-basis failures are batch-1's input.
2. Dry-run resolve (self-gate only, no IB) → review how many clear the gate.
3. `--apply --ib-verify --priority-order --resume` (2FA-gated; `--limit` to chunk sessions).
4. Freeze the three writers → `rebuild-silver --full` (NO `--allow-window-regression`) → reload writers.
5. apex adjusted-mode canary on a sample of newly published symbols.
6. STOP GATE: measure published vs review-queue ratio before committing IB time to the ~12K tail.

## Self-Review

- **Spec coverage:** IB anchor gate on post-last-split window → Task 1 + Task 3. Subcommand registration → Task 4. `--apply` requires `--ib-verify` → Task 3 Step 3. Resumable priority batching + failure-manifest input → Task 2. Verdict/review-queue (non-verified leaves bronze untouched) → Task 3 (`withheld_ib`, `continue` without write) + reuse of the resolver's existing fail-closed statuses. No-downgrade invariants (IB unreachable → abort/return 1/no checkpoint) → Task 3 Step 1 `test_ib_connection_failure_aborts_without_checkpoint` + Step 3. Testing with real frozen fixtures → all tasks reuse real AMC values. Rollout → documented, operator-run. Dividend-currency failures explicitly out of scope → not touched.
- **Placeholder scan:** none — every code step shows full code; the only "fill at authoring time" is naming real fixtures, and all fixtures reuse already-frozen real AMC values.
- **Type consistency:** `ib_anchor_verdict`/`AnchorVerdict`/`last_split_ex_date` names identical across Task 1 (def) and Task 3 (use). `_fetcher`/`_FakeIB`/`_AMC_IB_MATCH` defined in Task 3 tests before use. `run()` kwargs `ib_factory`/`ib_fetcher_factory` match `repair_legacy_basis`. `_store_split_ratios`, `_Resolution.corrected/.actions`, `_as_date` are existing symbols in `resolve_yahoo_basis.py`.
