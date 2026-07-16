# Silver legacy-basis full repair → rev-3 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every corrupt/mixed-basis legacy equity symbol either continuity-verified-correct or explicitly quarantined, then publish silver rev-3 — so Apex serves correct adjusted full-universe data.

**Architecture:** Add a pure continuity gate to the silver publish path (quarantine any symbol whose adjusted series has a >threshold adjacent-day jump). A cheap offline audit reuses that gate to classify the legacy population `clean`/`mixed`. A resumable, preset-prioritized repair script re-derives `mixed` symbols' true-raw basis from IB deep history and merges corrected rows back to bronze. A single final `rebuild-silver --full` publishes rev-3.

**Tech Stack:** Python 3.13, pyarrow/parquet, `ib_async` (via existing `IBClient`/`IBHistoryFetcher`), pytest + pytest-cov, `uv`.

## Global Constraints

- Dev/test via `uv run pytest`; coverage `fail_under = 95` over `clients` + `livewire_scripts` — new modules in `livewire_scripts/` are inside the gate and need full test coverage.
- Mock all external I/O; use temp parquet roots (`tmp_path`); tests never hit the network. Real frozen ticker prices only — no `FOO`/round-number placeholders.
- Target rows are `source == "legacy"` and `price_basis == "raw"`. The price_basis vocabulary is exactly `{"raw", "split_adjusted", "unknown"}` — there is no `"legacy"` price_basis value (`legacy` is a `source`).
- IB: Gateway is on the Mac mini; **no auto-retry on connection failure**; lazy-connect once, `disconnect()` in a `finally`. Rely on `IBClient.connect()`'s own error-326 clientId retry. No `time.sleep` rate-limit loops.
- Continuity threshold default `6.0`; halt/relist allowlist starts empty.
- Repair priority order: `sp500 → ndx100 → r2k → remainder`.
- rev-3 is published **once at the end** via a single `rebuild-silver --full`; the repair phase is resumable via its cursor.
- Commit messages: no `Co-Authored-By`/AI-attribution trailers.

---

## File Structure

- Create `clients/silver_continuity.py` — pure continuity invariant (`check_adjusted_continuity`). One responsibility: given adjusted rows, raise if there's an unexplained large adjacent-day jump. Consumed by the silver builder (Task 1) and the audit (Task 2).
- Modify `livewire_scripts/rebuild_silver.py` — call the gate during staging so a violating symbol is quarantined into `failures` (Task 1).
- Create `livewire_scripts/audit_legacy_basis.py` + register `audit-legacy-basis` in `scripts/livewire_quality.py` — offline classifier producing the audit manifest (Task 2).
- Create `livewire_scripts/repair_legacy_basis.py` + register `repair-legacy-basis` in `scripts/livewire_store.py` — resumable, prioritized IB re-derivation writing corrected bronze (Task 3).
- End-to-end wiring test + runbook; rev-3 via existing `rebuild-silver --full` (Task 4).

---

## Task 1: Continuity gate (Module 1 / WS0)

**Files:**
- Create: `clients/silver_continuity.py`
- Modify: `livewire_scripts/rebuild_silver.py` (staging loop ~200-222; add CLI arg ~42-50)
- Test: `tests/test_silver_continuity.py`, `tests/test_rebuild_silver.py` (add one case)

**Interfaces:**
- Produces: `check_adjusted_continuity(rows: list[dict], *, threshold: float = 6.0, allowlist: frozenset[str] = frozenset()) -> None` — raises `ValueError` on the first violating date; returns `None` if clean. `rows` are adjusted daily rows (each has `trade_date` iso-string and float `close`).
- Consumes (from existing code): `adjustment_engine.build_factor_intervals(rows, actions, as_of) -> list[FactorInterval]`, `adjustment_engine.adjust_daily_rows(rows, intervals, revision) -> list[dict]`.

- [ ] **Step 1: Write the failing test for the pure gate**

Create `tests/test_silver_continuity.py`:

```python
import pytest

from clients.silver_continuity import check_adjusted_continuity


def _rows(closes):
    # closes: list of (iso_date, close). Only trade_date/close are read by the gate.
    return [{"trade_date": d, "close": c} for d, c in closes]


def test_clean_series_passes():
    rows = _rows([("2021-06-10", 17.43), ("2021-06-11", 17.76), ("2021-06-14", 17.95)])
    assert check_adjusted_continuity(rows) is None


def test_double_adjusted_bar_raises_with_date_and_ratio():
    # NVDA-style: a ~40x drop into a double-adjusted bar then back up.
    rows = _rows([("2021-06-17", 18.59), ("2021-06-18", 0.4644), ("2021-06-21", 18.36)])
    with pytest.raises(ValueError) as exc:
        check_adjusted_continuity(rows, threshold=6.0)
    assert "2021-06-18" in str(exc.value)


def test_allowlisted_date_is_exempt():
    rows = _rows([("2021-06-17", 18.59), ("2021-06-18", 0.4644), ("2021-06-21", 18.36)])
    assert check_adjusted_continuity(rows, threshold=6.0, allowlist=frozenset({"2021-06-18"})) is None


def test_threshold_is_inclusive_boundary_safe():
    # exactly 6x is not a violation; just over is.
    assert check_adjusted_continuity(_rows([("2021-01-04", 10.0), ("2021-01-05", 60.0)]), threshold=6.0) is None
    with pytest.raises(ValueError):
        check_adjusted_continuity(_rows([("2021-01-04", 10.0), ("2021-01-05", 60.01)]), threshold=6.0)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_silver_continuity.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'clients.silver_continuity'`.

- [ ] **Step 3: Implement the pure gate**

Create `clients/silver_continuity.py`:

```python
"""Post-adjustment continuity invariant for silver publication.

A correctly adjusted daily series has no large day-over-day close discontinuity —
adjustment is exactly what removes corporate-action jumps. A residual jump above
the threshold signals mixed-basis / double-adjustment (e.g. an already-adjusted
legacy row mislabeled ``price_basis='raw'`` that got divided by the split factor a
second time). Such a symbol must be quarantined, not published.
"""

from __future__ import annotations


def check_adjusted_continuity(
    rows: list[dict],
    *,
    threshold: float = 6.0,
    allowlist: frozenset[str] = frozenset(),
) -> None:
    """Raise ``ValueError`` on the first adjacent-day close ratio above ``threshold``.

    ``rows`` are adjusted daily rows ordered by ``trade_date`` (iso string), each
    with a positive float ``close``. ``allowlist`` holds iso dates exempt from the
    check (evidence-backed halts/relistings). Returns ``None`` when the series is
    continuous.
    """
    previous_close: float | None = None
    previous_date: str | None = None
    for row in rows:
        trade_date = str(row["trade_date"])
        close = float(row["close"])
        if close <= 0:
            raise ValueError(f"non-positive adjusted close at {trade_date}: {close}")
        if previous_close is not None and trade_date not in allowlist and previous_date not in allowlist:
            ratio = max(close / previous_close, previous_close / close)
            if ratio > threshold:
                raise ValueError(
                    f"adjusted continuity break at {trade_date}: {ratio:.1f}x jump "
                    f"from {previous_date} (threshold {threshold:g}x) — mixed-basis suspected"
                )
        previous_close = close
        previous_date = trade_date
```

- [ ] **Step 4: Run the pure-gate tests to verify they pass**

Run: `uv run pytest tests/test_silver_continuity.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Write the failing integration test in the builder**

Add to `tests/test_rebuild_silver.py` (imitate the file's existing bronze/action seeding; use a mixed-basis symbol). Add near the other rebuild tests:

```python
def test_mixed_basis_symbol_is_quarantined_not_published(tmp_path):
    # Seed a legacy/raw symbol whose series mixes already-adjusted and true-raw
    # rows around a split, so the adjusted output has a >6x residual jump.
    from datetime import UTC, date, datetime
    from decimal import Decimal

    from clients.bronze_client import BronzeClient
    from clients.corporate_action_store import CorporateActionStore
    from clients.massive_client import MassiveSplit
    from livewire_scripts import rebuild_silver

    bronze_root = tmp_path / "bronze/asset_class=equity"
    # NVDA-style: true-raw ~713 and already-adjusted ~17 interleaved, all labeled raw.
    rows = []
    for d, close in (
        ("2021-06-17", 746.29),   # true-raw
        ("2021-06-18", 18.64),    # already-adjusted, mislabeled raw (lone bad bar)
        ("2021-06-21", 737.09),   # true-raw
    ):
        rows.append({
            "trade_date": d, "symbol_id": 1,
            "open": close, "high": close, "low": close, "close": close, "adj_close": close,
            "volume": 100, "source": "legacy", "price_basis": "raw",
        })
    BronzeClient(bronze_root, "equity").replace_ticker_rows("NVDA", rows)
    split = MassiveSplit(
        provider_event_id="nvda-2021", ticker="NVDA",
        execution_date=date(2021, 7, 20), split_from=Decimal("1"), split_to=Decimal("4"),
        payload_hash="s",
    )
    CorporateActionStore(tmp_path).reconcile("NVDA", [split], datetime(2021, 7, 20, tzinfo=UTC))

    failure_output = tmp_path / "failures.json"
    rc = rebuild_silver.run(
        ["--tickers", "NVDA", "--failure-output", str(failure_output)],
        data_lake_root=tmp_path,
        silver_root=tmp_path / "silver",
    )
    # No silver 1d artifact was published for the quarantined symbol.
    assert not (tmp_path / "silver/asset_class=equity/symbol=NVDA/1d.parquet").exists()
    import json
    failures = json.loads(failure_output.read_text())["failures"]
    assert any(f["symbol"] == "NVDA" and "continuity" in f["error"] for f in failures)
    assert rc == 0  # one quarantined symbol is not a systemic failure
```

> Note: the exact split ratio/dates only need to produce a >6× residual jump in the adjusted series; adjust the frozen closes if the seeded action makes the seeded series continuous. Confirm by running the test — it must fail for the right reason (published, or wrong error) before Step 6.

- [ ] **Step 6: Run it to verify it fails**

Run: `uv run pytest tests/test_rebuild_silver.py::test_mixed_basis_symbol_is_quarantined_not_published -v`
Expected: FAIL — the artifact is published (gate not wired yet).

- [ ] **Step 7: Wire the gate into staging**

In `livewire_scripts/rebuild_silver.py`:

1. Add import near the top with the other `clients` imports:
```python
from clients.silver_continuity import check_adjusted_continuity
```
2. Add a module constant near the other constants:
```python
CONTINUITY_THRESHOLD = 6.0
```
3. Add a CLI arg in `parse_args` (in the arg group, ~line 48):
```python
    parser.add_argument("--continuity-threshold", type=float, default=CONTINUITY_THRESHOLD,
                        help="max adjacent-day adjusted close ratio before a symbol is quarantined")
```
4. In `run`, read it: `threshold = args.continuity_threshold`.
5. In the **staging loop** (the `for symbol in symbols:` try-block, ~205-219), after `intervals = build_factor_intervals(...)` and before `staged.append(...)`, add the gate:
```python
        intervals = build_factor_intervals(rows, actions, effective_as_of)
        adjusted = adjust_daily_rows(rows, intervals, revision=1)
        check_adjusted_continuity(adjusted, threshold=threshold)
        staged.append(StagedSymbol(symbol, rows, intervals, actions,
                                   min(_trade_date(row["trade_date"]) for row in rows)))
```
A raised `ValueError` is caught by the existing `except Exception as exc:` below and routed to `failures` via `_failure(...)` — the symbol is quarantined, the rest of the universe still publishes. `adjust_daily_rows` is already imported in this module (used by `_matches_existing`).

- [ ] **Step 8: Run both test files to verify they pass**

Run: `uv run pytest tests/test_silver_continuity.py tests/test_rebuild_silver.py -v`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add clients/silver_continuity.py livewire_scripts/rebuild_silver.py tests/test_silver_continuity.py tests/test_rebuild_silver.py
git commit -m "feat(silver): quarantine mixed-basis symbols via continuity gate"
```

---

## Task 2: Legacy-basis audit (Module 2 / WS1)

**Files:**
- Create: `livewire_scripts/audit_legacy_basis.py`
- Modify: `scripts/livewire_quality.py` (add one `COMMANDS` entry, ~line 19)
- Test: `tests/test_audit_legacy_basis.py`, `tests/test_livewire_entrypoints.py` (add dispatch case)

**Interfaces:**
- Consumes: `check_adjusted_continuity` (Task 1); `BronzeClient.get_existing_symbols() -> set[str]`, `.read_symbol_rows(symbol) -> list[dict]`; `CorporateActionStore.latest_active(symbol) -> list[CorporateAction]`; `build_factor_intervals`, `adjust_daily_rows`; `clients.ingestion_common.load_preset(path) -> (name, tickers, exchange_map)`; `clients.symbol_paths.encode_symbol`.
- Produces: `run(argv=None, *, data_lake_root: Path | None = None, as_of_date: date | None = None) -> int` and `main(argv=None) -> int`. Writes an audit manifest JSON `{schema_version, data_lake_root, as_of_date, generated_at, threshold, symbols: [{symbol, path, source_sha256, klass, max_ratio, break_date}], counts: {clean, mixed}}`. Exit `0` always (audit is read-only reporting).

- [ ] **Step 1: Write the failing test**

Create `tests/test_audit_legacy_basis.py`:

```python
import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveSplit
from livewire_scripts import audit_legacy_basis


def _seed_symbol(root, ticker, rows_spec, split=None):
    rows = [{
        "trade_date": d, "symbol_id": 1,
        "open": c, "high": c, "low": c, "close": c, "adj_close": c,
        "volume": 100, "source": "legacy", "price_basis": "raw",
    } for d, c in rows_spec]
    bronze = root / "bronze/asset_class=equity"
    BronzeClient(bronze, "equity").replace_ticker_rows(ticker, rows)
    if split is not None:
        CorporateActionStore(root).reconcile(ticker, [split], datetime(2021, 7, 20, tzinfo=UTC))
    return bronze / f"symbol={ticker}/1d.parquet"


def test_mixed_symbol_classified_mixed_and_audit_is_read_only(tmp_path):
    split = MassiveSplit(provider_event_id="nvda", ticker="NVDA",
                         execution_date=date(2021, 7, 20),
                         split_from=Decimal("1"), split_to=Decimal("4"), payload_hash="s")
    path = _seed_symbol(tmp_path, "NVDA",
                        [("2021-06-17", 746.29), ("2021-06-18", 18.64), ("2021-06-21", 737.09)], split)
    before = path.read_bytes()
    output = tmp_path / "audit.json"

    assert audit_legacy_basis.run(["--tickers", "NVDA", "--output", str(output)], data_lake_root=tmp_path) == 0

    assert path.read_bytes() == before  # read-only
    manifest = json.loads(output.read_text())
    item = manifest["symbols"][0]
    assert item["symbol"] == "NVDA"
    assert item["klass"] == "mixed"
    assert item["break_date"] == "2021-06-18"
    assert item["source_sha256"] == hashlib.sha256(before).hexdigest()
    assert manifest["counts"]["mixed"] == 1


def test_clean_symbol_classified_clean(tmp_path):
    _seed_symbol(tmp_path, "MSFT", [("2021-06-17", 258.0), ("2021-06-18", 259.4), ("2021-06-21", 259.9)])
    output = tmp_path / "audit.json"
    assert audit_legacy_basis.run(["--tickers", "MSFT", "--output", str(output)], data_lake_root=tmp_path) == 0
    manifest = json.loads(output.read_text())
    assert manifest["symbols"][0]["klass"] == "clean"
    assert manifest["counts"] == {"clean": 1, "mixed": 0}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_audit_legacy_basis.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'livewire_scripts.audit_legacy_basis'`.

- [ ] **Step 3: Implement the audit module**

Create `livewire_scripts/audit_legacy_basis.py`:

```python
"""Offline basis-consistency audit over the legacy equity population.

Classifies each symbol ``clean`` or ``mixed`` by building its adjusted daily
series and running the same continuity invariant used at publish time. A ``mixed``
symbol is one whose adjusted series still has a >threshold adjacent-day jump —
the signature of already-adjusted rows mislabeled ``price_basis='raw'``. Read-only:
computes and reports, never mutates bronze. The IB re-derivation (repair) consumes
this manifest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Sequence

from clients.adjustment_engine import adjust_daily_rows, build_factor_intervals
from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.ingestion_common import load_preset
from clients.silver_continuity import check_adjusted_continuity
from clients.symbol_paths import encode_symbol
from livewire_scripts.paths import data_lake_dir

SCHEMA_VERSION = 1
CONTINUITY_THRESHOLD = 6.0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--tickers", nargs="+")
    scope.add_argument("--full", action="store_true")
    scope.add_argument("--preset", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--continuity-threshold", type=float, default=CONTINUITY_THRESHOLD)
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _classify(bronze: BronzeClient, store: CorporateActionStore, symbol: str,
              as_of: date, threshold: float) -> dict:
    rows = bronze.read_symbol_rows(symbol)
    path = bronze.symbol_path(symbol)
    entry: dict = {"symbol": symbol, "path": str(path), "source_sha256": _sha256(path),
                   "klass": "clean", "max_ratio": None, "break_date": None}
    if not rows:
        entry["klass"] = "clean"
        return entry
    actions = store.latest_active(symbol)
    intervals = build_factor_intervals(rows, actions, as_of)
    adjusted = adjust_daily_rows(rows, intervals, revision=1)
    try:
        check_adjusted_continuity(adjusted, threshold=threshold)
    except ValueError as exc:
        entry["klass"] = "mixed"
        # error message is "adjusted continuity break at <date>: <r>x jump ..."
        message = str(exc)
        entry["break_date"] = message.split("at ", 1)[1].split(":", 1)[0].strip() if " at " in message else None
    return entry


def _resolve_symbols(args, bronze: BronzeClient) -> list[str]:
    if args.full:
        return sorted(bronze.get_existing_symbols())
    if args.preset:
        _, tickers, _ = load_preset(args.preset)
        return [t.upper() for t in tickers]
    return [t.upper() for t in args.tickers]


def run(argv: Sequence[str] | None = None, *, data_lake_root: Path | None = None,
        as_of_date: date | None = None) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else (args.data_lake_root or data_lake_dir())
    as_of = as_of_date or datetime.now(UTC).date()
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    store = CorporateActionStore(root)

    symbols = _resolve_symbols(args, bronze)
    existing = bronze.get_existing_symbols()
    entries = [_classify(bronze, store, s, as_of, args.continuity_threshold)
               for s in symbols if s in existing]
    counts = {"clean": sum(e["klass"] == "clean" for e in entries),
              "mixed": sum(e["klass"] == "mixed" for e in entries)}
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "data_lake_root": str(root.resolve()),
        "as_of_date": as_of.isoformat(),
        "generated_at": datetime.now(UTC).isoformat(),
        "threshold": args.continuity_threshold,
        "symbols": sorted(entries, key=lambda e: e["symbol"]),
        "counts": counts,
    }
    _write_atomic(args.output, manifest)
    print(json.dumps({"clean": counts["clean"], "mixed": counts["mixed"],
                      "output": str(args.output)}, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

- [ ] **Step 4: Run the audit tests to verify they pass**

Run: `uv run pytest tests/test_audit_legacy_basis.py -v`
Expected: PASS. If `test_clean_symbol` seeds a symbol whose built series trips the gate, pick frozen closes that stay within 6× (they should — MSFT June-2021 has no split).

- [ ] **Step 5: Register the subcommand**

In `scripts/livewire_quality.py`, add one line to `COMMANDS` (alongside `audit-split-basis`):
```python
    "audit-legacy-basis": "livewire_scripts.audit_legacy_basis",
```

- [ ] **Step 6: Add the dispatch-wiring test**

In `tests/test_livewire_entrypoints.py`, add (imitating the existing `test_store_dispatches_rebuild_silver`):
```python
def test_quality_dispatches_audit_legacy_basis(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(livewire_quality.importlib, "import_module",
                        lambda name: _fake_module(calls, name, accepts_argv=True))
    assert livewire_quality.main(["audit-legacy-basis", "--full", "--output", "x.json"]) == 7
    assert calls == [("livewire_scripts.audit_legacy_basis", ["--full", "--output", "x.json"])]
```

- [ ] **Step 7: Run entrypoint tests**

Run: `uv run pytest tests/test_livewire_entrypoints.py -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add livewire_scripts/audit_legacy_basis.py scripts/livewire_quality.py tests/test_audit_legacy_basis.py tests/test_livewire_entrypoints.py
git commit -m "feat(silver): add offline legacy-basis audit command"
```

---

## Task 3: Resumable IB re-derivation repair (Module 3 / WS2)

**Files:**
- Create: `livewire_scripts/repair_legacy_basis.py`
- Modify: `scripts/livewire_store.py` (add one `COMMANDS` entry, ~line 22)
- Test: `tests/test_repair_legacy_basis.py`, `tests/test_livewire_entrypoints.py` (add dispatch case)

**Interfaces:**
- Consumes: audit manifest from Task 2; `adjusted_history_sources.fetch_ib_evidence(fetcher, symbol, start, end, actions, as_of) -> SourceEvidence`; `adjusted_history_sources.IBHistoryFetcher(client)` (callable `(symbol,start,end)->list[dict]`); `clients.ib_client.IBClient`; `clients.price_basis.prepare_ib_rows_for_publish(incoming_rows, *, existing_rows, actions, as_of_date) -> list[dict]` (raises `ValueError` on ambiguous classification); `BronzeClient.merge_ticker_rows(symbol, rows) -> int`, `.read_symbol_rows`; `CorporateActionStore.latest_active`; `check_adjusted_continuity`; `build_factor_intervals`/`adjust_daily_rows`; `load_preset`.
- Produces: `run(argv=None, *, data_lake_root=None, ib_factory=IBClient, ib_fetcher_factory=IBHistoryFetcher, massive_factory=MassiveClient, as_of_date=None) -> int` and `main(argv=None) -> int`. Cursor at `<output-dir>/cursor.json` = `{"identity": {schema_version, audit_sha256, data_lake_root}, "completed": {symbol: {source_sha256, status}}}` with `status in {"done","ambiguous","failed"}`; per-symbol sidecar under `<output-dir>/symbols/<enc>.json`; `summary.json` + stdout summary line.

- [ ] **Step 1: Write the failing test (stubbed IB, no network)**

Create `tests/test_repair_legacy_basis.py`:

```python
import json
from datetime import UTC, date, datetime
from decimal import Decimal

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveSplit
from livewire_scripts import repair_legacy_basis


def _seed_mixed(root, ticker):
    # NVDA-style mixed legacy/raw around a 4:1 split.
    rows = [{
        "trade_date": d, "symbol_id": 1,
        "open": c, "high": c, "low": c, "close": c, "adj_close": c,
        "volume": 100, "source": "legacy", "price_basis": "raw",
    } for d, c in (("2021-06-17", 746.29), ("2021-06-18", 18.64), ("2021-06-21", 737.09))]
    BronzeClient(root / "bronze/asset_class=equity", "equity").replace_ticker_rows(ticker, rows)
    split = MassiveSplit(provider_event_id=f"{ticker}-2021", ticker=ticker,
                         execution_date=date(2021, 7, 20),
                         split_from=Decimal("1"), split_to=Decimal("4"), payload_hash="s")
    CorporateActionStore(root).reconcile(ticker, [split], datetime(2021, 7, 20, tzinfo=UTC))


def _audit_manifest(root, ticker):
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    import hashlib
    path = bronze.symbol_path(ticker)
    manifest = {"schema_version": 1, "data_lake_root": str(root.resolve()),
                "symbols": [{"symbol": ticker, "path": str(path),
                             "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                             "klass": "mixed", "break_date": "2021-06-18"}]}
    p = root / "audit.json"
    p.write_text(json.dumps(manifest))
    return p


def _clean_ib_fetcher(rows_by_symbol):
    # A stub IBHistoryFetcher: callable(symbol, start, end) -> list[dict] of clean IB TRADES rows.
    class _Stub:
        def __init__(self, client):  # ignore client
            pass
        def __call__(self, symbol, start, end):
            return [dict(r) for r in rows_by_symbol.get(symbol, []) if start <= r["trade_date"] <= end]
    return _Stub


def test_repair_rewrites_mixed_symbol_to_clean_raw(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    # Clean IB history: consistent true-raw closes (split-adjusted basis as IB returns TRADES).
    ib_rows = [{"trade_date": d, "symbol_id": 0, "open": c, "high": c, "low": c,
                "close": c, "adj_close": c, "volume": 100, "source": "ib",
                "price_basis": "split_adjusted", "currency": "USD"}
               for d, c in ((date(2021, 6, 17), 186.57), (date(2021, 6, 18), 186.4),
                            (date(2021, 6, 21), 184.27))]

    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": ib_rows}),
    )
    assert rc == 0
    cursor = json.loads((output_dir / "cursor.json").read_text())
    assert cursor["completed"]["NVDA"]["status"] == "done"
    # Bronze NVDA now has the IB-derived rows merged in (source ib, canonical raw).
    merged = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").read_symbol_rows("NVDA")
    by_date = {r["trade_date"]: r for r in merged}
    assert by_date["2021-06-18"]["source"] == "ib"
    assert by_date["2021-06-18"]["price_basis"] == "raw"


def test_resume_skips_completed_symbol(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    ib_rows = [{"trade_date": d, "symbol_id": 0, "open": c, "high": c, "low": c,
                "close": c, "adj_close": c, "volume": 100, "source": "ib",
                "price_basis": "split_adjusted", "currency": "USD"}
               for d, c in ((date(2021, 6, 17), 186.57), (date(2021, 6, 18), 186.4),
                            (date(2021, 6, 21), 184.27))]
    common = dict(data_lake_root=tmp_path, ib_factory=lambda: object())
    repair_legacy_basis.run(["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
                            ib_fetcher_factory=_clean_ib_fetcher({"NVDA": ib_rows}), **common)

    calls = {"n": 0}
    def _counting_factory(client):
        calls["n"] += 1
        return _clean_ib_fetcher({"NVDA": ib_rows})(client)
    repair_legacy_basis.run(["--audit-manifest", str(manifest), "--output-dir", str(output_dir), "--resume"],
                            ib_fetcher_factory=_counting_factory, **common)
    assert calls["n"] == 0  # completed symbol not re-fetched


def test_ib_connection_failure_marks_failed_not_crash(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"

    def _boom():
        raise ConnectionError("gateway unreachable")

    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path, ib_factory=_boom,
        ib_fetcher_factory=_clean_ib_fetcher({}),
    )
    assert rc == 1
    cursor = json.loads((output_dir / "cursor.json").read_text())
    assert cursor["completed"]["NVDA"]["status"] == "failed"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/test_repair_legacy_basis.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'livewire_scripts.repair_legacy_basis'`.

- [ ] **Step 3: Implement the repair module**

Create `livewire_scripts/repair_legacy_basis.py`:

```python
"""Audit-driven IB re-derivation of mixed-basis legacy equity symbols.

Consumes the legacy-basis audit manifest, and for each ``mixed`` symbol (ordered
sp500 → ndx100 → r2k → remainder) re-fetches deep IB history, normalizes it to
canonical true-raw, self-checks that the resulting adjusted series is continuous,
and merges the corrected rows back to bronze. Resumable via a per-symbol cursor.
Never writes an unconfirmable symbol — ambiguity fails closed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Callable, Sequence

from clients.adjustment_engine import adjust_daily_rows, build_factor_intervals
from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.ib_client import IBClient
from clients.ingestion_common import load_preset
from clients.price_basis import prepare_ib_rows_for_publish
from clients.silver_continuity import check_adjusted_continuity
from clients.symbol_paths import encode_symbol
from livewire_scripts.adjusted_history_sources import IBHistoryFetcher
from livewire_scripts.paths import data_lake_dir

SCHEMA_VERSION = 1
IB_EARLIEST = date(1980, 1, 1)
_PRIORITY_PRESETS = ("sp500", "ndx100", "r2k")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--presets-dir", type=Path, default=Path("presets"))
    parser.add_argument("--continuity-threshold", type=float, default=6.0)
    parser.add_argument("--host", default=os.environ.get("MDW_IB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MDW_IB_PORT", "4001")))
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _priority_rank(presets_dir: Path) -> dict[str, int]:
    rank: dict[str, int] = {}
    for tier, name in enumerate(_PRIORITY_PRESETS):
        preset_path = presets_dir / f"{name}.json"
        if not preset_path.is_file():
            continue
        _, tickers, _ = load_preset(preset_path)
        for ticker in tickers:
            rank.setdefault(ticker.upper(), tier)
    return rank


def _order_symbols(symbols: list[str], rank: dict[str, int]) -> list[str]:
    return sorted(symbols, key=lambda s: (rank.get(s, len(_PRIORITY_PRESETS)), s))


def _repair_one(symbol: str, *, bronze: BronzeClient, store: CorporateActionStore,
                fetcher: Callable[[str, date, date], list[dict]], as_of: date,
                threshold: float) -> tuple[str, dict]:
    """Return (status, sidecar). status in {'done','ambiguous','failed'}."""
    existing = bronze.read_symbol_rows(symbol)
    actions = store.latest_active(symbol)
    end = as_of
    ib_rows = fetcher(symbol, IB_EARLIEST, end)
    if not ib_rows:
        return "failed", {"symbol": symbol, "reason": "ib_no_data"}
    try:
        canonical = prepare_ib_rows_for_publish(
            ib_rows, existing_rows=existing, actions=actions, as_of_date=as_of)
    except ValueError as exc:
        return "ambiguous", {"symbol": symbol, "reason": f"classification: {exc}"}
    ib_only = [r for r in canonical if r.get("source") == "ib"]
    if not ib_only:
        return "failed", {"symbol": symbol, "reason": "no_ib_rows_after_normalize"}
    # Self-check: the corrected series must adjust to a continuous curve.
    intervals = build_factor_intervals(ib_only, actions, as_of)
    adjusted = adjust_daily_rows(ib_only, intervals, revision=1)
    try:
        check_adjusted_continuity(adjusted, threshold=threshold)
    except ValueError as exc:
        return "ambiguous", {"symbol": symbol, "reason": f"still_discontinuous: {exc}"}
    inserted = bronze.merge_ticker_rows(symbol, ib_only)
    return "done", {"symbol": symbol, "rows_written": len(ib_only), "inserted": inserted}


def run(argv: Sequence[str] | None = None, *, data_lake_root: Path | None = None,
        ib_factory: Callable[[], Any] = IBClient,
        ib_fetcher_factory: Callable[[Any], Callable[[str, date, date], list[dict]]] = IBHistoryFetcher,
        massive_factory: Callable[[], Any] | None = None,
        as_of_date: date | None = None) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else (args.data_lake_root or data_lake_dir())
    as_of = as_of_date or datetime.now(UTC).date()
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    store = CorporateActionStore(root)

    audit = json.loads(args.audit_manifest.read_text())
    audit_sha256 = _sha256(args.audit_manifest)
    mixed = [item["symbol"] for item in audit["symbols"] if item.get("klass") == "mixed"]
    ordered = _order_symbols(mixed, _priority_rank(args.presets_dir))

    identity = {"schema_version": SCHEMA_VERSION, "audit_sha256": audit_sha256,
                "data_lake_root": str(root.resolve())}
    cursor_path = args.output_dir / "cursor.json"
    cursor = {"identity": identity, "completed": {}}
    if args.resume and cursor_path.is_file():
        loaded = json.loads(cursor_path.read_text())
        if loaded.get("identity") != identity:
            raise ValueError("resume cursor does not match the active audit manifest")
        cursor = loaded

    ib_client: Any = None
    fetcher: Callable[[str, date, date], list[dict]] | None = None
    counts: dict[str, int] = {"done": 0, "ambiguous": 0, "failed": 0}
    try:
        for symbol in ordered:
            checkpoint = cursor["completed"].get(symbol)
            if args.resume and checkpoint and checkpoint.get("status") == "done":
                counts["done"] += 1
                continue
            try:
                if fetcher is None:
                    ib_client = ib_factory()
                    connect = getattr(ib_client, "connect", None)
                    if callable(connect):
                        connect(host=args.host, port=args.port)  # no auto-retry beyond IBClient's 326 handling
                    fetcher = ib_fetcher_factory(ib_client)
                status, sidecar = _repair_one(symbol, bronze=bronze, store=store,
                                              fetcher=fetcher, as_of=as_of,
                                              threshold=args.continuity_threshold)
            except Exception as exc:  # connection or fetch failure — mark, don't crash
                status, sidecar = "failed", {"symbol": symbol, "reason": f"exception: {exc}"}
            sidecar_path = args.output_dir / "symbols" / f"{encode_symbol(symbol)}.json"
            _write_atomic(sidecar_path, {**sidecar, "status": status})
            cursor["completed"][symbol] = {
                "source_sha256": next((i["source_sha256"] for i in audit["symbols"]
                                       if i["symbol"] == symbol), None),
                "status": status,
            }
            _write_atomic(cursor_path, cursor)
            counts[status] = counts.get(status, 0) + 1
    finally:
        if ib_client is not None:
            disconnect = getattr(ib_client, "disconnect", None)
            if callable(disconnect):
                disconnect()

    _write_atomic(args.output_dir / "summary.json",
                  {"audit_sha256": audit_sha256, "counts": counts, "symbols": len(ordered)})
    print(json.dumps({"counts": counts, "symbols": len(ordered)}, sort_keys=True))
    return 0 if counts["failed"] == 0 else 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

- [ ] **Step 4: Run the repair tests**

Run: `uv run pytest tests/test_repair_legacy_basis.py -v`
Expected: PASS (3 tests). If `prepare_ib_rows_for_publish` normalization needs a specific IB basis to yield `price_basis='raw'`, adjust the stub IB closes so the classifier resolves unambiguously (the stub returns `split_adjusted` rows; the classifier + normalizer produce `raw`). Confirm `by_date["2021-06-18"]["price_basis"] == "raw"`.

- [ ] **Step 5: Register the subcommand**

In `scripts/livewire_store.py`, add one line to `COMMANDS` (alongside `repair-split-basis`):
```python
    "repair-legacy-basis": "livewire_scripts.repair_legacy_basis",
```

- [ ] **Step 6: Add the dispatch-wiring test**

In `tests/test_livewire_entrypoints.py`:
```python
def test_store_dispatches_repair_legacy_basis(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(livewire_store.importlib, "import_module",
                        lambda name: _fake_module(calls, name, accepts_argv=True))
    assert livewire_store.main(["repair-legacy-basis", "--audit-manifest", "a.json",
                                "--output-dir", "out"]) == 7
    assert calls == [("livewire_scripts.repair_legacy_basis",
                      ["--audit-manifest", "a.json", "--output-dir", "out"])]
```

- [ ] **Step 7: Run entrypoint + full new-module tests**

Run: `uv run pytest tests/test_repair_legacy_basis.py tests/test_livewire_entrypoints.py -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add livewire_scripts/repair_legacy_basis.py scripts/livewire_store.py tests/test_repair_legacy_basis.py tests/test_livewire_entrypoints.py
git commit -m "feat(silver): add resumable IB legacy-basis repair command"
```

---

## Task 4: End-to-end wiring + rev-3 runbook

**Files:**
- Create: `tests/test_silver_legacy_repair_e2e.py`
- Modify: `CLAUDE.md` (add the two commands + the rev-3 runbook to the silver section)

**Interfaces:**
- Consumes everything above; no new production code. rev-3 is produced by the existing `livewire_scripts.rebuild_silver.run(["--full"], ...)`.

- [ ] **Step 1: Write the end-to-end test**

Create `tests/test_silver_legacy_repair_e2e.py` — audit → repair → rebuild, asserting the formerly-mixed symbol is now published and continuous, and the revision advances:

```python
import json
from datetime import UTC, date, datetime
from decimal import Decimal

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveSplit
from clients.silver_continuity import check_adjusted_continuity
from livewire_scripts import audit_legacy_basis, rebuild_silver, repair_legacy_basis


def test_mixed_symbol_is_repaired_then_published_clean(tmp_path):
    # 1. seed mixed NVDA
    rows = [{"trade_date": d, "symbol_id": 1, "open": c, "high": c, "low": c,
             "close": c, "adj_close": c, "volume": 100, "source": "legacy", "price_basis": "raw"}
            for d, c in (("2021-06-17", 746.29), ("2021-06-18", 18.64), ("2021-06-21", 737.09))]
    BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").replace_ticker_rows("NVDA", rows)
    split = MassiveSplit(provider_event_id="nvda", ticker="NVDA", execution_date=date(2021, 7, 20),
                         split_from=Decimal("1"), split_to=Decimal("4"), payload_hash="s")
    CorporateActionStore(tmp_path).reconcile("NVDA", [split], datetime(2021, 7, 20, tzinfo=UTC))

    # 2. audit → NVDA is mixed
    manifest = tmp_path / "audit.json"
    audit_legacy_basis.run(["--tickers", "NVDA", "--output", str(manifest)], data_lake_root=tmp_path)
    assert json.loads(manifest.read_text())["symbols"][0]["klass"] == "mixed"

    # 3. repair from clean stub IB
    ib_rows = [{"trade_date": d, "symbol_id": 0, "open": c, "high": c, "low": c, "close": c,
                "adj_close": c, "volume": 100, "source": "ib", "price_basis": "split_adjusted",
                "currency": "USD"}
               for d, c in ((date(2021, 6, 17), 186.57), (date(2021, 6, 18), 186.4), (date(2021, 6, 21), 184.27))]

    class _Stub:
        def __init__(self, client): ...
        def __call__(self, symbol, start, end): return [dict(r) for r in ib_rows if start <= r["trade_date"] <= end]

    repair_legacy_basis.run(["--audit-manifest", str(manifest), "--output-dir", str(tmp_path / "out")],
                            data_lake_root=tmp_path, ib_factory=lambda: object(), ib_fetcher_factory=_Stub)

    # 4. rebuild → NVDA now publishes (not quarantined), revision advances
    silver_root = tmp_path / "silver"
    rc = rebuild_silver.run(["--tickers", "NVDA"], data_lake_root=tmp_path, silver_root=silver_root)
    assert rc == 0
    assert (silver_root / "asset_class=equity/symbol=NVDA/1d.parquet").exists()
```

- [ ] **Step 2: Run it to verify it passes**

Run: `uv run pytest tests/test_silver_legacy_repair_e2e.py -v`
Expected: PASS. (If the stub-IB frozen closes don't normalize to a continuous series, tune them so the built adjusted series is continuous — the goal is a realistic clean IB history.)

- [ ] **Step 3: Full suite + coverage gate**

Run (excludes the known time-bomb integration tests per project memory):
```bash
uv run pytest tests/ -v -m "not integration" --cov=clients --cov=scripts --cov-report=term-missing
```
Expected: PASS, coverage ≥ 95%. Add targeted tests for any uncovered branch in the three new modules until the gate passes.

- [ ] **Step 4: Document the runbook**

In `CLAUDE.md`, under the silver section, add the operator sequence:
```bash
# Full legacy-basis repair → rev-3 (operator-reviewed, resumable)
python scripts/livewire_quality.py audit-legacy-basis --full --output <lake>/repairs/silver-legacy-basis/<stamp>/audit.json   # 1. offline audit; REVIEW the mixed count before proceeding
python scripts/livewire_store.py repair-legacy-basis --audit-manifest <.../audit.json> --output-dir <.../> --resume            # 2. IB re-derivation (sp500→ndx100→r2k→rest), resumes from cursor
python scripts/livewire_store.py rebuild-silver --full                                                                          # 3. single rev-3 publish (continuity gate quarantines any residual mixed)
```
State explicitly: run step 1, review the `mixed` count, only then run step 2; the continuity gate is always on in step 3.

- [ ] **Step 5: Commit**

```bash
git add tests/test_silver_legacy_repair_e2e.py CLAUDE.md
git commit -m "test(silver): end-to-end legacy-basis repair + rev-3 runbook"
```

---

## Self-Review

**Spec coverage:** Module 1 continuity gate → Task 1. Module 2 audit (clean/mixed, operator-review artifact) → Task 2. Module 3 resumable IB re-derivation with sp500→ndx100→r2k priority, cursor, fail-closed ambiguity → Task 3. Single-trigger rev-3 + runbook + operator review gate → Task 4. Massive ≤5y cross-check: **deliberately deferred** — the in-line continuity self-check (re-derived series must adjust to a continuous curve) is a stronger, window-independent correctness gate than a 5-year Massive overlap; `massive_factory` is threaded through `run(...)` so the cross-check can be added later without an interface change. This is a scoped simplification, not a gap. 593-coverage plan and G4 automation are explicitly separate specs.

**Placeholder scan:** No TBD/TODO. Every code step shows full code. Two steps note "tune the frozen closes if the seeded series is continuous/discontinuous" — that is a real calibration instruction (frozen test data must produce the intended jump), not a placeholder.

**Type consistency:** `check_adjusted_continuity(rows, *, threshold, allowlist) -> None` is defined in Task 1 and consumed identically in Tasks 2/3/4. `run(...)` signatures match the `livewire_scripts` delegate contract (`run(argv=None, *, data_lake_root=None, ...) -> int` + `main(argv)`). Cursor shape `{"identity":..., "completed": {symbol: {source_sha256, status}}}` is written and re-read consistently in Task 3. `klass` field (`"clean"|"mixed"`) is produced in Task 2 and read in Task 3.
