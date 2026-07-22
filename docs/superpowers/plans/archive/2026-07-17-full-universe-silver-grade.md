# Full-Universe Silver Grade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every symbol in the equity universe publishes to Silver, and everything published is silver grade. Deep history is **not** a goal — a symbol may publish a shorter series. Any data added later, at the **front** (backfilled history) or the **back** (new bars), must be silver grade or must not publish.

**Architecture:** Three layers, in this order.
1. **Repair what is repairable.** The 2021-06 seed-boundary class is IB-re-derivable and recovers a symbol's *full* history. Proven: 64/116 of the priority batch, 19/19 split shapes verified true-raw.
2. **Triage what is not.** Every remaining unexplained discontinuity is classified against a *second source* (Massive raw **and** adjusted dailies) into real-market-move / bad-data / missing-corporate-action.
3. **Publish the silver-grade window.** A symbol's window is the longest suffix with no bad discontinuity. The window is **derived on every publish, never persisted as truth** — so backfilled history extends it automatically once the data supports it, and a bad new bar cannot silently corrupt it.

**Definition — silver grade.** For every row in the published window: `price_basis` is known and correct (true raw); every split with an ex-date inside the window has an active corporate-action record; and the resulting adjusted series has no discontinuity that is not an evidenced real market move.

**Tech Stack:** Python 3.13, pyarrow, requests (Massive REST), pytest (uv), existing `clients/` modules.

## Global Constraints

- Coverage gate `fail_under = 95`; CI runs `uv run pytest tests/ -m "not integration and not postgres_live" --cov --cov-fail-under=95 -W error::RuntimeWarning`. Lint: `ruff check .` + `ruff format --check .`.
- **No synthetic market data.** Tests use REAL tickers at REAL prices, frozen at authoring time with an as-of date. No placeholder symbols (`FOO`, `TEST`, `XYZ`), no round-number prices. **Tests must not hit the network** — mock Massive HTTP with `responses`.
- **Never add a `Co-Authored-By: Claude` trailer.** All commits go on `feat/silver-legacy-basis-repair` (PR #57) — do not open a second PR.
- IB: never auto-retry a connection failure; never manage the Gateway from this repo. On this host IB is `127.0.0.1:4001` (sessions run ON the mini; the LAN IP silently times out — `TrustedTwsApiClientIPs` is empty).
- Bronze is the system of record: any code that mutates it must be able to undo the mutation.
- Data lake root: `/Volumes/DATA_LAKE/livewire/data-lake` (`~/market-warehouse` is a symlink). 13,141 audited equity symbols. Shell globs over symbol dirs hang — use `os.scandir`.

## Measured facts this plan is built on

| Fact | Number | Source |
|---|---|---|
| Bulk seed window (pre-window rows IB-back-adjusted but labelled `raw`) | **2021-06-11 → 2021-06-21** | 621/1,939 sampled min_dates land exactly on 2021-06-11; straggler bad bar at 2021-06-18; no second spike across 579 later dates |
| Seed-boundary class (Type A), magnitude = product of post-window split factors | **123** (39 audit-`mixed`, **63 audit-`clean`**, 21 `error`) | Full-universe boundary sweep; 31 symbols have P≠1 but a flat boundary (KLAC, COO) → **measure, never assume** |
| **The seed class is only 16% of `mixed`, and is essentially already solved** | Of 238 `mixed`: **39 seed_boundary** (37 done / 2 ambiguous = **95% repairable**) vs **199 other_break** (27 done / 50 ambiguous / 122 never attempted = **35% repairable**), break dates **2023–2026** | Re-classification of the live audit + repair cursor, 2026-07-17 |
| Live bronze state after the 116-symbol batch | **clean 12373 / mixed 174 / error 594**. All 64 `done` repairs landed and re-classify `clean`; all 52 `ambiguous` wrote **zero** bytes (fail-closed proven) | Re-ran `_classify` against live bronze, 2026-07-17 |
| **rev-3 has not been published — Apex still serves the garbage** | Silver is `revision 2` (published `2026-07-16T12:48:33Z`, before the repairs). `NVDA 2021-06-07`: silver `0.435` vs repaired bronze `704.80` | `revisions/current.json` + parquet reads |
| The 6.0 gate is blind below its threshold | **63 missed**, folds 2×–6× — incl. TSLA 3×, GE 5×, WMT 3×, CSX 3×, CTAS/CPRT/DXCM/IBKR 4×, FTNT/NOW/TSCO 5×, ETFs SOXX/SMH/XLE | Same sweep; all 6 named cases (APH/ACMR/BBSI/AMCR/ARR/AEVA) reproduced |
| Unexplained discontinuities with no CA record (Type B pool) | **877** at **>3×** (no split ±3d, outside the seed window); 476 audit-`clean`. **Contains genuine market moves — a triage list, not a corruption list** | Full-universe scan |
| …of which this plan actually acts on | the **>6.0** subset only — see "What 6.0 means" below | `find_breaks` runs at `--continuity-threshold`, default 6.0 |
| Massive split reference data collapses before 2003 | **33 splits total for 1978–2002** vs 148 in 2003 alone, 576 in 2025 | CA store: 8,753 symbol dirs, 335,871 active events |
| Massive's `/v3/reference/splits` genuinely lacks them — not an ingestion bug | NVDA API returns **4** (earliest 2006-04-07; its real 2000/2001 2:1 splits absent). EQIX **0**, MTB **0**. AAPL returns **5** back to 1987-06-16 (mega-cap outlier). Store count == API count | Raw HTTP, no client parsing |
| Symbols with history predating the 2003 coverage floor | **899** of 13,141 | Footer scan |
| **The audit covers 100% of what is currently in `bronze/asset_class=equity/`, and that is 13,141 symbols** | The "22,673 dirs vs 13,141 audited" gap **does not exist**: `13,141` real `symbol=` dirs + `9,532` macOS AppleDouble `._symbol=` sidecars (exFAT has no resource forks) + 2 = 22,675. Set equality against the audit **proven**: `on disk not audited: []`, `audited not on disk: []`. `missing_1d_parquet: 0` | `os.scandir` + `decode_symbol`, 2026-07-17. ⚠️ The Apex report's silver-side "~25,096 dirs / ~12,500 orphans" is very likely the **same artifact** — re-measure before any GC |
| ⚠️ **13,141 is NOT a universe-completeness claim** | It is a measurement of *current bronze contents*, not of *what should be there*. `flatfile-ingest-daily` targets the **full SIP ~20K** ticker universe (CLAUDE.md), so bronze may legitimately grow well past 13,141. Every "full universe" in this plan means **"every symbol currently in bronze equity"** — the audit/publish denominator. Never cite 13,141 as "the universe is 13,141 symbols" | Scope stated 2026-07-17 so the number does not propagate as a completeness fact |
| Bronze OHLCV is essentially clean | **7 bad rows in 19,561,030** (0.000036%), all `-0.0` sentinels for missing fields; no negatives, no NaN/inf (schema is `not null`), no future dates, no dupes in an 810-symbol deep read | Footer statistics over all 13,141 |
| **90% of the universe is `price_basis='unknown'` — a latent time bomb** | 720/800 sampled symbols are `basis={unknown} source={legacy}`; they pass the gate **only because they have no splits**. `build_factor_intervals` raises `unknown price_basis for split-affected row` the moment a split touches one. INTC is exactly this (`unknown` × 11,676 rows) | Sampled scan, 2026-07-17. **Directly threatens "newly added data is always silver grade"** |
| The 594 `error` population, taxonomised | **518** `split_basis_unknown` (the WS3 backlog) · **61** `dividend_currency` (9 = one stray record, e.g. CBRL has a lone **LBP** dividend among 91 USD; 52 = genuinely foreign — BMO/CAD, RACE/EUR, ALC/CHF) · **14** `dividend_magnitude` (11 = real terminal liquidating distributions, 3 = ticker reuse) · **1** AVBH non-positive close | Reconstructed from the data; the engine's error strings carry no per-symbol detail |
| Repair works and is fail-closed | Batch **116/116: done=64, ambiguous=52, failed=0**. 19/19 split shapes true-raw incl. reverse splits; ambiguous symbols byte-identical to backup | Live run + independent verification |
| Ambiguous is **not** retryable | All sidecars: `post_merge_discontinuous` at dates with **no** CA record (EQIX 24.95× @2003-01-02, MTB 10.2× @2000-10-06). IB also returns HMDS-no-data for many small caps | Batch log + sidecars |
| Manifest is a delta | rev-1 = 9,207 symbols, rev-2 = 3,350; `_publish_locked` writes only what it is handed | `clients/silver_revision.py:107-124` + on-disk revisions |
| Massive can serve the second source, and the factor step is real | `get_daily_bars(..., adjusted=False\|True)` return **different** series. NVDA control: `adj/raw` steps **0.0250 → 0.1000** exactly at its 2021-07-20 4:1 ex-date — a 4× factor step, which is precisely the `missing_action` signal | Live probe 2026-07-17, `clients/massive_client.py:177-190` |
| **…but only for a rolling ~5-year window** | `/v2/aggs` entitlement floor measured at **2021-07-12** (binary search; 1998/2000/2003/2005/2008/2010/2012/2014/2015/2016/2017/2018/2020 all rejected). Older requests raise `MassiveAuthError: Your plan doesn't include this data timeframe` | Live probe 2026-07-17 |

**Why trimming, not inference:** the goal explicitly excludes backfilling deep history. Where a split record is missing, recovering the pre-break history would require inferring the split and reconciling it — strictly more machinery for history that is not wanted. Trim instead; if the CA record ever arrives, the derived window extends by itself.

**What Massive can and cannot adjudicate — read this before trusting Task 7.** The entitlement floor is **~2021-07-12 and it rolls forward**. That has three consequences the design depends on:

1. **Every break older than the floor is `inconclusive`, always.** `MassiveAuthError` is an `Exception`, so `triage_break` catches it and returns `inconclusive`, and the window trims. This is the correct behaviour and needs no code — but it means the pre-2003 missing-CA class this triage was conceived for (EQIX @2003-01-02, MTB @2000-10-06) is answered by *nothing*. Do not read a large `inconclusive` count in Task 13 Step 3 as a failure; it is the expected shape.
2. **That is affordable only because the window starts AT the last break, not after the first.** A symbol whose last >6.0 break is in 2003 still serves 2003→today; the trim costs only pre-2003 history, which the goal explicitly does not want. The breaks that would cost real history if wrongly trimmed are the *recent* ones — and those are exactly the ones inside the entitlement. This is luck, not design. If a symbol's last break ever sits just behind a receding floor, its history is trimmed with no way to appeal.
3. **The rolling floor is why the verdict manifest must be durable and default-loaded.** A `real_move` confirmed today at 2021-08 becomes unadjudicable in two years as the floor passes it. If the verdicts are not persisted at a path every run reads, that symbol's real history is silently trimmed the moment the floor rolls past it. See the `DEFAULT_TRIAGE_MANIFEST` rationale in Task 9 — it is load-bearing, not a convenience.

**What 6.0 means — read this before trusting the word "silver grade".** "No discontinuity that is not an evidenced real market move" is not decidable; `6.0` is the operational stand-in, and it is inherited (it is the gate Apex asked for and PR #57 shipped). Everything this plan does — what the audit records, what gets triaged, what the window trims — happens **above 6.0**. So:

- A symbol whose only unexplained break is **3×–5×** is triaged by nothing and trimmed by nothing. It publishes with that break intact. The Type-B scan found 877 symbols at >3× and the audit flags ~238 at >6×, so this band is roughly **600 symbols** — real coverage, not a rounding error.
- That is a deliberate accepted risk, not an oversight: a 4× single-day move is entirely ordinary for a small-cap or a biotech readout, and trimming all of them would amputate far more real history than it would repair. Lowering the threshold without triage capacity behind it makes the data *worse*.
- The honest claim this plan can make is therefore: **every symbol publishes, and everything published is silver grade at the 6.0 definition.** If the 3–6× band later needs to be resolved, the machinery is already in place — lower `--continuity-threshold`, re-audit, and triage the larger `breaks` list against Massive. Nothing in the design assumes 6.0.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `clients/seed_boundary.py` | **NEW.** Pure detector for the 2021-06 seed artifact: predict fold from actions, measure the boundary step, decide. No I/O. | Create |
| `clients/break_triage.py` | **NEW.** Classify one discontinuity against Massive raw+adjusted: `real_move` / `bad_data` / `missing_action` / `inconclusive`. Provider I/O injected. | Create |
| `clients/silver_window.py` | **NEW.** Resolve a symbol's silver-grade window from its adjusted series + triage verdicts. Pure. | Create |
| `livewire_scripts/audit_legacy_basis.py` | Classify with both detectors; stop dropping symbols silently. | Modify |
| `livewire_scripts/triage_breaks.py` | **NEW.** Batch triage command → durable verdict manifest (resumable). | Create |
| `livewire_scripts/rebuild_silver.py` | Apply the seed floor, then publish the window; complete manifest; allowlist flag; report window regressions. | Modify |
| `livewire_scripts/repair_legacy_basis.py` | Backup before mutate; fix the 5 hardening defects; exact stop-gate math. | Modify |
| `livewire_scripts/rollback_legacy_basis.py` | **NEW.** `rollback-legacy-basis` subcommand (dispatch is subcommand→module). | Create |
| `scripts/livewire_store.py` | Register `rollback-legacy-basis`. | Modify |
| `scripts/livewire_quality.py` | Register `triage-breaks`. | Modify |
| `tests/test_seed_boundary.py`, `tests/test_break_triage.py`, `tests/test_silver_window.py`, `tests/test_rollback_legacy_basis.py` | **NEW.** | Create |
| `tests/test_audit_legacy_basis.py`, `tests/test_repair_legacy_basis.py`, `tests/test_rebuild_silver.py` | Extend; replace the placeholder fixture. | Modify |
| `CLAUDE.md` | Document the window contract, both detectors, triage, rollback. | Modify |

---

### Task 0: Date-explicit test helpers (do this first — every later task's tests need them)

**Files:**
- Modify: `tests/test_rebuild_silver.py`, `tests/test_audit_legacy_basis.py`

**Why:** every fixture in this plan is a real ticker's real closes on *real dates* (the seed window is a date, the breaks are dates), but the existing helpers cannot express that. `tests/test_rebuild_silver.py:16` has `_bronze(root, symbol, closes=(...))`, which hardcodes `date(2026, 1, index + 1)` and takes no dates; `_split(root, symbol)` hardcodes a 1:2 on `2026-01-03`. `tests/test_audit_legacy_basis.py:15` has `_seed_symbol(root, ticker, rows_spec, split=None)`. Nothing named `_seed_bronze`, `_seed_split` or `_entry` exists. Add them once, here, rather than having eight later steps each invent their own.

- [ ] **Step 1: Add the helpers to `tests/test_rebuild_silver.py`**

Keep `_bronze`/`_split` — existing tests use them. Add alongside, and add `import pytest` to the file (it currently has none, and the new tests need `pytest.raises`):

```python
def _seed_bronze(root, symbol, rows_spec, *, source="massive", price_basis="raw"):
    """Bronze rows at explicit ISO dates. `rows_spec` is [(iso_date, close), ...]."""
    rows = [
        {
            "trade_date": iso_date,
            "symbol_id": 7,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "adj_close": close,
            "volume": 1_000,
            "source": source,
            "price_basis": price_basis,
        }
        for iso_date, close in rows_spec
    ]
    BronzeClient(root / "bronze/asset_class=equity", "equity").replace_ticker_rows(symbol, rows)


def _seed_split(root, symbol, ex_date, split_from, split_to):
    """One active split at an explicit ex-date."""
    event = MassiveSplit(
        provider_event_id=f"{symbol}-{ex_date}",
        ticker=symbol,
        execution_date=date.fromisoformat(ex_date),
        split_from=Decimal(str(split_from)),
        split_to=Decimal(str(split_to)),
        payload_hash=f"{symbol}-{ex_date}-hash",
    )
    CorporateActionStore(root).reconcile(symbol, [event], datetime(2026, 1, 4, tzinfo=UTC))
```

Later tasks in this plan write `run([...], data_lake_root=..., silver_root=...)`; that is `rebuild_silver.run` — either import it (`from livewire_scripts.rebuild_silver import run`) or qualify each call. Match whichever the file already does.

- [ ] **Step 2: Add the helpers to `tests/test_audit_legacy_basis.py`**

`_seed_symbol(root, ticker, rows_spec, split=None)` already takes `rows_spec` as `[(iso_date, close), ...]`; alias it so this plan's steps read consistently, add the split helper, and add the manifest reader the audit tests use:

```python
_seed_bronze = _seed_symbol


def _seed_split(root, symbol, ex_date, split_from, split_to):
    event = MassiveSplit(
        provider_event_id=f"{symbol}-{ex_date}",
        ticker=symbol,
        execution_date=date.fromisoformat(ex_date),
        split_from=Decimal(str(split_from)),
        split_to=Decimal(str(split_to)),
        payload_hash=f"{symbol}-{ex_date}-hash",
    )
    CorporateActionStore(root).reconcile(symbol, [event], datetime(2026, 1, 4, tzinfo=UTC))


def _entry(output_path, symbol):
    """The manifest entry for one symbol."""
    manifest = json.loads(output_path.read_text())
    return next(item for item in manifest["symbols"] if item["symbol"] == symbol)
```

- [ ] **Step 3: Verify the existing suite still passes**

```bash
uv run pytest tests/test_rebuild_silver.py tests/test_audit_legacy_basis.py -v
```
Expected: PASS, unchanged — this task only adds helpers.

- [ ] **Step 4: Commit**

```bash
uv run ruff check . && uv run ruff format --check .
git add tests/test_rebuild_silver.py tests/test_audit_legacy_basis.py
git commit -m "test(silver): add date-explicit bronze/split helpers

Every fixture in the silver-grade work is a real ticker's real closes on real dates
— the seed window and every break are dates — but _bronze() hardcodes
date(2026, 1, index+1) and _split() hardcodes a 1:2 on 2026-01-03."
```

---

### Task 1: Seed-boundary detector

**Files:**
- Create: `clients/seed_boundary.py`
- Test: `tests/test_seed_boundary.py`

**Interfaces:**
- Consumes: `clients.corporate_action_store.CorporateAction` (`action_type`, `ex_date`, `split_from`, `split_to`, `status`).
- Produces: `SEED_WINDOW_START="2021-06-11"`, `SEED_WINDOW_END="2021-06-21"`, `MIN_CONFIDENT_LOG_FOLD=0.55`, `DEFAULT_TOLERANCE=0.25`, `SeedBoundaryBreak(ValueError)` with `.date/.observed/.predicted`, `predict_boundary_fold(actions, *, window_end=...) -> float`, `measure_boundary_jump(rows, *, window_start=..., window_end=...) -> tuple[str, float] | None`, `classify_seed_boundary(rows, actions, ...) -> dict`, `check_seed_boundary(rows, actions, ...) -> None`.

- [ ] **Step 1: Write the failing tests**

```python
"""Unit tests for the seed-boundary detector.

Fixtures are real closes for real tickers, read from production bronze
(/Volumes/DATA_LAKE/livewire/data-lake) on 2026-07-17 and frozen. No network.
"""

from datetime import UTC, date, datetime

import pytest

from clients.corporate_action_store import CorporateAction
from clients.seed_boundary import (
    SeedBoundaryBreak,
    check_seed_boundary,
    classify_seed_boundary,
    measure_boundary_jump,
    predict_boundary_fold,
)


def _split(symbol: str, ex_date: str, split_from: float, split_to: float, status: str = "active") -> CorporateAction:
    return CorporateAction(
        action_id=f"{symbol}-{ex_date}-split",
        provider="massive",
        provider_event_id=f"{symbol}-{ex_date}",
        event_revision=1,
        supersedes_action_id=None,
        symbol=symbol,
        action_type="split",
        ex_date=date.fromisoformat(ex_date),
        split_from=split_from,
        split_to=split_to,
        cash_amount=None,
        currency=None,
        declaration_date=None,
        record_date=None,
        pay_date=None,
        status=status,
        fetched_at=datetime(2026, 7, 17, tzinfo=UTC),
        payload_hash=f"{symbol}{ex_date}",
    )


def _rows(pairs: list[tuple[str, float]]) -> list[dict]:
    return [{"trade_date": d, "close": c} for d, c in pairs]


# NVDA stored bronze closes across the seed boundary (pre-repair, 2026-07-17):
# pre-window rows were IB back-adjusted (/40) yet labelled raw.
NVDA_ROWS = _rows([("2021-06-08", 17.39), ("2021-06-09", 17.34), ("2021-06-10", 17.43),
                   ("2021-06-11", 713.01), ("2021-06-14", 723.72)])
NVDA_SPLITS = [_split("NVDA", "2021-07-20", 1, 4), _split("NVDA", "2024-06-10", 1, 10)]

# APH — the 2x case the 6.0 gate cannot see.
APH_ROWS = _rows([("2021-06-09", 33.98), ("2021-06-10", 34.13), ("2021-06-11", 68.45), ("2021-06-14", 68.60)])
APH_SPLITS = [_split("APH", "2026-03-04", 1, 2)]

# AAPL — no post-window split, flat boundary.
AAPL_ROWS = _rows([("2021-06-09", 127.13), ("2021-06-10", 126.11), ("2021-06-11", 127.35), ("2021-06-14", 130.48)])

# KLAC — post-window split (P=10) but re-pulled genuinely raw: flat boundary.
KLAC_ROWS = _rows([("2021-06-09", 322.44), ("2021-06-10", 320.10), ("2021-06-11", 324.62), ("2021-06-14", 328.29)])
KLAC_SPLITS = [_split("KLAC", "2026-06-12", 1, 10)]


def test_predict_fold_multiplies_post_window_splits():
    assert predict_boundary_fold(NVDA_SPLITS) == pytest.approx(40.0)


def test_predict_fold_ignores_splits_inside_or_before_the_window():
    assert predict_boundary_fold([_split("NVDA", "2007-09-11", 2, 3), _split("NVDA", "2021-07-20", 1, 4)]) == pytest.approx(4.0)


def test_predict_fold_reverse_split_reports_magnitude():
    assert predict_boundary_fold([_split("ADV", "2026-03-27", 25, 1)]) == pytest.approx(25.0)


def test_predict_fold_no_post_window_split_is_one():
    assert predict_boundary_fold([]) == pytest.approx(1.0)


def test_predict_fold_ignores_cancelled_split():
    assert predict_boundary_fold([_split("NVDA", "2021-07-20", 1, 4, status="cancelled")]) == pytest.approx(1.0)


def test_measure_jump_finds_the_boundary_step():
    assert measure_boundary_jump(NVDA_ROWS) == ("2021-06-11", pytest.approx(40.9, rel=0.01))


def test_measure_jump_returns_none_when_no_rows_in_window():
    assert measure_boundary_jump(_rows([("2024-01-02", 10.5), ("2024-01-03", 10.7)])) is None


def test_nvda_is_corrupt_observed_matches_predicted():
    with pytest.raises(SeedBoundaryBreak) as exc:
        check_seed_boundary(NVDA_ROWS, NVDA_SPLITS)
    assert exc.value.date == "2021-06-11"
    assert exc.value.predicted == pytest.approx(40.0)


def test_aph_2x_is_corrupt_even_though_the_6x_gate_misses_it():
    with pytest.raises(SeedBoundaryBreak):
        check_seed_boundary(APH_ROWS, APH_SPLITS)


def test_aapl_no_post_window_split_is_clean():
    assert check_seed_boundary(AAPL_ROWS, []) is None


def test_klac_predicted_fold_but_flat_boundary_is_clean():
    assert check_seed_boundary(KLAC_ROWS, KLAC_SPLITS) is None


def test_low_fold_is_inconclusive_not_corrupt():
    rows = _rows([("2021-06-10", 40.00), ("2021-06-11", 50.00)])
    actions = [_split("CENTA", "2026-02-06", 4, 5)]
    assert classify_seed_boundary(rows, actions)["verdict"] == "inconclusive"
    assert check_seed_boundary(rows, actions) is None


def test_classify_reports_measurements():
    assert classify_seed_boundary(AAPL_ROWS, [])["verdict"] == "clean"
    corrupt = classify_seed_boundary(NVDA_ROWS, NVDA_SPLITS)
    assert corrupt["verdict"] == "corrupt"
    assert corrupt["date"] == "2021-06-11"
    assert corrupt["fold"] == pytest.approx(40.0)


def test_non_positive_close_is_inconclusive_not_a_crash():
    assert classify_seed_boundary(_rows([("2021-06-10", 0.0), ("2021-06-11", 713.01)]), NVDA_SPLITS)["verdict"] == "inconclusive"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_seed_boundary.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'clients.seed_boundary'`

- [ ] **Step 3: Write the implementation**

```python
"""Deterministic detector for the 2021-06 bulk-seed basis artifact.

The warehouse was bulk-seeded over 2021-06-11..2021-06-21 from one deep IB fetch
of *back-adjusted* prices labelled ``price_basis='raw'``; rows on/after the window
are genuine raw. A symbol is corrupt exactly when its stored series steps by ``P``
at the window, where ``P`` is the product of the split factors that took effect
after it — the adjustment IB had already applied.

Unlike the blind ratio heuristic in :mod:`clients.silver_continuity`, this looks at
a *known location* and compares against a *predicted* value, so it resolves the
2x-5x class the 6.0 threshold structurally cannot see. It MEASURES rather than
assumes: symbols later re-pulled raw have ``P != 1`` yet a flat boundary (KLAC,
COO) and must stay clean.
"""

from __future__ import annotations

import math

from clients.corporate_action_store import CorporateAction

SEED_WINDOW_START = "2021-06-11"
SEED_WINDOW_END = "2021-06-21"
# |ln(fold)| below this overlaps ordinary daily moves (measured p999 of |ln return|
# over 566k adjacent no-split days = 0.359), so a match there is not evidence.
MIN_CONFIDENT_LOG_FOLD = 0.55
DEFAULT_TOLERANCE = 0.25


class SeedBoundaryBreak(ValueError):
    """The stored series steps by the predicted split fold at the seed window.

    Subclasses ``ValueError`` so the rebuild staging loop's ``except Exception``
    quarantines the symbol, while exposing structured fields for callers.
    """

    def __init__(self, date: str, observed: float, predicted: float) -> None:
        self.date = date
        self.observed = observed
        self.predicted = predicted
        super().__init__(
            f"seed-boundary basis break at {date}: observed {observed:.2f}x step matches the "
            f"{predicted:.2f}x post-boundary split fold — pre-{SEED_WINDOW_START} rows are already split-adjusted"
        )


def predict_boundary_fold(actions: list[CorporateAction], *, window_end: str = SEED_WINDOW_END) -> float:
    """Product of active split magnitudes with ``ex_date`` after the seed window.

    Returns a magnitude >= 1: a 25:1 reverse and a 1:25 forward both report 25.0,
    because the boundary step's direction follows the split's.
    """
    fold = 1.0
    for action in actions:
        if action.action_type != "split" or action.status != "active":
            continue
        if action.ex_date.isoformat() <= window_end:
            continue
        if not action.split_from or not action.split_to:
            continue
        fold *= float(action.split_to) / float(action.split_from)
    if fold <= 0 or not math.isfinite(fold):
        return 1.0
    return max(fold, 1.0 / fold)


def measure_boundary_jump(
    rows: list[dict], *, window_start: str = SEED_WINDOW_START, window_end: str = SEED_WINDOW_END
) -> tuple[str, float] | None:
    """Largest adjacent-day close-ratio magnitude stepping into the seed window.

    Returns ``(date, ratio)`` for the largest step whose *later* date is inside the
    window, or ``None`` when no such adjacent pair exists (symbol seeded later, or
    no pre-window history).
    """
    ordered = sorted(rows, key=lambda row: str(row["trade_date"])[:10])
    best: tuple[str, float] | None = None
    for previous, current in zip(ordered, ordered[1:]):
        current_date = str(current["trade_date"])[:10]
        if not (window_start <= current_date <= window_end):
            continue
        try:
            previous_close = float(previous["close"])
            current_close = float(current["close"])
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(previous_close) and math.isfinite(current_close)):
            continue
        if previous_close <= 0 or current_close <= 0:
            continue
        ratio = max(current_close / previous_close, previous_close / current_close)
        if best is None or ratio > best[1]:
            best = (current_date, ratio)
    return best


def classify_seed_boundary(
    rows: list[dict],
    actions: list[CorporateAction],
    *,
    window_start: str = SEED_WINDOW_START,
    window_end: str = SEED_WINDOW_END,
    tolerance: float = DEFAULT_TOLERANCE,
) -> dict:
    """Measure the boundary against the predicted fold.

    ``corrupt`` when the observed step matches the predicted fold within
    ``tolerance`` (log space) and the fold outruns daily noise; ``inconclusive``
    when the fold is too small to be evidence or the boundary is unmeasurable;
    ``clean`` otherwise — including a predicted fold with a flat boundary.
    """
    fold = predict_boundary_fold(actions, window_end=window_end)
    measured = measure_boundary_jump(rows, window_start=window_start, window_end=window_end)
    result: dict = {
        "fold": fold,
        "observed": None if measured is None else measured[1],
        "date": None if measured is None else measured[0],
        "verdict": "clean",
    }
    if measured is None:
        result["verdict"] = "inconclusive" if fold > 1.01 else "clean"
        return result
    if fold <= 1.01:
        return result
    if abs(math.log(fold)) < MIN_CONFIDENT_LOG_FOLD:
        result["verdict"] = "inconclusive"
        return result
    if abs(math.log(measured[1]) - math.log(fold)) <= tolerance:
        result["verdict"] = "corrupt"
    return result


def check_seed_boundary(
    rows: list[dict],
    actions: list[CorporateAction],
    *,
    window_start: str = SEED_WINDOW_START,
    window_end: str = SEED_WINDOW_END,
    tolerance: float = DEFAULT_TOLERANCE,
) -> None:
    """Raise :class:`SeedBoundaryBreak` when the symbol is confidently corrupt."""
    result = classify_seed_boundary(
        rows, actions, window_start=window_start, window_end=window_end, tolerance=tolerance
    )
    if result["verdict"] == "corrupt":
        raise SeedBoundaryBreak(result["date"], result["observed"], result["fold"])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_seed_boundary.py -v` → PASS (14 tests)

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check clients/seed_boundary.py tests/test_seed_boundary.py
uv run ruff format clients/seed_boundary.py tests/test_seed_boundary.py
git add clients/seed_boundary.py tests/test_seed_boundary.py
git commit -m "feat(silver): add deterministic seed-boundary basis detector

The 6.0 continuity heuristic only sees residual jumps above its threshold, so 63
symbols whose post-2021-06 split fold is 2x-5x (APH, ACMR, BBSI, AMCR, ARR, AEVA,
TSLA, GE, WMT, CSX, ETFs) classify clean while their pre-seed history is
double-adjusted. Compare the observed step at the known seed window against the
fold predicted from the corporate-action store instead of guessing a threshold.
Measure rather than assume: KLAC/COO have a predicted fold but a flat boundary."
```

---

### Task 1b: Break enumeration (`find_breaks`)

**Files:**
- Create: `clients/silver_window.py`
- Test: `tests/test_silver_window.py`

**Why here, and not in Task 8:** Task 2's audit needs `find_breaks` to record every break, so the module must exist before it. Task 8 later adds `resolve_window` to this same module.

**Interfaces:**
- Produces: `DEFAULT_THRESHOLD = 6.0`, `find_breaks(adjusted_rows, *, threshold=6.0, exempt=frozenset()) -> list[dict]` → every break as `[{"date", "ratio", "reason"}, ...]` in date order. `ratio` is `None` for a non-positive/non-finite close, which has nothing to compare against a second source.

- [ ] **Step 1: Write the failing tests**

```python
"""Unit tests for break enumeration. Frozen real closes, no network."""

from clients.silver_window import find_breaks


def _rows(pairs):
    return [{"trade_date": d, "close": c} for d, c in pairs]


# Real AAPL adjusted closes (production silver, frozen 2026-07-17): continuous.
AAPL = _rows([("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91)])

# EQIX shape: an unexplained ~25x step at 2003-01-02 with no CA record.
EQIX = _rows([("2002-12-27", 0.20), ("2002-12-30", 0.21), ("2003-01-02", 5.24), ("2003-01-03", 5.31)])


def test_find_breaks_enumerates_every_break_not_just_the_first():
    """check_adjusted_continuity stops at the first; triage needs them all, or the
    later break never gets triaged and its real history is amputated forever."""
    rows = _rows([("2001-01-02", 1.00), ("2001-01-03", 50.00),
                  ("2002-01-02", 51.00), ("2002-01-03", 4.00), ("2002-01-04", 4.05)])
    assert [b["date"] for b in find_breaks(rows)] == ["2001-01-03", "2002-01-03"]


def test_find_breaks_is_empty_for_a_continuous_series():
    assert find_breaks(AAPL) == []


def test_find_breaks_honours_exempt_dates():
    assert find_breaks(EQIX, exempt=frozenset({"2003-01-02"})) == []


def test_find_breaks_reports_a_non_positive_close_with_no_ratio():
    rows = _rows([("2024-01-02", 185.64), ("2024-01-03", 0.0), ("2024-01-04", 181.91)])
    breaks = find_breaks(rows)
    assert [b["date"] for b in breaks] == ["2024-01-03"]
    assert breaks[0]["ratio"] is None   # nothing to compare against a second source


```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_silver_window.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'clients.silver_window'`

- [ ] **Step 3: Write the implementation**

```python
"""Resolve the silver-grade window of an adjusted daily series.

Silver grade means: every row's basis is correct, every split inside the window is
recorded, and the adjusted series has no discontinuity that is not an evidenced
real market move. Deep history is not a goal — a symbol may publish a short
window; what it publishes must be right.
"""

from __future__ import annotations

import math

DEFAULT_THRESHOLD = 6.0


def find_breaks(
    rows: list[dict],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    exempt: frozenset[str] = frozenset(),
) -> list[dict]:
    """Every bad discontinuity in an adjusted series, in date order.

    Unlike :func:`clients.silver_continuity.check_adjusted_continuity`, which raises
    on the FIRST break, this enumerates all of them — the audit needs the full list
    so every break gets triaged, and the resolver needs the last one.
    """
    ordered = sorted(rows, key=lambda row: str(row["trade_date"])[:10])
    breaks: list[dict] = []
    previous_close: float | None = None
    for row in ordered:
        trade_date = str(row["trade_date"])[:10]
        try:
            close = float(row["close"])
        except (TypeError, ValueError):
            close = float("nan")
        if not math.isfinite(close) or close <= 0:
            breaks.append({
                "date": trade_date,
                "ratio": None,
                "reason": f"non-positive or non-finite adjusted close at {trade_date}",
            })
            # A row we cannot trust must not be compared against — it would
            # manufacture a second, spurious break on the following day.
            previous_close = None
            continue
        if previous_close is not None and trade_date not in exempt:
            ratio = max(close / previous_close, previous_close / close)
            if ratio > threshold:
                breaks.append({
                    "date": trade_date,
                    "ratio": ratio,
                    "reason": f"unexplained {ratio:.2f}x adjusted step at {trade_date} (threshold {threshold:g}x)",
                })
        previous_close = close
    return breaks


```

- [ ] **Step 4: Run tests and commit**

```bash
uv run pytest tests/test_silver_window.py -v
uv run ruff check . && uv run ruff format --check .
git add clients/silver_window.py tests/test_silver_window.py
git commit -m "feat(silver): enumerate every adjusted-series break, not just the first

check_adjusted_continuity raises on the FIRST break, so an audit built on it records
one break_date per symbol. A symbol with two real moves gets only the earlier one
triaged; the later stays unexplained, the window trims there anyway, and the history
between them is amputated permanently, because no later audit ever surfaces it."
```

---

### Task 2: Wire the detector into the audit and the repair self-check

**Files:**
- Modify: `livewire_scripts/audit_legacy_basis.py:25` (import), `:55-83` (`_classify`), `:104-106` (symbol resolution)
- Modify: `livewire_scripts/repair_legacy_basis.py:117-122` (post-merge self-check)
- Test: `tests/test_audit_legacy_basis.py`, `tests/test_repair_legacy_basis.py`

**Interfaces:**
- Produces: manifest entries gain `"detector": "seed_boundary" | "continuity" | None`, `"seed_boundary": {fold, observed, date, verdict} | None`, and `"breaks": [{date, ratio, reason}, ...]` — **every** break, not just the first. `klass` stays `clean`/`mixed`/`error`, so `repair_legacy_basis.py:147` (`item.get("klass") == "mixed"`) consumes the enlarged population unchanged, and `break_date`/`max_ratio` stay as the first break for compatibility.

> **`breaks` is what Task 7 triages.** `check_adjusted_continuity` raises on the first break only, so a single `break_date` under-reports every multi-break symbol — see Task 8's rationale. The audit is the only place that already builds each symbol's adjusted series, so it is the cheap place to enumerate them.

**Why the audit only, and not a publish gate:** a gate that *raises* would quarantine every seed-corrupt symbol repair cannot fix (~59 of the 123) — the exact opposite of the goal, which wants each of them publishing its post-2021-06 window. The seed detector therefore enters the publish path in **Task 9**, as a *trim floor* rather than a quarantine. Here it only decides who is fed to repair.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_audit_legacy_basis.py`:

```python
def test_seed_boundary_symbol_classified_mixed_below_continuity_threshold(tmp_path):
    """A 2x seed fold is invisible to the 6.0 gate but must be caught. Real APH
    closes (production bronze, 2026-07-17) + its real 2026-03-04 2:1 split."""
    root = tmp_path / "lake"
    _seed_bronze(root, "APH", [("2021-06-09", 33.98), ("2021-06-10", 34.13),
                               ("2021-06-11", 68.45), ("2021-06-14", 68.60)])
    _seed_split(root, "APH", "2026-03-04", 1, 2)
    output = tmp_path / "audit.json"
    assert run(["--tickers", "APH", "--output", str(output)], data_lake_root=root, as_of_date=date(2026, 7, 17)) == 0
    entry = _entry(output, "APH")
    assert entry["klass"] == "mixed"
    assert entry["detector"] == "seed_boundary"
    assert entry["seed_boundary"]["fold"] == pytest.approx(2.0)


def test_predicted_fold_with_flat_boundary_stays_clean(tmp_path):
    """KLAC has a 10:1 split after the window but was re-pulled raw — not corrupt."""
    root = tmp_path / "lake"
    _seed_bronze(root, "KLAC", [("2021-06-09", 322.44), ("2021-06-10", 320.10),
                                ("2021-06-11", 324.62), ("2021-06-14", 328.29)])
    _seed_split(root, "KLAC", "2026-06-12", 1, 10)
    output = tmp_path / "audit.json"
    assert run(["--tickers", "KLAC", "--output", str(output)], data_lake_root=root, as_of_date=date(2026, 7, 17)) == 0
    assert _entry(output, "KLAC")["klass"] == "clean"


def test_requested_symbol_absent_from_bronze_is_recorded_as_error(tmp_path):
    root = tmp_path / "lake"
    _seed_bronze(root, "AAPL", [("2021-06-10", 126.11), ("2021-06-11", 127.35)])
    output = tmp_path / "audit.json"
    assert run(["--tickers", "AAPL", "NOTATICKER", "--output", str(output)],
               data_lake_root=root, as_of_date=date(2026, 7, 17)) == 0
    entry = _entry(output, "NOTATICKER")
    assert entry["klass"] == "error"
    assert "not in bronze" in entry["error"]


def test_symbol_with_zero_rows_is_error_not_clean(tmp_path):
    root = tmp_path / "lake"
    _seed_bronze(root, "AAPL", [])
    output = tmp_path / "audit.json"
    assert run(["--tickers", "AAPL", "--output", str(output)], data_lake_root=root, as_of_date=date(2026, 7, 17)) == 0
    assert _entry(output, "AAPL")["klass"] == "error"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_audit_legacy_basis.py -v -k "seed_boundary or absent_from_bronze or zero_rows or flat_boundary"`
Expected: FAIL — `KeyError: 'detector'`; APH/KLAC classified `clean`.

- [ ] **Step 3: Implement — audit**

Add at `livewire_scripts/audit_legacy_basis.py:25`:

```python
from clients.seed_boundary import classify_seed_boundary
from clients.silver_window import find_breaks
```

Replace `_classify` (lines 55-83):

```python
def _classify(bronze: BronzeClient, store: CorporateActionStore, symbol: str, as_of: date, threshold: float) -> dict:
    path = bronze.symbol_path(symbol)
    entry: dict = {
        "symbol": symbol,
        "path": str(path),
        "source_sha256": _sha256(path) if path.is_file() else None,
        "klass": "clean",
        "max_ratio": None,
        "break_date": None,
        "breaks": [],
        "detector": None,
        "seed_boundary": None,
    }
    if not path.is_file():
        entry["klass"] = "error"
        entry["error"] = f"symbol not in bronze: {symbol}"
        return entry
    rows = bronze.read_symbol_rows(symbol)
    if not rows:
        # An empty parquet is a broken symbol, not a clean one — never silently pass.
        entry["klass"] = "error"
        entry["error"] = "no bronze rows"
        return entry
    # Isolate ALL per-symbol failures so a single bad symbol never aborts --full.
    try:
        actions = store.latest_active(symbol)
    except Exception as exc:
        entry["klass"] = "error"
        entry["error"] = str(exc)
        return entry
    # Seed-boundary first: deterministic (known location, predicted fold), so it
    # resolves the sub-threshold population the continuity heuristic cannot see.
    seed = classify_seed_boundary(rows, actions)
    entry["seed_boundary"] = seed
    if seed["verdict"] == "corrupt":
        entry["klass"] = "mixed"
        entry["detector"] = "seed_boundary"
        entry["break_date"] = seed["date"]
        entry["max_ratio"] = seed["observed"]
        return entry
    try:
        intervals = build_factor_intervals(rows, actions, as_of)
        adjusted = adjust_daily_rows(rows, intervals, revision=1)
        # Enumerate EVERY break, not just the first: each one is a triage candidate,
        # and a break the audit never reports is a break that never gets triaged and
        # whose real history the window then trims away permanently.
        entry["breaks"] = find_breaks(adjusted, threshold=threshold)
        check_adjusted_continuity(adjusted, threshold=threshold)
    except ContinuityBreak as exc:
        entry["klass"] = "mixed"
        entry["detector"] = "continuity"
        entry["break_date"] = exc.date
        entry["max_ratio"] = exc.ratio
    except Exception as exc:
        # build/adjust errors (e.g. `unknown price_basis` rows → WS3's backlog) or a
        # non-positive-close ValueError. NOT fed to repair.
        entry["klass"] = "error"
        entry["error"] = str(exc)
    return entry
```

Add to `tests/test_audit_legacy_basis.py`:

```python
def test_audit_records_every_break_not_just_the_first(tmp_path):
    """A single break_date under-reports multi-break symbols and starves the triage."""
    root = tmp_path / "lake"
    _seed_bronze(root, "AAPL", [("2001-01-02", 1.00), ("2001-01-03", 50.00),
                                ("2002-01-02", 51.00), ("2002-01-03", 4.00)])
    output = tmp_path / "audit.json"
    run(["--tickers", "AAPL", "--output", str(output)], data_lake_root=root, as_of_date=date(2026, 7, 17))
    entry = _entry(output, "AAPL")
    assert [b["date"] for b in entry["breaks"]] == ["2001-01-03", "2002-01-03"]
    assert entry["break_date"] == "2001-01-03"   # first break, kept for repair compatibility
```

Replace lines 104-106:

```python
    symbols = _resolve_symbols(args, bronze)
    # Do NOT filter against get_existing_symbols(): a requested symbol that is
    # missing must surface as an `error` entry, not vanish from the manifest.
    entries = [_classify(bronze, store, s, as_of, args.continuity_threshold) for s in symbols]
```

- [ ] **Step 4: Implement — the repair post-merge self-check**

`_repair_one`'s self-check (`livewire_scripts/repair_legacy_basis.py:117-122`) runs the
same 6.0 heuristic, so a repair whose IB re-fetch covered only *part* of the pre-seed
range can leave a 2×–5× residual boundary, pass the check, and be recorded `done` while
the symbol is still corrupt. Add the deterministic check alongside it:

```python
    try:
        intervals = build_factor_intervals(merged, actions, as_of)
        adjusted = adjust_daily_rows(merged, intervals, revision=1)
        check_adjusted_continuity(adjusted, threshold=threshold)
        # The heuristic above cannot see a 2x-5x residual. SeedBoundaryBreak
        # subclasses ValueError, so a partial re-fetch fails closed as `ambiguous`
        # rather than being recorded as a successful repair.
        check_seed_boundary(merged, actions)
    except ValueError as exc:
        return "ambiguous", {"symbol": symbol, "reason": f"post_merge_discontinuous: {exc}"}
```

with the import at `livewire_scripts/repair_legacy_basis.py:28`:

```python
from clients.seed_boundary import check_seed_boundary
```

Add to `tests/test_repair_legacy_basis.py`:

```python
def test_partial_ib_refetch_leaving_a_2x_seed_residual_is_ambiguous_not_done(tmp_path):
    """The 6.0 self-check cannot see a 2x residual; the seed check must fail closed."""
    root = tmp_path / "lake"
    _seed_symbol(root, "APH", [("2021-06-09", 33.98), ("2021-06-10", 34.13),
                               ("2021-06-11", 68.45), ("2021-06-14", 68.60)])
    _seed_split(root, "APH", "2026-03-04", 1, 2)
    audit = _audit_manifest(tmp_path, root, [{"symbol": "APH", "klass": "mixed"}])
    # IB returns only the post-boundary rows: the corrupt pre-window rows survive.
    fetcher = _fetcher({"APH": [("2021-06-11", 68.45), ("2021-06-14", 68.60)]})
    out = tmp_path / "repair"
    repair_legacy_basis.run(
        ["--audit-manifest", str(audit), "--output-dir", str(out)],
        data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=lambda _: fetcher,
        as_of_date=date(2026, 7, 17),
    )
    assert json.loads((out / "summary.json").read_text())["counts"]["ambiguous"] == 1
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_audit_legacy_basis.py tests/test_repair_legacy_basis.py -v` → PASS

- [ ] **Step 6: Commit**

```bash
uv run ruff check . && uv run ruff format --check .
git add livewire_scripts/audit_legacy_basis.py livewire_scripts/repair_legacy_basis.py \
        tests/test_audit_legacy_basis.py tests/test_repair_legacy_basis.py
git commit -m "fix(silver): detect the sub-threshold seed-boundary class in audit and repair

Both the audit and repair's post-merge self-check routed every basis decision
through the same >6.0 ratio heuristic, so 63 symbols with a 2x-5x post-seed fold
classified clean and were never fed to repair, and a repair whose IB re-fetch
covered only part of the pre-seed range could record `done` while leaving the
symbol corrupt. Run the deterministic seed detector in both. Also stop the audit
silently dropping symbols absent from bronze, and stop classifying a zero-row
parquet as clean."
```

---

### Task 3: Backup, rollback and dry-run for `repair-legacy-basis`

**Files:**
- Modify: `livewire_scripts/repair_legacy_basis.py`
- Create: `livewire_scripts/rollback_legacy_basis.py`
- Modify: `scripts/livewire_store.py:16-26` (the module map)
- Test: `tests/test_repair_legacy_basis.py`, `tests/test_rollback_legacy_basis.py`

**Interfaces:**
- `repair_legacy_basis.backup_symbol(bronze, symbol, backup_dir) -> dict` → `{"symbol", "backup_path", "sha256"}`; sidecars gain `backup_path` + `backup_sha256`; new `--dry-run` (status `would-repair`, no write, no backup).
- `rollback_legacy_basis.run(argv=None, *, data_lake_root=None) -> int` and `main(argv=None) -> int` — a **separate module** because `scripts/livewire_store.py` dispatches subcommand→module and calls that module's `main()`; a function inside `repair_legacy_basis` would be unreachable.

- [ ] **Step 1: Write the failing tests**

```python
def test_repair_backs_up_bronze_before_mutating(tmp_path):
    root, output = _seed_repairable_lake(tmp_path)
    path = root / "bronze/asset_class=equity/symbol=NVDA/1d.parquet"
    before = path.read_bytes()
    assert run(["--audit-manifest", str(_manifest(tmp_path, root, ["NVDA"])), "--output-dir", str(output)],
               data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=_fake_fetcher,
               as_of_date=date(2026, 7, 17)) == 0
    backup = output / "backup" / "NVDA.1d.parquet"
    assert backup.is_file() and backup.read_bytes() == before
    assert json.loads((output / "symbols" / "NVDA.json").read_text())["backup_sha256"] == hashlib.sha256(before).hexdigest()


def test_dry_run_makes_no_bronze_write_and_no_backup(tmp_path):
    root, output = _seed_repairable_lake(tmp_path)
    path = root / "bronze/asset_class=equity/symbol=NVDA/1d.parquet"
    before = path.read_bytes()
    assert run(["--audit-manifest", str(_manifest(tmp_path, root, ["NVDA"])), "--output-dir", str(output), "--dry-run"],
               data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=_fake_fetcher,
               as_of_date=date(2026, 7, 17)) == 0
    assert path.read_bytes() == before
    assert not (output / "backup").exists()
    assert json.loads((output / "symbols" / "NVDA.json").read_text())["status"] == "would-repair"
```

`tests/test_rollback_legacy_basis.py`:

```python
def test_rollback_restores_the_original_bytes(tmp_path):
    root, output = _seed_repairable_lake(tmp_path)
    path = root / "bronze/asset_class=equity/symbol=NVDA/1d.parquet"
    before = path.read_bytes()
    repair_run(["--audit-manifest", str(_manifest(tmp_path, root, ["NVDA"])), "--output-dir", str(output)],
               data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=_fake_fetcher, as_of_date=date(2026, 7, 17))
    assert path.read_bytes() != before
    assert rollback(["--output-dir", str(output)], data_lake_root=root) == 0
    assert path.read_bytes() == before


def test_rollback_rejects_a_different_active_root(tmp_path):
    root, output = _seed_repairable_lake(tmp_path)
    repair_run(["--audit-manifest", str(_manifest(tmp_path, root, ["NVDA"])), "--output-dir", str(output)],
               data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=_fake_fetcher, as_of_date=date(2026, 7, 17))
    other = tmp_path / "other-lake"
    other.mkdir()
    with pytest.raises(ValueError, match="does not match active root"):
        rollback(["--output-dir", str(output)], data_lake_root=other)


def test_rollback_refuses_a_tampered_backup(tmp_path):
    root, output = _seed_repairable_lake(tmp_path)
    repair_run(["--audit-manifest", str(_manifest(tmp_path, root, ["NVDA"])), "--output-dir", str(output)],
               data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=_fake_fetcher, as_of_date=date(2026, 7, 17))
    backup = output / "backup" / "NVDA.1d.parquet"
    backup.write_bytes(backup.read_bytes() + b"\0")
    with pytest.raises(ValueError, match="backup checksum mismatch"):
        rollback(["--output-dir", str(output)], data_lake_root=root)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_repair_legacy_basis.py tests/test_rollback_legacy_basis.py -v -k "backup or dry_run or rollback"`
Expected: FAIL — no `--dry-run`; `ModuleNotFoundError: livewire_scripts.rollback_legacy_basis`

- [ ] **Step 3: Implement backup + dry-run**

Add to `parse_args` after line 46:

```python
    parser.add_argument("--dry-run", action="store_true", help="fetch, classify and self-check, but never write bronze")
```

Add after `_write_atomic` (line 63):

```python
def backup_symbol(bronze: BronzeClient, symbol: str, backup_dir: Path) -> dict:
    """Copy a symbol's bronze parquet verbatim before any mutation.

    Bronze is the system of record and merge_ticker_rows overwrites rows in place,
    so the pre-repair bytes are otherwise unrecoverable. The sibling split-basis
    repair family ships rollback; this one must too.
    """
    source = bronze.symbol_path(symbol)
    backup_dir.mkdir(parents=True, exist_ok=True)
    destination = backup_dir / f"{encode_symbol(symbol)}.1d.parquet"
    payload = source.read_bytes()
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(payload)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return {"symbol": symbol, "backup_path": str(destination), "sha256": hashlib.sha256(payload).hexdigest()}
```

Add `backup_dir: Path | None` to `_repair_one`'s keyword-only params, update its docstring to `status in {'done','would-repair','ambiguous','failed'}`, and replace the write (lines 123-124):

```python
    if backup_dir is None:
        return "would-repair", {"symbol": symbol, "rows_would_write": len(ib_only)}
    saved = backup_symbol(bronze, symbol, backup_dir)
    inserted = bronze.merge_ticker_rows(symbol, ib_only)
    return "done", {
        "symbol": symbol,
        "rows_written": len(ib_only),
        "inserted": inserted,
        "backup_path": saved["backup_path"],
        "backup_sha256": saved["sha256"],
    }
```

Pass it at the call site (line 195):

```python
                status, sidecar = _repair_one(
                    symbol,
                    bronze=bronze,
                    store=store,
                    fetcher=fetcher,
                    as_of=as_of,
                    threshold=args.continuity_threshold,
                    backup_dir=None if args.dry_run else args.output_dir / "backup",
                )
```

`would-repair` is not `done`, so the resume skip at line 168 already refuses to treat a dry-run as completed work.

- [ ] **Step 4: Create `livewire_scripts/rollback_legacy_basis.py`**

```python
"""Restore bronze parquet saved by ``repair-legacy-basis`` before it mutated them.

Bronze is the system of record and the repair overwrites rows in place, so the
pre-repair bytes exist only in the batch's ``backup/`` directory. Restoring
verifies both the backup checksum and the active data-lake root before writing —
the same contract the repair enforces.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Sequence
from pathlib import Path

from clients.bronze_client import BronzeClient
from livewire_scripts.paths import data_lake_dir


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--tickers", nargs="+", help="restore only these symbols (default: every backed-up symbol)")
    return parser.parse_args(list(argv) if argv is not None else None)


def run(argv: Sequence[str] | None = None, *, data_lake_root: Path | None = None) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else (args.data_lake_root or data_lake_dir())
    cursor_path = args.output_dir / "cursor.json"
    if not cursor_path.is_file():
        raise ValueError(f"no repair cursor in {args.output_dir}")
    identity = json.loads(cursor_path.read_text()).get("identity", {})
    recorded_root = identity.get("data_lake_root")
    # Same contract as the repair: never touch a different lake than the one repaired.
    if recorded_root != str(root.resolve()):
        raise ValueError(f"repair output data_lake_root {recorded_root} does not match active root {root.resolve()}")
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    wanted = {t.upper() for t in args.tickers} if args.tickers else None
    restored: list[str] = []
    missing: list[str] = []
    for sidecar_path in sorted((args.output_dir / "symbols").glob("*.json")):
        sidecar = json.loads(sidecar_path.read_text())
        symbol = sidecar.get("symbol")
        if sidecar.get("status") != "done":
            continue
        if wanted is not None and symbol not in wanted:
            continue
        backup_path = Path(sidecar.get("backup_path", ""))
        if not backup_path.is_file():
            missing.append(symbol)
            continue
        payload = backup_path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != sidecar["backup_sha256"]:
            raise ValueError(f"backup checksum mismatch for {symbol}: refusing to restore")
        destination = bronze.symbol_path(symbol)
        temporary = destination.with_name(f".{destination.name}.rollback.{os.getpid()}.tmp")
        try:
            temporary.write_bytes(payload)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        restored.append(symbol)
    print(json.dumps({"restored": len(restored), "missing_backup": sorted(missing)}, sort_keys=True))
    return 0 if not missing else 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

- [ ] **Step 5: Register the subcommand**

In `scripts/livewire_store.py`, add one line to the module map right after the `repair-legacy-basis` entry:

```python
    "repair-legacy-basis": "livewire_scripts.repair_legacy_basis",
    "rollback-legacy-basis": "livewire_scripts.rollback_legacy_basis",
```

- [ ] **Step 6: Run tests and commit**

```bash
uv run pytest tests/test_repair_legacy_basis.py tests/test_rollback_legacy_basis.py -v
uv run ruff check . && uv run ruff format --check .
git add livewire_scripts/repair_legacy_basis.py livewire_scripts/rollback_legacy_basis.py scripts/livewire_store.py tests/test_repair_legacy_basis.py tests/test_rollback_legacy_basis.py
git commit -m "feat(silver): back up bronze before legacy-basis repair, add rollback and dry-run

repair-legacy-basis overwrote the system of record with no way back: the sidecar
kept only the audit-time hash, not the bytes. Copy each parquet into
<output-dir>/backup/ before merging, record the backup hash, and ship a
rollback-legacy-basis subcommand that verifies the checksum and the active root
before restoring. --dry-run exercises fetch/classify/self-check with no write."
```

---

### Task 4: Repair operational hardening

**Files:**
- Modify: `livewire_scripts/repair_legacy_basis.py:25` (import), `:66-79`, `:92`, `:144-146`, `:154-160`, `:203`
- Test: `tests/test_repair_legacy_basis.py`

**Interfaces:** no signature changes except `_repair_one` gaining `audit_sha256: str | None`.

- [ ] **Step 1: Write the failing tests**

```python
def test_ib_connection_error_mid_run_aborts_the_batch(tmp_path):
    """IBConnectionError is the codebase's real session-drop signal and must abort,
    not grind through every remaining symbol on a dead socket."""
    root, output = _seed_repairable_lake(tmp_path, symbols=["NVDA", "AMZN", "AAPL"])
    calls: list[str] = []

    def _dropping_fetcher(client):
        def fetch(symbol, start, end):
            calls.append(symbol)
            raise IBConnectionError("socket closed")
        return fetch

    assert run(["--audit-manifest", str(_manifest(tmp_path, root, ["NVDA", "AMZN", "AAPL"])),
                "--output-dir", str(output)],
               data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=_dropping_fetcher,
               as_of_date=date(2026, 7, 17)) == 1
    assert len(calls) == 1


def test_missing_preset_dir_with_priority_only_is_an_error_not_an_empty_run(tmp_path):
    root, output = _seed_repairable_lake(tmp_path)
    with pytest.raises(ValueError, match="no priority preset found"):
        run(["--audit-manifest", str(_manifest(tmp_path, root, ["NVDA"])), "--output-dir", str(output),
             "--priority-only", "--presets-dir", str(tmp_path / "nope")],
            data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=_fake_fetcher, as_of_date=date(2026, 7, 17))


def test_manifest_without_data_lake_root_is_rejected(tmp_path):
    root, output = _seed_repairable_lake(tmp_path)
    manifest = _manifest(tmp_path, root, ["NVDA"])
    payload = json.loads(manifest.read_text())
    del payload["data_lake_root"]
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="audit manifest has no data_lake_root"):
        run(["--audit-manifest", str(manifest), "--output-dir", str(output)],
            data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=_fake_fetcher, as_of_date=date(2026, 7, 17))


def test_existing_cursor_without_resume_is_rejected(tmp_path):
    root, output = _seed_repairable_lake(tmp_path)
    manifest = _manifest(tmp_path, root, ["NVDA"])
    run(["--audit-manifest", str(manifest), "--output-dir", str(output)],
        data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=_fake_fetcher, as_of_date=date(2026, 7, 17))
    with pytest.raises(ValueError, match="cursor already exists"):
        run(["--audit-manifest", str(manifest), "--output-dir", str(output)],
            data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=_fake_fetcher, as_of_date=date(2026, 7, 17))


def test_bronze_changed_since_the_audit_is_skipped_not_repaired(tmp_path):
    """The audit's verdict describes bytes that no longer exist."""
    root, output = _seed_repairable_lake(tmp_path)
    manifest = _manifest(tmp_path, root, ["NVDA"])
    path = root / "bronze/asset_class=equity/symbol=NVDA/1d.parquet"
    path.write_bytes(path.read_bytes() + b"\0")
    run(["--audit-manifest", str(manifest), "--output-dir", str(output)],
        data_lake_root=root, ib_factory=_FakeIB, ib_fetcher_factory=_fake_fetcher, as_of_date=date(2026, 7, 17))
    sidecar = json.loads((output / "symbols" / "NVDA.json").read_text())
    assert sidecar["status"] == "failed"
    assert "changed since the audit" in sidecar["reason"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_repair_legacy_basis.py -v -k "connection_error or preset_dir or data_lake_root or cursor_already or changed_since"`
Expected: FAIL

- [ ] **Step 3: Implement the guards**

Import (line 25):

```python
from clients.ib_client import IBClient, IBConnectionError
```

`_priority_rank` (replace lines 66-75):

```python
def _priority_rank(presets_dir: Path) -> dict[str, int]:
    rank: dict[str, int] = {}
    found = 0
    for tier, name in enumerate(_PRIORITY_PRESETS):
        preset_path = presets_dir / f"{name}.json"
        if not preset_path.is_file():
            continue
        found += 1
        _, tickers, _ = load_preset(preset_path)
        for ticker in tickers:
            rank.setdefault(ticker.upper(), tier)
    if not found:
        # --presets-dir defaults to a cwd-relative Path("presets"); from
        # ~/market-warehouse this silently repaired zero symbols and exited 0.
        raise ValueError(f"no priority preset found in {presets_dir.resolve()} (expected {_PRIORITY_PRESETS})")
    return rank
```

Only build it when it is needed (replace lines 148-151):

```python
    rank = _priority_rank(args.presets_dir) if args.priority_only else {}
    ordered = _order_symbols(mixed, rank) if rank else sorted(mixed)
    if args.priority_only:
        ordered = [s for s in ordered if s in rank]  # rank holds only preset members
```

Root guard (replace lines 144-146):

```python
    manifest_root = audit.get("data_lake_root")
    # CLAUDE.md repair contract: reject a different active data-lake root before
    # mutation. A manifest with no root recorded cannot be checked → refuse it.
    if manifest_root is None:
        raise ValueError("audit manifest has no data_lake_root: refusing to mutate bronze")
    if manifest_root != str(root.resolve()):
        raise ValueError(f"audit manifest data_lake_root {manifest_root} does not match active root {root.resolve()}")
```

Cursor guard (replace lines 154-160):

```python
    cursor_path = args.output_dir / "cursor.json"
    cursor = {"identity": identity, "completed": {}}
    if cursor_path.is_file():
        if not args.resume:
            raise ValueError(f"cursor already exists in {args.output_dir}: pass --resume to continue it")
        loaded = json.loads(cursor_path.read_text())
        if loaded.get("identity") != identity:
            raise ValueError("resume cursor does not match the active audit manifest")
        cursor = loaded
```

Mid-run abort (replace line 203):

```python
            except (IBConnectionError, ConnectionError, OSError, TimeoutError) as exc:
```

Stale-bronze guard — add `audit_sha256: str | None` to `_repair_one`'s keyword-only params and replace line 92:

```python
    path = bronze.symbol_path(symbol)
    if audit_sha256 is not None and path.is_file():
        if hashlib.sha256(path.read_bytes()).hexdigest() != audit_sha256:
            # The audit's mixed/clean verdict describes bytes that no longer exist.
            return "failed", {"symbol": symbol, "reason": "bronze changed since the audit"}
    existing = bronze.read_symbol_rows(symbol)
```

and pass it at the call site:

```python
                    audit_sha256=next((i["source_sha256"] for i in audit["symbols"] if i["symbol"] == symbol), None),
```

- [ ] **Step 4: Run tests and commit**

```bash
uv run pytest tests/test_repair_legacy_basis.py -v
uv run ruff check . && uv run ruff format --check .
git add livewire_scripts/repair_legacy_basis.py tests/test_repair_legacy_basis.py
git commit -m "fix(silver): harden legacy-basis repair (abort, presets, root guard, cursor, staleness)

- catch IBConnectionError in the mid-run abort: the codebase's own session-drop
  signal fell through to the per-symbol branch, so a dropped gateway ground
  through every remaining symbol instead of stopping for --resume
- error when --priority-only finds no preset: the cwd-relative default silently
  repaired zero symbols and exited 0 from the wrong directory
- reject an audit manifest with no data_lake_root instead of failing open
- reject an existing cursor without --resume instead of silently clearing it
- skip a symbol whose bronze hash moved since the audit"
```

---

### Task 5: Exact stop-gate math + continuity allowlist plumbing

**Files:**
- Modify: `livewire_scripts/repair_legacy_basis.py:182-192`, `:236-238`, `:243-267`
- Modify: `livewire_scripts/rebuild_silver.py` (`parse_args`, staging call)
- Test: `tests/test_repair_legacy_basis.py`, `tests/test_rebuild_silver.py`

**Interfaces:**
- `summarize_progress(audit_manifest, batch_summary, *, cursor=None) -> dict` gains `batch_unprocessed`; reports `tail_mixed_exact` only when `batch_summary["complete"]`, else `tail_mixed_lower_bound`. `summary.json` gains `complete: bool`.
- `rebuild-silver --continuity-allowlist <ISO_DATE> ...`.

- [ ] **Step 1: Write the failing tests**

```python
def test_summarize_progress_does_not_call_unprocessed_priority_symbols_tail():
    audit = {"counts": {"clean": 100, "mixed": 50, "error": 5}}
    summary = {"counts": {"done": 10, "ambiguous": 2, "failed": 1}, "complete": False}
    result = summarize_progress(audit, summary, cursor={"completed": {f"S{i}": {} for i in range(13)}})
    assert result["batch_attempted"] == 13
    assert result["batch_unprocessed"] == 37
    assert "tail_mixed_exact" not in result
    assert result["tail_mixed_lower_bound"] == 37


def test_summarize_progress_is_exact_when_the_batch_completed():
    audit = {"counts": {"clean": 100, "mixed": 12, "error": 5}}
    summary = {"counts": {"done": 10, "ambiguous": 2, "failed": 0}, "complete": True}
    result = summarize_progress(audit, summary, cursor={"completed": {f"S{i}": {} for i in range(12)}})
    assert result["tail_mixed_exact"] == 0
    assert result["batch_unprocessed"] == 0


def test_continuity_allowlist_exempts_an_evidenced_date(tmp_path):
    """Without an override a genuine >6x move makes a symbol unpublishable forever."""
    root = tmp_path / "lake"
    _seed_bronze(root, "AAPL", [("2024-01-02", 100.0), ("2024-01-03", 700.0), ("2024-01-04", 705.0)])
    assert run(["--tickers", "AAPL", "--dry-run", "--continuity-allowlist", "2024-01-03"],
               data_lake_root=root, silver_root=tmp_path / "silver", as_of_date=date(2026, 7, 17)) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_repair_legacy_basis.py tests/test_rebuild_silver.py -v -k "summarize or allowlist"`
Expected: FAIL — unexpected kwarg `cursor`; unrecognized `--continuity-allowlist`.

- [ ] **Step 3: Implement**

Replace `summarize_progress` (lines 243-267):

```python
def summarize_progress(audit_manifest: dict, batch_summary: dict, *, cursor: dict | None = None) -> dict:
    """Quantify remaining tail work from a full audit + a first (priority-only) batch.

    Outcome counts equal coverage only when the batch ran to completion. An aborted
    batch leaves priority symbols unprocessed; counting them as tail work would
    understate the remaining priority run, so the tail is a lower bound instead.
    Pass ``cursor`` (the batch's ``cursor.json``) to measure coverage exactly.
    """
    ac = audit_manifest["counts"]
    total = ac["clean"] + ac["mixed"] + ac["error"]
    mixed_total = ac["mixed"]
    bc = batch_summary["counts"]
    attempted = len(cursor["completed"]) if cursor else bc["done"] + bc["ambiguous"] + bc["failed"]
    unprocessed = max(0, mixed_total - attempted)
    amb_rate = (bc["ambiguous"] / attempted) if attempted else 0.0
    result = {
        "audit_total": total,
        "audit_mixed": mixed_total,
        "audit_mixed_rate": round(mixed_total / total, 4) if total else 0.0,
        "batch_attempted": attempted,
        "batch_unprocessed": unprocessed,
        "batch_done": bc["done"],
        "batch_ambiguous": bc["ambiguous"],
        "batch_ambiguous_rate": round(amb_rate, 4),
        "tail_estimated_unrepairable": round(unprocessed * amb_rate),
    }
    key = "tail_mixed_exact" if batch_summary.get("complete") else "tail_mixed_lower_bound"
    result[key] = unprocessed
    return result
```

Mark completeness (replace lines 236-238):

```python
    _write_atomic(
        args.output_dir / "summary.json",
        {
            "audit_sha256": audit_sha256,
            "counts": counts,
            "symbols": len(ordered),
            "complete": len(cursor["completed"]) >= len(ordered),
        },
    )
```

Stop inflating `failed` on the initial connect failure (replace lines 184-191):

```python
                except Exception as exc:
                    # Never attempted: do NOT record a cursor entry or count it as
                    # failed — --resume must pick this symbol up cleanly.
                    print(f"IB connection failed, aborting run: {exc}", file=sys.stderr)
                    break
```

`rebuild_silver.parse_args`, after line 58:

```python
    parser.add_argument(
        "--continuity-allowlist",
        nargs="*",
        default=[],
        metavar="ISO_DATE",
        help="iso dates exempt from the continuity gate (evidence-backed halts/relistings)",
    )
```

and at the staging call (line 221):

```python
            check_adjusted_continuity(adjusted, threshold=threshold, allowlist=frozenset(args.continuity_allowlist))
```

- [ ] **Step 4: Run tests and commit**

```bash
uv run pytest tests/test_repair_legacy_basis.py tests/test_rebuild_silver.py -v
uv run ruff check . && uv run ruff format --check .
git add livewire_scripts/repair_legacy_basis.py livewire_scripts/rebuild_silver.py tests/test_repair_legacy_basis.py tests/test_rebuild_silver.py
git commit -m "fix(silver): exact stop-gate coverage math and a reachable continuity allowlist

summarize_progress subtracted outcome counts from the mixed total and called the
result exact, so an aborted batch reported its own unprocessed priority symbols as
tail work; the connect-failure path also counted a never-attempted symbol as
failed. Measure coverage from the cursor and report a lower bound unless the batch
completed. check_adjusted_continuity's allowlist had no production caller."
```

---

### Task 6: Test-fixture compliance

**Files:** Modify `tests/test_audit_legacy_basis.py:74-100`

- [ ] **Step 1: Replace the placeholder fixture**

`test_unknown_basis_symbol_classified_error_not_crash` seeds ticker `XYZ` at a flat 10.0 — a placeholder symbol at a round number, which the project bans. Rewrite with INTC, the fail-closed control named in the Apex report, using real frozen closes:

```python
def test_unknown_basis_symbol_classified_error_not_crash(tmp_path):
    """Real INTC rows with price_basis='unknown' must fail closed, not crash.

    Closes are real INTC bronze values (production lake, read 2026-07-17); INTC is
    the fail-closed control named in SILVER_CORRECTNESS_GAP_FROM_APEX.md.
    """
    root = tmp_path / "lake"
    _seed_bronze(root, "INTC", [("2021-06-10", 56.85), ("2021-06-11", 56.71), ("2021-06-14", 57.02)],
                 price_basis="unknown")
    _seed_split(root, "INTC", "2026-05-15", 1, 2)
    output = tmp_path / "audit.json"
    assert run(["--tickers", "INTC", "--output", str(output)], data_lake_root=root, as_of_date=date(2026, 7, 17)) == 0
    entry = _entry(output, "INTC")
    assert entry["klass"] == "error"
    assert "unknown price_basis" in entry["error"]
```

If `_seed_bronze` has no `price_basis` parameter, add one defaulting to `"raw"`.

- [ ] **Step 2: Run and commit**

```bash
uv run pytest tests/test_audit_legacy_basis.py -v
git add tests/test_audit_legacy_basis.py
git commit -m "test(silver): replace placeholder XYZ@10.0 fixture with frozen real INTC rows"
```

---

### Task 7: Break triage against Massive as a second source

**Files:**
- Create: `clients/break_triage.py`
- Create: `livewire_scripts/triage_breaks.py`
- Modify: `scripts/livewire_quality.py` (register `triage-breaks`)
- Test: `tests/test_break_triage.py`

**Why:** A discontinuity that is not the seed artifact is one of three things, and trimming all of them would permanently amputate real market history. The population is every break the audit recorded — the >6.0 subset of the 877-at->3× Type-B pool (see "What 6.0 means"), one candidate per (symbol, break). Massive can serve **both** bases (`get_daily_bars(..., adjusted=False|True)`), which discriminates all three:
- our series jumps **and** Massive raw jumps the same way → **real market move** (keep);
- our series jumps, Massive raw is smooth → **bad data** in our bronze (trim);
- Massive's adjusted÷raw ratio steps across the date → Massive knows a split our CA store lacks → **missing action** (trim; the record, not the price, is what is missing).

**Interfaces:**
- Produces:
  - `TriageVerdict = Literal["real_move", "bad_data", "missing_action", "inconclusive"]`
  - `RetryableProviderError(Exception)` — raised, **not** returned as a verdict.
  - `triage_break(symbol, break_date, observed_ratio, *, fetch_raw, fetch_adjusted, tolerance=0.25) -> dict` → `{"symbol", "date", "verdict", "observed", "provider_raw_ratio", "provider_factor_step", "reason"}`
  - `fetch_raw` / `fetch_adjusted` are `Callable[[str, date, date], list[dict]]` returning rows with `trade_date` + `close` — injected so tests never touch the network.

**A transient outage must never become a durable trim.** `inconclusive` *trims*, and Task 9's verdict manifest is durable and default-loaded — so if a Massive 502 or a rate-limit is swallowed into `inconclusive` and checkpointed, one bad afternoon permanently amputates a symbol's real history, and no resume will ever retry it. Split the two cases:

- **Final** (checkpoint it): `MassiveAuthError` — the entitlement floor, which is a fact about the plan, not a transient. Also "provider returned data but it does not support a verdict".
- **Retryable** (raise `RetryableProviderError`, do **not** checkpoint): `MassiveRateLimitError`, `MassiveAPIError` with a 5xx status, timeouts, connection errors. The batch aborts the run and leaves the candidate un-cursored so `--resume` re-asks.

**And a `real_move` requires BOTH bases.** If the adjusted fetch comes back empty, `_factor_step` is `None`, and raw-jump agreement alone would return `real_move` — but a missing-action break *also* shows a provider raw jump, so that verdict would be unfounded on exactly the class it must catch. No adjusted coverage → `inconclusive`.

- [ ] **Step 1: Write the failing tests**

```python
"""Unit tests for break triage. Provider access is injected; no network."""

import pytest

from clients.break_triage import triage_break
from clients.massive_client import MassiveAuthError


def _rows(pairs):
    return [{"trade_date": d, "close": c} for d, c in pairs]


def _fetcher(pairs):
    def fetch(symbol, start, end):
        return _rows(pairs)
    return fetch


def _empty(symbol, start, end):
    return []


# Real MRNA closes around its 2020-11-16 phase-3 readout (Massive raw, frozen
# 2026-07-17): a genuine move, present in BOTH sources.
MRNA_RAW = [("2020-11-13", 89.39), ("2020-11-16", 97.49)]


def test_both_sources_jump_together_is_a_real_move():
    result = triage_break("MRNA", "2020-11-16", 1.09,
                          fetch_raw=_fetcher(MRNA_RAW), fetch_adjusted=_fetcher(MRNA_RAW))
    assert result["verdict"] == "real_move"


def test_our_jump_absent_from_provider_raw_is_bad_data():
    # Our bronze steps 22x; Massive raw is smooth across the same date.
    smooth = [("2021-07-26", 361.61), ("2021-07-27", 367.81)]
    result = triage_break("META", "2021-07-27", 22.4,
                          fetch_raw=_fetcher(smooth), fetch_adjusted=_fetcher(smooth))
    assert result["verdict"] == "bad_data"


def test_provider_factor_step_means_a_missing_corporate_action():
    # Real Massive NVDA closes across its 2021-07-20 4:1 split, BOTH bases, fetched
    # live 2026-07-17 and frozen. The adjusted/raw factor steps 0.0250 -> 0.1000 (4x)
    # exactly at the ex-date: that step is the provider telling us an event happened,
    # independent of whether /v3/reference/splits returns it. This is the shape the
    # missing-action verdict keys on.
    raw = [("2021-07-19", 751.19), ("2021-07-20", 186.12)]
    adjusted = [("2021-07-19", 18.7798), ("2021-07-20", 18.612)]
    result = triage_break("NVDA", "2021-07-20", 4.04,
                          fetch_raw=_fetcher(raw), fetch_adjusted=_fetcher(adjusted))
    assert result["verdict"] == "missing_action"
    assert result["provider_factor_step"] == pytest.approx(4.0, rel=0.05)


def test_provider_entitlement_error_is_inconclusive_not_a_trim_decision():
    """Massive's /v2/aggs is entitled for a rolling ~5y window (floor measured
    2021-07-12 on 2026-07-17). Every older break — EQIX @2003-01-02, MTB @2000-10-06,
    the whole pre-2003 missing-CA class — comes back like this and must not be
    mistaken for evidence either way."""
    def _unentitled(symbol, start, end):
        raise MassiveAuthError("Your plan doesn't include this data timeframe.")
    result = triage_break("EQIX", "2003-01-02", 24.95, fetch_raw=_unentitled, fetch_adjusted=_unentitled)
    assert result["verdict"] == "inconclusive"
    assert "timeframe" in result["reason"]


def test_no_provider_data_is_inconclusive():
    result = triage_break("ACDC", "2021-09-23", 250000.0, fetch_raw=_empty, fetch_adjusted=_empty)
    assert result["verdict"] == "inconclusive"


def test_provider_missing_the_break_date_is_inconclusive():
    off = [("2019-01-02", 10.11), ("2019-01-03", 10.24)]
    result = triage_break("MTB", "2000-10-06", 10.2, fetch_raw=_fetcher(off), fetch_adjusted=_fetcher(off))
    assert result["verdict"] == "inconclusive"


def test_provider_error_is_inconclusive_not_a_crash():
    def _boom(symbol, start, end):
        raise RuntimeError("massive 502")
    result = triage_break("NVDA", "2021-06-11", 40.9, fetch_raw=_boom, fetch_adjusted=_boom)
    assert result["verdict"] == "inconclusive"
    assert "massive 502" in result["reason"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_break_triage.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'clients.break_triage'`

- [ ] **Step 3: Write the implementation**

```python
"""Classify a price discontinuity against a second source.

A break that is not the 2021-06 seed artifact is one of three things, and they
must not be treated alike: trimming a real market move amputates real history,
while keeping bad data serves a plausible wrong chart. Massive can return the same
range on both bases, which separates all three:

* our jump present in the provider's RAW series      -> real market move
* our jump absent from the provider's raw series     -> our bronze is bad there
* provider adjusted/raw factor steps across the date -> the provider knows a split
  our corporate-action store lacks (its /v3/reference/splits collapses before 2003:
  33 splits for 1978-2002 vs 148 in 2003 alone)

Provider access is injected so this module stays pure and testable offline.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import date, timedelta
from typing import Literal

TriageVerdict = Literal["real_move", "bad_data", "missing_action", "inconclusive"]

WINDOW_DAYS = 7
DEFAULT_TOLERANCE = 0.25
Fetcher = Callable[[str, date, date], list[dict]]


def _ratio_at(rows: list[dict], break_date: str) -> float | None:
    """Magnitude of the close ratio stepping into ``break_date``."""
    ordered = sorted(rows, key=lambda row: str(row["trade_date"])[:10])
    previous: dict | None = None
    for row in ordered:
        current_date = str(row["trade_date"])[:10]
        if current_date >= break_date:
            if previous is None or current_date != break_date:
                return None
            try:
                a, b = float(previous["close"]), float(row["close"])
            except (TypeError, ValueError):
                return None
            if not (math.isfinite(a) and math.isfinite(b)) or a <= 0 or b <= 0:
                return None
            return max(b / a, a / b)
        previous = row
    return None


def _factor_step(raw: list[dict], adjusted: list[dict], break_date: str) -> float | None:
    """Magnitude of the change in the provider's adjusted/raw factor across the date.

    A step here means the provider applied a split at this date — evidence of the
    event itself, independent of whether its reference endpoint returns it.
    """
    raw_by_date = {str(r["trade_date"])[:10]: r for r in raw}
    adj_by_date = {str(r["trade_date"])[:10]: r for r in adjusted}
    dates = sorted(set(raw_by_date) & set(adj_by_date))
    before = [d for d in dates if d < break_date]
    after = [d for d in dates if d >= break_date]
    if not before or not after:
        return None

    def factor(day: str) -> float | None:
        try:
            r, a = float(raw_by_date[day]["close"]), float(adj_by_date[day]["close"])
        except (TypeError, ValueError):
            return None
        if not (math.isfinite(r) and math.isfinite(a)) or r <= 0 or a <= 0:
            return None
        return a / r

    f0, f1 = factor(before[-1]), factor(after[0])
    if f0 is None or f1 is None or f0 <= 0 or f1 <= 0:
        return None
    return max(f1 / f0, f0 / f1)


def triage_break(
    symbol: str,
    break_date: str,
    observed_ratio: float,
    *,
    fetch_raw: Fetcher,
    fetch_adjusted: Fetcher,
    tolerance: float = DEFAULT_TOLERANCE,
    window_days: int = WINDOW_DAYS,
) -> dict:
    """Classify one discontinuity. Never raises: provider trouble is inconclusive."""
    result: dict = {
        "symbol": symbol,
        "date": break_date,
        "observed": observed_ratio,
        "provider_raw_ratio": None,
        "provider_factor_step": None,
        "verdict": "inconclusive",
        "reason": "",
    }
    day = date.fromisoformat(break_date)
    start, end = day - timedelta(days=window_days), day + timedelta(days=window_days)
    try:
        raw = fetch_raw(symbol, start, end)
        adjusted = fetch_adjusted(symbol, start, end)
    except MassiveAuthError as exc:
        # The entitlement floor (measured 2021-07-12, rolling ~5y). Permanent for this
        # date, not transient: a final inconclusive, safe to checkpoint.
        result["reason"] = f"provider not entitled for this date: {exc}"
        return result
    except (MassiveRateLimitError, TimeoutError, ConnectionError) as exc:
        # NOT a verdict. inconclusive trims, and the verdict manifest is durable —
        # swallowing a transient outage here would permanently amputate real history.
        raise RetryableProviderError(f"{symbol}@{break_date}: {exc}") from exc
    except MassiveAPIError as exc:
        if exc.status_code is not None and exc.status_code >= 500:
            raise RetryableProviderError(f"{symbol}@{break_date}: {exc}") from exc
        result["reason"] = f"provider error: {exc}"
        return result
    if not raw:
        result["reason"] = "provider returned no raw bars for the window"
        return result
    if not adjusted:
        # Both bases are required. Raw agreement alone cannot separate a real move
        # from a missing corporate action — both show a provider raw jump.
        result["reason"] = "provider returned no adjusted bars: cannot separate a real move from a missing action"
        return result

    step = _factor_step(raw, adjusted, break_date)
    result["provider_factor_step"] = step
    # Check the factor step first: it is positive evidence of an event, and a
    # missing-action break also shows a provider raw jump, so raw-jump agreement
    # alone would misread it as a real move.
    if step is not None and abs(math.log(step)) > tolerance:
        result["verdict"] = "missing_action"
        result["reason"] = f"provider adjusted/raw factor steps {step:.2f}x at {break_date}"
        return result

    provider_ratio = _ratio_at(raw, break_date)
    result["provider_raw_ratio"] = provider_ratio
    if provider_ratio is None:
        result["reason"] = "provider has no adjacent pair at the break date"
        return result
    if abs(math.log(provider_ratio) - math.log(observed_ratio)) <= tolerance:
        result["verdict"] = "real_move"
        result["reason"] = f"provider raw shows the same {provider_ratio:.2f}x step"
        return result
    result["verdict"] = "bad_data"
    result["reason"] = f"provider raw steps {provider_ratio:.2f}x where bronze steps {observed_ratio:.2f}x"
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_break_triage.py -v` → PASS (6 tests)

- [ ] **Step 5: Write the batch command**

Create `livewire_scripts/triage_breaks.py`:

```python
"""Batch-triage audit-reported discontinuities against Massive as a second source.

Reads a legacy-basis audit manifest and, for every break it recorded — one symbol may
have several — asks `clients.break_triage` whether that break is a real market move,
bad bronze data, or a corporate action our store lacks. Read-only with respect to
bronze: it emits a verdict manifest that the silver-window resolver consumes to decide
what to keep. Resumable — each verdict is checkpointed as it lands, so a provider
rate-limit stall resumes instead of re-spending the whole population.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from clients.break_triage import DEFAULT_TOLERANCE, triage_break
from clients.massive_client import MassiveClient
from livewire_scripts.paths import data_lake_dir

SCHEMA_VERSION = 1
VERDICTS = ("real_move", "bad_data", "missing_action", "inconclusive")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--tickers", nargs="+", help="narrow to these symbols (default: every flagged symbol)")
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE)
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _key(candidate: dict) -> str:
    """Cursor key. Per (symbol, break) — a symbol may have several breaks to triage."""
    return f"{candidate['symbol']}@{candidate['date']}"


def _fetcher(client: Any, *, adjusted: bool) -> Callable[[str, date, date], list[dict]]:
    """Adapt MassiveClient's dataclass bars to the plain rows `triage_break` reads."""

    def fetch(symbol: str, start: date, end: date) -> list[dict]:
        bars = client.get_daily_bars(symbol, start, end, adjusted=adjusted)
        return [{"trade_date": bar.trade_date.isoformat(), "close": bar.close} for bar in bars]

    return fetch


def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    massive_factory: Callable[[], Any] = MassiveClient,
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else (args.data_lake_root or data_lake_dir())
    audit = json.loads(args.audit_manifest.read_text())
    audit_sha256 = _sha256(args.audit_manifest)
    # Same guard as repair: a manifest audited against another lake must never
    # decide what this lake trims.
    manifest_root = audit.get("data_lake_root")
    if manifest_root is not None and manifest_root != str(root.resolve()):
        raise ValueError(f"audit manifest data_lake_root {manifest_root} does not match active root {root.resolve()}")

    # One candidate per (symbol, break) — NOT per symbol. A multi-break symbol whose
    # later break is never triaged has that break trimmed away with its real history.
    # Breaks with no ratio (a non-positive close) are unusable as a second-source
    # comparison and are left to the window, which drops them regardless.
    candidates = [
        {"symbol": entry["symbol"], "date": str(brk["date"])[:10], "ratio": float(brk["ratio"])}
        for entry in audit["symbols"]
        for brk in entry.get("breaks") or []
        if brk.get("ratio") is not None
    ]
    if args.tickers:
        wanted = {t.upper() for t in args.tickers}
        candidates = [c for c in candidates if c["symbol"] in wanted]

    identity = {"schema_version": SCHEMA_VERSION, "audit_sha256": audit_sha256, "data_lake_root": str(root.resolve())}
    cursor_path = args.output.with_name(f"{args.output.name}.cursor.json")
    cursor: dict = {"identity": identity, "verdicts": {}}
    if args.resume and cursor_path.is_file():
        loaded = json.loads(cursor_path.read_text())
        if loaded.get("identity") != identity:
            raise ValueError("resume cursor does not match the active audit manifest")
        cursor = loaded

    todo = [c for c in candidates if _key(c) not in cursor["verdicts"]]
    if todo:
        # Construct the client OUTSIDE triage_break: a missing MASSIVE_API_KEY raises
        # MassiveAuthError here and aborts, rather than reading as N "inconclusive"
        # verdicts that would silently trim the entire population.
        with massive_factory() as client:
            fetch_raw = _fetcher(client, adjusted=False)
            fetch_adjusted = _fetcher(client, adjusted=True)
            for candidate in todo:
                cursor["verdicts"][_key(candidate)] = triage_break(
                    candidate["symbol"],
                    candidate["date"],
                    candidate["ratio"],
                    fetch_raw=fetch_raw,
                    fetch_adjusted=fetch_adjusted,
                    tolerance=args.tolerance,
                )
                _write_atomic(cursor_path, cursor)  # checkpoint per break, not per run

    verdicts = [cursor["verdicts"][_key(c)] for c in candidates if _key(c) in cursor["verdicts"]]
    counts = {name: sum(v["verdict"] == name for v in verdicts) for name in VERDICTS}
    _write_atomic(
        args.output,
        {
            "schema_version": SCHEMA_VERSION,
            "data_lake_root": str(root.resolve()),
            "audit_sha256": audit_sha256,
            "generated_at": datetime.now(UTC).isoformat(),
            "tolerance": args.tolerance,
            "counts": counts,
            "verdicts": sorted(verdicts, key=lambda v: (v["symbol"], v["date"])),
        },
    )
    print(json.dumps({**counts, "output": str(args.output)}, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

Register it in `scripts/livewire_quality.py`'s module map, in the existing alphabetical block:

```python
    "triage-breaks": "livewire_scripts.triage_breaks",
```

- [ ] **Step 6: Write the batch-command tests**

`livewire_scripts/` is inside the `fail_under = 95` coverage scope (`pyproject.toml` `source = ["clients", "livewire_scripts"]`), so this module ships with its own tests or it takes CI red.

Create `tests/test_triage_breaks.py`:

```python
"""Tests for the batch triage command. The provider is faked; no network."""

import json

import pytest

from livewire_scripts import triage_breaks


class _FakeBar:
    def __init__(self, trade_date, close):
        self.trade_date = trade_date
        self.close = close


class _FakeMassive:
    """Stands in for MassiveClient: same context-manager + get_daily_bars shape."""

    def __init__(self, series):
        self._series = series  # {(ticker, adjusted): [(iso_date, close), ...]}
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_daily_bars(self, ticker, start, end, *, adjusted=False):
        self.calls.append((ticker, adjusted))
        from datetime import date

        return [
            _FakeBar(date.fromisoformat(d), c)
            for d, c in self._series.get((ticker.upper(), adjusted), [])
        ]


def _audit(tmp_path, root, entries):
    path = tmp_path / "audit.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "data_lake_root": str(root.resolve()),
                "symbols": entries,
                "counts": {"clean": 0, "mixed": len(entries), "error": 0},
            }
        )
    )
    return path


# Real EQIX closes across its 2003-01-02 reverse-split boundary (frozen 2026-07-17):
# raw steps ~25x and the provider's adjusted/raw factor steps with it.
EQIX_RAW = [("2002-12-30", 0.21), ("2003-01-02", 5.24)]
EQIX_ADJ = [("2002-12-30", 5.24), ("2003-01-02", 5.24)]


def test_flagged_symbol_is_triaged_and_written_to_the_manifest(tmp_path):
    root = tmp_path / "lake"
    root.mkdir()
    audit = _audit(tmp_path, root, [{"symbol": "EQIX", "klass": "mixed", "breaks": [{"date": "2003-01-02", "ratio": 24.95}]}])
    out = tmp_path / "verdicts.json"
    client = _FakeMassive({("EQIX", False): EQIX_RAW, ("EQIX", True): EQIX_ADJ})

    rc = triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(out)],
        data_lake_root=root,
        massive_factory=lambda: client,
    )

    assert rc == 0
    manifest = json.loads(out.read_text())
    assert manifest["counts"]["missing_action"] == 1
    assert manifest["verdicts"][0]["symbol"] == "EQIX"
    assert manifest["data_lake_root"] == str(root.resolve())


def test_every_break_of_a_multi_break_symbol_is_triaged(tmp_path):
    """One verdict per (symbol, break). A break the triage never sees is a break the
    window trims away — taking its real history with it, permanently."""
    root = tmp_path / "lake"
    root.mkdir()
    audit = _audit(tmp_path, root, [{"symbol": "EQIX", "klass": "mixed", "breaks": [
        {"date": "2003-01-02", "ratio": 24.95}, {"date": "2015-06-01", "ratio": 8.1}]}])
    out = tmp_path / "verdicts.json"
    client = _FakeMassive({("EQIX", False): EQIX_RAW, ("EQIX", True): EQIX_ADJ})

    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(out)], data_lake_root=root, massive_factory=lambda: client
    )

    assert [v["date"] for v in json.loads(out.read_text())["verdicts"]] == ["2003-01-02", "2015-06-01"]


def test_a_break_with_no_ratio_is_not_sent_to_the_provider(tmp_path):
    """A non-positive close has nothing to compare; the window drops it regardless."""
    root = tmp_path / "lake"
    root.mkdir()
    audit = _audit(tmp_path, root, [{"symbol": "EQIX", "klass": "mixed",
                                     "breaks": [{"date": "2003-01-02", "ratio": None}]}])
    client = _FakeMassive({})
    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(tmp_path / "v.json")],
        data_lake_root=root, massive_factory=lambda: client,
    )
    assert client.calls == []


def test_clean_symbols_without_a_break_are_not_triaged(tmp_path):
    root = tmp_path / "lake"
    root.mkdir()
    audit = _audit(tmp_path, root, [{"symbol": "AAPL", "klass": "clean", "breaks": []}])
    out = tmp_path / "verdicts.json"
    client = _FakeMassive({})

    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(out)], data_lake_root=root, massive_factory=lambda: client
    )

    assert json.loads(out.read_text())["verdicts"] == []
    assert client.calls == []  # a clean symbol must never cost a provider call


def test_resume_skips_symbols_already_triaged(tmp_path):
    root = tmp_path / "lake"
    root.mkdir()
    audit = _audit(tmp_path, root, [{"symbol": "EQIX", "klass": "mixed", "breaks": [{"date": "2003-01-02", "ratio": 24.95}]}])
    out = tmp_path / "verdicts.json"
    first = _FakeMassive({("EQIX", False): EQIX_RAW, ("EQIX", True): EQIX_ADJ})
    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(out)], data_lake_root=root, massive_factory=lambda: first
    )

    second = _FakeMassive({("EQIX", False): EQIX_RAW, ("EQIX", True): EQIX_ADJ})
    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(out), "--resume"],
        data_lake_root=root,
        massive_factory=lambda: second,
    )

    assert second.calls == []  # resumed run re-spent nothing
    assert json.loads(out.read_text())["counts"]["missing_action"] == 1


def test_resume_rejects_a_cursor_from_a_different_audit(tmp_path):
    root = tmp_path / "lake"
    root.mkdir()
    entry = {"symbol": "EQIX", "klass": "mixed", "breaks": [{"date": "2003-01-02", "ratio": 24.95}]}
    audit = _audit(tmp_path, root, [entry])
    out = tmp_path / "verdicts.json"
    client = _FakeMassive({("EQIX", False): EQIX_RAW, ("EQIX", True): EQIX_ADJ})
    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(out)], data_lake_root=root, massive_factory=lambda: client
    )

    audit.write_text(json.dumps({"schema_version": 1, "data_lake_root": str(root.resolve()), "symbols": [entry, entry]}))
    with pytest.raises(ValueError, match="resume cursor does not match"):
        triage_breaks.run(
            ["--audit-manifest", str(audit), "--output", str(out), "--resume"],
            data_lake_root=root,
            massive_factory=lambda: client,
        )


def test_manifest_from_another_lake_is_rejected_before_any_provider_call(tmp_path):
    root = tmp_path / "lake"
    root.mkdir()
    audit = _audit(tmp_path, tmp_path / "other-lake", [{"symbol": "EQIX", "klass": "mixed", "breaks": [{"date": "2003-01-02", "ratio": 24.95}]}])
    client = _FakeMassive({})

    with pytest.raises(ValueError, match="does not match active root"):
        triage_breaks.run(
            ["--audit-manifest", str(audit), "--output", str(tmp_path / "v.json")],
            data_lake_root=root,
            massive_factory=lambda: client,
        )
    assert client.calls == []


def test_tickers_flag_narrows_the_population(tmp_path):
    root = tmp_path / "lake"
    root.mkdir()
    audit = _audit(
        tmp_path,
        root,
        [
            {"symbol": "EQIX", "klass": "mixed", "breaks": [{"date": "2003-01-02", "ratio": 24.95}]},
            {"symbol": "MTB", "klass": "mixed", "breaks": [{"date": "2000-10-06", "ratio": 10.2}]},
        ],
    )
    out = tmp_path / "verdicts.json"
    client = _FakeMassive({("EQIX", False): EQIX_RAW, ("EQIX", True): EQIX_ADJ})

    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(out), "--tickers", "EQIX"],
        data_lake_root=root,
        massive_factory=lambda: client,
    )

    assert {v["symbol"] for v in json.loads(out.read_text())["verdicts"]} == {"EQIX"}


def test_main_wraps_run(tmp_path, monkeypatch):
    seen = {}
    monkeypatch.setattr(triage_breaks, "run", lambda argv: seen.setdefault("argv", argv) or 0)
    assert triage_breaks.main(["--audit-manifest", "a", "--output", "b"]) == 0
    assert seen["argv"] == ["--audit-manifest", "a", "--output", "b"]
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/test_break_triage.py tests/test_triage_breaks.py -v` → PASS (13 tests)
Run: `uv run pytest tests/ -q --cov=clients --cov=livewire_scripts --cov-report=term-missing` → the 95% gate still holds.

- [ ] **Step 8: Commit**

```bash
uv run ruff check . && uv run ruff format --check .
git add clients/break_triage.py livewire_scripts/triage_breaks.py scripts/livewire_quality.py \
        tests/test_break_triage.py tests/test_triage_breaks.py
git commit -m "feat(silver): triage discontinuities against Massive raw and adjusted bars

A non-seed break is a real market move, bad bronze data, or a corporate action the
CA store lacks — and the candidate breaks provably contain all three, so trimming
them all would amputate real history. Massive serves both bases, which separates
them: raw-jump agreement means a real move; a smooth provider raw means our data
is bad; a step in the provider's adjusted/raw factor means it knows a split our
store does not (its split endpoint collapses before 2003: 33 events for 1978-2002
vs 148 in 2003 alone). One verdict per (symbol, break), not per symbol: a symbol
may have several breaks and an untriaged one is trimmed with its real history."
```

---

### Task 8: Silver-grade window resolver

**Files:**
- Modify: `clients/silver_window.py` (created in Task 1b with `find_breaks`; this task adds `resolve_window`)
- Modify: `tests/test_silver_window.py`

**Interfaces:**
- Produces:
  - `resolve_window(adjusted_rows, *, threshold=6.0, allowlist=frozenset(), keep_dates=frozenset()) -> dict` → `{"start": str | None, "trimmed_at": str | None, "reason": str, "rows_dropped": int}`
- Consumes: `find_breaks` from Task 1b.
  - `start` is the first `trade_date` of the silver-grade window (`None` only when the series is empty).
  - `keep_dates` are triage-confirmed `real_move` dates — treated exactly like the operator `allowlist`, so a genuine move never trims.

**Why `find_breaks` is a separate primitive with three consumers.** `check_adjusted_continuity` raises on the **first** break (`clients/silver_continuity.py:58`), so an audit built on it records exactly one `break_date` per symbol. But `resolve_window` trims at the **last** break. A symbol with two real moves — say 2003 and 2015 — would have only its 2003 break triaged; 2015 stays unexplained, the window trims there anyway, and 2003–2015 of real history is amputated *permanently*, because no later audit ever surfaces the 2015 break for triage. Enumerating every break once, and letting the audit, the triage command and the resolver all read the same list, is what makes the triage actually cover what it claims to.

**Why derived, never persisted:** the window is recomputed on every publish, so backfilling older history extends it automatically once the data supports it, and a bad new bar cannot silently corrupt the published series — it just fails to extend the window and is reported.

- [ ] **Step 1: Write the failing tests**

```python
"""Unit tests for the silver-grade window resolver. Frozen real closes, no network."""

import pytest

from clients.silver_window import resolve_window


def _rows(pairs):
    return [{"trade_date": d, "close": c} for d, c in pairs]


# Real AAPL adjusted closes (production silver, frozen 2026-07-17): continuous.
AAPL = _rows([("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91)])

# EQIX shape: an unexplained ~25x step at 2003-01-02 with no CA record.
EQIX = _rows([("2002-12-27", 0.20), ("2002-12-30", 0.21), ("2003-01-02", 5.24), ("2003-01-03", 5.31)])


def test_continuous_series_keeps_its_whole_history():
    result = resolve_window(AAPL)
    assert result["start"] == "2024-01-02"
    assert result["trimmed_at"] is None
    assert result["rows_dropped"] == 0


def test_unexplained_break_trims_to_the_break_date():
    result = resolve_window(EQIX)
    assert result["start"] == "2003-01-02"
    assert result["trimmed_at"] == "2003-01-02"
    assert result["rows_dropped"] == 2


def test_window_starts_after_the_LAST_break_not_the_first():
    rows = _rows([("2001-01-02", 1.00), ("2001-01-03", 50.00),   # break 1
                  ("2002-01-02", 51.00), ("2002-01-03", 4.00),   # break 2 (later)
                  ("2002-01-04", 4.05)])
    result = resolve_window(rows)
    assert result["start"] == "2002-01-03"
    assert result["rows_dropped"] == 3


def test_keep_dates_from_triage_do_not_trim():
    result = resolve_window(EQIX, keep_dates=frozenset({"2003-01-02"}))
    assert result["start"] == "2002-12-27"
    assert result["trimmed_at"] is None


def test_operator_allowlist_does_not_trim():
    result = resolve_window(EQIX, allowlist=frozenset({"2003-01-02"}))
    assert result["start"] == "2002-12-27"


def test_empty_series_has_no_window():
    assert resolve_window([])["start"] is None


def test_non_positive_close_trims_that_row_out():
    rows = _rows([("2024-01-02", 185.64), ("2024-01-03", 0.0), ("2024-01-04", 181.91)])
    result = resolve_window(rows)
    assert result["start"] == "2024-01-04"
    assert "non-positive" in result["reason"]


def test_a_break_on_the_last_row_leaves_a_single_row_window():
    """The window is never empty for a non-empty series: it starts AT the break, so
    the worst case is a one-row window, not a dropped symbol."""
    rows = _rows([("2024-01-02", 1.0), ("2024-01-03", 100.0)])
    result = resolve_window(rows)
    assert result["start"] == "2024-01-03"
    assert result["rows_dropped"] == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_silver_window.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'clients.silver_window'`

- [ ] **Step 3: Write the implementation**

```python
"""Resolve the silver-grade window of an adjusted daily series.

Silver grade means: every row's basis is correct, every split inside the window is
recorded, and the adjusted series has no discontinuity that is not an evidenced
real market move. Deep history is not a goal — a symbol may publish a short
window; what it publishes must be right.

The window is the longest SUFFIX with no bad discontinuity: start the day of the
last unexplained break and keep everything after it. Derived on every publish and
never persisted, so backfilled history extends the window by itself once the data
supports it, and a bad new bar cannot silently corrupt what is already published.
"""

from __future__ import annotations

import math

DEFAULT_THRESHOLD = 6.0


def resolve_window(
    rows: list[dict],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    allowlist: frozenset[str] = frozenset(),
    keep_dates: frozenset[str] = frozenset(),
) -> dict:
    """Return the silver-grade window of an adjusted series.

    ``allowlist`` is the operator override (evidence-backed halts/relistings);
    ``keep_dates`` are triage-confirmed real market moves. Both mean "this step is
    real, do not trim". The window starts at the LAST remaining break, so everything
    it serves is downstream of every known problem.
    """
    ordered = sorted(rows, key=lambda row: str(row["trade_date"])[:10])
    if not ordered:
        return {"start": None, "trimmed_at": None, "reason": "empty series", "rows_dropped": 0}
    breaks = find_breaks(ordered, threshold=threshold, exempt=allowlist | keep_dates)
    if not breaks:
        return {
            "start": str(ordered[0]["trade_date"])[:10],
            "trimmed_at": None,
            "reason": "continuous",
            "rows_dropped": 0,
        }
    last = breaks[-1]
    dropped = sum(1 for row in ordered if str(row["trade_date"])[:10] < last["date"])
    return {"start": last["date"], "trimmed_at": last["date"], "reason": last["reason"], "rows_dropped": dropped}
```

- [ ] **Step 4: Run tests and commit**

```bash
uv run pytest tests/test_silver_window.py -v
uv run ruff check . && uv run ruff format --check .
git add clients/silver_window.py tests/test_silver_window.py
git commit -m "feat(silver): resolve each symbol's silver-grade window

Full-universe silver grade does not require full history: publish the longest
suffix with no unexplained discontinuity rather than quarantining the whole
symbol. Derived on every publish and never persisted, so backfilled history
extends the window once the data supports it and a bad new bar cannot silently
corrupt the published series. Triage-confirmed real moves and the operator
allowlist both suppress a trim."
```

---

### Task 9: Publish the window, and publish a complete manifest

**Files:**
- Modify: `livewire_scripts/rebuild_silver.py` (imports, `parse_args`, staging, transaction block)
- Test: `tests/test_rebuild_silver.py`

**Interfaces:**
- `rebuild-silver --triage-manifest <path>` — overrides the default location; supplies `keep_dates` per symbol from Task 7's verdicts.
- `DEFAULT_TRIAGE_MANIFEST = "repairs/triage/current.json"`, resolved against the data-lake root and loaded whenever the file exists.

**Why the triage manifest MUST have a default path, not just a flag.** The nightly job runs `rebuild-silver --full` with no extra arguments (`livewire_scripts/run_daily_update_job.py:129`). If the verdicts only arrive via an optional flag, then the night after rev-3 publishes, `keep_by_symbol` is empty, every triage-confirmed `real_move` is re-read as an unexplained break, and each of those symbols' windows collapses — real history amputated, and a `window_regression` fired for every one of them. The triage verdicts are durable *evidence*; only the window is derived from them. So they live at a well-known path that every caller picks up, and the flag exists to point at a candidate manifest during review.
- `StagedSymbol` gains `window: dict`. Staging trims `rows` to the window before publishing; `earliest_date` becomes the window start.
- The manifest handed to `commit` lists **every symbol that currently has valid published artifacts**, not just this run's rebuilds.

**Two trims, in this order.** They catch different things and neither subsumes the other:
1. **The seed floor** (`classify_seed_boundary`) — deterministic, known location, predicted fold. It is the *only* thing that sees the 2×–5× class, and it applies to raw bronze **before** adjustment. A corrupt symbol's floor is the seed-window date itself: rows on/after it are genuine raw.
2. **The window** (`resolve_window`) — a blind >threshold scan of the *adjusted* series, for everything else.

The seed detector enters here as a floor, **not** as the raising gate Task 2 originally proposed: quarantining a seed-corrupt symbol would drop a symbol that has ~5 years of perfectly good post-2021-06 history, which is precisely what the goal forbids.

**Why the complete manifest:** `clients/silver_revision.py:107-124` writes exactly the artifacts it is handed and never merges `current.artifacts` — rev-1 = 9,207 symbols, rev-2 = 3,350. A complete manifest is correct whether the consumer replaces or unions revisions; Task 11 settles which.

- [ ] **Step 1: Write the failing tests**

```python
def test_symbol_with_an_unexplained_break_publishes_its_window_not_nothing(tmp_path):
    """Full universe: a trimmed symbol still publishes — correct, just shorter."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "EQIX", [("2002-12-30", 0.21), ("2003-01-02", 5.24), ("2003-01-03", 5.31)])
    assert run(["--tickers", "EQIX"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17)) == 0
    published = pq.ParquetFile(silver / "asset_class=equity" / "symbol=EQIX" / "1d.parquet").read().to_pylist()
    assert [str(r["trade_date"]) for r in published] == ["2003-01-02", "2003-01-03"]


def test_seed_corrupt_symbol_publishes_its_post_seed_window_rather_than_quarantining(tmp_path):
    """The 2x class the 6.0 scan cannot see: APH must publish from the seed date on,
    NOT be dropped. Real APH closes + its real 2026-03-04 2:1 split."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "APH", [("2021-06-09", 33.98), ("2021-06-10", 34.13),
                               ("2021-06-11", 68.45), ("2021-06-14", 68.60)])
    _seed_split(root, "APH", "2026-03-04", 1, 2)
    failures = tmp_path / "failures.json"
    assert run(["--tickers", "APH", "--failure-output", str(failures)],
               data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17)) == 0
    assert json.loads(failures.read_text())["failures"] == []   # published, not quarantined
    published = pq.ParquetFile(silver / "asset_class=equity" / "symbol=APH" / "1d.parquet").read().to_pylist()
    assert [str(r["trade_date"]) for r in published] == ["2021-06-11", "2021-06-14"]


def test_factor_intervals_still_cover_dates_trimmed_out_of_the_daily_window(tmp_path):
    """Factors must NOT be narrowed to the daily window. Apex LEFT JOINs bronze
    intraday onto these intervals and 500s on any uncovered bronze bar
    (apex ohlc_provider.py:236-240); bronze intraday predates the trimmed window."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "EQIX", [("2002-12-30", 0.21), ("2003-01-02", 5.24), ("2003-01-03", 5.31)])
    run(["--tickers", "EQIX"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    daily = pq.ParquetFile(silver / "asset_class=equity" / "symbol=EQIX" / "1d.parquet").read().to_pylist()
    factors = pq.ParquetFile(
        silver / "adjustments" / "asset_class=equity" / "symbol=EQIX" / "factors.parquet"
    ).read().to_pylist()
    assert min(str(r["trade_date"]) for r in daily) == "2003-01-02"      # daily IS trimmed
    assert min(str(f["effective_start"]) for f in factors) == "2002-12-30"  # factors are NOT


def test_triage_confirmed_real_move_is_not_trimmed(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "MRNA", [("2020-11-13", 89.39), ("2020-11-16", 900.00), ("2020-11-17", 905.00)])
    triage = tmp_path / "triage.json"
    triage.write_text(json.dumps({"verdicts": [{"symbol": "MRNA", "date": "2020-11-16", "verdict": "real_move"}]}))
    run(["--tickers", "MRNA", "--triage-manifest", str(triage)],
        data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    published = pq.ParquetFile(silver / "asset_class=equity" / "symbol=MRNA" / "1d.parquet").read().to_pylist()
    assert len(published) == 3


def test_verdicts_at_the_default_path_are_honoured_without_any_flag(tmp_path):
    """The nightly job passes no flags (run_daily_update_job.py:129). If the verdicts
    are not found by default, every real move is re-trimmed the night after rev-3."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "MRNA", [("2020-11-13", 89.39), ("2020-11-16", 900.00), ("2020-11-17", 905.00)])
    default = root / "repairs" / "triage" / "current.json"
    default.parent.mkdir(parents=True)
    default.write_text(json.dumps({"verdicts": [{"symbol": "MRNA", "date": "2020-11-16", "verdict": "real_move"}]}))
    run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    published = pq.ParquetFile(silver / "asset_class=equity" / "symbol=MRNA" / "1d.parquet").read().to_pylist()
    assert len(published) == 3


def test_an_explicitly_named_missing_triage_manifest_is_an_error_not_silence(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    with pytest.raises(SystemExit, match="triage manifest not found"):
        run(["--tickers", "AAPL", "--triage-manifest", str(tmp_path / "nope.json")],
            data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))


def test_targeted_rebuild_keeps_previously_published_symbols_in_the_manifest(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    _seed_bronze(root, "MSFT", [("2024-01-02", 370.87), ("2024-01-03", 370.60)])
    run(["--tickers", "AAPL", "MSFT"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    _seed_bronze(root, "MSFT", [("2024-01-02", 370.87), ("2024-01-03", 370.60), ("2024-01-04", 367.94)])
    run(["--tickers", "MSFT"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    current = json.loads((silver / "revisions" / "current.json").read_text())
    symbols = {a["path"].split("symbol=")[1].split("/")[0] for a in current["artifacts"]}
    assert symbols == {"AAPL", "MSFT"}   # AAPL must not vanish
    assert current["revision"] == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_rebuild_silver.py -v -k "window or real_move or previously_published"`
Expected: FAIL — unrecognized `--triage-manifest`; EQIX publishes 3 rows; manifest holds only `{"MSFT"}`.

- [ ] **Step 3: Implement the window in staging**

Imports (line 24):

```python
from clients.seed_boundary import classify_seed_boundary
from clients.silver_client import PublishedArtifact, SilverClient
from clients.silver_revision import AffectedSymbol, ManifestArtifact, SilverRevision, SilverRevisionPublisher
from clients.silver_window import resolve_window
```

Module constant, next to `CONTINUITY_THRESHOLD`:

```python
# Resolved against the data-lake root. The nightly job passes no flags
# (run_daily_update_job.py:129), so the verdicts must be found, not passed.
DEFAULT_TRIAGE_MANIFEST = "repairs/triage/current.json"
```

`parse_args`:

```python
    parser.add_argument(
        "--triage-manifest",
        type=Path,
        help=f"break-triage verdicts; real_move dates are kept rather than trimmed "
        f"(default: <data-lake-root>/{DEFAULT_TRIAGE_MANIFEST} when present)",
    )
```

Add `window: dict` to `StagedSymbol`. In `run()`, load the triage keep-dates before the staging loop:

```python
    triage_path = args.triage_manifest or (root / DEFAULT_TRIAGE_MANIFEST)
    keep_by_symbol: dict[str, frozenset[str]] = {}
    if triage_path.is_file():
        payload = json.loads(triage_path.read_text())
        for verdict in payload.get("verdicts", []):
            if verdict.get("verdict") == "real_move":
                symbol = str(verdict["symbol"]).upper()
                keep_by_symbol[symbol] = keep_by_symbol.get(symbol, frozenset()) | {str(verdict["date"])}
    elif args.triage_manifest is not None:
        # An explicitly-named manifest that does not exist is an operator error, not
        # "no verdicts" — silently trimming every real move is the failure we are
        # trying to prevent.
        raise SystemExit(f"triage manifest not found: {triage_path}")
```

In the staging loop, `check_adjusted_continuity(...)` is replaced by the seed floor plus the window resolve:

```python
            actions = action_store.latest_active(symbol)
            # Trim 1 — the seed floor, applied to RAW bronze before adjustment. The
            # only detector that sees the 2x-5x class; a corrupt symbol's pre-window
            # rows are IB back-adjusted, its rows on/after the window are true raw.
            # Trim rather than quarantine: the post-seed years are perfectly good.
            seed = classify_seed_boundary(rows, actions)
            if seed["verdict"] == "corrupt":
                rows = [row for row in rows if str(row["trade_date"])[:10] >= seed["date"]]
            intervals = build_factor_intervals(rows, actions, effective_as_of)
            adjusted = adjust_daily_rows(rows, intervals, revision=1)
            # Trim 2 — the blind window scan over the ADJUSTED series, for every other
            # unexplained break. Keep triage-confirmed real moves and allowlisted dates.
            window = resolve_window(
                adjusted,
                threshold=threshold,
                allowlist=frozenset(args.continuity_allowlist),
                keep_dates=keep_by_symbol.get(symbol, frozenset()),
            )
            kept = [row for row in rows if str(row["trade_date"])[:10] >= window["start"]]
            # NOTE: `intervals` stay built over the FULL pre-trim `rows`. Do NOT rebuild
            # them over `kept` to "make the factor file match the daily file" — that is
            # a correctness trap. Apex's adjusted-intraday path LEFT JOINs BRONZE
            # intraday bars onto these factor intervals and hard-fails when any bronze
            # bar has no interval (apex `ohlc_provider.py:236-240`,
            # "incomplete or overlapping factor coverage" -> HTTP 500). Bronze intraday
            # extends before the trimmed daily window, so narrowing the factors to the
            # daily window breaks intraday for exactly the symbols we just trimmed.
            # Factors WIDER than the daily rows are harmless; narrower is fatal.
            staged.append(
                StagedSymbol(
                    symbol,
                    kept,
                    intervals,
                    actions,
                    min(_trade_date(row["trade_date"]) for row in kept),
                    window,
                )
            )
```

Remove the old `check_adjusted_continuity` call and the old `staged.append(...)` block it guarded — the window resolver subsumes the gate for publication, while the gate's threshold and allowlist now feed it. Drop the import of `check_adjusted_continuity` from `livewire_scripts/rebuild_silver.py:23` (the module stays; `audit_legacy_basis` still uses it).

> **No `window["start"] is None` / empty-`kept` guard is needed.** `rows` is already known non-empty (line 216 raises `missing equity bronze rows`), the seed floor is a date measured *in* the series, and `resolve_window` returns a `start` that is always a date present in the series — so `kept` always holds at least that row. A guard here would be unreachable code that the coverage gate then reports as a miss.

- [ ] **Step 4: Implement the complete manifest**

Add next to `_matches_existing`:

```python
def _carry_forward(
    client: SilverClient,
    current: SilverRevision | None,
    staged: list[StagedSymbol],
    changed: list[StagedSymbol],
    scope: set[str],
) -> tuple[list[PublishedArtifact], list[AffectedSymbol]]:
    """Re-list still-valid symbols this run did not republish.

    Carried: symbols outside ``scope`` (a targeted rebuild must not evict the
    universe) and in-scope symbols that staged cleanly but were byte-identical to
    what is published. NOT carried: symbols republished here (already added), and
    in-scope symbols that failed staging — dropping them is the quarantine.
    """
    if current is None:
        return [], []
    staged_ok = {item.symbol for item in staged}
    republished = {item.symbol for item in changed}
    previous_affected = {item.symbol: item for item in current.affected}
    by_symbol: dict[str, list[ManifestArtifact]] = {}
    for artifact in current.artifacts:
        if "symbol=" not in artifact.path:
            continue
        by_symbol.setdefault(artifact.path.split("symbol=")[1].split("/")[0], []).append(artifact)

    artifacts: list[PublishedArtifact] = []
    affected: list[AffectedSymbol] = []
    for symbol, entries in sorted(by_symbol.items()):
        if symbol in republished:
            continue
        if symbol in scope and symbol not in staged_ok:
            continue
        previous = previous_affected.get(symbol)
        if previous is None:
            continue
        resolved: list[PublishedArtifact] = []
        for artifact in entries:
            path = client.root / artifact.path
            if not path.is_file():
                resolved = []
                break
            # row_count is not serialized into the manifest but PublishedArtifact
            # requires it — read the footer rather than inventing a number.
            resolved.append(PublishedArtifact(path, artifact.sha256, pq.ParquetFile(path).metadata.num_rows))
        if not resolved:
            continue  # a vanished artifact must not be manifested
        artifacts.extend(resolved)
        affected.append(previous)   # one per symbol: _validate_affected rejects dupes
    return artifacts, affected
```

Replace the transaction block (lines 293-314):

```python
    with publisher.transaction() as transaction:
        changed = [item for item in staged if not _matches_existing(client, item)]
        if not changed:
            revision = 0 if transaction.current is None else transaction.current.revision
            rebuilt = 0
            unchanged = len(staged)
        else:
            revision = transaction.revision
            artifacts = []
            affected = []
            actions_as_of = datetime.now(UTC)
            for item in changed:
                daily_rows = adjust_daily_rows(item.rows, item.intervals, revision=revision)
                intervals = [replace(interval, adjustment_revision=revision) for interval in item.intervals]
                artifacts.append(client.publish_daily(item.symbol, daily_rows))
                artifacts.append(client.publish_factors(item.symbol, intervals))
                affected.append(AffectedSymbol(item.symbol, item.earliest_date, TIMEFRAMES))
                if item.actions:
                    actions_as_of = max(actions_as_of, *(action.fetched_at for action in item.actions))
            carried_artifacts, carried_affected = _carry_forward(
                client, transaction.current, staged, changed, {s.upper() for s in symbols}
            )
            artifacts.extend(carried_artifacts)
            affected.extend(carried_affected)
            revision = transaction.commit(artifacts, affected, actions_as_of).revision
            rebuilt = len(changed)
            unchanged = len(staged) - rebuilt
```

Add `trimmed` to **all three** `_summary(...)` calls — the dry-run one (line 266), the
no-change one (line 280) and the final one (line 316): `trimmed=sum(1 for item in staged if item.window["trimmed_at"])`.
The dry-run one matters most: Task 13 Step 4 gates the real publish on reading it.

- [ ] **Step 5: Run tests and commit**

```bash
uv run pytest tests/test_rebuild_silver.py -v
uv run ruff check . && uv run ruff format --check .
git add livewire_scripts/rebuild_silver.py tests/test_rebuild_silver.py
git commit -m "feat(silver): publish each symbol's silver-grade window, and a complete manifest

Quarantining a whole symbol over one unexplained break traded full-universe
coverage for correctness; publishing its window keeps both — the series is shorter
but right, which is what the goal asks for. Separately, the publisher writes
exactly the artifacts it is handed and never merges the previous revision, so a
targeted rebuild produced a manifest listing only the rebuilt symbols (rev-1 =
9,207, rev-2 = 3,350). Carry forward every still-valid symbol and let only
quarantined ones drop out."
```

---

### Task 10: Keep new data silver grade (the prevention invariant)

**Files:**
- Modify: `livewire_scripts/rebuild_silver.py` (`parse_args`, staging→transaction, `_summary`, failure output)
- Modify: `livewire_scripts/run_daily_update_job.py` (nightly job: surface the regression)
- Test: `tests/test_rebuild_silver.py`

**Interfaces:**
- `rebuild-silver` reports `window_regressions: [{symbol, previous_start, new_start, reason}]` in `--failure-output` and counts them in `SUMMARY_JSON`.
- A **regression** is a symbol whose window start moved **later** than the currently published `earliest_date`.
- `rebuild-silver --allow-window-regression` publishes them anyway. **Required exactly once, for the rev-3 bootstrap.**

**Why this must fail closed, not merely alert — the suffix rule is wrong at the right-hand edge.** `resolve_window` starts the window at the *last* break, which encodes the assumption "the newer side is the trustworthy side". That holds for the seed artifact, where the old side is the back-adjusted one. It is **false for a bad new bar**: a single corrupt close arriving tonight is the last break, so the window collapses to start at it, and the symbol publishes *one garbage row* having dropped years of good history. Nothing in the resolver can tell which side of a one-sided boundary is trustworthy — only the comparison against what we already published can.

So the rule is: a symbol whose window start moved later **does not republish**. Its previously published artifacts are carried forward unchanged, and the regression is reported and alerted. The bad bar cannot corrupt what ships, *and* it cannot cost us history — which is exactly the goal's "data added at either end must be silver grade or must not publish".

**The rev-3 bootstrap is the one deliberate exception.** rev-2 published untrimmed full history, so on the first run under these rules essentially every trimmed symbol looks like a regression and nothing would publish. Task 13 Step 4 therefore runs rev-3 once with `--allow-window-regression`, after reviewing the dry-run's `window_regressions` list. Every run after that fails closed by default, because from rev-3 on, a shrinking window means new data went bad.

- [ ] **Step 1: Write the failing test**

```python
def test_a_new_bad_bar_that_shortens_the_window_does_not_publish(tmp_path):
    """THE core invariant. A corrupt bar arriving at the newest edge is the LAST
    break, so the suffix rule would start the window at it and publish that single
    garbage row, dropping all real history. It must fail closed instead: keep serving
    what was published, and alert."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91)])
    run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    # A corrupt bar lands at the newest edge.
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25),
                                ("2024-01-04", 181.91), ("2024-01-05", 4.20)])
    failures = tmp_path / "failures.json"
    run(["--tickers", "AAPL", "--failure-output", str(failures)],
        data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))

    regression = json.loads(failures.read_text())["window_regressions"][0]
    assert regression["symbol"] == "AAPL"
    assert regression["previous_start"] == "2024-01-02"
    assert regression["new_start"] == "2024-01-05"
    # The published artifact is UNCHANGED — the garbage singleton never shipped.
    published = pq.ParquetFile(silver / "asset_class=equity" / "symbol=AAPL" / "1d.parquet").read().to_pylist()
    assert [str(r["trade_date"]) for r in published] == ["2024-01-02", "2024-01-03", "2024-01-04"]
    # ...and the symbol is still in the manifest, not evicted.
    current = json.loads((silver / "revisions" / "current.json").read_text())
    assert any("symbol=AAPL" in a["path"] for a in current["artifacts"])


def test_allow_window_regression_publishes_the_shorter_window(tmp_path):
    """The rev-3 bootstrap: rev-2 published untrimmed history, so the intentional
    mass trim must be able to land once, under operator review."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91)])
    run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25),
                                ("2024-01-04", 181.91), ("2024-01-05", 4.20)])
    run(["--tickers", "AAPL", "--allow-window-regression"],
        data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    published = pq.ParquetFile(silver / "asset_class=equity" / "symbol=AAPL" / "1d.parquet").read().to_pylist()
    assert [str(r["trade_date"]) for r in published] == ["2024-01-05"]


def test_no_regression_reported_when_the_window_is_stable(tmp_path):
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    run(["--tickers", "AAPL"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25), ("2024-01-04", 181.91)])
    failures = tmp_path / "failures.json"
    run(["--tickers", "AAPL", "--failure-output", str(failures)],
        data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    assert json.loads(failures.read_text())["window_regressions"] == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_rebuild_silver.py -v -k "regression"`
Expected: FAIL — `KeyError: 'window_regressions'`

- [ ] **Step 3: Implement**

Add the flag in `parse_args`:

```python
    parser.add_argument(
        "--allow-window-regression",
        action="store_true",
        help="publish symbols whose window start moved later (required once, for the rev-3 bootstrap)",
    )
```

`run()` already reads the published revision at line 248 (`current = publisher.read_current()`); reuse that binding rather than re-reading. Immediately after it, compare each staged window against what is published, and **withhold the regressed symbols from publication**:

```python
    previous_start = {item.symbol: item.earliest_date for item in (current.affected if current else ())}
    regressions = [
        {
            "symbol": item.symbol,
            "previous_start": previous_start[item.symbol].isoformat(),
            "new_start": item.window["start"],
            "reason": item.window["reason"],
        }
        for item in staged
        if item.symbol in previous_start and item.window["start"] > previous_start[item.symbol].isoformat()
    ]
    # Fail closed. The resolver cannot tell which side of a one-sided boundary is
    # trustworthy; only this comparison against what we already serve can.
    regressed = set() if args.allow_window_regression else {item["symbol"] for item in regressions}
    publishable = [item for item in staged if item.symbol not in regressed]
```

> **Do NOT rebind `staged`.** A withheld symbol must be *carried forward*, not evicted, and `_carry_forward` decides that from `staged_ok = {item.symbol for item in staged}` — dropping it from `staged` puts it in neither `changed` nor the carried set, which evicts exactly the symbol you were protecting. Keep `staged` as the "staged cleanly" set that feeds `_carry_forward`, and introduce `publishable` as the "may be republished" subset.

Then thread `publishable` (not `staged`) into the republish decision, in both the dry-run predicate and the transaction block:

```python
    changed = [item for item in publishable if not _matches_existing(client, item)]
```

`_carry_forward(client, transaction.current, staged, changed, scope)` keeps taking `staged`, so a regressed symbol is in `staged_ok`, is not in `republished`, and is therefore carried on its previous artifacts — serving its old, longer, still-valid window.

Include `regressions` in the `--failure-output` payload under `window_regressions`, and add `window_regressions=len(regressions)` to **all three** `_summary(...)` calls (dry-run line 266, no-change line 280, final line 316) so both the pre-publish dry-run review and the nightly `SUMMARY_JSON` carry it.

Note the deliberate asymmetry: the `--failure-output` payload gains `window_regressions` **alongside** `failures`, not inside it. A regression is not a staging failure — the symbol still publishes, just shorter — so it must not inflate `failed` and must not affect `resolve_exit_code`. It is an alert, not a job failure.

In `scripts/livewire_ops.py`, treat a non-zero `window_regressions` in the rebuild-silver phase as an alert-worthy condition in the nightly digest — new data that costs published history must page a human, even though it can no longer corrupt what ships.

- [ ] **Step 4: Run tests and commit**

```bash
uv run pytest tests/test_rebuild_silver.py -v
uv run ruff check . && uv run ruff format --check .
git add livewire_scripts/rebuild_silver.py scripts/livewire_ops.py tests/test_rebuild_silver.py
git commit -m "feat(silver): report window regressions so new data stays silver grade

Data added at either end — backfilled history or a new daily bar — can introduce a
break. The window resolver already stops that corrupting what ships, but a silent
loss of published history is its own failure: report any symbol whose window start
moved later than the revision currently serving it, and alert on it nightly."
```

---

### Task 11: Evict quarantined symbols by deleting their artifacts

**Files:**
- Modify: `livewire_scripts/rebuild_silver.py` (evict dropped symbols)
- Test: `tests/test_rebuild_silver.py`

**This task was a spike. It is now ANSWERED — and the answer changes the design.** Verified against apex `origin/master` @ `4df7b62` (the local checkout is 7 commits behind and its `ohlc_provider.py` has no Silver code at all).

**Apex is neither replace nor union — the manifest is not a view definition.** It is a cache-invalidation / reseed signal. The served view is **the filesystem**, resolved by pure path construction at request time:

- `ohlc_provider.py:141-145` builds `<silver_root>/asset_class=equity/symbol=<enc>/1d.parquet` via `daily_silver_path` (`paths.py:48-50`) and reads whatever is there. **The manifest is never consulted for membership.**
- `revision.affected` is used only to reseed *already-subscribed* symbols (`manager.py:163-164`).
- `revision=N.json` is **never read** — only `revisions/current.json` (`revisions.py:49-54`).
- `per_symbol_revision` (`revision_watcher.py:120`) and `_applied_revisions` (`manager.py:174`) are write-only accumulating dicts. **No eviction path exists anywhere.**
- A `removed: [...]` manifest key would be **silently ignored**: `read_current` reads six named keys (`revisions.py:58-74`) and nothing else. Shipping one accomplishes nothing without an apex-side change.
- The `9,207 + 3,350 = 12,557 ≈ 12,548` arithmetic was a **coincidence**. Apex's ~12.5K is a *bronze* count — production runs `APEX_LIVEWIRE_PRICE_MODE=raw` (`docker-compose.yml:31,40`), so every bar it serves today comes from bronze.

**Therefore: dropping a symbol from the manifest does nothing. Deleting its file is the only removal signal apex can perceive.** Leave a quarantined symbol's stale `1d.parquet` on disk and apex serves that corrupt data forever, with no error and no staleness signal. Delete it and apex fails closed with HTTP 500 (`AdjustedDataUnavailable`, `ohlc_provider.py:39-40,144`, never caught, propagates through `chart.py:86`) — the observed INTC behaviour, and the correct outcome.

Two further verified facts that matter:
- **sha256 is verified for every artifact on every 30s poll** (`revisions.py:150-155`), and a mismatch rejects the **whole** revision atomically before any symbol is touched (`_verify_artifacts` runs first). So the complete manifest from Task 9 is still required — it is what keeps apex's verification passing — it just is not what evicts.
- **Revision state is in-memory only** (`manager.py:92`, `revision_watcher.py:43-45`); no DB table, no state file. Gaps are legal, only regression is rejected (`revision_watcher.py:93-96`), and a restart zeroes `observed_revision`. **rev-3 applies directly on top of rev-1 with no sequencing requirement.**

- [ ] **Step 1: Write the failing test**

```python
def test_a_quarantined_symbol_s_stale_artifact_is_deleted_not_just_unmanifested(tmp_path):
    """Apex resolves symbols by path construction and never consults the manifest
    (apex ohlc_provider.py:141-145). Un-manifesting a symbol leaves it serving stale
    corrupt data forever; deleting the file is the only eviction apex can perceive."""
    root, silver = tmp_path / "lake", tmp_path / "silver"
    _seed_bronze(root, "AAPL", [("2024-01-02", 185.64), ("2024-01-03", 184.25)])
    _seed_bronze(root, "INTC", [("2024-01-02", 47.09), ("2024-01-03", 46.28)])
    run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))
    assert (silver / "asset_class=equity" / "symbol=INTC" / "1d.parquet").exists()

    # INTC now fails staging (unknown-basis rows against a split).
    _seed_bronze(root, "INTC", [("2024-01-02", 47.09), ("2024-01-03", 46.28)], price_basis="unknown")
    _seed_split(root, "INTC", "2024-01-03", 1, 2)
    run(["--full"], data_lake_root=root, silver_root=silver, as_of_date=date(2026, 7, 17))

    assert not (silver / "asset_class=equity" / "symbol=INTC" / "1d.parquet").exists()
    assert (silver / "asset_class=equity" / "symbol=AAPL" / "1d.parquet").exists()
    current = json.loads((silver / "revisions" / "current.json").read_text())
    assert not any("symbol=INTC" in a["path"] for a in current["artifacts"])
```

- [ ] **Step 2: Implement the eviction**

Inside the transaction, after `_carry_forward` decides what is manifested: any **in-scope** symbol that has published artifacts but is in neither `changed` nor the carried set is quarantined — remove its daily artifact from the tree so apex fails closed instead of serving it. Move it aside rather than unlinking, so an eviction is reversible:

```python
    evicted = _evict_unmanifested(client, transaction.current, artifacts, {s.upper() for s in symbols})
```

`_evict_unmanifested` moves each dropped symbol's artifacts to `<silver_root>/evicted/<revision>/<original relative path>` and returns the list. Do **not** delete the factor artifact when the daily one is evicted — see Task 9's note: apex's intraday path joins bronze bars onto factors independently, and a missing factor file is its own 500.

Report the count as `evicted=len(evicted)` in all three `_summary(...)` calls, and list them in `--failure-output`. An eviction is a symbol going dark for a consumer; it must never be silent.

- [ ] **Step 3: Commit**

```bash
uv run pytest tests/test_rebuild_silver.py -v
uv run ruff check . && uv run ruff format --check .
git add livewire_scripts/rebuild_silver.py tests/test_rebuild_silver.py
git commit -m "fix(silver): evict a quarantined symbol by moving its artifact, not by un-manifesting it

Apex resolves a symbol by constructing <root>/asset_class=equity/symbol=<S>/1d.parquet
and reading whatever is there (apex ohlc_provider.py:141-145); the manifest is a
reseed signal, not a view definition, and its per-symbol revision maps are write-only
with no eviction path. Dropping a symbol from the manifest therefore leaves it serving
its stale corrupt artifact forever. Moving the file aside is the only removal signal
apex can perceive: it then fails closed with AdjustedDataUnavailable -> HTTP 500."
```

---

### Task 12: Documentation, full gate, PR

**Files:** Modify `CLAUDE.md`

- [ ] **Step 1: Update CLAUDE.md**

Replace the "Silver publish continuity gate" section with a "Silver-grade window" section stating the contract: **every symbol publishes the longest suffix of its history that is silver grade; deep history is not a goal; data added at either end must be silver grade or it does not publish.** Document:
- the two trims and why neither subsumes the other — the deterministic seed-boundary floor (`clients/seed_boundary.py`, known 2021-06-11→21 window, fold predicted from the CA store, no threshold to tune, applied to raw bronze before adjustment) and the blind window scan over the adjusted series (`--continuity-threshold`, default 6.0, exemptions via `--continuity-allowlist`). State plainly that the heuristic alone missed 63 symbols with a 2×–5× fold, and that a seed-corrupt symbol is **trimmed to its post-seed window, not quarantined**;
- `triage-breaks` and how `--triage-manifest` keeps real market moves;
- `window_regressions` in the nightly digest;
- the repair runbook with `--dry-run`, the built-in backup, and `rollback-legacy-basis`; note `--presets-dir` is cwd-relative so `--priority-only` must run from the repo root (it now errors instead of repairing nothing);
- that IB on this host is `127.0.0.1:4001` (sessions run ON the mini; the LAN IP silently times out — `TrustedTwsApiClientIPs` is empty), correcting the "not on this MacBook" framing.

- [ ] **Step 2: Run the full CI-equivalent gate**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest tests/ -m "not integration and not postgres_live" --cov=clients --cov=scripts --cov=livewire_scripts --cov-fail-under=95 -W error::RuntimeWarning
```

- [ ] **Step 3: Commit and push to PR #57**

```bash
git add CLAUDE.md
git commit -m "docs(silver): document the silver-grade window contract and both detectors"
git push origin feat/silver-legacy-basis-repair
gh pr checks 57 --watch
```

- [ ] **Step 4: Merge only when CI is green**

---

### Task 13 (operations, after merge): reach full-universe silver grade

Not code. Each step's output gates the next.

- [ ] **Step 1: Re-audit the full universe with both detectors**

```bash
uv run python scripts/livewire_quality.py audit-legacy-basis --full \
    --output <lake>/repairs/silver-full-grade/<stamp>/audit.json
```
Write the manifest **into the lake**, never to `/tmp`. The previous batch's manifest sat in a session scratchpad, and the repair cursor binds to its exact sha256 — one eviction would have voided 116 symbols of 2FA-gated IB work. (That manifest has since been copied to `<lake>/repairs/silver-legacy-basis/2026-07-17T04-01-53Z/audit.json`, sha `bd37961d…`.)

Live baseline to compare against: **clean 12373 / mixed 174 / error 594**. Expect `mixed` ≈ 174 + up to 63 newly visible sub-threshold seed symbols, less overlap. Review `counts` and the `detector` split before touching IB.

**Read the split before planning the IB run.** The seed class is 39 of the 238 original `mixed` and is already 95% repaired (37/39). The remaining 199 are `other_break` with break dates in **2023–2026** and a ~65% ambiguous rate — a different, harder class. Do not budget for it as "more of the same". The good news: those dates are **inside** Massive's ~5-year entitlement window, so Step 3's triage can actually adjudicate them, which is where the leverage now is.

- [ ] **Step 2: Repair the seed-boundary class (recovers full history where possible)**

Scope note: this is now the *small* lane — 39 mixed symbols, 37 already done. Its real value is the **63 sub-threshold symbols Task 1 makes visible for the first time**, which are today shipping double-adjusted data to Apex classified as `clean`.

```bash
uv run python scripts/livewire_store.py repair-legacy-basis \
    --audit-manifest <.../audit.json> --output-dir <.../repair-v2> --priority-only --dry-run
uv run python scripts/livewire_store.py repair-legacy-basis \
    --audit-manifest <.../audit.json> --output-dir <.../repair-v2> --priority-only --resume
```
IB is `127.0.0.1:4001`. Expect ~45% `ambiguous` (fail-closed, no write) — those are the missing-CA-record class; they are **not** retryable and are handled by the window instead.

**Freeze the writers for this step too, not just for Step 4.** Repair keys each symbol on the `source_sha256` the audit recorded and skips anything that changed since (Task 4). A batch takes ~40 min; if `com.livewire.daily-update` lands a new bar on a queued symbol mid-batch, that symbol's hash no longer matches and repair silently skips it — a partial repair that looks like a clean run. Freeze before, restore after, exactly as Step 4 does:

```bash
WRITERS="com.livewire.daily-update com.livewire.intraday-catchup com.livewire.daily-update-watchdog"
for L in $WRITERS; do launchctl unload ~/Library/LaunchAgents/$L.plist; done
# ... run the dry-run and the repair ...
for L in $WRITERS; do launchctl load ~/Library/LaunchAgents/$L.plist; done   # restore regardless of outcome
```

- [ ] **Step 3: Triage every remaining break**

```bash
uv run python scripts/livewire_quality.py triage-breaks \
    --audit-manifest <.../audit.json> --output <.../triage.json> --resume

# Review the verdicts, THEN install them at the path every run reads:
mkdir -p <lake>/repairs/triage && cp <.../triage.json> <lake>/repairs/triage/current.json
```
The install step is not optional. `rebuild-silver` reads `<lake>/repairs/triage/current.json` by default because the nightly job passes no flags; if the verdicts only ever live in the dated run directory, the first nightly rebuild after rev-3 re-trims every confirmed real move. It is also the only durable record of a verdict the provider will refuse to re-issue once the ~5-year entitlement floor rolls past that date — **never delete this file to "force a re-triage"; the answer is not re-derivable.**

**Expect `inconclusive` to dominate, and do not treat that as failure.** The entitlement floor was measured at 2021-07-12 (rolling), so every break older than roughly five years — which is most of the Type-B pool, including the EQIX/MTB missing-CA class — returns `MassiveAuthError` and lands `inconclusive`. Those trim, which is the intended outcome: the trim costs only pre-break history, and the goal does not want deep history. The verdicts that *matter* are the recent ones, where a wrong trim would cost years of history a consumer is actually using.

Scope comes from the audit manifest (every symbol it flagged with a `break_date`) — there is no `--full`, because a symbol with no break has nothing to triage and must not cost a provider call. Review the verdict split. `real_move` verdicts protect real history from the trim; `bad_data` / `missing_action` / `inconclusive` all trim.

**Ordering is mandatory, not advisory: re-run Step 1's audit after Step 2's repair, before triaging.** Every successful repair rewrites bronze and changes that symbol's `source_sha256`, so the pre-repair manifest lists breaks that no longer exist. Triaging it spends provider calls on repaired symbols and can attach a verdict to a break that is gone. Re-audit, then triage against the fresh manifest.

- [ ] **Step 4: Publish rev-3 with the writers frozen**

`--allow-window-regression` is **required for this run and this run only**: rev-2 published untrimmed history, so every intentionally trimmed symbol registers as a regression and, under Task 10's fail-closed default, nothing would publish. Every run after this one must NOT pass it.

Use a `trap` so an interrupt, a dropped SSH session or a non-zero exit cannot leave the writers unloaded, and preserve the publish's exit code — otherwise the final `launchctl load` becomes the shell's status and a failed publish reads as success:

```bash
WRITERS="com.livewire.daily-update com.livewire.intraday-catchup com.livewire.daily-update-watchdog"
restore() { for L in $WRITERS; do launchctl load ~/Library/LaunchAgents/$L.plist 2>/dev/null; done; }
trap restore EXIT INT TERM
for L in $WRITERS; do launchctl unload ~/Library/LaunchAgents/$L.plist; done

uv run python scripts/livewire_store.py rebuild-silver --full --dry-run \
    --triage-manifest <lake>/repairs/triage/current.json --allow-window-regression \
    --failure-output <.../rev3-dryrun.json>
# REVIEW rev3-dryrun.json: failed / trimmed / window_regressions / evicted. Only then:
uv run python scripts/livewire_store.py rebuild-silver --full \
    --triage-manifest <lake>/repairs/triage/current.json --allow-window-regression \
    --failure-output <.../rev3.json>
RC=$?
exit $RC   # trap restores the writers; the publish's status survives
```
**Success criterion: `failed` ≈ 0** — every symbol publishes a window. Anything still failing is a genuinely empty series, or one of the 594 `error` symbols, and must be listed explicitly.

- [ ] **Step 5: Smoke-test after Apex adopts rev-3 — against an ADJUSTED-mode canary**

Per `SILVER_CORRECTNESS_GAP_FROM_APEX.md:143`: NVDA, AMZN, GOOGL, AGL, AVGO (formerly corrupt) plus INTC (fail-closed control). Add **TSLA and APH** (the sub-6× class made visible for the first time) and **EQIX** (a trimmed window: expect data from 2003-01-02, not an error).

**A smoke test against production proves nothing.** Production runs `APEX_LIVEWIRE_PRICE_MODE=raw` (apex `docker-compose.yml:31,40`), so every bar it serves comes from bronze and rev-3 changes nothing it returns. Run the smoke test against an adjusted-mode canary, or the test is theatre. The corollary is reassuring: **the adjusted cutover, not this publish, is the risk event — so there is time to get rev-3 right.**

---

## Explicitly out of scope

| Item | Why | Effect on the goal |
|---|---|---|
| Backfilling pre-2003 history behind a missing split | The goal excludes deep history; inferring and reconciling the split is strictly more machinery for history that is not wanted. If Massive ever ships the record, the derived window extends by itself. | None — those symbols reach silver grade via their window |
| ~~The other ~9,500 bronze equity symbols (22,673 dirs vs 13,141 audited)~~ | **RESOLVED — the gap does not exist.** See the facts table: it was an AppleDouble miscount. The audit already covers 100% of bronze equity. | Nothing to decide; "full universe" = every symbol currently in bronze equity, today 13,141 — a denominator, not a completeness claim |
| Silver orphan directories (~25,096 dirs vs 12,548 manifested) | Harmless to serving (Apex reads by manifest); makes `ls | wc -l` lie by ~2× | None; a GC pass is hygiene |
| Dividend error lanes (61 currency-mismatch, 14 magnitude) | No tool covers either; reference-data fix | These symbols reach silver grade only if the dividend data is fixed — they currently land in `error` and would publish no window. **Quantify in Task 13 Step 1 and decide** |
| `source='legacy'` on 100% of 19.5M pre-repair rows | The documented provider-source contract was never realised; `price_basis` is what Silver reads, and that is what this plan fixes | Cosmetic |
