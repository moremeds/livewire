# Silver legacy-basis full repair → rev-3 — design

**Date:** 2026-07-16
**Status:** Draft (Spec 1 of a 3-spec program)
**Author:** Livewire silver session
**Supersedes framing in:** `SILVER_CORRECTNESS_GAP_FROM_APEX.md`, `docs/superpowers/plans/2026-07-16-silver-full-coverage-apex-adjusted-promotion.md`

## 1. Context and problem

Silver revision 2 (published 2026-07-16T12:48Z) reached 12,548 / 13,141 symbols
"covered". But **coverage is not correctness.** At least 165 published symbols —
including NVDA, AMZN, GOOGL, AVGO, NFLX, BKNG — serve **double-adjusted garbage**
and Apex is serving them live in `effective_price_mode=adjusted`. A missing
symbol fails visibly (HTTP 500, fail-closed); a corrupt symbol renders a
plausible, wrong chart. This is the dominant gap from the consumer side, and it
has no phase in the prior coverage-only plan.

### 1.1 Verified evidence (this session, against the live lake)

Silver `1d` for NVDA, June 2021 (correct adjusted ≈ 17–18):

```
2021-06-10  0.4342   << double-adjusted garbage
2021-06-11  17.7638  correct
2021-06-17  18.5929  correct
2021-06-18  0.4644   << garbage — lone bad bar inside a good run
2021-06-21  18.3637  correct
```

AMZN, GOOGL, AGL reproduce the same interleaved pattern. The corruption is
**per-row, not a clean before/after boundary.**

### 1.2 Root cause (verified in bronze)

NVDA bronze, every row `source='legacy'`, `price_basis='raw'`:

```
2021-06-10  close=17.43   ← already-adjusted value, mislabeled 'raw'
2021-06-11  close=713.01  ← true-raw value, correctly 'raw'
2021-06-18  close=18.64   ← already-adjusted, mislabeled 'raw' (lone bar)
2021-06-21  close=737.09  ← true-raw
```

The corruption is **in bronze, not in the silver logic.** Silver correctly
divides every `raw` row by NVDA's cumulative split factor (~40): `713 ÷ 40 ≈ 17.8`
✓, but `17.43 ÷ 40 ≈ 0.44` ✗. **The legacy `raw` label is untrustworthy — a
silent per-row mix of already-adjusted and true-raw closes**, stitched by an old
ingestion/merge around mid-June 2021. The engine's `unknown`-basis quarantine
never fired because these rows *claim* to be `raw`.

`clients/price_basis.py` only normalizes rows where `source=='ib'` and
`price_basis in {'unknown','split_adjusted'}` — the entire `legacy`/`raw`
population is explicitly skipped and assumed canonical. That assumption is the
bug.

### 1.3 Denominator correction

The Apex audit's "22,673 bronze / ~9,500 unexamined" and "~25,096 silver dirs"
claims are a **measurement artifact**: the external volume is non-APFS, so every
real entry has a macOS AppleDouble `._` sidecar. Direct counts:

- bronze equity (canonical, real dirs) = **13,141** (= codex's full universe)
- bronze-delisted equity = 8,620 (already out of the sync path)
- `._symbol=*` AppleDouble files = 9,532 → `13,141 + 9,532 = 22,673`
- silver equity real dirs = 12,548 (`12,548 + 12,548 = 25,096`)

**There is no hidden 9,500-symbol gap.** The target universe is the 13,141
canonical bronze equity tree; delisted are already separated. (A `dot_clean` / GC
pass on the volume is recommended so future `ls | wc -l` audits stop doubling,
but that is out of scope here.)

## 2. Goals / non-goals

**Goals**
- Every symbol whose legacy basis is mixed/ambiguous is either repaired to a
  continuity-verified correct adjusted series, or explicitly quarantined
  (fail-closed) — never silently wrong.
- Corrupt-but-charted symbols (NVDA/AMZN/…) become **correct and available**.
- Publish **rev-3** over the whole population (not just rev-2's 3,350 affected).
- Deliver a **resumable, preset-prioritized** repair script.

**Non-goals (separate specs)**
- WS3: the existing 593 coverage plan (447 split repairs → 71 ambiguous → 61
  dividend currency → 14 magnitude). Unchanged; runs after Spec 1.
- WS4: operationalizing so scheduled writers auto-produce correct silver (goal
  G4).
- AppleDouble GC on the data volume.

## 3. Universe and scope

- Target: **13,141** canonical bronze equity symbols.
- Excluded by design: delisted (`bronze-delisted/`), warrants, pink sheets, OTC.
- Full-repair means: no legacy `raw` symbol is trusted blindly — every symbol
  passes a basis-consistency audit; those that fail are re-derived from IB. This
  achieves full correctness while bounding expensive IB work to the broken set.

## 4. Architecture

Three modules plus the rev-3 rebuild.

### 4.1 Module 1 — WS0 continuity gate (correctness invariant)

- **Where:** the `rebuild_silver.py` **staging loop**, on the in-memory adjusted
  series (`adjust_daily_rows` output) before publish. A violation raises
  `ValueError`, which the existing staging `try/except` routes into `failures` —
  the symbol is quarantined and the rest of the universe still publishes.
  Implemented as a pure function
  `clients/silver_continuity.py::check_adjusted_continuity`.
- **Rule:** on the adjusted daily series, if
  `max over t of max(c[t]/c[t-1], c[t-1]/c[t])` exceeds the threshold
  (**default 6×**, configurable) and the offending date is not on a halt/relist
  allowlist, the symbol is **not published** — it is quarantined.
- **Rationale:** a correct adjusted series has no large adjacent-day
  discontinuity — adjustment is what removes action-date jumps. Coverage gate +
  correctness gate, both required before publish. This catches the 15 rev-2
  escapes.
- **Allowlist:** empty initially (no exemptions). Populated later only with
  evidence-backed genuine halts/relistings.

### 4.2 Module 2 — WS1 basis-consistency audit (offline, cheap)

- **Command:** `scripts/livewire_quality.py audit-legacy-basis`.
- **Per legacy symbol:** build its adjusted daily series (`build_factor_intervals`
  + `adjust_daily_rows`) and run the **same continuity invariant used at publish
  time** (`check_adjusted_continuity`). A symbol whose adjusted series has a
  >threshold adjacent-day jump is `mixed` (the signature of already-adjusted rows
  mislabeled `raw`, e.g. NVDA 2021-06-18); otherwise `clean`. This reuses Module 1
  rather than a separate per-row classifier.
- **`ambiguous` is not a static audit class** — it is a repair-phase outcome
  (Module 3), because distinguishing "unrepairable" from "mixed" needs the IB
  evidence the offline audit deliberately doesn't fetch.
- **Output:** an audit manifest (symbol → `{klass, break_date, source_sha256}`,
  plus counts).
- **Key property:** detection uses only the known split schedule + the series'
  own discontinuities — **no external fetch**. So the audit runs full-universe
  cheaply; the expensive IB fetch (Module 3) hits only `mixed`/`ambiguous`
  symbols.

### 4.3 Module 3 — WS2 audit-driven IB re-derivation (the repair script)

- **Script:** `livewire_scripts/repair_legacy_basis.py`, entry point
  `scripts/livewire_store.py repair-legacy-basis`.
- **Flow:**
  0. **Operator review gate:** Module 2's audit is run first and on its own. Its
     `mixed`/`ambiguous` symbol list and counts are surfaced to the operator for
     confirmation **before any IB re-derivation begins** — the expensive,
     rate-limited IB phase is not entered until the blast radius is reviewed.
  1. **Priority queue:** skip `clean`; enqueue `mixed`/`ambiguous`, ordered
     **sp500 → ndx100 → r2k → remainder** (read the corresponding presets).
  2. **Per symbol re-derivation** (respecting IB constraints — Gateway on the
     Mac mini, 2FA, rate limits, **no auto-retry on connection failure**):
     - `adjusted_history_sources.fetch_ib_evidence` fetches deep IB history;
     - `price_basis.prepare_ib_rows_for_publish` classifies IB basis per boundary
       (via `classify_split_events`; IB basis is not fixed) → normalizes to
       canonical true-raw, raising on ambiguous classification → `ambiguous`;
     - **correctness self-check:** the re-derived series must itself adjust to a
       continuous curve (`check_adjusted_continuity`); if not → `ambiguous`, do
       not write. This is a stronger, window-independent gate than the Massive
       cross-check and **replaces it in Spec 1**;
     - Massive ≤5-year cross-check is a **deferred optional enhancement** — add a
       `massive_factory` param and the cross-check when needed (YAGNI: not wired in
       Spec 1, since the continuity self-check already gates correctness);
     - write back via `BronzeClient.merge_ticker_rows` (canonical
       temp → validate → `os.replace`, per-symbol lock) + an audit sidecar.
  3. **Resumable cursor:**
     `~/market-warehouse/cursors/cursor_legacy_basis_repair.json` records each
     symbol's `done`/`failed`/`ambiguous`; interrupts resume from the cursor
     (same pattern as the resolver / backfill runners).
  4. **Rebuild (single trigger):** after the whole priority queue is drained,
     run **one** `rebuild-silver` (with Module 1 gate) over the repaired symbols
     → advance to **rev-3** via a single atomic revision publish. The cursor
     makes the repair phase resumable across interrupts; rev-3 is published
     **once at the end, not per batch.**
- **Failure / ambiguity handling:** IB no-data / timeout / cross-check
  disagreement → mark and skip; do **not** block subsequent symbols. These join
  the "needs evidence/manual" residual set alongside WS3's 71 ambiguous. **Never
  write an unconfirmable row** — fail-closed beats a wrong value.

### 4.4 Massive entitlement constraint

Massive is a **~5-year rolling window** (reaches back to ~mid-2021, i.e. right at
the corruption edge — not safely inside it). Therefore **IB is the authoritative
deep-history source** for re-derivation; Massive is only a recent-overlap
cross-check, never the sole basis for pre-window rows.

## 5. Data flow

```
corporate_action_store ─┐
bronze 1d (legacy/raw) ──┼─▶ [M2 audit] ─▶ mixed list ─▶ (operator review)
                         │                                            │
IB deep history ─────────┼─▶ [M3 re-derive + normalize] ◀────────────┘
Massive (≤5y, deferred)──┘        │  canonical raw → bronze  (resumable via cursor)
                                  ▼
              queue drained ─▶ [ONE rebuild-silver + M1 gate] ─▶ rev-3 (single atomic publish)
```

## 6. Interfaces / artifacts

- `audit-legacy-basis` → audit manifest JSON (symbol classes + per-bar
  evidence), written under `data-lake/repairs/silver-legacy-basis/<stamp>/`.
- `repair-legacy-basis` → cursor JSON (resume state) + per-symbol repair sidecar
  (symbol, status, reason, timestamp, data-lake root), following the resolver
  evidence-sidecar pattern so repairs stay auditable.
- rev-3 manifest via the existing `silver_revision` atomic publish
  (`revisions/current.json`).

## 7. Error handling & operational constraints

- IB: lazy-connect once; a connection failure **aborts the run** and is recorded
  (no auto-retry — connection failures mean 2FA/maintenance/session conflict, not
  something to retry; re-entering the loop must not reconnect per symbol). The run
  is resumable via its cursor, so an aborted run continues later. Follows the
  `resolve_split_basis` pattern; no separate preflight.
- Writer coordination: as in the rev-2 rebuild, freeze the three Livewire
  writers during the final publish and restore them even if publish fails.
- Every mutation goes through the canonical repair path with a resolved
  data-lake-root guard (reject a different active root before mutating).
- **Revision removal (open item):** a symbol published in a prior revision that
  now quarantines is *not* automatically made unavailable — its prior artifact
  lingers on disk and in the manifest, so Apex would keep serving stale data
  instead of failing closed. rev-3 must make quarantined-but-previously-published
  symbols fail-closed (manifest omission or artifact tombstone, depending on
  Apex's consumption model). See plan Task 5; may need a first-class
  revision-removal contract in `silver_revision`.

## 8. Testing (repo rules; 95% coverage gate)

- **Module 1:** an "NVDA-style" mixed-basis fixture (real frozen prices) →
  assert the gate quarantines rather than publishes; a normal series is not
  falsely quarantined.
- **Module 2:** per-row detection unit tests — isolated bad bar; distinguishing a
  real ex-date jump from a non-ex-date artifact jump; `clean`/`mixed`/`ambiguous`
  classification.
- **Module 3:** stub IB fetcher (injected factory) → classify → normalize → write-back;
  cursor interrupt/resume; priority ordering (sp500→ndx100→r2k→rest); ambiguous
  is never written.
- All external I/O mocked; temp parquet roots; `-W error::RuntimeWarning` on
  async-mocking tests.

## 9. Configuration

| Knob | Default | Notes |
|---|---|---|
| continuity threshold | `6×` | adjacent-day close ratio ceiling before quarantine |
| halt/relist allowlist | empty | evidence-backed exemptions only, added later |
| priority presets | sp500, ndx100, r2k, then remainder | read from `presets/` |
| repair cursor | `~/market-warehouse/cursors/cursor_legacy_basis_repair.json` | resume state |

## 10. Success criteria

- Full basis-consistency audit covers all 13,141; every `mixed`/`ambiguous`
  symbol is either re-derived to a continuity-passing series or explicitly
  quarantined.
- rev-3 published; formerly corrupt symbols (NVDA, AMZN, GOOGL, AVGO, AGL) serve
  correct adjusted data; a control quarantined symbol (e.g. INTC) still
  fail-closes.
- Continuity gate is part of the publish path — no symbol with a >threshold
  adjacent-day jump can be published again.
- The repair is a resumable script that picks up from its cursor and honors the
  preset priority order.

## 11. Sequencing after Spec 1

- **Spec 2 (WS3):** the existing 593 coverage plan (447 → 71 → 61 → 14).
- **Spec 3 (WS4):** operationalize — scheduled writers → corporate-actions →
  rebuild-silver with the continuity gate always on, so new bronze auto-yields
  correct silver and republishes revisions (goal G4).
