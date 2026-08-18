# Silver correctness gap — reported from Apex/Argon side

**Prepared:** 2026-07-16 (Asia/Hong_Kong)
**Reporter:** Apex session (read-only audit of the production data lake)
**For:** the next Livewire silver session
**Data lake audited:** `/Volumes/DATA_LAKE/livewire/data-lake/` (silver revision 2, published 2026-07-16T12:48Z)

## 1. Bottom line

The Livewire silver handover frames the remaining work as **coverage**: 12,548 of
13,141 symbols published, 593 to repair. That coverage accounting is correct and
its repair plan (447 split repairs → 71 ambiguous → 61 currency → 14 magnitude)
is sound.

But coverage is not correctness. **At least 165 of the published symbols are
corrupt** — double-adjustment garbage — and Apex is serving them live in
`effective_price_mode=adjusted` right now. This population is **absent from the
593 failure record** and has **no phase in the current plan**. It is the
dominant gap from a consumer's point of view: a missing symbol fails visibly
(HTTP 500, fail-closed); a corrupt symbol renders a plausible, wrong chart.

## 2. Evidence (all verified against the live lake and live Apex)

### 2.1 The defect, live in production

`GET http://127.0.0.1:8322/bars/NVDA?timeframe=1d` (adjusted mode) returns for
June 2021:

```
2021-06-07  0.4350   << double-adjusted garbage (correct adjusted ≈ 17.6)
2021-06-10  0.4342   << garbage
2021-06-11  17.7638  correct
2021-06-17  18.5929  correct
2021-06-18  0.4644   << garbage (lone bad bar inside a good run)
2021-06-21  18.3637  correct
```

AMZN serves **$8.00** for June 2021 (correct adjusted ≈ $167; $8 = 167 ÷ 20, its
2022 20:1 split applied twice). GOOGL serves ~$6. These are among the most-charted
US equities.

### 2.2 Root cause (Livewire's own known defect, not re-audited)

Legacy bronze (`source='legacy'`) mixes true-raw pre-split rows and already-
IB-adjusted rows under a single `price_basis='raw'` label. The silver factor
pipeline trusts the label and applies the cumulative split factor uniformly:

- applied to a true-raw row → correct;
- applied to an already-adjusted row → divided by the split factor a second time
  → ~1/40th garbage.

This is exactly the `unknown price_basis for split-affected row` condition that
the **current** engine detects and quarantines (the 518 split-basis failures).
The corrupt-but-published symbols are the same defect that the **rev-1** engine
did not yet detect — it silently double-adjusted and published instead of
quarantining. rev-2 expanded coverage to previously-failed symbols; it did **not**
re-process the rev-1 population, so their corruption was never measured.

### 2.3 The corruption boundary is an ingestion artifact, not a corporate action

Across nearly every affected symbol the discontinuity falls in
**2021-06-11 → 2021-06-21**, regardless of that symbol's actual split dates.
That points to a specific legacy fetch/merge around mid-June 2021 that stitched
raw-basis and adjusted-basis bars together under one label. It is not aligned to
split ex-dates. A basis repair keyed only on corporate-action dates will miss it.

### 2.4 Full-universe audit

Detector: a correct adjusted series is smooth, so any day-over-day close ratio
≥ 8× (that is not a real market move) is a mixed-basis/double-adjustment artifact.
Run over every on-disk silver daily artifact:

| Metric | Value |
|---|---|
| Silver daily artifacts scanned | 12,506 |
| **Corrupt (≥8× intra-series jump)** | **165** (floor — see 2.5) |
| — from rev-1 (never re-audited) | 150 |
| — from rev-2 (current engine still shipped them) | 15 |
| Suspect (4–8× jump) | 299 more |

rev-2 escapes (current gate did not catch): `AGL, ALPS, AVGO, BARK, BNY, BVC,
CTO, DKI, FOA, NCL, OPI, POM, RVI, VIVO, WW`. Verified real, not false positives:
AGL swings 21,706 → 920 → 27,031; BARK 4,620 → 237 → 4,256 within days.

Notable large-caps corrupt and live: `NVDA, GOOGL, GOOG, AMZN, AVGO, NFLX, CMG,
BKNG, ORLY, ANET`.

Machine-readable output (on the audited host):
`.../scratchpad/silver_corrupt_165.csv` (the 165, ranked) and
`.../scratchpad/silver_audit.csv` (per-symbol: art_rev, n_bars, jumps_8x,
jumps_4to8, max_jump, first/last jump date).

### 2.5 165 is a floor, not a ceiling

The detector undercounts: (a) when garbage dominates a region the local median
follows the garbage; (b) the 8× threshold misses 2:1–4:1-split names whose
double-adjustment jump is only 2–4×. 299 symbols sit in the 4–8× suspect band and
need triage. Expect the true count above 165.

## 3. Denominator discrepancies worth confirming

- **Bronze equity = 22,673 symbol directories, not 13,141.** The "13,141 universe"
  is the examined/eligible subset (~57%). ~9,500 bronze equity tickers were never
  attempted for silver. If that exclusion is deliberate (ETFs, warrants, dead
  tickers), document it as an explicit business exclusion; if not, it is the
  largest coverage gap and it is currently invisible.
- **Silver has ~25,096 `symbol=` directories but only 12,548 are manifested and
  12,506 have a readable `1d.parquet`.** ~12,500 orphan/empty dirs from earlier
  build generations. Harmless to serving (Apex reads by applied revision) but it
  makes any `ls silver/ | wc -l` coverage estimate wrong by ~2×. Consider a GC
  pass so directory counts stop lying.

## 4. What we ask Livewire to do

1. **Treat the published set as suspect, not done.** Re-run the entire rev-1
   population (all 9,207) through the current (rev-2) engine and rebuild →
   publish rev-3. Symbols the engine cannot safely adjust will quarantine and
   then fail closed in Apex (like INTC today) instead of serving garbage.

2. **Add a post-adjustment continuity invariant to the builder's success gate.**
   A correct adjusted series should have no large day-over-day close discontinuity
   — adjustment is precisely what removes action-date jumps. Quarantine any symbol
   whose max adjacent-day close ratio exceeds a threshold (~6×, tunable), unless
   it coincides with a recorded halt/relisting on the allowlist. This is exactly
   what would have caught the 15 rev-2 escapes. Coverage gate + correctness gate,
   both required before publish.

3. **Extend basis detection beyond corporate-action dates.** The 2021-06-11..21
   boundary is an ingestion artifact. Detect already-adjusted legacy rows by the
   intra-series ~split-factor discontinuity itself, not only at split ex-dates.

4. **Then proceed with the existing 593 coverage plan** (447 → 71 → 61 → 14) —
   that part is good and unchanged.

## 5. Coordination note on Apex

Apex is already `effective_price_mode=adjusted` and has fully applied revision 2,
so items 1–2 above are an active production incident, not a pre-launch risk. Apex
side has two interim options, both imperfect (raw NVDA/AMZN are also wrong —
pre-split ~$700/$3300 bars): stay adjusted and accept ≥165 corrupt symbols until
rev-3, or flip to raw. The durable fix is rev-3; the Apex owner will decide the
interim posture. When rev-3 publishes, Apex's watcher will apply it automatically;
smoke-test NVDA, AMZN, GOOGL, AGL, AVGO (formerly corrupt) plus INTC (fail-closed
control) after adoption.

## 6. Verification commands (read-only, reproducible)

```bash
# live corrupt serve
curl -fsS "http://127.0.0.1:8322/bars/NVDA?timeframe=1d&start=2021-06-05T00:00:00Z&end=2021-06-22T00:00:00Z&limit=0"

# per-symbol silver vs the jump detector (swap NVDA for any symbol)
uv run python3 -c "import duckdb;f='/Volumes/DATA_LAKE/livewire/data-lake/silver/asset_class=equity/symbol=AMZN/1d.parquet';[print(r) for r in duckdb.sql(f\"select trade_date,round(close,4) from read_parquet('{f}') where trade_date between '2021-06-07' and '2021-06-21' order by trade_date\").fetchall()]"
```
