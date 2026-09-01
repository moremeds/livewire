# Terminus vs. no-trade — why coverage cannot see a delisted S&P 500 member

Measured 2026-09-01 on the production host (`moremeds-Mini`), against the live
lake at `~/market-warehouse/data-lake`. Input: the 501-finding production spike
of the §4 registry denominator over `sp500` equity `1d`.

## Question

`livewire_scripts/coverage_report.py:322` exempts a symbol that is absent from
the day's raw `_symbols.parquet` traded set — *no-trade is not missing*. The
exemption is load-bearing: without it the interior scan flags 96.6% of the
universe (`CLAUDE.md`). The gap engine has no equivalent rule.

Does the exemption also swallow the true findings the new denominator produces?

## Method

Three read-only passes over data already on disk. No writes, no provider calls.

1. Split the 501 spike findings by session count.
2. For each multi-session finding, test every missing session against that day's
   `_symbols.parquet` traded set (`minute_aggs_v1`), and independently against
   `day_aggs_v1`.
3. Run the same suffix test across the whole `sp500 + ndx100` universe to measure
   the false-positive rate.

## Result 1 — the 501 split

```
total findings: 501
gap types: {'G1': 500, 'G3': 1}
sessions-per-finding: {1: 497, 10: 1, 11: 1, 19: 1, 21: 1}
single-session findings, all on 2026-08-31: 497
```

The 497 are the ingestion lag: the spike ran at 04:21 UTC, and the job filling
session 2026-08-31 starts 06:00 UTC on 2026-09-01. Four findings remain.

## Result 2 — all four are absent from the raw tape on every missing session

```
BK    1d.parquet=MISSING      sessions= 21  traded=  0  NOT-traded= 21
EA    1d.parquet=9285 rows    sessions= 19  traded=  0  NOT-traded= 19
AVB   1d.parquet=8158 rows    sessions= 11  traded=  0  NOT-traded= 11
EQR   1d.parquet=8056 rows    sessions= 10  traded=  0  NOT-traded= 10
```

The tape is healthy on the last of those sessions — 11,913 tickers on
2026-08-31, with `AAPL`, `SPY`, `MSFT`, `NVDA` all present. So the absence is
per-symbol, not a broken file.

Each symbol has a terminus, and both independently published tapes agree on it:

| Symbol | last on `minute_aggs_v1` | last on `day_aggs_v1` | first missing session | in `bronze-delisted/` | latest corporate action |
| --- | --- | --- | --- | --- | --- |
| BK  | never (0 of 43 days from 2026-07-01) | never (0 of 21) | 2026-08-03 | **yes** | 2026-04-27 cash dividend |
| EA  | 2026-08-04 | 2026-08-04 | 2026-08-05 | no | 2026-05-27 cash dividend |
| AVB | 2026-08-14 | 2026-08-14 | 2026-08-17 | no | 2026-06-30 cash dividend |
| EQR | 2026-08-17 | 2026-08-17 | 2026-08-18 | no | 2026-06-29 cash dividend |

Every terminus is exactly one session before the gap starts. Nothing in the
warehouse explains why EA, AVB or EQR stopped: they are not in the delisted
archive, and their newest stored corporate action is an ordinary quarterly
dividend two to three months earlier.

## Result 3 — signal-to-noise over the full equity universe

`sp500 + ndx100` = 515 members, trailing 20 sessions (2026-08-04 → 2026-08-31):

```
TERMINUS (absent all 20 sessions): 1
   BK     in-delisted-archive=True
PARTIAL absence (present on some, stopped later): 3
   AVB    present 9/20,  last=2026-08-14, delisted-archive=False
   EA     present 1/20,  last=2026-08-04, delisted-archive=False
   EQR    present 10/20, last=2026-08-17, delisted-archive=False
```

**511 of 515 members produce nothing.** The no-trade population that the 96.6%
interior-scan disease is made of does not appear here, because those symbols
return to the tape within the window.

## Conclusions

1. **Coverage is blind to this population through two independent mechanisms.**
   The disk-glob denominator never sees BK (no `1d.parquet` exists). The
   no-trade exemption grades EA/AVB/EQR present on every missing session.
   Fixing either one alone still reports green.
2. **The distinction is a suffix test, not a threshold.** Absent for one session
   with presence on both sides is a no-trade day; absent from date X through the
   as-of date is a terminus. Both readings use files coverage already opens.
3. **Tier A was wrong for all four.** The engine emitted `source: massive`,
   `heal_by_days: 1798`. Massive's own tape is exactly what lacks them, so the
   repair would fetch nothing, forever. Tier was assigned from the asset class
   without asking whether the named store holds the session.
4. **BK proves the two delisting producers are not one chain.** It is in
   `bronze-delisted/` *and* in `presets/sp500.json`. The archive move ran;
   nothing removed it from the universe the denominator is built from.
5. **G2 and G13 produced zero true findings** out of 501. The taxonomy branches
   the data supports here are G1, G3, and a terminus class.

## Reproduction

Scripts are reproduced below verbatim. They read only `_symbols.parquet`,
parquet footers and the corporate-action store; none of them writes.

### `notrade_check.py` — Result 2, per-finding
```python
import json, os
from pathlib import Path
import pyarrow.parquet as pq

LAKE = Path(os.path.expanduser("~/market-warehouse/data-lake"))
RAW = LAKE / "raw" / "massive" / "us_stocks_sip" / "minute_aggs_v1"
BRONZE = LAKE / "bronze" / "asset_class=equity"

real4 = json.load(open("/tmp/real4.json"))   # {symbol: [missing sessions]}
cache = {}
def traded(d):
    if d not in cache:
        p = RAW / f"date={d}" / "_symbols.parquet"
        cache[d] = (set(pq.read_table(p, columns=["ticker"]).column("ticker").to_pylist())
                    if p.exists() else None)
    return cache[d]

for sym, sessions in real4.items():
    f = BRONZE / f"symbol={sym}" / "1d.parquet"
    disk = "MISSING" if not f.exists() else str(pq.ParquetFile(f).metadata.num_rows) + " rows"
    tr = ab = nofile = 0
    for d in sessions:
        t = traded(d)
        if t is None: nofile += 1
        elif sym in t: tr += 1
        else: ab += 1
    print(f"{sym:5s} 1d.parquet={disk:12s} sessions={len(sessions):3d}  "
          f"traded={tr:3d}  NOT-traded={ab:3d}  no-rawfile={nofile:3d}")
```

### `terminus.py` — Result 3, universe-wide
```python
import json, os, sys
from pathlib import Path
import pyarrow.parquet as pq

REPO = Path(sys.argv[1])
LAKE = Path(os.path.expanduser("~/market-warehouse/data-lake"))
RAW = LAKE / "raw" / "massive" / "us_stocks_sip" / "minute_aggs_v1"
DEL = LAKE / "bronze-delisted" / "asset_class=equity"

members = set()
for name in ("sp500", "ndx100"):
    members |= set(json.loads((REPO / "presets" / f"{name}.json").read_text())["tickers"])

dates = sorted(p.name.split("=")[1] for p in RAW.glob("date=2026-0[78]-*")
               if (p / "_symbols.parquet").exists())
window = dates[-20:]
sets = {d: set(pq.read_table(RAW / f"date={d}" / "_symbols.parquet",
                             columns=["ticker"]).column("ticker").to_pylist()) for d in window}

never, partial = [], []
for s in sorted(members):
    present = [d for d in window if s in sets[d]]
    if not present: never.append(s)
    elif len(present) < len(window): partial.append((s, len(present), present[-1]))

print(f"TERMINUS (absent all {len(window)} sessions): {len(never)}")
for s in never:
    print(f"   {s:6s} in-delisted-archive={(DEL / f'symbol={s}').exists()}")
print(f"PARTIAL absence: {len(partial)}")
for s, n, lastd in partial:
    print(f"   {s:6s} present {n}/{len(window)}, last={lastd}, "
          f"delisted-archive={(DEL / f'symbol={s}').exists()}")
```

Invoked as `./.venv/bin/python terminus.py ~/market-warehouse/current` so the
presets come from the served release, not from a checkout.

## Verification (post-implementation)

Phase 1 landed on `feat/gap-autoheal-phase1` (Tasks 1–8). This section records
what Task 9 could and could not measure, run from a **checkout of that branch**
(`.worktrees/gap-autoheal-phase1`), never from `~/market-warehouse/current`,
which is an immutable export that does not carry this code.

### ⚠️ Blocked: this host is not the production warehouse

Task 9 steps 1, 3, 3b and 4 could not run. Measured 2026-09-01:

| Check | Expected (CLAUDE.md) | Observed |
| --- | --- | --- |
| `/Volumes/DATA_LAKE` mounted | yes — `data-lake` is a symlink to it | **not mounted**; `data-lake` is a real local directory |
| equity bronze symbols | ~13,270 | **653** |
| sp500+ndx100 members on disk | 515 | **288** |
| `raw/massive/us_stocks_sip/minute_aggs_v1/` | the tape the terminus test reads | **does not exist** |
| `bronze/asset_class=corporate_action/` | the store the G14 gate reads | **does not exist** |
| `com.livewire.coverage` loaded | count 1 | **no `com.livewire.*` plist installed at all** |
| newest `coverage_*.log` | under 3 days old | **2026-04-07** |

Task 9 step 4 names this exact case as a stop condition: *"If the coverage job
is not loaded, stop — every conclusion in this plan about 'coverage already runs
this at 11:00 UTC' is false and the plan needs a scheduling task."* That
conclusion is now open. Nothing was relaxed to work around it, and no expected
value was adjusted to match an output.

`MIN_TERMINUS_SESSIONS` therefore stays at **5**, the value the pre-implementation
measurement in this document chose. It has **not** been re-derived against the
production tape by this branch.

### Verified here

**Criterion 11 — no phantom tail gaps before the ingestion deadline.** Run
through `compute_coverage`, not `build_denominator`, because
`as_of=session_due_at(target_date)` makes the due filter tautologically true for
a single-session window and a helper-level test cannot catch a regression:

```
hour total present missing terminus
4    0     0       0       0
11   880   0       880     0
```

Session 2026-08-31 at 04:00 UTC on 2026-09-01 is before its 10:00 UTC deadline,
so the denominator is empty — zero of zero, not one tail gap per symbol. At
11:00 UTC the same session is due and the denominator is populated. The 880 is
`on_disk | registry` on this host; `present=0` because this lake's newest equity
bar predates 2026-06.

**Cost.** `scan_findings` over all six registry rows: **595 findings in 0.9s**,
far under the ~300s stop threshold. This is a lower bound — the production
equity universe is ~46× larger — so it does not retire the measurement, and the
run must be re-timed once the warehouse volume is available.

**Fail-closed behaviour, observed rather than asserted.** With no raw tape and no
corporate-action store, **zero G14 findings were emitted**. Both criterion-8
gates withheld, and every symbol fell through to the ordinary repairable path.
"We could not check" rendered as a repairable gap, not as a delisting.

**Tier honesty.** Every Tier B finding was IB-sourced:

```
equity     G1 A massive  288      volatility G1 A cboe   15
equity     G3 A massive  227      futures    G3 B ib     11
volatility G3 A cboe      28      rates      G1 A fred    4
fx         G3 A yahoo     21      cmdty      G1 B ib      1
```

`futures` and `cmdty` are Tier B by construction — IB is 2FA-gated and never
auto-retries, so its repair is a decision, not an unattended action. The equity
288 + 227 = 515 is the full registry universe, which is the point of the
registry-backed denominator: a member with no file is now countable. `fx` and
`cmdty` appear at all only because Task 5 replaced the hardcoded
`("volatility", "futures", "rates")` tuple with the registry.

### Still owed

1. Steps 1, 3, 3b, 4 against the mounted production warehouse.
2. A ruling on scheduling: nothing is installed on this host, so the claim that
   `com.livewire.coverage` already runs this code path at 11:00 UTC is unverified
   here.
3. Re-measure `scan_findings` wall clock at production scale.
