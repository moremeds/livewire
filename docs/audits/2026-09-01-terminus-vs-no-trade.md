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
