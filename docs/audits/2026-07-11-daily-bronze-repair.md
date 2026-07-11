# Daily bronze staged-authority repair (2026-07-11)

## Scope

Audited every existing equity bronze `1d.parquet` row whose date overlaps a
complete staged Massive SIP `day_aggs_v1` partition. The staged range was
2021-06-11 through 2026-07-09 (1,273 dates).

## Finding

The QQQ files were structurally valid Parquet, but the 2026-06-05 through
2026-06-11 daily rows contained partial-session IB values. The issue was wider
than QQQ: the staged overlap audit compared 10,933,981 rows across 13,080
existing bronze symbols and found 3,416,856 mismatched or missing rows affecting
12,410 symbols.

| Classification | Rows |
| --- | ---: |
| Volume only | 1,813,044 |
| Price and volume | 1,052,068 |
| Missing from bronze | 551,735 |
| Price only | 9 |

The root cause of the partial-session rows was the old scheduled daily job
targeting the current date before the U.S. close. The current implementation
already prevents recurrence by selecting the previous trading day before
16:00 America/New_York.

## Repair

Replaced or inserted only keys present in the pre-audit manifests. Every update
used the existing locked atomic bronze merge. Non-overlap history was preserved.
The authoritative staged rows passed finite-value, OHLC relationship, and
non-negative-volume checks before repair.

The complete independent post-audit re-compared all 10,933,981 overlap rows and
found zero mismatches.

QQQ after repair:

| Date | Open | High | Low | Close | Volume |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2026-06-05 | 730.06 | 731.69 | 704.32 | 705.06 | 99,606,571 |
| 2026-06-08 | 717.81 | 723.03 | 713.07 | 716.07 | 47,401,451 |
| 2026-06-09 | 722.98 | 725.66 | 686.37 | 707.83 | 91,932,171 |
| 2026-06-10 | 701.66 | 711.28 | 692.93 | 693.69 | 65,334,283 |
| 2026-06-11 | 699.29 | 718.3721 | 695.00 | 717.12 | 71,798,857 |

## Wider-history sample

Ten common tickers (`QQQ`, `SPY`, `IWM`, `DIA`, `AAPL`, `MSFT`, `NVDA`,
`TSLA`, `AMZN`, `META`) and ten deterministic uncommon tickers (`BWMX`, `DIVO`,
`ISNRU`, `KRMD`, `M`, `MWA`, `OKE`, `RFAIR`, `SZZL`, `VFL`) were validated over
their entire stored histories. All 20 files had sorted unique dates, the expected
schema, and zero invalid OHLCV rows. Sample histories ranged from 17 to 11,483
rows and from 1980-12-12 through 2026-07-09.

Massive REST returned only its current five-year window, starting 2021-07-12,
so it cannot validate pre-staged history. Within returned sample overlap, common
ticker OHLC matched 12,523 of 12,540 rows. Historical REST volume often differs
from immutable staged flat files because the REST series is revised; staged
`day_aggs_v1` remained the declared repair authority.

## Artifacts

The rollback-capable pre-audit manifests and validation output are stored under:

`~/market-warehouse/data-lake/repairs/daily-bronze-20260711/`

- `pre-audit/`: original and authoritative values for every repaired key
- `post-audit/`: zero-mismatch manifests and aggregate summary
- `full-history-structural-sample.json`: common/uncommon structural results
- `full-history-rest-sample.json`: REST overlap comparison and limitations
