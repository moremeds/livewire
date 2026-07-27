# Yahoo/Massive FX + DXY ingest — design

**Date:** 2026-07-27
**Status:** approved, pending implementation

## Problem

1. The warehouse holds no DXY (US Dollar Index) data at all — no daily, no intraday.
2. `asset_class=fx` holds exactly one symbol, `USDEUR`, IB-sourced, daily-only, 2,630 rows
   from 2016-05-24. No FX intraday exists anywhere in the lake.
3. The nightly IB fx lane cannot be widened: `clients/ingestion_common.resolve_fx_pair()`
   raises `ValueError` for any pair outside the hardcoded 36-entry `SUPPORTED_IB_FX_PAIRS`.
   Every common NDF currency (KRW, TWD, INR, BRL, CLP, COP, PHP, IDR, …) is absent from
   that set, and `DXY` is not a six-letter pair so it fails the same length check.

## Measured source matrix

All figures below were measured live on 2026-07-25/27 against the production credentials
in `.env`. Nothing here is from recall — re-measure before trusting it, the entitlement
floors roll.

### DXY

| Source | Symbol | Daily | Intraday |
|---|---|---|---|
| Yahoo | `DX-Y.NYB` | **1971-01-04 → today; 17,219 timestamps, 14,108 real bars** | 1m/7d, 5m/60d, 30m/60d, 1h/730d — rolling |
| Yahoo | `DX=F` | Not Found | Not Found |
| IB | `IND DX @NYBOT` (conId 49315275) | **error 162, no market-data permission** | blocked |
| IB | `CONTFUT DX @NYBOT` | 736 bars, only back to 2023-09-20 | 5m/1m fetchable |
| IB | `FUT DX @NYBOT` (DXU6/DXZ6/…) | permitted, per-contract only | fetchable |
| Massive | `I:DXY`, `C:DXY`, `I:USDX` | **0 rows — not carried** | none |
| FRED | DTWEXBGS / AFEGS / EMEGS | 2006-01-02 → 2026-07-17, trade-weighted (not DXY) | none |

Yahoo `DX-Y.NYB` is the only source with both deep daily history and any intraday.

### FX pairs

| Source | Daily | Intraday | Coverage |
|---|---|---|---|
| Yahoo `<PAIR>=X` | **1996–2006 → today** (JPY 1996-10-30, IDR 2001-06-27, CNY 2001-06-24, PEN 2001-05-30, COP 2003-01-02, EUR/GBP/KRW/INR/BRL/CLP/PHP 2003-12-01, TWD 2004-03-24, AUD 2006-05-15) | 1m/7d, 5m/60d, 30m/60d, 1h/730d | broad |
| Massive REST `C:<PAIR>` | 2 years rolling | **2 years rolling, 1m/5m/30m/1h all supported** | 1,208 fx tickers |
| Massive S3 `global_forex/` | dirs exist 2010→2026 | dirs exist 2010→2026 | **GetObject 403 — not entitled** |
| IB `Forex` MIDPOINT | — | — | 36-pair whitelist, zero NDF |

**Massive REST FX entitlement floor: 2024-07-24** (binary-searched; identical for
1m/5m/30m/1h and for daily). Pre-floor requests return HTTP 403.

Massive covers all 9 G10 pairs and all 16 probed NDF currencies
(KRW TWD INR BRL CLP COP PHP IDR CNY RUB VND PEN ARS KZT MYR THB).

### The S3 listing trap

`list_objects_v2` on `flatfiles/global_forex/minute_aggs_v1/` succeeds and returns year
directories 2010 through 2026. `head_object`/`get_object` on any key under it returns
**403**. The flat-file entitlement covers `us_stocks_sip` only — verified by contrast:

```
OK    us_stocks_sip/minute_aggs_v1/2026/07/2026-07-24.csv.gz   26,437,138 bytes
OK    us_stocks_sip/day_aggs_v1/2026/07/2026-07-24.csv.gz         317,628 bytes
DENY  global_forex/minute_aggs_v1/2026/07/2026-07-24.csv.gz          403
DENY  global_forex/day_aggs_v1/2026/07/2026-07-24.csv.gz             403
DENY  us_indices/day_aggs_v1/2026/07/2026-07-24.csv.gz               403
```

Concluding from the listing alone would promise 16 years of FX minute history that the
credentials cannot fetch. Probe permission boundaries with GET, never with LIST.

## Design

### Source assignment

One source per (symbol, timeframe) file — never mixed, so a file's provenance is never
ambiguous.

| Layer | Symbols | Source | Reason |
|---|---|---|---|
| Daily | all pairs + DXY | Yahoo | the only source with deep history (Massive daily is 2y) |
| Intraday 1m/5m/30m | pairs | Massive REST `C:<PAIR>` | 2 years vs Yahoo's 7d/60d/60d |
| Intraday 1h | pairs | Yahoo `range=730d` | measured **deeper** than Massive — see below |
| Intraday 1m/5m/30m/1h | DXY | Yahoo `range=` | Massive does not carry DXY |

**1h belongs to Yahoo even for pairs.** Yahoo's 1h window measured back to 2023-10-09
(EURUSD) and 2023-10-10 (USDKRW) — past Massive's 2024-07-24 floor — and costs one
unthrottled request instead of several rate-limited ones. Massive is the right source at
every timeframe below 1h and the wrong one at 1h; assuming a single provider wins at all
resolutions would have silently cost ~9 months of history per pair.

Each intraday timeframe is fetched directly at its own maximum range rather than derived
from 1m. Deriving would make a file's depth the *minimum* of its inputs — Yahoo serves
1h back 730 days but 1m back only 7, so deriving DXY 1h from DXY 1m would throw away
723 days of available history.

### Rate limiting

The REST plan allows **5 requests per minute** (measured 2026-07-27: five succeed, the
sixth 429s, and the response carries no `Retry-After` header). `MassiveClient`'s reactive
backoff is 1s/2s/4s over three attempts, which cannot clear a per-minute window — so the
fx lane paces preemptively via a new `min_interval_seconds` constructor argument (12s).
It defaults to 0, leaving every existing caller unchanged.

Request cost follows directly:

| Run | Massive requests | Wall clock |
|---|---|---|
| Nightly `--days 7` | 20 pairs × 3 timeframes = 60 | ~12 min |
| Full seed (760d) | ~31/pair × 20 ≈ 620 | ~2 h |

The 1m seed dominates and is irreducible: 760 days × 1440 bars is ~1.09M rows per pair,
and the page limit is 50,000. Chunk spans are therefore sized per timeframe (1m: 30 days,
5m: 150, 30m: 240) to put as many bars in each response as a page allows.

A chunk starting below the rolling floor returns 403. The walk **skips** those chunks
rather than aborting, so the seed reaches maximum available depth without hardcoding the
floor date — at the cost of losing up to one chunk-width at the deep end, which is
acceptable because the accumulation design makes the deep end a one-time bonus.

The seed is not resumable. It is idempotent (every write is a merge), so an interrupted
run is re-run, and it can be split across sessions with `--tickers`.

### Accumulation

Yahoo intraday and Massive REST intraday are both rolling windows, so history is built by
repeated merges rather than fetched once. `IntradayBronzeClient.merge_ticker_rows()`
already dedups on `bar_timestamp`, which is exactly the needed primitive — no new
storage code.

The rolling floor bounds only the **initial backfill depth**. After the seed, each nightly
run merges the recent window into the accumulated file, so held history grows past the
floor: two years after seeding at the 2024-07-24 floor, the lake holds four years.

### Yahoo API asymmetry

Daily uses `period1`/`period2`. Intraday **must** use `range=` — `period1`/`period2` with
an intraday interval returns "Unprocessable Entity". Separately, `range=max` with
`interval=1d` silently downsamples (168 rows for a 41-year span); explicit
`period1=0&period2=<now>` returns the true 17,219 daily rows. Both behaviours are encoded
in the client rather than left to the caller.

### Symbol convention

Local storage uses market-convention six-letter pairs (`EURUSD`, `USDJPY`, `USDKRW`) plus
`DXY`. Provider symbols derive by rule, so there is no mapping table to maintain:

- Yahoo pair → `f"{pair}=X"`. Verified uniform: `USDJPY=X` and `JPY=X` return byte-identical
  series (7,759 bars from 1996-10-30), as do `USDCHF=X`/`CHF=X`, `USDKRW=X`/`KRW=X`,
  `USDCNY=X`/`CNY=X`, `USDPEN=X`/`PEN=X`.
- Yahoo DXY → `DX-Y.NYB` (the single special case).
- Massive pair → `f"C:{pair}"`.

The existing `USDEUR` is the IB-era inverted spelling. It is replaced by `EURUSD` from
Yahoo, which is both correctly oriented and deeper (2003-12-01 vs 2016-05-24), and the old
file is archived under `bronze-delisted/asset_class=fx/`.

### Universe

21 symbols: DXY, 9 G10 pairs, 11 common NDF pairs.

```
DXY
EURUSD USDJPY GBPUSD USDCHF USDCAD AUDUSD NZDUSD USDSEK USDNOK
USDKRW USDTWD USDINR USDBRL USDCLP USDCOP USDPHP USDIDR USDCNY USDRUB USDPEN
```

### Storage

`data-lake/bronze/asset_class=fx/symbol=<SYMBOL>/{1d,1m,5m,30m,1h}.parquet`

The `fx` daily schema profile is `_BASE_COLUMNS` — no `source`/`price_basis` columns, so
none of the equity basis machinery (split classification, Silver's fail-closed unknown-basis
gate) applies. Intraday uses the standard `_INTRADAY_SCHEMA`. FX has no splits or dividends,
so `adj_close = close`.

### IB fx lane removal

`livewire_scripts/run_daily_update_job.py:31` drops `"fx"` from `ASSET_CLASSES`, leaving
`["equity", "futures", "cmdty"]`. That is the only occurrence of `fx` in the file, so the
change is one line.

`resolve_fx_pair()` and `SUPPORTED_IB_FX_PAIRS` stay untouched — `make_contract()` still
routes through them for any caller that asks for an fx contract explicitly (e.g. a manual
`historical --asset-class fx`). Only the nightly loop stops driving them.

## Rejected alternatives

- **Keep both sources, guard the IB lane against non-whitelisted symbols.** Two sources
  writing one asset class means every future symbol needs a routing decision, and a symbol
  present in both would silently take whichever lane ran last.
- **A separate asset_class for Yahoo-sourced symbols.** Splits FX across two trees for a
  provenance distinction that the source assignment table already records.
- **Derive 5m/30m/1h locally from 1m.** Costs 723 days of DXY 1h history for no benefit;
  each timeframe is directly fetchable at greater depth.
- **Massive S3 forex flat files.** Not entitled (403 on GET). Revisit if the subscription
  is upgraded — it would extend intraday to 2010.

## Verification

- Unit tests for the new client methods with frozen real fixtures (real symbols, real
  prices, captured with an as-of date, no network at runtime), per the project's
  no-synthetic-data rule.
- A live seed run measured against the figures in this document.

**Result (2026-07-27).** All 21 symbols seeded. DXY published **14,108 bars,
1971-01-04 → 2026-07-27** — 253.9 bars/year against a ~252-day US trading calendar,
confirming the 3,111 dropped timestamps really were Yahoo's null holiday padding and
not lost data. The published OHLC for 2025-07-09 (97.5500 / 97.7500 / 97.4600 / 97.4700)
matches the frozen test fixture exactly, so the fixture and the live pipeline agree.
