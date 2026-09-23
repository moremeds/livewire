# EIA API v2 — catalog and lake plan (exploration, 2026-09-23)

Read-only. Nothing written to the lake, no code in `clients/` or `livewire_scripts/`.
Predecessor: `docs/audits/price-discovery/ENERGY_SPECTRUM.md` on `feat/price-discovery`
(no-key HTML/XLS catalog). This pass uses the keyed JSON API, which removes that
audit's `.xls` reader blocker.

Decided with the user 2026-09-23: v1 = daily **and** weekly; one directory per
product; keep raw responses (vintages); electricity included; `EIA_API_KEY` lives
in the mini's `~/market-warehouse/.env` (added 2026-09-23, mode 600, 40 chars,
backup `.env.bak-<stamp>`; never echoed).

## Evidence (all run on the mini, key read from its `.env`)

| File | What | Rerun |
|---|---|---|
| `evidence/eia_catalog.py` → `catalog-2026-09-23.jsonl` | every leaf route under `/v2/` (232): frequencies, period range, facets, total rows per frequency, rows in a recent window | `python3 eia_catalog.py [root/ …] > catalog-<date>.jsonl` |
| `evidence/eia_discover.py` → `routes-2026-09-23.json` | petroleum + natural-gas leaves with the **series list** live at daily/weekly (obs in last 21 days) | `python3 eia_discover.py petroleum/ natural-gas/ > routes-<date>.json` |
| `evidence/eia_probe.py` | first smoke probe (key on stdin) | — |

Catalog gaps to rerun: `ieo/2023` (429), `aeo/2025`, `aeo/2026` counts (429),
`electricity/electric-power-operational-data` monthly count (503).

## Hosts and egress

- MacBook: TLS reset on `www.`/`api.eia.gov` (and `fred.stlouisfed.org`); other
  sites fine. Dev tests must mock; live calls only from the mini.
- mini: 200 with key. Reached over LAN `192.168.50.232` because the MacBook's
  Tailscale was offline.

## Everything EIA offers (catalog, 232 leaves)

Rows ≈ sum over leaves of the largest frequency's total; a download is ≥ rows/5000 requests.

| Root | Leaves | Rows ≈ | Frequencies | Sub-weekly? |
|---|---|---|---|---|
| petroleum | 112 | 18.7M | monthly/annual; weekly 14 leaves; daily 2 | **yes** |
| natural-gas | 53 | 1.5M | monthly/annual; weekly 2; daily 1 | **yes** |
| electricity | 19 | 106.0M | hourly 4, daily 4, monthly/quarterly/annual | **yes** |
| nuclear-outages | 3 | 1.4M | daily | **yes** |
| coal | 13 | 0.6M | quarterly/annual | no |
| crude-oil-imports | 1 | 0.6M | monthly | no |
| international | 1 | 4.3M | monthly/quarterly/annual | no |
| steo (short-term outlook) | 1 | 0.5M | monthly/quarterly/annual | no — forecast, re-issued monthly |
| total-energy | 1 | 0.6M | monthly/annual | no |
| seds | 1 | 2.6M | annual | no |
| aeo / ieo (long-term outlooks) | 13 / 4 | 70.2M / 0.7M | annual, one leaf per edition | no — scenario projections |
| densified-biomass, co2-emissions | 8 / 2 | small | monthly/annual | no |

Monthly/annual/outlook data is out of v1 scope; the catalog keeps it findable.

## Every live daily/weekly/hourly dataset

| Product | Route | Freq | Live series / facets | Rows (total) | Since |
|---|---|---|---|---|---|
| petroleum | `pri/spt` spot prices | daily | 11 series (WTI, Brent, NYH/Gulf gasoline, LA RBOB, NYH heating oil, NYH/Gulf ULSD, LA CARB diesel, Gulf jet, Mont Belvieu propane) | 92,090 | 1986 |
| petroleum | `pri/gnd` retail gasoline + diesel | weekly | 310 series (14 U.S.-level, rest PADD/state/city) | 434,107 | 1990 |
| petroleum | `sum/sndw` weekly supply estimates (full Weekly Petroleum Status Report) | weekly | 561 series | 776,792 | 1982 |
| ↳ contains | `stoc/wstk` **stocks (inventory)** | weekly | 173 (34 U.S.-level: crude incl./excl. SPR, SPR, gasoline, distillate, jet, propane, resid…) | 270,055 | 1982 |
| ↳ contains | `pnp/wiup` refinery input & utilization | weekly | 60 | 73,250 | 1982 |
| ↳ contains | `pnp/wprodrb` refiner + blender net production | weekly | 109 | 163,414 | 1982 |
| ↳ contains | `pnp/wprode` oxygenate production | weekly | 6 | 5,100 | 2010 |
| ↳ contains | `move/wkly` imports / exports / net | weekly | 185 | 226,423 | 1982 |
| ↳ contains | `cons/wpsup` product supplied (demand proxy) | weekly | 7 | 11,644 | 1990 |
| ↳ sndw-only | — | weekly | 21: crude field production (U.S./L48/AK), days of supply ×5, 12 extra gasoline stock series, 1 adjustment | — | — |
| petroleum | `pnp/wprodr` refiner production by district | weekly | 21 (not in sndw) | 17,850 | 2010 |
| petroleum | `pnp/wprodb` blender production | weekly | 20 (not in sndw) | 17,000 | 2010 |
| petroleum | `move/wimpc` imports by country | weekly | 19 (not in sndw) | 15,672 | 2010 |
| petroleum | `pri/wfr` heating oil & propane retail | weekly | 0 live; last obs 2026-03-30 | 72,135 | 1990 |
| natural gas | `pri/fut` → `RNGWHHD` Henry Hub spot | daily | 1 live | 7,461 | 1997 |
| natural gas | `stor/wkly` **working gas in storage** | weekly | 8 (Lower 48, East, Midwest, Mountain, Pacific, South Central, Salt, Nonsalt) | 6,976 | 2010 |
| electricity | `rto/daily-region-data` demand, forecast, generation, interchange | daily | facets respondent × type × timezone | 4.0M | 2019 |
| electricity | `rto/daily-fuel-type-data` generation by fuel | daily | respondent × fueltype × timezone | 5.7M | 2019 |
| electricity | `rto/daily-region-sub-ba-data` subregion demand | daily | subba × parent × timezone | 1.1M | 2019 |
| electricity | `rto/daily-interchange-data` flows between BAs | daily | fromba × toba × timezone | 4.7M | 2019 |
| electricity | `rto/region-data`, `fuel-type-data`, `region-sub-ba-data`, `interchange-data` | hourly (UTC) | same facets | 19.4M / 27.4M / 5.5M / 22.9M | 2019 |
| nuclear | `nuclear-outages/us-nuclear-outages` | daily | U.S. total | 7,205 | 2007 |
| nuclear | `facility-…` / `generator-nuclear-outages` | daily | facility / generator | 711,047 each | 2007 |

Dead or not current:
- `petroleum/pri/fut` (EIA's NYMEX futures prices) ends **2024-04-05**, so futures come from IB only.
  The CL/RB/HO/NG futures in `natural-gas/pri/fut` also have no rows in the recent window; only RNGWHHD does.
- `petroleum/pri/wfr` ends 2026-03-30. It looks like a winter-season survey
  (Oct–Mar), which is **inference, not verified**; check again after October.

## v1 download list (daily + weekly + electricity daily)

| # | Dataset | Series / partitions | Backfill rows | Backfill requests ≈ | Weekly update requests |
|---|---|---|---|---|---|
| 1 | petroleum spot | 11 | 92,090 | ~25 (RWTC alone is 3 pages) | 1 |
| 2 | petroleum retail | 310 | 434,107 | ~310 (one per series) | 1 |
| 3 | petroleum weekly supply (sndw, incl. inventory) | 561 | 776,792 | ~561 | 1 |
| 4 | refiner by district + blender + imports by country | 60 | 50,522 | ~60 | 3 |
| 5 | Henry Hub spot | 1 | 7,461 | 2 | 1 |
| 6 | natural-gas storage | 8 | 6,976 | 8 | 1 |
| 7 | electricity daily ×4 | respondent/BA × type | 15.5M | ≥3,100 | ~4–20 |
| 8 | nuclear outages daily ×3 | U.S. / facility / generator | 1.43M | ≥290 | 3 |
| | **total** | ~951 series + electricity/nuclear facets | **~18.3M** | **~4,400** | **~15–35** |

Deferred: electricity hourly (75M rows, ≥15,000 requests); decide after the
daily set runs. Not in scope: monthly/annual/outlooks.

## Proposed lake layout (one directory per product)

```
bronze/asset_class=energy/
  product=petroleum/
    dataset=spot_price/series=RWTC/1d.parquet
    dataset=retail_price/series=EMM_EPM0_PTE_NUS_DPG/1w.parquet
    dataset=stocks/series=WCESTUS1/1w.parquet          # wstk + the 12 sndw-only stock series
    dataset=refinery/series=…/1w.parquet                # wiup, wprodrb, wprode, wprodr, wprodb
    dataset=movements/series=…/1w.parquet               # move/wkly, wimpc
    dataset=product_supplied/series=…/1w.parquet        # wpsup
    dataset=supply_other/series=…/1w.parquet            # field production, days of supply, adjustment
  product=natural_gas/
    dataset=spot_price/series=RNGWHHD/1d.parquet
    dataset=storage/series=NW2_EPG0_SWO_R48_BCF/1w.parquet
  product=electricity/
    dataset=region/respondent=PJM/1d.parquet            # type is a column (D, DF, NG, TI)
    dataset=fuel_type/respondent=PJM/1d.parquet
    dataset=sub_ba/parent=PJM/1d.parquet
    dataset=interchange/fromba=PJM/1d.parquet
  product=nuclear/
    dataset=outages_us/1d.parquet
    dataset=outages_facility/facility=…/1d.parquet
```

- `product` = EIA's top-level family. The finer "crude / gasoline / distillate"
  split is a column (`product` facet), not a directory.
- Series rows: `period, series, value, units, duoarea, product, process, source`.
  Electricity rows: `period, respondent|subba|fromba, type|fueltype|toba, timezone, value, units`.
- The dataset for each sndw series comes from the sub-route it also belongs to, else `supply_other`.
- Raw responses go into the existing source-evidence CAS, committed once per run
  (not per response; pm:2026-08-31-source-evidence-per-response-cost). EIA has no
  vintages, so this is the only as-of record.
- New `energy` bronze profile. **Not** `cmdty`: that profile is OHLCV, and a
  single spot value written as O=H=L=C would fabricate a bar.

## Traps

1. **5,000 rows per JSON response**; a larger request returns `warnings: incomplete return`.
   Paginate with `offset`; a residual warning is a failure, never a partial success.
2. **HTTP 429 exists.** It was hit during a sequential metadata walk (~800 calls).
   The limit value is **not verified**. Measure it or find EIA's published figure
   before sizing the backfill pace (rule: a rate constant is published or measured).
3. **`value` type varies**: string on petroleum spot (`'107.02'`), number on NG.
4. **Metadata `endPeriod` lags the data** (says 09-11, data has 09-15). Judge freshness from the rows.
5. **Publication lag differs by dataset**: daily spot is published weekly (Wed, prior
   week); weekly supply on Wed; NG storage on Thu (from EIA's release calendar, unverified here);
   electricity daily is T+1–3 by the observed `endPeriod` (09-20…09-22 on 09-23).
   Each dataset needs its own `DUE_LAG_DAYS`, measured over several releases.
6. **`local-hourly` returns 500** on count queries; use `hourly` (UTC).
7. **`electric-power-operational-data` returned 503** once. The retry must go through `clients/http_retry.py`.

## Fit into livewire (pattern: FRED rates)

| Piece | EIA |
|---|---|
| client | `clients/eia_client.py`: `get_with_retry`, offset pagination, fail on a residual `incomplete return`, 429 treated as retryable with backoff |
| lane | `livewire_scripts/fetch_eia.py`, one dataset at a time, per-series isolation, still unfetched → exit 1 |
| universe | `presets/eia-*.json` per product (series lists taken from `routes-<date>.json`) |
| denominator | one `registry/gaps.json` row per product × timeframe + its test; per-dataset `DUE_LAG_DAYS` |
| schedule | a phase next to FRED in `sync_runner`; `eia` entry in `scripts/livewire_ingest.py` |
| catalog | `energy` added to `duckdb_catalog`; `gap_engine` source map `energy: eia` (deep history, no rolling floor) |

Open:
- The 429 limit: measure it or cite EIA's number before the ~4,400-request backfill.
- Electricity hourly: in or out after the daily run.

## Bulk files (checked 2026-09-23, from the mini)

`https://api.eia.gov/bulk/manifest.txt` lists one zip per dataset family, each a
text file of one JSON object per series (`series_id`, `name`, `units`, `f`,
`data: [[period, value], …]`). Sizes and contents as downloaded:

| zip | size (unzipped) | series | frequencies | download |
|---|---|---|---|---|
| `EBA.zip` grid monitor | 691 MB (4.35 GB) | 3,074 = 1,537 UTC (`.H`) + 1,537 local (`.HL`) | hourly only | 70 s |
| `PET.zip` | 55 MB (367 MB) | 181,143 | W 1,157 · D 31 · M 84,748 · A 94,773 · 4-weekly 434 | 15 s |
| `NG.zip` | 4.4 MB (25 MB) | 16,080 | D 5 · W 13 · M 3,151 · A 12,911 | 11 s |
| `NUC_STATUS.zip` | 11 MB (57 MB) | 522 | daily | 12 s |

Also listed: `ELEC` (288 MB), `PET_IMPORTS`, `COAL`, `INTL`, `SEDS`, `STEO`,
`TOTAL`, `EMISS`, `AEO.*`, `IEO.*`.

Used for (a) every monthly/quarterly/annual series of PET, PET_IMPORTS, NG, ELEC,
COAL, TOTAL, SEDS, INTL, EMISS and STEO (per release), and (b) hourly electricity
history from EBA. Daily/weekly petroleum and gas stay on the API, which carries the
facet columns (`duoarea`, `product`, `process`) the bulk file does not.

What the files contain that the parser has to handle (profiled 2026-09-23):
- periods `YYYYMMDD`, `YYYYMM`, `YYYYQn`, `YYYY`; PET's `4` (4-week) uses `YYYYMMDD`;
- non-numeric values only in TOTAL (`NA` 48,847, `-` 14,445, `- -`, `W`, `--`) and
  INTL (`--` 321,641, `NA` 95,821, `w`, `ie`) — stored null with the marker kept;
- the same period listed twice inside one series (PET 30, NG 6), always equal;
- EMISS opens with a non-JSON line `discontinued`.

EBA vs API, 2026-09-14..15: every bulk key exists in the API, names and units
agree; 3.7% of `region` values differ by rounding (bulk keeps decimals) and a
handful by revision. The bulk file lacks series the API has (BA `SWPW`; fuel
types `BAT`, `SNB`, `PS`; several sub-BAs), so those come from the API after the
import. It ends ~1 day behind the API (`EBA.PJM-ALL.D.H` ended 2026-09-22T19
while the API served 09-24T04 forecasts), so the daily refresh stays on the API.
