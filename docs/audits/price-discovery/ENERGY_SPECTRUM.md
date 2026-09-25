# ENERGY_SPECTRUM.md — Task 2 energy-spectrum + crack-spread input inventory

Bounded READ-ONLY investigation per `ENERGY_SPECTRUM_TASK.md` (incl. explicit
crack-spread priority). Same worktree, same contract as Task 1/2 gasoline
sub-probe. No code changes, no lake writes, no ingestion, no spread
calculations. Capture window: **2026-09-23 ~02:20–02:35 UTC** via `ssh macmini`
egress + mini lake reads (PyArrow) + live Apex GETs. Agent: SWE-2 Max, cwd
`/Users/chenxi/projects/livewire/.worktrees/price-discovery`.

## 1. Evidence manifest (new downloads this pass — 628 KB total, cap 20 MB)

| File (under `evidence/energy-access/`) | URL | HTTP | Bytes | SHA-256 |
|---|---|---|---|---|
| `EMM_EPM0_PTE_NUS_DPGw.xls` | https://www.eia.gov/dnav/pet/hist_xls/EMM_EPM0_PTE_NUS_DPGw.xls (link discovered on captured LeafHandler page) | 200 | 120,320 | `340e4f45eda7e05352e1033392d9638c24539913f076bc1c6f4d18a8cdd6aff0` |
| `eia-pet-spot-s1-d.html` | https://www.eia.gov/dnav/pet/pet_pri_spt_s1_d.htm | 200 | 19,301 | `308f084b887d346911e1b778dc86871e5f23ff69813410e3979a116b72f4300d` |
| `eia-ng-rngwhhd-d.html` | https://www.eia.gov/dnav/ng/hist/rngwhhdD.htm | 200 | ~307,390 | `115c1d4f70a20f3caee3e0a7523ba3369b04d35eb68f8506a207be6591d8bb40` |
| `eia-coal.html` | https://www.eia.gov/coal/ | 200 | 52,324 | `f76e21f7ceade076756e746469d7748611b24b7e6fdad277ec78b79c9c71c38b` |
| `eia-epm.html` | https://www.eia.gov/electricity/monthly/ | 200 | 131,995 | `b8af236592e38d9cbfdbd6c35eece3b1c3039dc5ff025eca6eeb4d3422833868` |

Preserved gasoline artifacts (prior pass, not re-downloaded):
`evidence/gasoline-access/leafhandler-EMM_EPM0_PTE_NUS_DPG.html` (200, ~183 KB,
sha256 `7b472a11a735155160327df83e83dcc5753b8c0888cd70e4be5537c1eb13f30f`),
plus MPD00 capture `2026-09-22-capture/eia-pswrgvwall.xls` + `eia-weekly-us.html`.

Failed routes (≤1 retry each, per contract): `fred.stlouisfed.org/series/
CUUR0000SETB01` (HTTP/2 error, then http1.1 timeout), `data.bls.gov/timeseries/
CUUR0000SETB01` (403), `download.bls.gov/pub/time.series/cu/*` (403),
`LeafHandler.ashx?n=NG&s=RNGWHHD` (404 — NG section does not use LeafHandler;
corrected to the `rngwhhdD.htm` hist page), `eia.gov/dnav/electricity/` (404 —
electricity lives under `/electricity/`, confirmed 200 there).

## 2. Existing local coverage (observed on mini lake + Apex live)

Bronze futures partition (bounded `ls`, 14 contract dirs — full set, not a sample
of a larger scan): `GC_202607/08/10/12/202702`, `CL_202608/09/10/11/12`,
`BZ_202609/10/11/12`. **No RB, HO, NG, or any other roots.**

Per-contract probe (all 14 dirs; last `trade_date` + stored `expiry_date`):

| Contract | Rows | Last trade_date | Stored expiry_date | State |
|---|---|---|---|---|
| `CL_202610` | 2,136 | 2026-09-21 | 2026-10-01 | accruing |
| `CL_202611` | 2,137 | 2026-09-21 | 2026-11-01 | accruing |
| `CL_202612` | 2,197 | 2026-09-21 | 2026-12-01 | accruing |
| `CL_202608` | 2,097 | 2026-07-22 | 2026-08-01 | stopped ~9 d before stored expiry — consistent with contract end, **not verified** |
| `CL_202609` | 2,115 | 2026-08-20 | 2026-09-01 | stopped ~12 d before stored expiry — same caveat |
| `GC_202610` | 454 | 2026-09-21 | 2026-10-01 | accruing |
| `GC_202612` | 1,420 | 2026-09-21 | 2026-12-01 | accruing |
| `GC_202702` | 390 | 2026-09-21 | 2027-02-01 | accruing |
| `GC_202607` | 368 | 2026-07-30 | 2026-07-01 | last trade **29 d after** stored expiry — proves stored `expiry_date` ≠ verified last-trade |
| `GC_202608` | 476 | 2026-08-20 | 2026-08-01 | same anomaly (last trade > stored expiry) |
| `BZ_202609` | 1,894 | 2026-07-02 | 2026-09-01 | stopped ~2 mo before stored expiry — cause unresolved |
| `BZ_202610` | 1,902 | 2026-07-31 | 2026-10-01 | **stale ~7 wk, still in `futures-active`** — cause unresolved |
| `BZ_202611` | 1,902 | 2026-07-31 | 2026-11-01 | same |
| `BZ_202612` | 1,905 | 2026-07-31 | 2026-12-01 | same |

All four BZ contracts stopped accruing between 2026-07-02 and 2026-07-31 while
three of them remain in `futures-active` (preset claims IB-verified 2026-08-02).
Provider no-data vs lane exclusion not distinguished — nightly lane logs not
inspected. No expiry-based classification is claimed anywhere: stored
`expiry_date` is a derived field (see note below) and no last-trade calendar was
verified.

**Field-provenance caveat (`clients/ingestion_common.py:156-171`)**:
`bars_to_futures_rows` writes `"settlement": float(bar.close)` (line 166) — the
lake's settlement column is a **copied close**, not an independently verified
exchange settlement — and `"open_interest": 0` (line 168) is hardcoded. The
`expiry_date` stored per row is the constructor-supplied contract month (e.g.
`202610` → `2026-10-01`), not a verified exchange last-trade date — GC rows
trading past stored expiry demonstrate this. For crack work: any
"settlement"-based spread is actually a close-based spread, and contract-month
alignment cannot rely on stored expiry alone.

Schema (all futures dirs): `trade_date, contract_id, root_symbol, expiry_date,
OHLC, settlement, volume, open_interest, asset_class, symbol`; 0 dup keys in
probes; `source:"ib"` in meta.json; interior-gap flags exist on all four probed
metas (45 missing days CL_202610 window 2018–2024, etc.).

Presets (catalog declarations, not proof of coverage):
- `presets/futures-active.json` — GC(COMEX)+CL(NYMEX)+BZ(NYMEX) only; the set
  the nightly `daily --asset-class futures` lane maintains.
- `presets/futures-energy.json` — CL+NG quarterly strips 202606–202803 NYMEX;
  **NG is absent from the inspected lake now** (0 NG dirs) — current absence
  observed; whether it was ever ingested and later removed is not determined.
- No preset contains **RB** or **HO** anywhere.

Other lanes: `cmdty` = XAUUSD only; `rates` = DGS* Treasury constants. No energy
symbol exists in cmdty/rates.

Apex live (`127.0.0.1:8322`): `GET /v1/futures/CL_202610/bars?tf=1d&…` → **200**,
contract block `{root_symbol:CL, expiry_date:2026-10-01}` + 5 daily bars
09-15→09-21 (settlement=close, `open_interest:0` in payload). `NG_202612` →
**404**, consistent with lake absence. Futures are 1d-only in
`asset_classes.py` (no intraday surface).

## 3. Official source catalog (EIA, no-key, verified this pass)

**Daily spot prices** — `pet_pri_spt_s1_d.htm` ("Spot Prices for Crude Oil and
Petroleum Products", release 9/16/2026, next 9/23/2026; $/bbl crude, $/gal
products). Heading→series pairing read off the page DOM:

| Product (page heading) | Area | Series | Range |
|---|---|---|---|
| WTI | Cushing, OK | `RWTC` | D 1986–2026 |
| Brent | Europe | `RBRTE` | D 1987–2026 |
| Conventional Gasoline | NY Harbor / Gulf Coast | `EER_EPMRU_PF4_Y35NY_DPG` / `_RGC_` | D 1986–2026 |
| RBOB Regular Gasoline | Los Angeles (Y05LA) | `EER_EPMRR_PF4_Y05LA_DPG` | D 2003–2026 |
| No. 2 Heating Oil | NY Harbor | `EER_EPD2F_PF4_Y35NY_DPG` | D 1986–2026 |
| Ultra-Low-Sulfur No. 2 Diesel | NY Harbor / Gulf / LA | `EER_EPD2DXL0_PF4_Y35NY/RGC`, `EER_EPD2DC_PF4_Y05LA` | D 2006– / 1996–2026 |
| Kerosene-Type Jet Fuel | Gulf Coast | `EER_EPJK_PF4_RGC_DPG` | D 1990–2026 |
| Propane | Mont Belvieu (Y44MB) | `EER_EPLLPA_PF4_Y44MB_DPG` | D 1992–2026 |

**Residual fuel oil: not present in the daily spot catalog** — no residual
series observed; catalog-level gap (monthly/other sections not probed, bounded).

**Weekly retail** (from prior captures): gasoline retail series on
`pet_pri_gnd` page — `EMM_EPM0_PTE_NUS_DPG` (all grades/all formulations,
verified), `EMM_EPMU/EPMR/EPM0R/EPMRU/EPMM…` regular/mid/premium ×
conventional/reformulated/all-formulations; diesel retail `EMD_EPD2D_PTE_NUS_DPG`
(No. 2 diesel), `EMD_EPD2DXL0_…` (ULSD), `EMD_EPD2DM10_…`. Methodology note on the
captured page: since **2010-07-26** the retail "Diesel Average All Types" is
fully represented by ULSD (LSD publication ended Dec 2008).

**Gasoline sample verified**: LeafHandler page title "Weekly U.S. All Grades All
Formulations Retail Gasoline Prices (Dollars per Gallon)", obs 1993-Apr-05 →
**2026-Sep-21 (4.610 $/gal, observed page value)**, Release Date 9/22/2026, next
9/29/2026. XLS history file downloaded & frozen (manifest above; `.xls` binary —
no xlrd/pandas reader available in mini venv or sandbox → contents verified via
the HTML table only).

**Natural gas spot**: `rngwhhdD.htm` = "Henry Hub Natural Gas Spot Price
(Dollars per Million Btu)", daily obs from **1997-Jan**, page groups daily
values by week; last week Sep 14–18 partially populated (Mon 2.85, Tue 2.97 —
observed page values) under Release 9/16 → next 9/23, i.e. **the daily series is
published weekly each Wednesday**. XLS link exposed: `hist_xls/RNGWHHDd.xls`
(discovered, not downloaded — one representative sample per family).

**Electricity / coal (catalog level only)**: `/electricity/monthly/` (Electric
Power Monthly) 200; `/coal/` (EIA coal portal) 200. No series-level crawl per
bounds.

**Not verified this pass**: `CUUR0000SETB01` (target gasoline CPI) metadata —
FRED timed out, data.bls.gov + download.bls.gov both 403 through mini egress.
ID-structure reading (CPI-U, unadjusted, US city average, gasoline-all-types
item SETB01) is convention only, **not verified against an authoritative page**.
NG regional benchmarks (e.g. other hubs), LNG price series, NYMEX/CME contract
specs (multipliers, HO-vs-ULSD futures naming, exchange-listed crack spread
instruments), and any vintage/as-of history for EIA series — all **unverified**.

## 4. Product-label distinctions (explicit)

- **Heating oil vs diesel vs ULSD**: EIA treats them as distinct series —
  `EPD2F` (No. 2 heating oil spot, 1986–) vs `EPD2DXL0` (ULSD spot, 2006–) vs
  `EPD2DC` (LA diesel, 1996–). At retail, "diesel all types" = ULSD since
  2010-07-26 (page methodology). NYMEX `HO` futures contract identity
  (historically heating oil, now NY Harbor ULSD per contract spec) **not
  verified** — CME spec not fetched.
- **RBOB vs retail gasoline**: retail weekly series (`EMM_*`) are survey prices
  $/gal; `RB` futures are a NYMEX blendstock contract — different instrument,
  not interchangeable. EIA spot catalog carries conventional gasoline
  (NYH/Gulf) and an "RBOB Regular Gasoline" heading that maps only to the LA
  reformulated series.
- **Henry Hub spot vs futures vs utility CPI**: `RNGWHHD` = daily physical spot
  assessment ($/MMBtu); `NG` = NYMEX futures root (declared preset, absent from
  lake); retail piped-gas CPI is a BLS survey series — three different things,
  kept separate.
- **XAUUSD** (cmdty lane) is unrelated to energy; no energy spot commodity in
  cmdty lane.

## 5. Crack spreads — first-class inventory (no calculations performed)

Leg reality vs required basket:

| Crack leg | Benchmark/geo | Unit | Instrument type | Status |
|---|---|---|---|---|
| Crude leg | WTI Cushing (spot `RWTC` / futures `CL` NYMEX) | $/bbl | source-published assessment + listed futures | spot: verified catalog; futures: accruing locally (5 CL dirs) |
| Crude leg | Brent Europe (spot `RBRTE` / futures: `BZ` NYMEX dead at IB; **`COIL` @ IPE = ICE Brent, verified Task 3**) | $/bbl | same | spot: verified catalog; futures: BZ stale+unresolvable since 2026-07-31, COIL@IPE qualified (head 2019-04-17) but not yet in presets/lake |
| Gasoline leg | RBOB NY Harbor (futures `RB` NYMEX) | $/gal | listed futures | absent from lake/presets; **IB-verified Task 3** (RBX6 qualified, head 2022-11-30, daily+hourly served) — needs `ROOT_EXCHANGE_MAP` entry |
| Gasoline leg | conventional/RBOB gasoline spot NYH/Gulf/LA | $/gal | source-published assessment | verified catalog (1986–/2003–) |
| Distillate leg | heating oil/ULSD (futures `HO` NYMEX) | $/gal | listed futures | absent from lake/presets; **IB-verified Task 3** (HOZ6 qualified, head 2022-11-30, daily+hourly served) — needs `ROOT_EXCHANGE_MAP` entry |
| Distillate leg | No.2 HO / ULSD spot NYH/Gulf | $/gal | source-published assessment | verified catalog (1986–/2006–) |
| 3-2-1 basket | **3 crude : 2 gasoline + 1 distillate** | normalized basket | locally derived only | **no aligned legs exist locally today** (RB+HO missing) |

3-2-1 spec (per official CME references supplied by the lead — pages not
re-fetched this pass, cmegroup.com returned 403 through mini egress):
- https://www.cmegroup.com/articles/2024/trading-crack-spreads.html
- https://www.cmegroup.com/education/articles-and-reports/introduction-to-crack-spreads

Confirmed there: the 3-2-1 crack is **3 crude contracts vs 2 gasoline (RBOB) +
1 distillate (HO/ULSD)**; one RB or HO contract = 42,000 gal = 1,000 bbl (same
notional as CL). The crack margin per barrel of crude is the **net basket value
— i.e. (2·RBOB + 1·HO product value − 3·CL crude cost) ÷ 3** — product revenue
alone is not a margin. No prices computed or cited.

Alignment requirements recorded, none verified against contract specs:
- Delivery month: futures legs are contract-month instruments; spot
  assessments are prompt physical — a spot-vs-futures crack mixes tenors.
  Matching CL/RB/HO contract months is required before any basket work.
- Units: crude $/bbl vs products $/gal → 42 gal/bbl conversion + contract
  multipliers (CL/RB/HO/NG specs) must be verified against exchange specs —
  not fetched this pass.
- Timestamps: spot "daily closing spot price" (EIA note) vs futures
  `settlement` — different fixing semantics.
- Leg type: locally available legs would be *derived spreads* (from futures or
  spot legs); exchange-listed crack-spread instruments exist on NYMEX but were
  **not verified** against IB or local catalogs — do not assume availability.

Caveats (required by contract): independently rolled continuous series are not
automatically aligned legs; a crack spread is a gross refining-margin proxy, not
net realized refinery profitability; predictive value is a hypothesis only;
natural-gas processing/power "spark spreads" are different economics and must
not be mislabeled as petroleum cracks.

## 6. Clock / vintage summary

All publication rows below are **single observed page-date pairs**, not verified
publication policy — one Release/Next pair does not establish permanent
cadence.

| Source | Last observation on page | Page Release Date | Page Next Release | Vintage/revision support |
|---|---|---|---|---|
| EIA weekly retail gasoline | wk ending Mon 2026-09-21 | 9/22/2026 | 9/29/2026 | single current file; **no historical vintage** evidenced |
| EIA daily spot (all products) | Tue 2026-09-15 | 9/16/2026 | 9/23/2026 | same — no as-of history evidenced |
| EIA Henry Hub daily | Tue 2026-09-15 (Mon–Tue of Sep-14 week shown) | 9/16/2026 | 9/23/2026 | same |
| IB futures daily | trade session | lake accrual observed to 2026-09-21 (CL/GC) | — | contract-level files; no roll/adjustment layer (basis `unadjusted`); `settlement` col = copied close, `open_interest` = 0 (see §2 caveat) |

Retrieval timestamps prove capture, not historical availability.

## 7. Use-case grouping

1. **Consumer inflation pass-through**: retail gasoline weekly (verified, 1993–,
   $/gal) + retail diesel weekly + CPI `CUUR0000SETB01` (metadata unverified —
   BLS/FRED blocked through mini egress; next: BLS flat-file via different
   egress or keyed API, or FRED retry).
2. **Spot/futures price discovery**: EIA daily spot catalog (11 series verified)
   + accruing CL/GC futures via Apex `/v1/futures` (200 verified); BZ lane
   anomalous; NG/RB/HO futures absent.
3. **Refinery/crack-spread research**: spot legs fully cataloged (WTI/Brent ×
   gasoline/HO/ULSD), futures legs incomplete (only CL accruing). No
   exchange-listed crack instrument evidence; any crack series would be a
   locally derived spread pending month/unit/time alignment verification.

## 8. Ranked next batch (coverage × cost, inventory only — nothing executed)

Verification precedes any fetch: a blanket `historical --preset
futures-energy.json` run would request already-expired months (202606) plus
contracts already covered, and RB/HO are not even qualified yet.

1. **IB qualification + head/depth probes for RB, HO, NG** — can these roots be
   qualified on IB like GC, and how deep does history go? Required before any
   futures-based crack leg or NG coverage. → executed as Task 3
   (`IB_ENERGY_FEASIBILITY_TASK.md`).
2. **BZ staleness diagnosis** — three in-preset BZ contracts stopped accruing
   after 2026-07-31; check nightly lane logs on mini (read-only) before
   assuming provider loss vs lane exclusion.
3. **Only then**: bounded `historical` fetches for verified live contract
   months — with a curated contract list, not the stale preset file.
4. **Retail gasoline EIA ingestion** — LeafHandler→`hist_xls` pattern proven;
   blocker: `.xls` parsing needs a reader dep (no xlrd anywhere); vintage is
   current-file-only, so available-at reconstruction needs scheduled captures
   going forward.
5. **CPI metadata** — BLS/FRED unreachable from mini egress this pass; retry
   later or alternate route.

## 9. Deviations

- NG LeafHandler route 404 → corrected to `rngwhhdD.htm` hist page (within
  one-retry bound); `LeafHandler.ashx` is PET-section-only.
- `fred.stlouisfed.org`, `data.bls.gov`, `download.bls.gov` all failed through
  mini egress → `CUUR0000SETB01` metadata deferred (unverified), no CPI numbers
  cited anywhere.
- `.xls` sample frozen as raw bytes; content claims sourced from the HTML page
  (no xlrd reader available locally or on mini).
- Electricity catalog URL corrected from `/dnav/electricity/` (404) to
  `/electricity/monthly/` (200).
- One over-broad scan avoided; all lake probes were targeted `ls`/single-file
  PyArrow reads.
