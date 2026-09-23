# IB_ENERGY_FEASIBILITY.md — Task 3: RB/HO/NG via existing IB path

Bounded READ-ONLY probe per `IB_ENERGY_FEASIBILITY_TASK.md`. Ran **2026-09-23
02:41–02:52 UTC** on the mini using the production release checkout's own venv
(`releases/f83e3df0…`, ib_async 2.1.0) and `clients.ib_client.IBClient` — the
existing connection mechanism: Gateway `127.0.0.1:4001`, base clientId **7**
(unused in `CLIENT_IDS`), error-326 escalation available but unneeded
(connected at 7). All requests serial, 2 s spacing, `TRADES`/`useRTH=1`
(mirroring `daily_update` futures params), disconnect in `finally`. Probe
script + full JSON transcript: `evidence/ib-energy/`.

Note: mid-probe (~02:44–02:49 UTC) the Gateway↔IBKR link degraded (2110/2103/
2105/2157 farm dropouts) then restored (1102). Requests waited through it; all
results below are post-restore. No pacing or permission errors seen.

## 1. Per-root results (one unexpired contract each, discovered not guessed)

`reqContractDetails(Future(root, NYMEX, USD))` returned the live chain; the
contract probed is the first listed month with `realExpirationDate` > today.

| Root | Qualified contract | Chain | Multiplier | Head ts (TRADES,RTH) | 1 M daily | 2 D 1-hour |
|---|---|---|---|---|---|---|
| RB | conId 600696516 `RBX6`, NYMEX, USD, "NYMEX RBOB Gasoline Index", realExp **2026-10-30** 14:30 | 40 contracts, monthly → 2028 | 42,000 (gal) | **2022-11-30** | 21 rows 08-24→09-22 | 16 bars 09-21 09:30→09-22 16:00 ET |
| HO | conId 600696468 `HOZ6`, NYMEX, USD, "Heating Oil", realExp **2026-11-30** 14:30 | 40 contracts, monthly → 2028 | 42,000 (gal) | **2022-11-30** | 21 rows 08-24→09-22 | 16 bars, same window |
| NG | conId 269459992 `NGF27`, NYMEX, USD, "Henry Hub Natural Gas", realExp **2026-12-29** 14:30 | 147 contracts, monthly → 2029 | 10,000 (mmBtu) | **2017-03-13** | 21 rows 08-24→09-22 | 16 bars, same window |

Separation of concerns, per contract:
- **Qualification**: all three qualify on NYMEX with real `conId`/`localSymbol`/
  `tradingClass`/`multiplier`/`realExpirationDate`/`lastTradeTime` — exchange
  ticker = IB root symbol here (no rename needed, unlike BZ).
- **Subscription/permission**: no entitlement errors (none of 354/10167/162…);
  historical TRADES served → entitled at least for historical data.
- **History depth**: `reqHeadTimeStamp` gives RB/HO ≈ 2022-11-30, NG ≈
  2017-03-13 — head timestamps are per-contract; other months may differ. IB
  head-ts is also known to under-report (repo comment: empty head ts ≠ no data).
- **Bar frequencies**: `1 day` and `1 hour` both returned data. Apex still only
  surfaces futures at `1d` — an API-side constraint, not an IB one.
- **Returned values are OHLC closes**, not exchange settlements (lake
  `settlement` = copied close; `open_interest` hardcoded 0 — Task 2 §2).

## 2. BZ staleness — resolved to evidence, not inference

Nightly lane log `logs/daily_update_2026-09-22.log` (excerpt saved):
`qualifyContracts` on `Future(BZ, 202610, exchange='CME')` → **Error 200 "No
security definition"** for all four BZ months; lane then records
`no trade (no bars returned)`. So:

- The lane **does** attempt BZ every night — it is not excluded.
- It builds the contract with `exchange='CME'` — the `ROOT_EXCHANGE_MAP`
  default (BZ has no map entry). The preset's `exchange:"NYMEX"` is **not**
  consulted by the ticker→contract path.
- Probe control: `BZ 202610` on **NYMEX** → 0 details, Error 200; on **CME** →
  0 details, Error 200. So the contract is unresolvable under symbol `BZ` on
  *both* venues today — fixing the exchange default alone would not restore it.
- Expiry vs delisting unresolved: BZ_202610 stored expiry 2026-10-01, but
  BZ_202611/12 (stored expiries Nov/Dec) also went silent at 2026-07-31 — a
  shared July cutoff across distinct months points to a listing/feed change,
  not per-contract expiry.

**Brent listing re-discovery (user steering: "brt ticker is lco at ice")** —
settled this pass, evidence in `ib-brent-*.json` / `ib-coil-probe-*.json`:

- `Future(LCO|B, ICEEU|IPE)` and bare-symbol `Future(LCO|BRN|B)` → all Error
  200. The LCO ticker does not exist in this Gateway's contract DB.
- `reqMatchingSymbols("Brent")` → found `COIL` (IND, USD) with `FUT/FOP/IOPT/
  BAG` derivative types, and a `BZ` index likewise carrying FUT derivatives.
- `Future(COIL, IPE, USD)` → **120 contracts, "ICE Brent Crude"**, NYMEX-parallel
  monthly chain; qualified `COILX6` conId 361002948, realExp 2026-10-01,
  multiplier **1,000** (bbl), USD.
- `reqHeadTimeStamp` COILX6 → **2019-04-17**; 1 M daily → 22 rows
  (2026-08-25→09-23); 2 D 1 h → 25 bars. ICE historical data IS entitled on
  this account — no permission errors.
- So Brent futures **are** obtainable from this IB connection — as `COIL` on
  exchange `IPE` (IB's code for ICE Futures Europe), not `BZ`. The dead `BZ_*`
  lake dirs are a separate legacy series, not the ICE Brent contract.

## 3. Production-ingestion support (code path)

- `daily_update` iterates existing bronze symbol dirs → `_make_contract` →
  `Future(root, expiry, ROOT_EXCHANGE_MAP.get(root, "CME"), USD)`. The preset
  `exchange` field is ignored on this path (BZ log is the proof).
- `ROOT_EXCHANGE_MAP` has `NG:"NYMEX"` but **not** `RB` or `HO` → as-is, the
  lane would query RB/HO at CME and fail exactly like BZ.
- New roots enter the lake only via a `historical` run (preset-seeded dirs);
  `futures-energy.json` declares NG months that are now partly expired
  (202606) — curated current months needed, not the stale file.
- Expired contracts are re-queried nightly forever (14 scanned, 8 `no_trade`)
  — dead-contract requests are a standing waste and produced the misleading
  stale-file pattern. `reqContractDetails` returns real `realExpirationDate`;
  a skip-if-expired gate has the data it needs today.
- Lake `settlement` is copied close and `open_interest` is hardcoded 0 — plan
  crack work on closes; true settlement/OI would need a different request path.

## 4. Per-root verdict

- **NG**: works with the current stack — NYMEX already in `ROOT_EXCHANGE_MAP`;
  IB qualifies (147-month chain) and serves daily+hourly history (head
  2017-03). Missing piece is only a curated contract-month preset entry.
- **RB**: supported by IB but needs a bounded change — `ROOT_EXCHANGE_MAP`
  entry `"RB": "NYMEX"` (or preset-exchange plumbing) + current-month preset
  entries. Depth ~2022-11 for the probed contract.
- **HO**: identical to RB — supported by IB, needs the same two-line class of
  change. Depth ~2022-11.
- **BZ**: dead as listed — `BZ` yields Error 200 on NYMEX, CME, ICEEU and IPE
  alike; do not revive it. **Brent futures instead = `COIL` @ IPE** (ICE Brent
  Crude, verified this pass): needs `"COIL": "IPE"` in `ROOT_EXCHANGE_MAP` plus
  curated preset months — same bounded-change class as RB/HO. EIA `RBRTE`
  Brent spot remains available regardless.

## 5. Minimum follow-up for aligned RB/HO/CL crack legs

1. Add `RB`/`HO` → NYMEX to `ROOT_EXCHANGE_MAP` (or make the lane honor preset
   `exchange`; the map entry is the smaller change).
2. One preset carrying **same delivery month** across CL/RB/HO (e.g. all
   202611) — legs must share month; refresh on a roll schedule since these
   roots are monthly chains (a static preset goes stale — the exact failure
   seen in `futures-active`/`futures-energy`).
3. Seed via bounded `historical` per verified contract; expect ≈3.8 y depth
   for RB/HO, ≈9.5 y for NG (per-contract; verify per month).
4. Retention/roll: keep expired dirs for history but stop re-querying dead
   contracts — use `realExpirationDate` (available via contract details), not
   the derived stored `expiry_date`.
5. Brent leg: use `COIL` @ IPE (verified, head 2019-04-17) — add
   `"COIL": "IPE"` to `ROOT_EXCHANGE_MAP`; drop `BZ` from `futures-active`
   (dead symbol, costs 4 wasted nightly requests). COIL monthly chain means
   the same aligned-month + roll-schedule requirement as RB/HO applies.

## 5a. Extension — common softs/grains/livestock (user request, same mechanism)

Same probe pattern applied to other common commodities (03:09–03:11 UTC;
evidence `ib-softs-probe-2026-09-23.json` + `ib_softs_probe.py`). Result: **12
of 13 roots qualify and serve data**; the discovered IB exchange names differ
from the obvious guesses — softs are `NYBOT` (not `ICEUS`), grains are `CBOT`
(not `ECBOT`). Each row = first unexpired contract by `realExpirationDate`.

| Root | Qualified | IB exch | Instrument | Mult | Head ts | 1 M daily | 2 D 1 h |
|---|---|---|---|---|---|---|---|
| SB sugar | SBV6 exp 2026-09-30 | NYBOT | Sugar No. 11 | 112,000 lb | 2023-11-02 | 21 r | 20 r |
| KC coffee | KCZ6 exp 2026-12-21 | NYBOT | Coffee "C" | 37,500 lb | 2024-01-03 | 21 r | 20 r |
| CC cocoa | CCZ6 exp 2026-12-16 | NYBOT | Cocoa | 10 t | 2025-01-03 | 21 r | 20 r |
| CT cotton | CTV6 exp 2026-10-15 | NYBOT | Cotton No. 2 | 50,000 lb | 2023-11-02 | 20 r | 11 r |
| OJ orange juice | OJX6 exp 2026-11-20 | NYBOT | FC OJ "A" | 15,000 lb | 2023-12-04 | 21 r | 12 r |
| ZS soybeans | ZSX6 exp 2026-11-13 | CBOT | Soybean | 5,000 bu | 2022-11-15 | 21 r | 12 r |
| ZM soymeal | ZMZ6 exp 2026-12-14 | CBOT | Soybean Meal | 100 t | 2022-12-15 | 21 r | 12 r |
| ZL soy oil | ZLZ6 exp 2026-12-14 | CBOT | Soybean Oil | 60,000 lb | 2022-12-15 | 21 r | 12 r |
| ZC corn | ZCZ6 exp 2026-12-14 | CBOT | Corn | 5,000 bu | 2022-12-15 | 21 r | 12 r |
| ZW wheat | ZWZ6 exp 2026-12-14 | CBOT | Wheat | 5,000 bu | 2024-07-15 | 21 r | 12 r |
| LE live cattle | LEV6 exp 2026-10-30 | CME | Live Cattle | 40,000 lb | 2025-05-01 | 21 r | 12 r |
| HE lean hogs | HEV6 exp 2026-10-16 | CME | Lean Hogs | 40,000 lb | 2025-05-15 | 21 r | 12 r |
| FCPO palm oil | — | — | BMD / MYX / bare-symbol all returned 0 details | — | — | — | — |

Read: entitlement exists for all 12 — no permission errors anywhere; head
timestamps are shallower than energy (≈1–4 y, per-contract). Palm oil (Bursa
Malaysia FCPO) is **not available** through this IB connection. Production
support would need the same `ROOT_EXCHANGE_MAP` additions (none of these roots
are mapped) plus curated monthly presets — softs/grains are also monthly-ish
chains, so the roll-schedule point applies identically.

## 6. Evidence

| File | Content |
|---|---|
| `evidence/ib-energy/ib_energy_probe.py` | RB/HO/NG + BZ-control probe script (exact methods/params) |
| `evidence/ib-energy/ib-probe-2026-09-23.json` | full RB/HO/NG/BZ JSON transcript, 23 KB |
| `evidence/ib-energy/ib_lco_probe.py` + `ib_lco_search.py` + `ib_sym_search.py` + `ib_coil_probe.py` | Brent-discovery probe scripts, in execution order |
| `evidence/ib-energy/ib-lco-probe-2026-09-23.json` | LCO/B × ICEEU/IPE → all Error 200 |
| `evidence/ib-energy/ib-brent-fut-search-2026-09-23.json` | bare-symbol LCO/BRN/B Future search → all Error 200 |
| `evidence/ib-energy/ib-brent-matching-symbols-2026-09-23.json` | `reqMatchingSymbols("Brent")` → COIL/BZ index rows |
| `evidence/ib-energy/ib-coil-probe-2026-09-23.json` | COIL@IPE contract chain + qualified contract + head ts + history |
| `evidence/ib-energy/daily-update-bz-excerpt-2026-09-22.txt` | nightly-lane BZ/Error-200 excerpt + SUMMARY_JSON |
| `evidence/ib-energy/ib_softs_probe.py` + `ib-softs-probe-2026-09-23.json` | softs/grains/livestock probe script + full transcript |

(SHA-256 of every file is in the directory; compute with `shasum -a 256 evidence/ib-energy/*`.)

## 7. Deviations

- IB Gateway↔IBKR connectivity degraded mid-probe (2110/2103/2105/2157 →
  restored 1102); probe took ~11 min instead of ~2. All listed results are
  post-restore; no request was retried in a loop.
- BZ control probed on both exchanges (contract-details only) to settle the
  wrong-exchange hypothesis — it falsified it: both venues return Error 200.
- Post-report user steering ("brt ticker is lco at ice") triggered a bounded
  second pass: LCO/B/BRN probes all 200, `reqMatchingSymbols` located `COIL`,
  and `COIL@IPE` verified with live history — the Brent answer changed from
  "blocked" to "works, different symbol/venue than BZ".
- User request added a third pass covering softs/grains/livestock (§5a):
  discovered real IB venues are NYBOT (softs) / CBOT (grains) / CME
  (livestock) — not ICEUS/ECBOT; palm oil FCPO not resolvable on this account.
- No orders, subscriptions, streaming, or writes of any kind; clientId 7
  disconnected cleanly in `finally`.
- cmegroup.com returned 403 through mini egress (Task 2 already noted); CME
  crack spec cited per lead-supplied URLs.
