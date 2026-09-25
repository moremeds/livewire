# COMMODITY_CONTRACT_INVENTORY_2026-09-23.md — Task 1 read-only contract inventory

herd-report price-discovery-runner task 1: commit none, evidence docs/audits/price-discovery/COMMODITY_CONTRACT_INVENTORY_2026-09-23.md, deviations: first probe pass hit IB pacing 162 (reqHeadTimeStamp is paced as historical data), rerun at 12 s head-ts spacing completed clean; COIL_202704 qualified via supplemental bounded retry after transient 1100/1102 farm dropout; SI chains carry dual SI/SIL tradingClass per month, SI selected

Agent: Devin SWE-2 Max, role `price-discovery-runner`, pane w2:p1.
Host: Mac mini (local). cwd `/Users/moremeds/projects/livewire/.worktrees/price-discovery-runner`,
HEAD `f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2` (== deployed release sha).
Connection: IB Gateway `127.0.0.1:4001` via `clients.ib_client.IBClient`,
clientId base 7 (unused in `CLIENT_IDS`), ib_async 2.1.0, release venv
interpreter `~/market-warehouse/releases/f83e3df0…/.venv/bin/python` with the
worktree on `sys.path`. Probe window **2026-09-23 04:05:14 → 04:31:11 UTC**;
supplemental retry 04:38:23 → 04:38:28 UTC.

## Boundaries kept

Read-only `reqContractDetails` / `qualifyContracts` / `reqHeadTimeStamp` only.
**No `reqHistoricalData` / no price bars anywhere** — artifacts contain contract
metadata and timestamps only. No orders, subscriptions, Gateway restart, lake /
Bronze writes, ingestion, release/.env/credential edits, commit/push/PR/deploy.
Code and config untouched (probe scripts + outputs are evidence artifacts under
`docs/audits/price-discovery/`).

## Selection rule (applied exactly)

Delivery month is taken **only** from `ContractDetails.contractMonth`
(verified `^\d{6}$`). `lastTradeDateOrContractMonth` / `realExpirationDate` are
recorded raw and never used to infer the month — e.g. COIL_202611 has
`lastTradeDateOrContractMonth="20260930 19:30:00 GB"`. Selected = verified
`contractMonth` ∧ in scope ∧ qualified with a real conId and no error < 2100.
No root needed the UNKNOWN stop: every returned chain carried parseable
`contractMonth`. Raw IB detail objects are preserved per contract under
`roots.<ROOT>.all_contracts[].raw` in the JSON.

## Commands (rerunnable)

```bash
cd /Users/moremeds/projects/livewire/.worktrees/price-discovery-runner
# main inventory (writes raw JSON transcript)
/Users/moremeds/market-warehouse/releases/f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2/.venv/bin/python \
  docs/audits/price-discovery/evidence/ib-energy/ib_contract_inventory_probe.py \
  > docs/audits/price-discovery/COMMODITY_CONTRACT_INVENTORY_2026-09-23.json   # exit 0
# supplemental single-contract retry for COIL_202704
/Users/moremeds/market-warehouse/releases/f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2/.venv/bin/python \
  docs/audits/price-discovery/evidence/ib-energy/ib_coil_202704_retry.py \
  > docs/audits/price-discovery/evidence/ib-energy/ib-coil-202704-retry-2026-09-23.json   # exit 0
```

Pacing config inside the probe: serial calls, 2 s step spacing, **12 s between
`reqHeadTimeStamp` calls**, one 65 s cooldown-retry per 162 (cap 5 → abort).
Run 2 used 0 cooldowns. An earlier pass at 2 s spacing hit error 162 at ~61
head-ts requests in <5 min (≈60/10 min historical pacing) and was discarded;
the JSON on disk is the complete second pass plus the noted retry.

## Observed counts per root

| root | exch used | chain size | selected | months | first→last sel |
|---|---|---|---|---|---|
| CL | NYMEX | 125 | 15 | monthly | 202610→202712 |
| NG | NYMEX | 147 | 15 | monthly | 202610→202712 |
| COIL | IPE | 120 | 14* | monthly | 202611→202712 |
| RB | NYMEX | 40 | 15 | monthly | 202610→202712 |
| HO | NYMEX | 40 | 15 | monthly | 202610→202712 |
| GC | COMEX | 34 | 2 | — | 202609, 202610 |
| SI | COMEX | 40 | 2 | — | 202609, 202610 |
| HG | COMEX | 47 | 2 | — | 202609, 202610 |
| SB | NYBOT | 12 | 2 | — | 202610, 202703 |
| KC | NYBOT | 7 | 2 | — | 202612, 202703 |
| CC | NYBOT | 9 | 2 | — | 202612, 202703 |
| CT | NYBOT | 15 | 2 | — | 202610, 202612 |
| OJ | NYBOT | 18 | 2 | — | 202609, 202611 |
| ZS | CBOT | 17 | 2 | — | 202611, 202701 |
| ZM | CBOT | 21 | 2 | — | 202610, 202612 |
| ZL | CBOT | 21 | 2 | — | 202610, 202612 |
| ZC | CBOT | 13 | 2 | — | 202612, 202703 |
| ZW | CBOT | 14 | 2 | — | 202612, 202703 |
| LE | CME | 10 | 2 | — | 202610, 202612 |
| HE | CME | 11 | 2 | — | 202610, 202612 |

\* 13 in-run + COIL_202704 via supplemental retry. All roots resolved on the
primary exchange; the bare-symbol fallback was never needed. **Total selected:
104 tickers.** Error codes seen: only farm-status info (1100×4, 1102, 2103×2,
2104×4, 2119×2) during one transient Gateway↔IBKR blip; no 200/354/162/10167
in the final run.

## Selected manifest (ROOT_YYYYMM — contractMonth-verified)

| ticker | conId | localSymbol | contractMonth | lastTradeDateOrContractMonth | realExpirationDate | exch | tc | head_ts UTC |
|---|---|---|---|---|---|---|---|---|---|
| CL_202610 | 304037496 | CLV6 | 202610 | 20260922 | 20260922 | NYMEX | CL | 2018-01-24 14:30:00+00:00 |
| CL_202611 | 304037511 | CLX6 | 202611 | 20261020 | 20261020 | NYMEX | CL | 2018-01-24 14:30:00+00:00 |
| CL_202612 | 296574787 | CLZ6 | 202612 | 20261120 | 20261120 | NYMEX | CL | 2017-11-22 14:30:00+00:00 |
| CL_202701 | 304037396 | CLF7 | 202701 | 20261221 | 20261221 | NYMEX | CL | 2018-01-24 14:30:00+00:00 |
| CL_202702 | 304037415 | CLG7 | 202702 | 20270120 | 20270120 | NYMEX | CL | 2018-01-24 14:30:00+00:00 |
| CL_202703 | 342054394 | CLH7 | 202703 | 20270222 | 20270222 | NYMEX | CL | 2018-11-21 14:30:00+00:00 |
| CL_202704 | 342054399 | CLJ7 | 202704 | 20270322 | 20270322 | NYMEX | CL | 2018-11-21 14:30:00+00:00 |
| CL_202705 | 342054403 | CLK7 | 202705 | 20270420 | 20270420 | NYMEX | CL | 2018-11-21 14:30:00+00:00 |
| CL_202706 | 342054406 | CLM7 | 202706 | 20270520 | 20270520 | NYMEX | CL | 2018-11-21 14:30:00+00:00 |
| CL_202707 | 342054409 | CLN7 | 202707 | 20270622 | 20270622 | NYMEX | CL | 2018-11-21 14:30:00+00:00 |
| CL_202708 | 342054412 | CLQ7 | 202708 | 20270720 | 20270720 | NYMEX | CL | 2018-11-21 14:30:00+00:00 |
| CL_202709 | 342054418 | CLU7 | 202709 | 20270820 | 20270820 | NYMEX | CL | 2018-11-21 14:30:00+00:00 |
| CL_202710 | 342054423 | CLV7 | 202710 | 20270921 | 20270921 | NYMEX | CL | 2018-11-21 14:30:00+00:00 |
| CL_202711 | 342054424 | CLX7 | 202711 | 20271020 | 20271020 | NYMEX | CL | 2018-11-21 14:30:00+00:00 |
| CL_202712 | 342054429 | CLZ7 | 202712 | 20271119 | 20271119 | NYMEX | CL | 2018-11-21 14:30:00+00:00 |
| NG_202610 | 269460151 | NGV26 | 202610 | 20260928 | 20260928 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202611 | 269460170 | NGX26 | 202611 | 20261028 | 20261028 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202612 | 269460190 | NGZ26 | 202612 | 20261125 | 20261125 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202701 | 269459992 | NGF27 | 202701 | 20261229 | 20261229 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202702 | 269460011 | NGG27 | 202702 | 20270127 | 20270127 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202703 | 269460026 | NGH27 | 202703 | 20270224 | 20270224 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202704 | 269460040 | NGJ27 | 202704 | 20270329 | 20270329 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202705 | 269460057 | NGK27 | 202705 | 20270428 | 20270428 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202706 | 269460074 | NGM27 | 202706 | 20270526 | 20270526 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202707 | 269460094 | NGN27 | 202707 | 20270628 | 20270628 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202708 | 269460112 | NGQ27 | 202708 | 20270728 | 20270728 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202709 | 269460134 | NGU27 | 202709 | 20270827 | 20270827 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202710 | 269460152 | NGV27 | 202710 | 20270928 | 20270928 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202711 | 269460175 | NGX27 | 202711 | 20271027 | 20271027 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| NG_202712 | 269460194 | NGZ27 | 202712 | 20271126 | 20271126 | NYMEX | NG | 2017-03-14 13:30:00+00:00 |
| COIL_202611 | 361002948 | COILX6 | 202611 | 20260930 19:30:00 GB | 20261001 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202612 | 361002955 | COILZ6 | 202612 | 20261030 19:30:00 GB | 20261102 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202701 | 361002958 | COILF7 | 202701 | 20261130 19:30:00 GB | 20261201 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202702 | 361002960 | COILG7 | 202702 | 20261230 19:30:00 GB | 20261231 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202703 | 361002970 | COILH7 | 202703 | 20270129 19:30:00 GB | 20270201 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202704 | 361002975 | COILJ7 | 202704 | 20270226 19:30:00 GB | 20270301 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202705 | 361002979 | COILK7 | 202705 | 20270331 19:30:00 GB | 20270401 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202706 | 361002982 | COILM7 | 202706 | 20270430 19:30:00 GB | 20270503 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202707 | 361002988 | COILN7 | 202707 | 20270528 19:30:00 GB | 20270531 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202708 | 361002994 | COILQ7 | 202708 | 20270630 19:30:00 GB | 20270701 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202709 | 361002999 | COILU7 | 202709 | 20270730 19:30:00 GB | 20270802 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202710 | 361003000 | COILV7 | 202710 | 20270831 19:30:00 GB | 20270901 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202711 | 361003005 | COILX7 | 202711 | 20270930 19:30:00 GB | 20271001 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| COIL_202712 | 361003010 | COILZ7 | 202712 | 20271029 19:30:00 GB | 20271101 | IPE | COIL | 2019-04-17 00:00:00+00:00 |
| RB_202610 | 600696556 | RBV6 | 202610 | 20260930 | 20260930 | NYMEX | RB | 2022-12-01 14:30:00+00:00 |
| RB_202611 | 600696516 | RBX6 | 202611 | 20261030 | 20261030 | NYMEX | RB | 2022-12-01 14:30:00+00:00 |
| RB_202612 | 600696562 | RBZ6 | 202612 | 20261130 | 20261130 | NYMEX | RB | 2022-12-01 14:30:00+00:00 |
| RB_202701 | 600696528 | RBF7 | 202701 | 20261231 | 20261231 | NYMEX | RB | 2022-12-01 14:30:00+00:00 |
| RB_202702 | 668806868 | RBG7 | 202702 | 20270129 | 20270129 | NYMEX | RB | 2023-12-01 14:30:00+00:00 |
| RB_202703 | 668806878 | RBH7 | 202703 | 20270226 | 20270226 | NYMEX | RB | 2023-12-01 14:30:00+00:00 |
| RB_202704 | 668806899 | RBJ7 | 202704 | 20270331 | 20270331 | NYMEX | RB | 2023-12-01 14:30:00+00:00 |
| RB_202705 | 668806853 | RBK7 | 202705 | 20270430 | 20270430 | NYMEX | RB | 2023-12-01 14:30:00+00:00 |
| RB_202706 | 668806882 | RBM7 | 202706 | 20270528 | 20270528 | NYMEX | RB | 2023-12-01 14:30:00+00:00 |
| RB_202707 | 668806888 | RBN7 | 202707 | 20270630 | 20270630 | NYMEX | RB | 2023-12-01 14:30:00+00:00 |
| RB_202708 | 668806865 | RBQ7 | 202708 | 20270730 | 20270730 | NYMEX | RB | 2023-12-01 14:30:00+00:00 |
| RB_202709 | 668806858 | RBU7 | 202709 | 20270831 | 20270831 | NYMEX | RB | 2023-12-01 14:30:00+00:00 |
| RB_202710 | 668806875 | RBV7 | 202710 | 20270930 | 20270930 | NYMEX | RB | 2023-12-01 14:30:00+00:00 |
| RB_202711 | 668806887 | RBX7 | 202711 | 20271029 | 20271029 | NYMEX | RB | 2023-12-01 14:30:00+00:00 |
| RB_202712 | 668806893 | RBZ7 | 202712 | 20271130 | 20271130 | NYMEX | RB | 2023-12-01 14:30:00+00:00 |
| HO_202610 | 600696488 | HOV6 | 202610 | 20260930 | 20260930 | NYMEX | HO | 2022-12-01 14:30:00+00:00 |
| HO_202611 | 600696478 | HOX6 | 202611 | 20261030 | 20261030 | NYMEX | HO | 2022-11-30 23:00:00+00:00 |
| HO_202612 | 600696468 | HOZ6 | 202612 | 20261130 | 20261130 | NYMEX | HO | 2022-11-30 23:00:00+00:00 |
| HO_202701 | 600696482 | HOF7 | 202701 | 20261231 | 20261231 | NYMEX | HO | 2022-11-30 23:00:00+00:00 |
| HO_202702 | 668806810 | HOG7 | 202702 | 20270129 | 20270129 | NYMEX | HO | 2023-11-30 23:00:00+00:00 |
| HO_202703 | 668806834 | HOH7 | 202703 | 20270226 | 20270226 | NYMEX | HO | 2023-11-30 23:00:00+00:00 |
| HO_202704 | 668806815 | HOJ7 | 202704 | 20270331 | 20270331 | NYMEX | HO | 2023-11-30 23:00:00+00:00 |
| HO_202705 | 668806848 | HOK7 | 202705 | 20270430 | 20270430 | NYMEX | HO | 2023-11-30 23:00:00+00:00 |
| HO_202706 | 668806839 | HOM7 | 202706 | 20270528 | 20270528 | NYMEX | HO | 2023-11-30 23:00:00+00:00 |
| HO_202707 | 668806805 | HON7 | 202707 | 20270630 | 20270630 | NYMEX | HO | 2023-11-30 23:00:00+00:00 |
| HO_202708 | 668806819 | HOQ7 | 202708 | 20270730 | 20270730 | NYMEX | HO | 2023-11-30 23:00:00+00:00 |
| HO_202709 | 668806845 | HOU7 | 202709 | 20270831 | 20270831 | NYMEX | HO | 2023-11-30 23:00:00+00:00 |
| HO_202710 | 668806840 | HOV7 | 202710 | 20270930 | 20270930 | NYMEX | HO | 2023-11-30 23:00:00+00:00 |
| HO_202711 | 668806825 | HOX7 | 202711 | 20271029 | 20271029 | NYMEX | HO | 2023-11-30 23:00:00+00:00 |
| HO_202712 | 668806828 | HOZ7 | 202712 | 20271130 | 20271130 | NYMEX | HO | 2023-11-30 23:00:00+00:00 |
| GC_202609 | 760200565 | GCU6 | 202609 | 20260928 | 20260928 | COMEX | GC | 2025-02-09 23:00:00+00:00 |
| GC_202610 | 744880148 | GCV6 | 202610 | 20261028 | 20261028 | COMEX | GC | 2024-11-27 23:00:00+00:00 |
| SI_202609 | 738529133 | SIU6 | 202609 | 20260928 | 20260928 | COMEX | SI | 2024-10-30 22:00:00+00:00 |
| SI_202610 | 760200581 | SIV6 | 202610 | 20261028 | 20261028 | COMEX | SI | 2025-02-09 23:00:00+00:00 |
| HG_202609 | 499901736 | HGU6 | 202609 | 20260928 | 20260928 | COMEX | HG | 2021-06-29 22:00:00+00:00 |
| HG_202610 | 738529103 | HGV6 | 202610 | 20261028 | 20261028 | COMEX | HG | 2024-10-30 22:00:00+00:00 |
| SB_202610 | 663548820 | SBV6 | 202610 | 20260930 | 20260930 | NYBOT | SB | 2023-11-02 07:30:00+00:00 |
| SB_202703 | 694583788 | SBH7 | 202703 | 20270226 | 20270226 | NYBOT | SB | 2024-04-02 07:30:00+00:00 |
| KC_202612 | 675565688 | KCZ6 | 202612 | 20261218 | 20261221 | NYBOT | KC | 2024-01-03 09:15:00+00:00 |
| KC_202703 | 694583782 | KCH7 | 202703 | 20270318 | 20270319 | NYBOT | KC | 2024-04-02 08:15:00+00:00 |
| CC_202612 | 752092906 | CCZ6 | 202612 | 20261215 | 20261216 | NYBOT | CC | 2025-01-03 09:45:00+00:00 |
| CC_202703 | 773019144 | CCH7 | 202703 | 20270315 | 20270316 | NYBOT | CC | 2025-04-02 08:45:00+00:00 |
| CT_202610 | 663548817 | CTV6 | 202610 | 20261008 | 20261015 | NYBOT | CT | 2023-11-02 01:00:00+00:00 |
| CT_202612 | 675565687 | CTZ6 | 202612 | 20261208 | 20261215 | NYBOT | CT | 2024-01-03 02:00:00+00:00 |
| OJ_202609 | 657732422 | OJU6 | 202609 | 20260910 | 20260923 | NYBOT | OJ | 2023-10-03 12:00:00+00:00 |
| OJ_202611 | 669231441 | OJX6 | 202611 | 20261109 | 20261120 | NYBOT | OJ | 2023-12-04 13:00:00+00:00 |
| ZS_202611 | 597391703 | ZSX6 | 202611 | 20261113 | 20261113 | CBOT | ZS | 2022-11-15 01:00:00+00:00 |
| ZS_202701 | 741752289 | ZSF7 | 202701 | 20270114 | 20270114 | CBOT | ZS | 2024-11-15 01:00:00+00:00 |
| ZM_202610 | 602619866 | ZMV6 | 202610 | 20261014 | 20261014 | CBOT | ZM | 2022-12-15 01:00:00+00:00 |
| ZM_202612 | 602619821 | ZMZ6 | 202612 | 20261214 | 20261214 | CBOT | ZM | 2022-12-15 01:00:00+00:00 |
| ZL_202610 | 602619551 | ZLV6 | 202610 | 20261014 | 20261014 | CBOT | ZL | 2022-12-15 01:00:00+00:00 |
| ZL_202612 | 602619503 | ZLZ6 | 202612 | 20261214 | 20261214 | CBOT | ZL | 2022-12-15 01:00:00+00:00 |
| ZC_202612 | 602619735 | ZCZ6 | 202612 | 20261214 | 20261214 | CBOT | ZC | 2022-12-15 01:00:00+00:00 |
| ZC_202703 | 748101140 | ZCH7 | 202703 | 20270312 | 20270312 | CBOT | ZC | 2024-12-16 01:00:00+00:00 |
| ZW_202612 | 715358279 | ZWZ6 | 202612 | 20261214 | 20261214 | CBOT | ZW | 2024-07-15 00:00:00+00:00 |
| ZW_202703 | 715358285 | ZWH7 | 202703 | 20270312 | 20270312 | CBOT | ZW | 2024-07-15 00:00:00+00:00 |
| LE_202610 | 781181245 | LEV6 | 202610 | 20261030 | 20261030 | CME | LE | 2025-05-01 13:30:00+00:00 |
| LE_202612 | 790105750 | LEZ6 | 202612 | 20261231 | 20261231 | CME | LE | 2025-06-09 13:30:00+00:00 |
| HE_202610 | 784098146 | HEV6 | 202610 | 20261014 | 20261016 | CME | HE | 2025-05-15 13:30:00+00:00 |
| HE_202612 | 791718404 | HEZ6 | 202612 | 20261214 | 20261216 | CME | HE | 2025-06-16 13:30:00+00:00 |

## Notes for the lead

- **COIL_202704** (`conId 361002975`, `COILJ7`, realExp 2027-03-01): in-run
  qualify coincided with the 1100/1102 dropout; re-qualified + head ts
  (2019-04-17) in `evidence/ib-energy/ib-coil-202704-retry-2026-09-23.json`.
  Included in the manifest above.
- **SI dual listing**: each SI month returns both `SI` (5,000 oz) and `SIL`
  (mini) trading classes under symbol SI — 8 duplicate `contractMonth` pairs in
  raw. Selected contracts are `tradingClass=SI` (SIU6/SIV6).
- **Boundary months**: GC/SI/HG `202609` realExp 2026-09-28 — still listed,
  "currently available" stands. `OJ_202609` realExp 2026-09-23 (today, last
  trade 2026-09-10) — still listed today; flagged for lead awareness. COIL's
  first available month is 202611 (Oct-delivery already expired ~Aug 31; Brent
  expiry precedes delivery month by ~2 months).
- **Head timestamps** are per-contract `reqHeadTimeStamp` (TRADES, RTH) values,
  recorded per selected contract; ranges observed: NG/COIL uniform
  (2017-03-14 / 2019-04-17), CL 2017-11→2018-11, RB/HO 2022-11→2023-12,
  metals 2021-06→2025-02, ags 2022-11→2025-06. IB head-ts is known to
  under-report; treat as lower bound for the later full-history step.
- Complete IB head history per ROOT_YYYYMM is a **later, separately authorized
  step** — not started.
- FCPO excluded (prior probe: no contract details anywhere), BZ excluded
  (retired; Error 200 on NYMEX/CME/ICEEU/IPE).

## Evidence

| file | content |
|---|---|
| `COMMODITY_CONTRACT_INVENTORY_2026-09-23.json` | immutable raw transcript: per-root attempts, all returned contracts with full `ContractDetails` (`raw`), selected records, exclusions, errors, pacing config |
| `evidence/ib-energy/ib_contract_inventory_probe.py` | main probe script (exact methods/pacing) |
| `evidence/ib-energy/ib_coil_202704_retry.py` + `ib-coil-202704-retry-2026-09-23.json` | supplemental retry script + transcript |
| `evidence/ib-energy/*` (prior) | source evidence this task consumed: energy/softs/COIL probes establishing exchanges NYMEX/IPE/COMEX/NYBOT/CBOT/CME |
