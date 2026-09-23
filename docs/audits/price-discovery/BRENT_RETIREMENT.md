# BZ Retirement, COIL Onboarding, and Commodity Backfill

As of 2026-09-23. User decision: BZ is retired from active ingestion; COIL is
the separate IPE series. BZ history remains in its original symbol directory.
No renaming, deletion, or BZ-to-COIL stitching was performed.

## Configuration

- Shared futures contract construction rejects BZ, and automatic daily futures
  discovery filters pre-existing BZ directories while leaving their stored bars
  readable.
- `futures-active.json` contains COIL on IPE and no BZ contracts. The energy
  strip contains CL, NG, COIL, RB, and HO through 2027-12. Metals and agriculture
  presets contain their first two live delivery months.
- ROOT_EXCHANGE_MAP now covers the selected energy, metal, and agriculture roots.
- SI contract construction explicitly selects `tradingClass=SI`, multiplier
  5,000 to distinguish the standard silver future from the IB `SIL` mini chain.
- `close` remains the settlement placeholder and open interest remains zero;
  neither is represented as an observed settlement or OI field.

## IB Contract Inventory and Approved Manifest

The Mini probe used TWS API loopback at 127.0.0.1:4001 on 2026-09-23. Delivery
months were read from IB `ContractDetails.contractMonth`; contracts were
qualified and daily TRADES head timestamps recorded. The raw 20-root inventory
is in [COMMODITY_CONTRACT_INVENTORY_2026-09-23.json](COMMODITY_CONTRACT_INVENTORY_2026-09-23.json),
with the probe script and stderr transcript under `evidence/ib-energy/`.

The reviewed [backfill manifest](COMMODITY_BACKFILL_MANIFEST_2026-09-23.json)
contains 103 distinct, IB-qualified contracts:

- CL 2026-11 through 2027-12 (14); NG, RB, and HO 2026-10 through 2027-12
  (15 each); COIL 2026-11 through 2027-12 (14).
- GC, SI, and HG: 2026-09 and 2026-10 (two each).
- Agriculture (two each): SB 2026-10/2027-03; KC and CC 2026-12/2027-03;
  CT 2026-10/2026-12; OJ 2026-11/2027-01; ZS 2026-11/2027-01; ZM and ZL
  2026-10/2026-12; ZC and ZW 2026-12/2027-03; LE and HE 2026-10/2026-12.

CL_202610 and OJ_202609 were present in the returned contract chains, but their
IB last-trade dates (2026-09-22 and 2026-09-10) had passed by the as-of date;
they are excluded from the live manifest. OJ_202701 was separately qualified
after that filter and is the second live OJ month. COIL_202704 was separately
requalified after a transient TWS connection loss during the full inventory.
The targeted IB responses are preserved in
`COIL_202704_REQUALIFICATION.json` and `OJ_202701_REQUALIFICATION.json`.
BZ is excluded by user decision; FCPO is excluded because its saved probe did
not qualify.

## Historical Backfill

The authorized full-history Bronze backfill ran on the Mini against only the
reviewed manifest, using a task-specific cursor/log directory. The initial
SI_202609 and SI_202610 requests were ambiguous between standard SI and mini
SIL. Adding `tradingClass=SI,multiplier=5000` allowed both requests to qualify
and the retry to write 474 and 404 rows. SSH became temporarily unreachable
during the run; the independent worker continued. The worker also removed
those two task-created cursor entries before retrying, contrary to the
instruction not to reset cursors; other entries were untouched, and successful
retries restored the SI completion entries.

Final Bronze verification found 103/103 expected files readable with 131,436
total rows, spanning trade dates 2017-03-14 through 2026-09-23. The three files
present before the run (`CL_202611`, `CL_202612`, `GC_202610`) returned
`ok-noop` and already matched IB heads. Exact per-contract outcomes are in the
[Task 2 report](TASK2_COMMODITY_BACKFILL_2026-09-23.md) and
[verification JSON](COMMODITY_BACKFILL_VERIFICATION_2026-09-23.json). No code
was committed, deployed, or released.

## Verification

`uv run pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing`:
3,061 passed, 96.02% coverage. Ruff check, format, and `git diff --check`
passed. Production runtime remains on its existing revision; local changes
have not been deployed, so the active nightly scheduler has not yet retired BZ
or adopted the new COIL configuration.
