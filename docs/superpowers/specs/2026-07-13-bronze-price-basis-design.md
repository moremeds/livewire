# Bronze Price-Basis Normalization and Repair Design

## Problem

Livewire's canonical equity Bronze files do not record row provenance or price
basis. Equity IB requests use `whatToShow="TRADES"`; IB documents those bars as
split-adjusted but not dividend-adjusted. Massive daily requests explicitly use
`adjusted=false` and therefore return raw corporate-action discontinuities.

Historical merges and repairs have created mixed basis inside individual
symbols. Local evidence includes:

- AAPL 1987, 2000, and 2005 split boundaries that appear raw, followed by 2014
  and 2020 boundaries that appear split-adjusted;
- NVDA 2006 and 2007 boundaries that appear split-adjusted, followed by 2021
  and 2024 boundaries that appear raw;
- MSFT 2003 data that appears raw.

The Silver adjustment engine currently assumes every Bronze row is raw. It can
therefore double-adjust already split-adjusted history. This is independent of
the future-action cutoff fixed in PR #50.

## Architecture

Bronze remains the canonical raw layer. Every new equity daily row will carry
explicit provenance and price-basis metadata. IB rows will be normalized to raw
before publication; Massive `adjusted=false` rows are raw by contract. Existing
ambiguous history will be migrated to an unknown state, audited against split
events and authoritative raw data, then repaired through a stale-safe manifest.

Silver will consume the row basis rather than assume it. A row with an
applicable split and unknown basis blocks publication. The canary will validate
basis-aware factor semantics and reject remaining mechanical split jumps.

This work lives on `feat/bronze-price-basis`, stacked from PR #50's verified
head. PR #50 remains unchanged. After PR #50 merges, this branch will be rebased
or retargeted to `main` before its own pull request is opened.

## Bronze Schema Contract

Equity daily Parquet gains two non-null string columns:

- `source`: `ib`, `massive`, `nasdaq`, `stooq`, or `legacy`;
- `price_basis`: `raw`, `split_adjusted`, or `unknown`.

The change applies only to `asset_class=equity` daily files. Volatility,
commodity, FX, futures, rates, and intraday schemas remain unchanged.

New rows must satisfy:

- Massive REST/grouped/flat-file daily data requested with `adjusted=false` is
  published as `source=massive`, `price_basis=raw`;
- IB `TRADES` data enters normalization as split-adjusted and may be published
  as `source=ib`, `price_basis=raw` only after successful normalization;
- Nasdaq and Stooq fallback rows are labelled with their provider, and their
  basis must be verified by provider calibration before being declared raw;
- pre-migration rows are `source=legacy`, `price_basis=unknown` until audited or
  repaired.

Bronze merge operations replace metadata and OHLCV as one row-level unit. They
must never retain provenance from the old row while replacing its prices.

## Provider Calibration Gate

Implementation cannot assume IB's historical volume convention. Before volume
normalization is coded, a calibration command will compare IB `TRADES`, Massive
`adjusted=false`, and Massive `adjusted=true` for known 2:1, 4:1, and 10:1 split
windows such as MSFT 2003, AAPL 2020, and NVDA 2024.

The calibration records OHLC and volume on both sides of each event and derives
the exact reversible transformation. If credentials or IB connectivity are
unavailable, implementation may proceed for schema and price-only logic, but
the IB publisher must fail closed rather than publish unverified volume as raw.

## IB Raw Normalization

For one IB batch, the normalizer receives the rows, active effective split
events, and one New York as-of date. For each row it computes the cumulative
split factor from events strictly after the row date through the as-of date.
It reverses IB's split price adjustment using Decimal arithmetic to restore raw
historical OHLC. Volume is transformed according to the verified calibration
contract.

The normalizer rejects:

- missing or malformed split ratios;
- unknown provider basis;
- required split history that is unavailable for the covered period;
- non-finite or non-positive normalized prices;
- a volume convention that has not passed calibration.

Normalization is deterministic and idempotence is enforced at the boundary:
only rows explicitly marked `split_adjusted` may enter it, and its output is
always `raw`. A raw row cannot be normalized twice.

## Legacy Schema Migration

Migration is atomic per Parquet file and resumable across symbols. It adds
`source=legacy` and `price_basis=unknown` without changing OHLCV. Each file is
validated before `os.replace`, and a failure leaves the original bytes intact.

The migration supports dry-run, explicit tickers, full discovery, progress
counters, and a cursor. It emits source and target SHA-256 values so operators
can prove that only the schema and default metadata changed.

## Split-Boundary Audit

The audit is read-only. For every active effective split and every covered
equity symbol, it finds the nearest Bronze sessions before and on/after the
event and records:

- symbol and corporate-action identity;
- split ratio and ex-date;
- adjacent trading dates and observed OHLCV ratio;
- current `source` and `price_basis` values;
- raw-versus-adjusted inference and confidence;
- authoritative Massive `adjusted=false` replacement values;
- original row values and source Parquet SHA-256;
- proposed replacement values and metadata.

Ratio inference is diagnostic only. It may identify likely mixed segments but
cannot authorize a repair by itself. An approved replacement must come from an
authoritative unadjusted provider response.

The audit manifest is deterministic, versioned, and contains enough original
data to perform rollback. Audit execution never writes Bronze or Silver.

## Repair Workflow

Repair accepts only an explicit audit manifest. Before any write it verifies:

- manifest schema and status;
- current Parquet SHA-256 equals the audited source hash;
- every replacement came from `adjusted=false` data;
- symbol/date uniqueness;
- OHLC validity and non-negative volume;
- sufficient free space for atomic replacement and rollback artifacts.

Stale manifests fail without mutation. Repair locks one symbol, rechecks its
hash under the lock, applies only approved rows, validates the complete Parquet
file, and atomically replaces it. Repaired rows become
`source=massive`, `price_basis=raw`. The manifest preserves the original values
for exact rollback.

Repair is rehearsed in a disposable lake first. Production execution requires
a separate explicit user go-ahead after manifest review. It runs in small
batches with a full re-audit between batches.

## Silver Basis-Aware Semantics

Silver factor construction becomes row-aware:

- `raw`: apply effective split price and volume factors normally;
- `split_adjusted`: do not apply a second split factor;
- `unknown` with any applicable effective split: fail the symbol and block the
  batch revision;
- `unknown` with no applicable split: identity split contribution is safe;
- dividend adjustment remains applicable because IB `TRADES` is not
  dividend-adjusted.

Factor intervals may split at basis transitions in addition to corporate-action
dates. Daily adjusted rows retain their existing Silver schema and transactional
revision protocol.

## Canary Invariants

The Silver canary will additionally require:

- every split-affected Bronze row has a known basis;
- persisted factors agree with row-basis-aware expected factors;
- no split is applied twice;
- adjusted returns around split boundaries do not retain a mechanical ratio
  corresponding to 2:1, 3:2, 4:1, 7:1, 10:1, or reverse splits;
- Bronze hashes remain unchanged during validation.

The mechanical-jump check uses the declared corporate-action ratio and a
documented market-move tolerance. It is a validation alarm, not an inference
engine or repair authorization.

## Verification

### Provider calibration

- Compare IB, Massive raw, and Massive adjusted OHLCV around representative
  2:1, 4:1, and 10:1 splits.
- Prove the transformation reproduces Massive raw values within tolerance.
- Block IB publication if volume semantics remain unverified.

### Unit and schema tests

- Cover 2:1, 3:2, 4:1, 7:1, 10:1, cumulative, and reverse splits;
- cover same-day split/dividend, future/cancelled actions, calendar gaps,
  missing history, rounding, and idempotence;
- round-trip `source` and `price_basis` through Parquet;
- prove merges replace metadata with OHLCV;
- prove failed migrations preserve original bytes;
- prove non-equity schemas remain unchanged.

### Full read-only audit

- Classify every locally covered split boundary;
- require known AAPL/NVDA/MSFT examples to match observed basis;
- verify audit produces no writes and Bronze hashes remain unchanged.

### Disposable repair rehearsal

- Apply only approved manifest rows;
- re-audit to zero covered ambiguities;
- rollback to exact original SHA-256;
- reapply deterministically.

### Silver and Apex

- Require continuous adjusted series around AAPL 2020, NVDA 2024, and MSFT
  2003 without artificial 4x, 10x, or 2x moves;
- verify raw, split-adjusted, and unknown basis paths;
- verify dividend factors, no-op second rebuild, correction revision, and
  atomic manifest behavior;
- run the AAPL/MSFT/NVDA/SPY/PLTR canary;
- start local Apex and compare daily and intraday reads across split windows.

### Repository gates

Run lockfile, Ruff lint/format, Pyright, and the CI-equivalent pytest command
with RuntimeWarning enforcement and at least 95 percent configured coverage.

## Rollout and Safety

No production Bronze repair or Silver advancement occurs as part of ordinary
implementation verification. The PR delivers code, tests, audit artifacts from
read-only/disposable runs, and an operator-reviewed manifest. Production repair
requires a later explicit go-ahead, current-hash verification, available-space
verification, small batches, rollback readiness, and a clean post-batch audit.
