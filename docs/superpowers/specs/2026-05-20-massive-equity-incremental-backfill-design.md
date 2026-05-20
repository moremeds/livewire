# Massive Equity Incremental Backfill Design

## Goal

After the equity daily warehouse has bronze Parquet snapshots, incremental daily backfill and gap recovery should prefer Massive because it is faster for U.S. equity bars than IB's historical daily path.

## Scope

- Daily equity historical backfill gets a provider selector: `auto`, `ib`, or `massive`.
- `auto` keeps deep older-history `historical --backfill` on IB because live Massive validation showed long daily aggregate requests can return partial ranges.
- Recent equity target-date recovery uses Massive through `daily --source massive --target-date <date> --tickers ...`.
- Non-equity historical fetch/backfill remains IB-backed through the existing contracts.
- Daily coverage recovery uses the Massive daily command for `1d` equity gaps.
- `backfill-all` Phase 2 keeps using `historical --backfill --source auto`, which resolves to IB for deep older-history backfill.

## Data Flow

Massive daily bars are normalized through the existing `MassiveClient`, converted into the same bronze daily row schema as IB bars, and merged with `BronzeClient.merge_ticker_rows()`. The canonical output remains per-ticker bronze Parquet at `asset_class=equity/symbol=<ticker>/1d.parquet`.

## Operator Behavior

- Default `historical --backfill --asset-class equity` resolves to IB.
- Operators can force IB with `--source ib`.
- Operators can force Massive with `--source massive`.
- `livewire_ingest.py` skips the IB Gateway preflight when the resolved historical command does not need IB.

## Verification

Tests must prove that forced Massive equity backfill does not instantiate `IBClient`, Massive partial long-history responses do not complete cursors, IB forced/default mode still works, robust backfill passes the source option through, coverage recovery uses Massive daily repair, and `backfill-all` keeps the daily backfill source selector explicit.
