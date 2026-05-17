# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Align spot gold with IB's `CMDTY` contract type and add `USDEUR` as a true FX pair.

## Success Criteria
- `XAUUSD` is fetched/stored with `asset_class=cmdty`, not `fx`.
- `USDEUR` is fetched/stored with `asset_class=fx`.
- Both use IB MIDPOINT daily bars and normalize midpoint volume to `0`.
- Focused tests and the repo coverage gate pass.

## Dependency Graph
- T1 -> T2
- T2 -> T3
- T3 -> T4

## Tasks
- [x] T1 Update tests to expect `cmdty` for `XAUUSD` and `fx` for `USDEUR`
  depends_on: []
- [x] T2 Update fetch/update scripts and bronze schema profiles
  depends_on: [T1]
- [x] T3 Update presets and local data paths
  depends_on: [T2]
- [x] T4 Run verification and summarize results
  depends_on: [T3]

## Review
- Outcome:
  - Added `cmdty` as a daily bronze asset class for IB `CMDTY` contracts.
  - Added `fx` as a daily bronze asset class for IB `Forex` contracts.
  - `XAUUSD` is fetched from IB `CMDTY` MIDPOINT and stored under `asset_class=cmdty`.
  - `USDEUR` is stored under `asset_class=fx`; IB does not support direct `USDEUR`, so the fetch uses `EURUSD` and stores inverted OHLC rows.
  - MIDPOINT volume is normalized to `0`.
- Verification:
  - `uv run --python 3.13 --with ib-async --with pyarrow --with duckdb --with rich --with pytest --with pytest-cov --with responses --with requests --with pandas --with polars --with boto3 --with httpx python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing`
  - `uv run --python 3.13 --with ib-async --with pyarrow --with duckdb --with rich --with pytest --with pytest-cov --with responses --with requests --with pandas --with polars --with boto3 --with httpx python -m pytest tests -q -W error::RuntimeWarning`
  - Live script e2e commands reached IB for `cmdty/XAUUSD` and `fx/USDEUR`, but IB HMDS timed out and returned zero bars, so live publication was not proven in this run.
