# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Add automatic FX cross flipping so unsupported local pairs can fetch the IB-supported reverse pair and store inverted rows.

## Success Criteria
- Known IB-supported pairs fetch directly.
- Reverse local pairs such as `USDEUR` automatically fetch the supported source pair and invert OHLC rows.
- Unsupported malformed or unknown pairs fail with a clear `ValueError`.
- Focused tests and the repo coverage gate pass.

## Dependency Graph
- T1 -> T2
- T2 -> T3
- T3 -> T4

## Tasks
- [x] T1 Add failing tests for direct, inverted, and unsupported FX pair resolution
  depends_on: []
- [x] T2 Implement FX source-pair resolver in fetch/update scripts
  depends_on: [T1]
- [x] T3 Run focused tests, coverage gate, and RuntimeWarning gate
  depends_on: [T2]
- [x] T4 Commit the auto-flip change
  depends_on: [T3]

## Review
- Outcome:
  - Replaced the single hardcoded `USDEUR -> EURUSD` mapping with automatic FX pair resolution.
  - Known IB-supported FX pairs fetch directly.
  - Reverse local pairs fetch the supported source pair and store inverted OHLC rows.
  - Malformed or unknown FX pairs raise `ValueError` before requesting IB data.
- Verification:
  - `uv run --python 3.13 --with ib-async --with pyarrow --with duckdb --with rich --with pytest --with pytest-cov --with responses --with requests --with pandas --with polars --with boto3 --with httpx python -m pytest tests/test_daily_update.py::TestMakeContract tests/test_fetch_ib_historical.py::TestMakeContract -q`
  - `uv run --python 3.13 --with ib-async --with pyarrow --with duckdb --with rich --with pytest --with pytest-cov --with responses --with requests --with pandas --with polars --with boto3 --with httpx python -m pytest tests/test_daily_update.py tests/test_fetch_ib_historical.py -q`
  - `uv run --python 3.13 --with ib-async --with pyarrow --with duckdb --with rich --with pytest --with pytest-cov --with responses --with requests --with pandas --with polars --with boto3 --with httpx python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing`
  - `uv run --python 3.13 --with ib-async --with pyarrow --with duckdb --with rich --with pytest --with pytest-cov --with responses --with requests --with pandas --with polars --with boto3 --with httpx python -m pytest tests -q -W error::RuntimeWarning`
