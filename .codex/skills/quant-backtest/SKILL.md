---
name: quant-backtest
description: Institutional-grade Python backtesting framework builder for Codex. Use ONLY when the user explicitly asks to build, extend, or run a backtest or systematic strategy test (e.g. "backtest this idea", "test my trading strategy", "build a walk-forward harness"). Do NOT trigger on incidental mentions of signals, positions, risk, coverage, Sharpe, or other market vocabulary during ordinary data-warehouse work in this repo.
---

# Quant Backtesting Framework

Build modular, institutional-grade Python backtesting systems. Keep the architecture strategy-agnostic and enforce rigorous data handling, transaction cost modeling, and performance attribution.

## Purpose

Use this skill to:
- design a reusable Python backtesting framework
- implement or extend strategy modules
- add risk, attribution, or transaction cost logic
- debug look-ahead bias and data leakage
- scaffold walk-forward and out-of-sample validation workflows

## Inputs to Gather

Collect as many of these as are available from the user or repository:
- target asset class or instruments
- bar frequency and date range
- data source or storage format
- strategy hypothesis and signal logic
- position sizing method
- transaction cost assumptions
- benchmark or factor model
- desired outputs such as charts, metrics, reports, or tests

If important inputs are missing, make reasonable defaults explicit and proceed.

## Operating Principles

1. Prevent look-ahead bias structurally.
2. Use log returns internally for compounding math.
3. Prefer vectorized pandas and numpy implementations.
4. Treat transaction costs as mandatory, not optional.
5. Separate in-sample fitting from out-of-sample evaluation.
6. Keep modules loosely coupled and easy to swap.

## Execution Plan

### 1) Inspect the repo or task shape
- Find whether the user wants a fresh framework, a strategy added, a bug fixed, or metrics extended.
- Identify existing file layout, coding style, and tests.
- If the repo already has conventions, follow them.

### 2) Map the work to this module layout
Use or adapt this structure:

```text
backtest/
├── __init__.py
├── data.py
├── signals.py
├── positions.py
├── tca.py
├── risk.py
├── regime.py
├── strategy.py
├── dashboard.py
└── engine.py
```

### 3) Build or update the core modules

#### Data layer
- Define a `DataProvider` abstract base class.
- Default backend should support DuckDB + Parquet.
- Preserve timezone-aware timestamps where possible.
- Forward-fill sparse data carefully, then drop leading invalid rows.
- Standardize outputs to a consistent tabular format.

#### Signal engine
- Use a `SignalGenerator` base class.
- Put raw predictive logic in `generate_raw()`.
- Apply `.shift(1)` in `generate()` as a non-negotiable invariant.
- Keep signals vectorized and timestamp aligned.

#### Position and accounting engine
- Track positions, cash, gross exposure, net exposure, leverage, and mark-to-market equity.
- Support at least equal-weight, fully deployed, and volatility-aware sizing.
- Ensure position application timing is consistent with shifted signals.

#### Transaction cost analysis
- Model commissions, slippage, and regulatory fees where relevant.
- Base cost calculations on turnover or notional traded.
- Report both gross and net results, but emphasize net results.

#### Risk and metrics
Compute at least:
- total return
- CAGR
- annualized volatility
- Sharpe
- Sortino
- max drawdown
- Calmar
- VaR 95 and 99, historical and parametric

Where feasible, also include:
- expected shortfall
- beta / alpha attribution
- factor regression
- turnover
- hit rate
- exposure statistics

#### Regime filter
- Keep regime logic separate from alpha logic.
- Support simple threshold filters and optional probabilistic models such as HMMs.
- Shift regime masks when needed so they do not leak future information.

#### Strategy abstraction
- Create a base `Strategy` class that composes:
  - a signal generator
  - a position manager
  - an optional regime filter

#### Orchestrator
- Build a backtest engine that wires together:
  1. data fetch
  2. signal generation
  3. sizing
  4. cost application
  5. equity curve construction
  6. metric computation

### 4) Validate correctness
Before finalizing:
- confirm signals are shifted
- confirm returns and positions are aligned
- confirm transaction costs are applied
- confirm metrics run on realized strategy returns
- confirm tests or sanity checks cover edge cases

### 5) Deliver useful outputs
Depending on the task, provide:
- code changes
- a concise architecture summary
- example usage
- tests
- assumptions and next steps
- notes on limitations or future extension points

## Reference Implementation Notes

### Data layer skeleton

```python
from abc import ABC, abstractmethod
import numpy as np
import pandas as pd

class DataProvider(ABC):
    @abstractmethod
    def fetch(self, symbols: list[str], start: str, end: str) -> pd.DataFrame:
        pass

    def log_returns(self, prices: pd.DataFrame) -> pd.DataFrame:
        return np.log(prices / prices.shift(1))

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.ffill().dropna(how="all")
```

### Signal safety skeleton

```python
from abc import ABC, abstractmethod
import pandas as pd

class SignalGenerator(ABC):
    @abstractmethod
    def generate_raw(self, data: pd.DataFrame) -> pd.DataFrame:
        pass

    def generate(self, data: pd.DataFrame) -> pd.DataFrame:
        raw = self.generate_raw(data)
        return raw.shift(1)
```

### Core invariant checklist
- never override the shifted execution invariant without a very explicit reason
- never present uncosted performance as the primary result
- never treat in-sample performance as expected live performance
- never mix raw prices and return series without clear alignment

## Task-Specific Playbooks

### A. Build from scratch
1. Create the module layout.
2. Implement base abstractions first.
3. Add one example strategy end to end.
4. Add a fee model and risk metrics.
5. Add a minimal dashboard or report output.
6. Add tests for alignment, look-ahead protection, and metric sanity.

### B. Add a strategy
1. Inspect the current base strategy and signal interfaces.
2. Implement new signal logic in `generate_raw()`.
3. Reuse existing sizing, cost, and metrics machinery.
4. Add tests showing no future leakage.
5. Include a usage example with realistic parameters.

### C. Add risk metrics
1. Extend the metrics engine rather than scattering calculations.
2. Keep annualization assumptions centralized.
3. Document formulas and units.
4. Add regression or benchmark alignment checks when attribution is involved.

### D. Debug look-ahead bias
1. Check whether signals are shifted.
2. Check whether rolling windows include the current bar improperly.
3. Check whether positions and returns are multiplied on the right dates.
4. Check whether regime filters or benchmark series leak future data.
5. Add explicit tests that fail when shift protection is removed.

### E. Add transaction cost modeling
1. Identify the portfolio turnover source.
2. Compute trades from position deltas, not level positions.
3. Apply cost assumptions consistently by asset class.
4. Report performance degradation from gross to net.

## Output Style

When using this skill, prefer:
- production-leaning code over pseudo-code
- minimal but clear comments
- explicit assumptions
- concise summaries of what changed and why
- tests when files are modified

## Example Quick Start

```python
provider = DuckDBParquetProvider("./data")
strategy = MyStrategy("example")
engine = BacktestEngine(provider, FeeSchedule(pct_of_notional=0.001, slippage_bps=5))
results = engine.run(strategy, ["SPY"], "2010-01-01", "2024-01-01")
print(results["metrics"])
```

## Final Checks

Before finishing, verify:
- the framework remains strategy-agnostic
- data, signal, execution, accounting, and metrics are modular
- performance metrics are based on net realized returns
- the result is easy for a future engineer to extend
