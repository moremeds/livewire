"""Expected coverage = presets x trading calendar x timeframe.

The denominator must never be derived from what is already on disk: a symbol
that never landed has to stay visible. See section 4 of
docs/superpowers/specs/2026-08-31-livewire-gap-autoheal-design.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from clients.ingestion_common import load_preset
from clients.trading_calendar import trading_dates_in_range


@dataclass(frozen=True)
class ExpectedSeries:
    symbol: str
    asset_class: str
    timeframe: str
    sessions: tuple[date, ...]


def _contract_expiry(symbol: str) -> date | None:
    """Month-start expiry for a composite futures ticker (ES_202506), else None."""
    _root, _sep, expiry = symbol.partition("_")
    if len(expiry) != 6 or not expiry.isdigit():
        return None
    return date(int(expiry[:4]), int(expiry[4:]), 1)


def build_denominator(
    preset_paths: list[Path],
    asset_class: str,
    timeframe: str,
    start: date,
    end: date,
    as_of: date,
) -> list[ExpectedSeries]:
    sessions = tuple(trading_dates_in_range(start, end))
    current_month = as_of.replace(day=1)
    out: list[ExpectedSeries] = []
    for preset_path in preset_paths:
        _name, tickers, _exchange_map = load_preset(preset_path)
        for ticker in tickers:
            expiry = _contract_expiry(ticker)
            if expiry is not None and expiry < current_month:
                continue
            out.append(ExpectedSeries(ticker, asset_class, timeframe, sessions))
    return out
