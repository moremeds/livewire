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
    # A session at or after `as_of` has not closed yet, so it cannot be missing.
    # Without this an `--end` in the future manufactures phantom G1 findings.
    sessions = tuple(d for d in trading_dates_in_range(start, end) if d < as_of)
    # Expiry is judged against the WINDOW, not against `as_of`: a contract that
    # was live during the scanned range belongs in that range's denominator even
    # if it has expired by today. Judging against `as_of` made the same window
    # yield a different denominator depending on when you scanned it.
    window_month = end.replace(day=1)
    out: list[ExpectedSeries] = []
    seen: set[str] = set()
    for preset_path in preset_paths:
        _name, tickers, _exchange_map = load_preset(preset_path)
        for ticker in tickers:
            # Presets overlap (sp500 n ndx100 = 87 symbols). Without this the
            # symbol is scanned twice and every gap it has lands in the Tier A
            # manifest twice — two repair instructions for one parquet path.
            if ticker in seen:
                continue
            seen.add(ticker)
            expiry = _contract_expiry(ticker)
            if expiry is not None and expiry < window_month:
                continue
            out.append(ExpectedSeries(ticker, asset_class, timeframe, sessions))
    return out
