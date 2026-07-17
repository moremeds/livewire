"""Classify a price discontinuity against a second source.

A break that is not the 2021-06 seed artifact is one of three things, and they
must not be treated alike: trimming a real market move amputates real history,
while keeping bad data serves a plausible wrong chart. Massive can return the same
range on both bases, which separates all three:

* our jump present in the provider's RAW series      -> real market move
* our jump absent from the provider's raw series     -> our bronze is bad there
* provider adjusted/raw factor steps across the date -> the provider knows a split
  our corporate-action store lacks (its /v3/reference/splits collapses before 2003:
  33 splits for 1978-2002 vs 148 in 2003 alone)

Provider access is injected so this module stays pure and testable offline.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import date, timedelta
from typing import Literal

from clients.massive_client import (
    MassiveAPIError,
    MassiveAuthError,
    MassiveRateLimitError,
    MassiveServerError,
)

TriageVerdict = Literal["real_move", "bad_data", "missing_action", "inconclusive"]

WINDOW_DAYS = 7
DEFAULT_TOLERANCE = 0.25
Fetcher = Callable[[str, date, date], list[dict]]


class RetryableProviderError(Exception):
    """A transient provider failure that must NOT be recorded as a verdict.

    ``inconclusive`` trims, and the verdict manifest is durable and default-loaded,
    so swallowing a rate-limit or a 502 into a verdict would turn one bad afternoon
    into a permanent amputation of real history that no resume ever revisits.
    """


def _ratio_at(rows: list[dict], break_date: str) -> float | None:
    """Magnitude of the close ratio stepping into ``break_date``."""
    ordered = sorted(rows, key=lambda row: str(row["trade_date"])[:10])
    previous: dict | None = None
    for row in ordered:
        current_date = str(row["trade_date"])[:10]
        if current_date >= break_date:
            if previous is None or current_date != break_date:
                return None
            try:
                a, b = float(previous["close"]), float(row["close"])
            except (TypeError, ValueError):
                return None
            if not (math.isfinite(a) and math.isfinite(b)) or a <= 0 or b <= 0:
                return None
            return max(b / a, a / b)
        previous = row
    return None


def _factor_step(raw: list[dict], adjusted: list[dict], break_date: str) -> float | None:
    """Magnitude of the change in the provider's adjusted/raw factor across the date.

    A step here means the provider applied a split at this date — evidence of the
    event itself, independent of whether its reference endpoint returns it.
    """
    raw_by_date = {str(r["trade_date"])[:10]: r for r in raw}
    adj_by_date = {str(r["trade_date"])[:10]: r for r in adjusted}
    dates = sorted(set(raw_by_date) & set(adj_by_date))
    before = [d for d in dates if d < break_date]
    after = [d for d in dates if d >= break_date]
    if not before or not after:
        return None

    def factor(day: str) -> float | None:
        try:
            r, a = float(raw_by_date[day]["close"]), float(adj_by_date[day]["close"])
        except (TypeError, ValueError):
            return None
        if not (math.isfinite(r) and math.isfinite(a)) or r <= 0 or a <= 0:
            return None
        return a / r

    f0, f1 = factor(before[-1]), factor(after[0])
    if f0 is None or f1 is None or f0 <= 0 or f1 <= 0:
        return None
    return max(f1 / f0, f0 / f1)


def triage_break(
    symbol: str,
    break_date: str,
    observed_ratio: float,
    *,
    fetch_raw: Fetcher,
    fetch_adjusted: Fetcher,
    tolerance: float = DEFAULT_TOLERANCE,
    window_days: int = WINDOW_DAYS,
) -> dict:
    """Classify one discontinuity.

    Returns a verdict for anything the provider can answer, including a final
    ``inconclusive``. Raises :class:`RetryableProviderError` — never returns — when
    the provider failed transiently, so the caller can abort instead of checkpointing.
    """
    result: dict = {
        "symbol": symbol,
        "date": break_date,
        "observed": observed_ratio,
        "provider_raw_ratio": None,
        "provider_factor_step": None,
        "verdict": "inconclusive",
        "reason": "",
    }
    day = date.fromisoformat(break_date)
    start, end = day - timedelta(days=window_days), day + timedelta(days=window_days)
    try:
        raw = fetch_raw(symbol, start, end)
        adjusted = fetch_adjusted(symbol, start, end)
    except MassiveAuthError as exc:
        # The entitlement floor (measured 2021-07-12, rolling ~5y). Permanent for this
        # date, not transient: a final inconclusive, safe to checkpoint. The caller is
        # responsible for having proved the credentials work on an entitled date —
        # otherwise a bad key looks exactly like this on every candidate.
        result["reason"] = f"provider not entitled for this date: {exc}"
        return result
    except (MassiveRateLimitError, MassiveServerError, TimeoutError, ConnectionError) as exc:
        # NOT a verdict. inconclusive trims, and the verdict manifest is durable —
        # swallowing a transient outage here would permanently amputate real history.
        raise RetryableProviderError(f"{symbol}@{break_date}: {exc}") from exc
    except MassiveAPIError as exc:
        # The provider only gets to end the conversation when it actually answered.
        # A MassiveAPIError with no status_code is a transport/protocol failure, not a
        # verdict — MassiveClient wraps an exhausted connection retry as a bare
        # MassiveAPIError ("Connection failed after N attempts", massive_client.py:276),
        # and checkpointing that as inconclusive would trim on a network blip.
        if exc.status_code is None or exc.status_code >= 500:
            raise RetryableProviderError(f"{symbol}@{break_date}: {exc}") from exc
        result["reason"] = f"provider error: {exc}"
        return result
    except Exception as exc:
        # An exception we do not model is the LEAST evidence we have that the provider
        # actually answered, so it must not become the most permanent kind of answer.
        # Only the cases above — where the provider demonstrably replied — may
        # checkpoint. Retryable does not crash the batch: triage_breaks aborts without
        # checkpointing and --resume re-asks. Failing loudly on an unmodelled error
        # beats silently trimming real history on it.
        raise RetryableProviderError(f"{symbol}@{break_date}: unexpected {type(exc).__name__}: {exc}") from exc
    if not raw:
        result["reason"] = "provider returned no raw bars for the window"
        return result
    if not adjusted:
        # Both bases are required. Raw agreement alone cannot separate a real move
        # from a missing corporate action — both show a provider raw jump.
        result["reason"] = "provider returned no adjusted bars: cannot separate a real move from a missing action"
        return result

    step = _factor_step(raw, adjusted, break_date)
    result["provider_factor_step"] = step
    # Check the factor step first: it is positive evidence of an event, and a
    # missing-action break also shows a provider raw jump, so raw-jump agreement
    # alone would misread it as a real move.
    if step is not None and abs(math.log(step)) > tolerance:
        result["verdict"] = "missing_action"
        result["reason"] = f"provider adjusted/raw factor steps {step:.2f}x at {break_date}"
        return result

    provider_ratio = _ratio_at(raw, break_date)
    result["provider_raw_ratio"] = provider_ratio
    if provider_ratio is None:
        result["reason"] = "provider has no adjacent pair at the break date"
        return result
    if abs(math.log(provider_ratio) - math.log(observed_ratio)) <= tolerance:
        result["verdict"] = "real_move"
        result["reason"] = f"provider raw shows the same {provider_ratio:.2f}x step"
        return result
    result["verdict"] = "bad_data"
    result["reason"] = f"provider raw steps {provider_ratio:.2f}x where bronze steps {observed_ratio:.2f}x"
    return result
