"""Unit tests for break triage. Provider access is injected; no network."""

import pytest

from clients.break_triage import RetryableProviderError, triage_break
from clients.massive_client import (
    MassiveAPIError,
    MassiveAuthError,
    MassiveRateLimitError,
    MassiveServerError,
)


def _rows(pairs):
    return [{"trade_date": d, "close": c} for d, c in pairs]


def _fetcher(pairs):
    def fetch(symbol, start, end):
        return _rows(pairs)

    return fetch


def _empty(symbol, start, end):
    return []


# Real MRNA closes around its 2020-11-16 phase-3 readout (Massive raw, frozen
# 2026-07-17): a genuine move, present in BOTH sources.
MRNA_RAW = [("2020-11-13", 89.39), ("2020-11-16", 97.49)]


def test_both_sources_jump_together_is_a_real_move():
    result = triage_break("MRNA", "2020-11-16", 1.09, fetch_raw=_fetcher(MRNA_RAW), fetch_adjusted=_fetcher(MRNA_RAW))
    assert result["verdict"] == "real_move"


def test_our_jump_absent_from_provider_raw_is_bad_data():
    # Our bronze steps 22x; Massive raw is smooth across the same date.
    smooth = [("2021-07-26", 361.61), ("2021-07-27", 367.81)]
    result = triage_break("META", "2021-07-27", 22.4, fetch_raw=_fetcher(smooth), fetch_adjusted=_fetcher(smooth))
    assert result["verdict"] == "bad_data"


def test_provider_factor_step_means_a_missing_corporate_action():
    # Real Massive NVDA closes across its 2021-07-20 4:1 split, BOTH bases, fetched
    # live 2026-07-17 and frozen. The adjusted/raw factor steps 0.0250 -> 0.1000 (4x)
    # exactly at the ex-date: that step is the provider telling us an event happened,
    # independent of whether /v3/reference/splits returns it. This is the shape the
    # missing-action verdict keys on.
    raw = [("2021-07-19", 751.19), ("2021-07-20", 186.12)]
    adjusted = [("2021-07-19", 18.7798), ("2021-07-20", 18.612)]
    result = triage_break("NVDA", "2021-07-20", 4.04, fetch_raw=_fetcher(raw), fetch_adjusted=_fetcher(adjusted))
    assert result["verdict"] == "missing_action"
    assert result["provider_factor_step"] == pytest.approx(4.0, rel=0.05)


def test_provider_entitlement_error_is_inconclusive_not_a_trim_decision():
    """Massive's /v2/aggs is entitled for a rolling ~5y window (floor measured
    2021-07-12 on 2026-07-17). Every older break — EQIX @2003-01-02, MTB @2000-10-06,
    the whole pre-2003 missing-CA class — comes back like this and must not be
    mistaken for evidence either way."""

    def _unentitled(symbol, start, end):
        raise MassiveAuthError("Your plan doesn't include this data timeframe.")

    result = triage_break("EQIX", "2003-01-02", 24.95, fetch_raw=_unentitled, fetch_adjusted=_unentitled)
    assert result["verdict"] == "inconclusive"
    assert "timeframe" in result["reason"]


def test_no_provider_data_is_inconclusive():
    result = triage_break("ACDC", "2021-09-23", 250000.0, fetch_raw=_empty, fetch_adjusted=_empty)
    assert result["verdict"] == "inconclusive"


def test_raw_data_without_adjusted_coverage_is_inconclusive_not_a_real_move():
    """A missing-action break also shows a provider raw jump, so raw agreement alone
    is unfounded on exactly the class the triage exists to catch."""
    result = triage_break("MRNA", "2020-11-16", 1.09, fetch_raw=_fetcher(MRNA_RAW), fetch_adjusted=_empty)
    assert result["verdict"] == "inconclusive"
    assert "cannot separate" in result["reason"]


def test_provider_missing_the_break_date_is_inconclusive():
    off = [("2019-01-02", 10.11), ("2019-01-03", 10.24)]
    result = triage_break("MTB", "2000-10-06", 10.2, fetch_raw=_fetcher(off), fetch_adjusted=_fetcher(off))
    assert result["verdict"] == "inconclusive"


def test_an_unmodelled_adapter_error_is_retryable_not_a_permanent_verdict():
    """An exception we do not model is the LEAST evidence the provider answered, so it
    must not produce the most permanent kind of answer: inconclusive trims, and the
    verdict store is durable, so one unmodelled blip would amputate real history for
    good. Retryable is not a crash — the batch aborts without checkpointing and
    --resume re-asks. Only a provider that demonstrably replied may checkpoint."""

    def _boom(symbol, start, end):
        raise RuntimeError("bar payload had no 'c' key")

    with pytest.raises(RetryableProviderError, match="unexpected RuntimeError"):
        triage_break("NVDA", "2021-06-11", 40.9, fetch_raw=_boom, fetch_adjusted=_boom)


def test_a_wrapped_connection_failure_is_retryable_not_a_verdict():
    """MassiveClient wraps an exhausted connection retry as a bare MassiveAPIError
    with no status_code (massive_client.py:276). Reading that as a final verdict
    would trim a symbol's real history because the network blipped."""

    def _unreachable(symbol, start, end):
        raise MassiveAPIError("Connection failed after 3 attempts: [Errno 61] Connection refused")

    with pytest.raises(RetryableProviderError):
        triage_break("NVDA", "2021-07-20", 4.04, fetch_raw=_unreachable, fetch_adjusted=_unreachable)


@pytest.mark.parametrize(
    "error",
    [
        MassiveRateLimitError("429 slow down", status_code=429),
        MassiveServerError("502 bad gateway", status_code=502),
        TimeoutError("read timed out"),
        ConnectionError("connection reset"),
    ],
)
def test_transient_provider_failure_raises_and_is_never_a_verdict(error):
    """inconclusive trims and the manifest is durable, so a transient outage must
    abort the batch rather than checkpoint a permanent amputation."""

    def _flaky(symbol, start, end):
        raise error

    with pytest.raises(RetryableProviderError):
        triage_break("NVDA", "2021-07-20", 4.04, fetch_raw=_flaky, fetch_adjusted=_flaky)


def test_a_4xx_provider_error_is_a_final_verdict_not_a_retry():
    from clients.massive_client import MassiveNotFoundError

    def _gone(symbol, start, end):
        raise MassiveNotFoundError("404 unknown ticker", status_code=404)

    result = triage_break("ACDC", "2021-09-23", 250000.0, fetch_raw=_gone, fetch_adjusted=_gone)
    assert result["verdict"] == "inconclusive"
    assert "404" in result["reason"]


def test_a_non_positive_provider_close_cannot_manufacture_a_ratio():
    """A zero close would make the ratio infinite and read as a giant 'real move'."""
    rows = [("2021-07-19", 0.0), ("2021-07-20", 186.12)]
    result = triage_break("NVDA", "2021-07-20", 4.04, fetch_raw=_fetcher(rows), fetch_adjusted=_fetcher(rows))
    assert result["verdict"] == "inconclusive"
    assert result["provider_raw_ratio"] is None


def test_a_non_numeric_provider_close_is_inconclusive():
    rows = [("2021-07-19", "n/a"), ("2021-07-20", 186.12)]
    result = triage_break("NVDA", "2021-07-20", 4.04, fetch_raw=_fetcher(rows), fetch_adjusted=_fetcher(rows))
    assert result["verdict"] == "inconclusive"


def test_a_non_positive_close_cannot_manufacture_a_factor_step():
    """factor = adjusted/raw; a zero raw close would divide by zero."""
    raw = [("2021-07-19", 0.0), ("2021-07-20", 186.12)]
    adjusted = [("2021-07-19", 18.7798), ("2021-07-20", 18.612)]
    result = triage_break("NVDA", "2021-07-20", 4.04, fetch_raw=_fetcher(raw), fetch_adjusted=_fetcher(adjusted))
    assert result["provider_factor_step"] is None
