from __future__ import annotations

from datetime import UTC, date, datetime

from clients.corporate_action_store import CorporateAction
from clients.split_basis_evidence import classify_split_from_reference, correct_invalid_ohlc_from_reference


def _split(split_from: float = 1, split_to: float = 4) -> CorporateAction:
    return CorporateAction(
        action_id="split-1",
        provider="massive",
        provider_event_id="provider-split-1",
        event_revision=1,
        supersedes_action_id=None,
        symbol="TEST",
        action_type="split",
        ex_date=date(2024, 6, 10),
        split_from=split_from,
        split_to=split_to,
        cash_amount=None,
        currency=None,
        declaration_date=None,
        record_date=None,
        pay_date=None,
        status="active",
        fetched_at=datetime(2026, 7, 13, tzinfo=UTC),
        payload_hash="split-hash",
    )


def _rows(pre: float, post: float) -> list[dict]:
    return [
        {"trade_date": "2024-06-07", "close": pre},
        {"trade_date": "2024-06-10", "close": post},
    ]


def test_reference_consensus_classifies_raw_boundary():
    result = classify_split_from_reference(
        _rows(400.0, 101.0),
        [_rows(100.0, 101.0), _rows(100.0, 101.0)],
        _split(),
    )

    assert result.treatment == "raw"
    assert result.request_count == 2
    assert result.pre_date == date(2024, 6, 7)
    assert result.post_date == date(2024, 6, 10)


def test_reference_consensus_classifies_adjusted_boundary():
    result = classify_split_from_reference(
        _rows(100.0, 101.0),
        [_rows(100.0, 101.0), _rows(100.0, 101.0)],
        _split(),
    )

    assert result.treatment == "adjusted"


def test_reference_requests_must_agree_pointwise():
    result = classify_split_from_reference(
        _rows(400.0, 101.0),
        [_rows(100.0, 101.0), _rows(80.0, 101.0)],
        _split(),
    )

    assert result.treatment == "ambiguous"
    assert result.reason == "reference_disagreement"


def test_reference_requires_sessions_on_both_sides():
    result = classify_split_from_reference(
        _rows(400.0, 101.0),
        [[{"trade_date": "2024-06-10", "close": 101.0}]] * 2,
        _split(),
    )

    assert result.treatment == "ambiguous"
    assert result.reason == "missing_reference_boundary"


def test_reference_neither_fit_remains_ambiguous():
    result = classify_split_from_reference(
        _rows(220.0, 101.0),
        [_rows(100.0, 101.0), _rows(100.0, 101.0)],
        _split(),
    )

    assert result.treatment == "ambiguous"
    assert result.reason == "neither_hypothesis_fit"


def test_reference_uses_multi_session_median_across_boundary_outlier():
    bronze = [
        {"trade_date": "2024-06-06", "close": 400.0},
        {"trade_date": "2024-06-07", "close": 404.0},
        {"trade_date": "2024-06-10", "close": 108.0},
        {"trade_date": "2024-06-11", "close": 102.0},
        {"trade_date": "2024-06-12", "close": 103.0},
    ]
    reference = [
        {"trade_date": "2024-06-06", "close": 100.0},
        {"trade_date": "2024-06-07", "close": 101.0},
        {"trade_date": "2024-06-10", "close": 100.0},
        {"trade_date": "2024-06-11", "close": 102.0},
        {"trade_date": "2024-06-12", "close": 103.0},
    ]

    result = classify_split_from_reference(bronze, [reference, reference], _split())

    assert result.treatment == "raw"


def test_exact_reference_match_resolves_small_stock_distribution():
    result = classify_split_from_reference(
        _rows(100.0, 101.0),
        [_rows(100.0, 101.0), _rows(100.0, 101.0)],
        _split(52, 53),
    )

    assert result.treatment == "adjusted"


def test_tiny_distribution_resolves_when_one_hypothesis_is_ten_times_better():
    result = classify_split_from_reference(
        _rows(1.001, 1.0),
        [_rows(1.0, 1.0), _rows(1.0, 1.0)],
        _split(1000, 1001),
    )

    assert result.treatment == "raw"


def test_invalid_ohlc_is_repaired_from_repeated_normalized_reference():
    bronze = {
        "trade_date": "2024-06-07",
        "open": 100.0,
        "high": 102.0,
        "low": 0.0,
        "close": 101.0,
        "adj_close": 101.0,
        "volume": 1000,
    }
    reference = {
        **bronze,
        "low": 99.0,
        "source": "ib",
        "price_basis": "split_adjusted",
    }

    result = correct_invalid_ohlc_from_reference(
        bronze,
        [[reference], [reference]],
    )

    assert result.status == "resolved"
    assert result.proposed_values == {"low": 99.0}
    assert result.close_error_bps == 0.0


def test_invalid_ohlc_reference_disagreement_fails_closed():
    bronze = {
        "trade_date": "2024-06-07",
        "open": 100.0,
        "high": 102.0,
        "low": 0.0,
        "close": 101.0,
        "adj_close": 101.0,
        "volume": 1000,
    }
    first = {**bronze, "low": 99.0, "source": "ib", "price_basis": "split_adjusted"}
    second = {**first, "low": 90.0}

    result = correct_invalid_ohlc_from_reference(
        bronze,
        [[first], [second]],
    )

    assert result.status == "ambiguous"
    assert result.reason == "reference_disagreement"


def test_invalid_ohlc_correction_must_preserve_bar_invariants():
    bronze = {
        "trade_date": "2024-06-07",
        "open": 100.0,
        "high": 102.0,
        "low": 0.0,
        "close": 101.0,
        "adj_close": 101.0,
        "volume": 1000,
    }
    reference = {
        **bronze,
        "low": 103.0,
        "source": "ib",
        "price_basis": "split_adjusted",
    }

    result = correct_invalid_ohlc_from_reference(bronze, [[reference], [reference]])

    assert result.status == "ambiguous"
    assert result.reason == "invalid_corrected_ohlc"
