from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from clients.corporate_action_store import CorporateAction
from clients.price_basis import classify_split_events, normalize_ib_rows, normalize_split_adjusted_rows


def _split(action_id: str, ex_date: date, split_from: float, split_to: float) -> CorporateAction:
    return CorporateAction(
        action_id=action_id,
        provider="massive",
        provider_event_id=action_id,
        event_revision=1,
        supersedes_action_id=None,
        symbol="TEST",
        action_type="split",
        ex_date=ex_date,
        split_from=split_from,
        split_to=split_to,
        cash_amount=None,
        currency=None,
        declaration_date=None,
        record_date=None,
        pay_date=None,
        status="active",
        fetched_at=datetime(2026, 1, 1, tzinfo=UTC),
        payload_hash=action_id,
    )


def _row(trade_date: date, close: float = 25.0, volume: int = 400) -> dict:
    return {
        "trade_date": trade_date.isoformat(),
        "symbol_id": 1,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "adj_close": close,
        "volume": volume,
        "source": "ib",
        "price_basis": "split_adjusted",
    }


@pytest.mark.parametrize(
    "split_from,split_to,adjusted,raw",
    [
        (1, 2, 50.0, 100.0),
        (2, 3, 60.0, 90.0),
        (1, 4, 25.0, 100.0),
        (1, 7, 10.0, 70.0),
        (1, 10, 12.0, 120.0),
        (2, 1, 200.0, 100.0),
    ],
)
def test_normalizes_split_adjusted_prices(split_from, split_to, adjusted, raw):
    rows = [_row(date(2026, 1, 2), adjusted)]
    actions = [_split("split", date(2026, 1, 3), split_from, split_to)]

    result = normalize_split_adjusted_rows(rows, actions, date(2026, 1, 3), volume_mode="raw")

    assert result[0]["close"] == pytest.approx(raw)
    assert result[0]["source"] == "ib"
    assert result[0]["price_basis"] == "raw"
    assert result[0]["volume"] == 400


def test_cumulative_splits_are_reversed():
    rows = [_row(date(2026, 1, 1), 12.5)]
    actions = [
        _split("two", date(2026, 1, 2), 1, 2),
        _split("four", date(2026, 1, 3), 1, 4),
    ]

    result = normalize_split_adjusted_rows(rows, actions, date(2026, 1, 3), volume_mode="raw")

    assert result[0]["close"] == pytest.approx(100.0)


def test_future_and_cancelled_splits_are_excluded():
    future = _split("future", date(2026, 1, 4), 1, 2)
    cancelled = _split("cancelled", date(2026, 1, 3), 1, 4)
    cancelled = CorporateAction(**{**cancelled.__dict__, "status": "cancelled"})

    result = normalize_split_adjusted_rows(
        [_row(date(2026, 1, 2), 100.0)],
        [future, cancelled],
        date(2026, 1, 3),
        volume_mode="raw",
    )

    assert result[0]["close"] == 100.0


def test_split_adjusted_volume_is_reversed():
    result = normalize_split_adjusted_rows(
        [_row(date(2026, 1, 2), 25.0, volume=400)],
        [_split("split", date(2026, 1, 3), 1, 4)],
        date(2026, 1, 3),
        volume_mode="split_adjusted",
    )

    assert result[0]["volume"] == 100


def test_unverified_volume_mode_fails_closed():
    with pytest.raises(ValueError, match="volume convention"):
        normalize_split_adjusted_rows([], [], date(2026, 1, 3), volume_mode="unverified")


def test_raw_input_cannot_be_normalized_twice():
    row = _row(date(2026, 1, 2))
    row["price_basis"] = "raw"
    with pytest.raises(ValueError, match="split_adjusted"):
        normalize_split_adjusted_rows([row], [], date(2026, 1, 3), volume_mode="raw")


def test_malformed_split_is_rejected():
    with pytest.raises(ValueError, match="split ratio"):
        normalize_split_adjusted_rows(
            [_row(date(2026, 1, 2))],
            [_split("bad", date(2026, 1, 3), 0, 2)],
            date(2026, 1, 3),
            volume_mode="raw",
        )


def test_classifies_adjusted_split_boundary():
    rows = [_row(date(2020, 8, 28), 124.81), _row(date(2020, 8, 31), 129.04)]

    result = classify_split_events(rows, [_split("aapl", date(2020, 8, 31), 1, 4)], date(2020, 8, 31))

    assert result[0].treatment == "adjusted"
    assert result[0].observed_ratio == pytest.approx(129.04 / 124.81)
    assert result[0].adjusted_error < result[0].raw_error


def test_classifies_raw_split_boundary():
    rows = [_row(date(2003, 2, 14), 48.30), _row(date(2003, 2, 18), 24.96)]

    result = classify_split_events(rows, [_split("msft", date(2003, 2, 18), 1, 2)], date(2003, 2, 18))

    assert result[0].treatment == "raw"
    assert result[0].raw_error < result[0].adjusted_error


def test_classification_is_ambiguous_when_neither_hypothesis_is_close():
    rows = [_row(date(2026, 1, 2), 100.0), _row(date(2026, 1, 3), 70.0)]

    result = classify_split_events(rows, [_split("split", date(2026, 1, 3), 1, 2)], date(2026, 1, 3))

    assert result[0].treatment == "ambiguous"


def test_classification_is_ambiguous_without_bars_on_both_sides():
    result = classify_split_events(
        [_row(date(2026, 1, 2), 100.0)],
        [_split("split", date(2026, 1, 3), 1, 2)],
        date(2026, 1, 3),
    )

    assert result[0].treatment == "ambiguous"
    assert result[0].observed_ratio is None


def test_selective_normalization_reverses_only_adjusted_events():
    rows = [_row(date(2026, 1, 1), 25.0, volume=400)]
    actions = [
        _split("raw", date(2026, 1, 2), 1, 2),
        _split("adjusted", date(2026, 1, 3), 1, 4),
    ]
    classifications = classify_split_events(
        [
            _row(date(2026, 1, 1), 25.0),
            _row(date(2026, 1, 2), 12.5),
            _row(date(2026, 1, 3), 12.6),
        ],
        actions,
        date(2026, 1, 3),
    )

    result = normalize_ib_rows(rows, classifications)

    assert [item.treatment for item in classifications] == ["raw", "adjusted"]
    assert result[0]["close"] == pytest.approx(100.0)
    assert result[0]["volume"] == 100
    assert result[0]["price_basis"] == "raw"


def test_selective_normalization_rejects_ambiguous_event():
    classifications = classify_split_events(
        [_row(date(2026, 1, 2), 100.0)],
        [_split("split", date(2026, 1, 3), 1, 2)],
        date(2026, 1, 3),
    )

    with pytest.raises(ValueError, match="ambiguous"):
        normalize_ib_rows([_row(date(2026, 1, 2), 100.0)], classifications)
