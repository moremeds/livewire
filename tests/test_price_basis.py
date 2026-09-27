from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from clients.corporate_action_store import CorporateAction
from clients.price_basis import (
    classify_split_events,
    normalize_ib_rows,
    normalize_split_adjusted_rows,
    prepare_ib_rows_for_publish,
)


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


def test_split_at_or_before_first_stored_session_is_out_of_scope():
    rows = [_row(date(2026, 1, 2), 100.0), _row(date(2026, 1, 5), 101.0)]

    result = classify_split_events(
        rows,
        [_split("prehistory", date(2026, 1, 2), 1, 4)],
        date(2026, 1, 5),
    )

    assert result == []


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


def test_prepare_full_ib_history_normalizes_adjusted_rows_to_raw():
    incoming = [
        _row(date(2020, 8, 28), 124.81, volume=400),
        _row(date(2020, 8, 31), 129.04, volume=500),
    ]

    result = prepare_ib_rows_for_publish(
        incoming,
        existing_rows=[],
        actions=[_split("aapl", date(2020, 8, 31), 1, 4)],
        as_of_date=date(2020, 8, 31),
    )

    assert result[0]["close"] == pytest.approx(499.24)
    assert result[0]["volume"] == 100
    assert result[1]["close"] == pytest.approx(129.04)
    assert {row["price_basis"] for row in result} == {"raw"}


def test_prepare_incremental_post_split_row_uses_existing_raw_boundary():
    existing = [_row(date(2024, 6, 7), 1208.88, volume=41_238_580)]
    incoming = [_row(date(2024, 6, 10), 121.79, volume=314_162_650)]

    result = prepare_ib_rows_for_publish(
        incoming,
        existing_rows=existing,
        actions=[_split("nvda", date(2024, 6, 10), 1, 10)],
        as_of_date=date(2024, 6, 10),
    )

    assert result[0]["close"] == pytest.approx(121.79)
    assert result[0]["volume"] == 314_162_650
    assert result[0]["price_basis"] == "raw"


def test_prepare_preserves_non_ib_recovery_rows():
    massive = {**_row(date(2026, 1, 2), 100.0), "source": "massive", "price_basis": "raw"}

    result = prepare_ib_rows_for_publish(
        [massive],
        existing_rows=[],
        actions=[],
        as_of_date=date(2026, 1, 2),
    )

    assert result == [massive]


def test_prepare_does_not_require_old_split_before_incoming_window():
    incoming = [_row(date(2025, 1, 2), 200.0)]

    result = prepare_ib_rows_for_publish(
        incoming,
        existing_rows=[],
        actions=[_split("old", date(2020, 8, 31), 1, 4)],
        as_of_date=date(2025, 1, 2),
    )

    assert result[0]["close"] == 200.0
    assert result[0]["price_basis"] == "raw"


# ── Seam classification: splits after the incoming IB window ──────────
#
# Production incident 2026-09-27: an IB backfill of SVXY/VXX/XLK/... inserted
# rows for dates that end well before existing bronze rows begin. Splits whose
# ex_date falls in that gap can't be measured at their own boundary (both
# adjacent rows are existing, already-raw data) — they must be read off the
# seam between the last incoming IB row and the first existing row after it.
# Real bronze values below (source=legacy/ib), pulled read-only from macmini
# `data-lake/bronze/asset_class=equity/symbol=<SYM>/1d.parquet`, as of 2026-09-27.


def _existing_row(trade_date: date, close: float) -> dict:
    return {**_row(trade_date, close), "source": "legacy", "price_basis": "raw"}


def test_svxy_post_window_split_classified_from_seam():
    """SVXY 1:2 split on 2024-04-11 falls after the incoming IB window (ends
    2021-06-10). IB already back-adjusted the whole incoming block for it —
    the seam step (2021-06-11 existing / 2021-06-10 incoming) is ~2.03x, not
    an ordinary daily move.
    """
    incoming = [
        _row(date(2021, 6, 9), 26.290),
        _row(date(2021, 6, 10), 27.045),
    ]
    existing = [
        _existing_row(date(2021, 6, 11), 54.830),
        _existing_row(date(2021, 6, 14), 54.570),
        _existing_row(date(2024, 4, 10), 109.170),
        _existing_row(date(2024, 4, 11), 55.080),
    ]
    actions = [_split("svxy-2024", date(2024, 4, 11), 1, 2)]

    # Real callers (backfill_ticker) pass as_of_date=max(incoming dates), not
    # real today — prepare_ib_rows_for_publish must widen the effective as-of
    # date using the existing rows it already has, not rely on the caller.
    result = prepare_ib_rows_for_publish(
        incoming, existing_rows=existing, actions=actions, as_of_date=date(2021, 6, 10)
    )

    assert result[0]["price_basis"] == "raw"
    assert result[1]["close"] == pytest.approx(54.09, abs=0.01)  # 27.045 * 2
    # Seam step is now an ordinary daily move, not an artificial fold.
    seam_ratio = existing[0]["close"] / result[1]["close"]
    assert seam_ratio == pytest.approx(1.0137, abs=0.001)


def test_vxx_two_post_window_reverse_splits_classified_from_seam():
    """VXX has two 4:1 reverse splits (2023-03-07, 2024-07-24) after the
    incoming IB window (ends 2021-06-10) — combined fold 16x.
    """
    incoming = [
        _row(date(2021, 6, 9), 538.88),
        _row(date(2021, 6, 10), 507.68),
    ]
    existing = [
        _existing_row(date(2021, 6, 11), 30.82),
        _existing_row(date(2021, 6, 14), 31.11),
        _existing_row(date(2024, 7, 24), 49.19),
        _existing_row(date(2024, 7, 25), 49.59),
    ]
    actions = [
        _split("vxx-2023", date(2023, 3, 7), 4, 1),
        _split("vxx-2024", date(2024, 7, 24), 4, 1),
    ]

    result = prepare_ib_rows_for_publish(
        incoming, existing_rows=existing, actions=actions, as_of_date=date(2021, 6, 10)
    )

    assert result[0]["price_basis"] == "raw"
    assert result[1]["close"] == pytest.approx(507.68 / 16, rel=0.01)
    seam_ratio = existing[0]["close"] / result[1]["close"]
    assert seam_ratio == pytest.approx(0.971, abs=0.02)


def test_xlk_post_window_split_classified_from_seam():
    """Sector-ETF case: XLK 1:2 split on 2025-12-05, same 2021-06 seam shape
    as SVXY/VXX (issue157 sector backfill hit this too).
    """
    incoming = [
        _row(date(2021, 6, 9), 70.050),
        _row(date(2021, 6, 10), 70.565),
    ]
    existing = [
        _existing_row(date(2021, 6, 11), 141.970),
        _existing_row(date(2021, 6, 14), 143.410),
        _existing_row(date(2025, 12, 5), 146.60),
        _existing_row(date(2025, 12, 8), 147.63),
    ]
    actions = [_split("xlk-2025", date(2025, 12, 5), 1, 2)]

    result = prepare_ib_rows_for_publish(
        incoming, existing_rows=existing, actions=actions, as_of_date=date(2021, 6, 10)
    )

    assert result[0]["price_basis"] == "raw"
    assert result[1]["close"] == pytest.approx(70.565 * 2, rel=0.01)


def test_post_window_split_out_of_scope_without_existing_rows_past_it():
    """A split after the incoming window, with no existing rows extending
    past it, has no evidence it has already happened — the effective as-of
    date widening only reaches as far as the existing rows actually go, so
    this split is left out of scope entirely (matching the pre-widening
    behaviour for a split truly in the caller's future), not flagged
    ambiguous.
    """
    incoming = [_row(date(2021, 6, 9), 26.290), _row(date(2021, 6, 10), 27.045)]
    actions = [_split("svxy-2024", date(2024, 4, 11), 1, 2)]

    result = prepare_ib_rows_for_publish(incoming, existing_rows=[], actions=actions, as_of_date=date(2021, 6, 10))

    assert result[1]["close"] == 27.045
    assert result[1]["price_basis"] == "raw"


def test_qld_in_window_split_with_real_ib_values_classifies_cleanly():
    """QLD's 2015-05-20 1:2 split is IN-WINDOW (both boundary rows are
    incoming IB rows, not a seam case) — the production run raised
    'ambiguous split classifications: 40a5b16c...' for exactly this action.
    Re-fetched read-only from IB (2026-09-27, single continuous request, no
    bronze write) the real 2015-05-19/20 closes are smooth (4.8078 ->
    4.8131, an ordinary ~0.1% daily move) and classify cleanly as
    'adjusted' — the opposite of what production saw. This rules out the
    seam/future-split misclassification fixed above as QLD's cause: the
    real values IB serves for that date pair are not ambiguous. The
    production failure most likely came from a discontinuity introduced by
    QLD's 15-window chunked historical fetch (2006-06-21 -> 2021-06-11,
    logs/volETF-ib-backfill-20260927T1414Z.log:212) landing near this
    split, not from classify_split_events itself — a separate, unfixed
    issue tracked in the post-mortem.
    """
    rows = [_row(date(2015, 5, 19), 4.8078), _row(date(2015, 5, 20), 4.8131)]

    result = classify_split_events(rows, [_split("qld-2015", date(2015, 5, 20), 1, 2)], date(2015, 5, 20))

    assert result[0].treatment == "adjusted"
