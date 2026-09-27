from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from clients.corporate_action_store import CorporateAction
from clients.price_basis import (
    classify_source_seam_breaks,
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
    'ambiguous split classifications: 40a5b16c...' for exactly this action,
    with no SplitClassification fields logged
    (logs/volETF-ib-backfill-20260927T1414Z.log:286-317).

    Reconstructed read-only from IB (2026-09-27, no bronze write), replaying
    the EXACT backfill chunk call for the window covering this date
    (duration='1 Y', end_date='20150613-00:00:00', matching
    compute_date_windows for QLD's real 2006-06-21 -> 2021-06-11 backfill
    range) the row pair the classifier would compare is 2015-05-19 close
    4.8078 / 2015-05-20 close 4.8131 — an ordinary ~0.1% daily move, and it
    classifies cleanly as 'adjusted', the opposite of what production saw.
    This rules out both the seam/future-split misclassification fixed above
    AND a window-chunk-boundary artifact (2015-05-20 sits mid-window, 11
    months from the near boundary and 3+ weeks from the far one) as QLD's
    cause. The real cause is unresolved: production's actual fetch used
    concurrent `asyncio.gather` over all 15 windows, which this read-only,
    sequential re-fetch cannot replay — a pacing/dedup artifact under
    concurrency remains the only unruled-out explanation, tracked as an open
    follow-up in the post-mortem rather than assumed.
    """
    rows = [_row(date(2015, 5, 19), 4.8078), _row(date(2015, 5, 20), 4.8131)]

    result = classify_split_events(rows, [_split("qld-2015", date(2015, 5, 20), 1, 2)], date(2015, 5, 20))

    assert result[0].treatment == "adjusted"


# ── Seam group refinement: gap splits vs. later splits ─────────────────
#
# Follow-up fix: a split whose ex_date falls in the gap itself (after the
# last incoming row but on/before the first existing row past it) has
# already actually happened by the time the existing series resumes — its
# real jump is already embedded on the existing side, same as an in-window
# split observed at the seam. The original seam formula treated it as if it
# were a purely-future split, which classified it "raw" and left the IB
# rows unreversed — the original defect again.


def test_gap_split_classified_like_in_window_not_purely_future():
    """Coordinator's failing case: IB ends 2024-04-10 already IB-adjusted for
    a 1:2 split whose ex_date (2024-04-11) is the very next existing row —
    the gap step IS the split boundary, so it must classify like an
    in-window split (raw target = factor, adjusted target = 1), not the
    purely-future formula (raw target = 1, adjusted target = 1/factor),
    which would wrongly read this observed ratio (~1.0) as raw.
    """
    incoming = [_row(date(2024, 4, 9), 27.10), _row(date(2024, 4, 10), 27.00)]
    existing = [_existing_row(date(2024, 4, 11), 27.03), _existing_row(date(2024, 4, 12), 26.90)]
    actions = [_split("gap-split", date(2024, 4, 11), 1, 2)]

    result = prepare_ib_rows_for_publish(
        incoming, existing_rows=existing, actions=actions, as_of_date=date(2024, 4, 10)
    )

    assert result[1]["price_basis"] == "raw"
    assert result[1]["close"] == pytest.approx(27.00 / 0.5, rel=0.01)  # 54.0-scale


def test_existing_rows_sourced_ib_after_seam_still_widen_and_classify():
    """B1: recent bronze equity rows are commonly source='ib' too (IB is the
    primary daily provider) — an existing row dated after the split must not
    be mistaken for part of the incoming batch when computing last_ib_date,
    or the seam branch never fires and the original defect returns.
    """
    incoming = [_row(date(2021, 6, 9), 26.290), _row(date(2021, 6, 10), 27.045)]
    existing = [
        _existing_row(date(2021, 6, 11), 54.830),
        {**_existing_row(date(2024, 4, 10), 109.170), "source": "ib"},
        {**_existing_row(date(2024, 4, 11), 55.080), "source": "ib"},
    ]
    actions = [_split("svxy-2024", date(2024, 4, 11), 1, 2)]

    result = prepare_ib_rows_for_publish(
        incoming, existing_rows=existing, actions=actions, as_of_date=date(2021, 6, 10)
    )

    assert result[1]["price_basis"] == "raw"
    assert result[1]["close"] == pytest.approx(54.09, abs=0.01)


def test_mixed_gap_and_later_splits_partition_independently():
    """One split lands in the gap (ex_date on the first existing row, real
    factor 0.5) and shows its real raw jump at the seam; a second, unrelated
    4:1 reverse split lands later (existing series has already resumed well
    before it). The gap split's own real jump must classify this seam
    'raw' using ONLY the gap group's factor — if the later split's factor
    leaked into the raw target too (0.5 * 4 = 2.0 instead of 0.5), the fit
    would be far worse and could misclassify or flip to ambiguous.
    """
    incoming = [_row(date(2024, 4, 9), 27.10), _row(date(2024, 4, 10), 27.00)]
    existing = [
        _existing_row(date(2024, 4, 11), 13.50),  # real 1:2 raw jump: 27.00 * 0.5
        _existing_row(date(2024, 4, 12), 13.45),
        _existing_row(date(2025, 12, 5), 53.00),  # later 4:1 reverse split, already resumed on the existing side
        _existing_row(date(2025, 12, 8), 52.80),
    ]
    actions = [
        _split("gap-split", date(2024, 4, 11), 1, 2),
        _split("later-split", date(2025, 12, 5), 4, 1),
    ]

    result = prepare_ib_rows_for_publish(
        incoming, existing_rows=existing, actions=actions, as_of_date=date(2024, 4, 10)
    )

    assert result[1]["price_basis"] == "raw"
    assert result[1]["close"] == pytest.approx(27.00, rel=0.01)  # already raw — not reversed


def test_long_gap_seam_is_ambiguous():
    """More than 5 trading days between the last incoming row and the first
    existing row past it makes the seam ratio a multi-period return, not a
    single boundary step — indistinguishable from a genuine large move.
    """
    incoming = [_row(date(2024, 4, 1), 27.10), _row(date(2024, 4, 2), 27.00)]
    existing = [_existing_row(date(2024, 4, 20), 27.03)]  # > 5 trading days later
    actions = [_split("gap-split", date(2024, 4, 11), 1, 2)]

    with pytest.raises(ValueError, match="ambiguous"):
        prepare_ib_rows_for_publish(incoming, existing_rows=existing, actions=actions, as_of_date=date(2024, 4, 2))


def test_health_check_shaped_gap_fill_reverses_post_gap_split():
    """health_check.py's IB gap-fill path: existing rows sit on BOTH sides of
    the incoming block (an interior gap, not a trailing backfill). The
    effective-as-of widening must still reach the post-gap split using the
    existing rows *after* the gap.
    """
    existing_before = [_existing_row(date(2021, 6, 1), 25.50)]
    incoming = [_row(date(2021, 6, 9), 26.290), _row(date(2021, 6, 10), 27.045)]
    existing_after = [
        _existing_row(date(2021, 6, 11), 54.830),
        _existing_row(date(2024, 4, 10), 109.170),
        _existing_row(date(2024, 4, 11), 55.080),
    ]
    actions = [_split("svxy-2024", date(2024, 4, 11), 1, 2)]

    result = prepare_ib_rows_for_publish(
        incoming,
        existing_rows=existing_before + existing_after,
        actions=actions,
        as_of_date=date(2021, 6, 10),
    )

    assert result[1]["price_basis"] == "raw"
    assert result[1]["close"] == pytest.approx(54.09, abs=0.01)


# ── classify_source_seam_breaks: both transition directions ─────────────
#
# _classify_seam_group is not symmetric in which side is IB: adjusted_target
# is 1/f_later for an ib->non-ib seam (f_gap cancels against the real gap
# jump) but f_gap*f_later for a non-ib->ib seam (f_gap does not cancel — it
# is baked into the real raw price on the "after" side, not into a factor
# being undone). Using the ib->non-ib formula for both directions used to
# make a non-ib->ib seam land ambiguous and get silently dropped.


def test_source_seam_ib_before_non_ib_after_matches_prior_direction():
    """ib -> non-ib (an incoming backfill batch ending before existing data,
    SVXY-shaped): adjusted_target = 1/f_later, unchanged from before this fix.
    """
    rows = [
        _row(date(2021, 6, 9), 26.290),
        _row(date(2021, 6, 10), 27.045),
        _existing_row(date(2021, 6, 11), 54.830),
        _existing_row(date(2021, 6, 14), 54.570),
    ]
    actions = [_split("svxy-2024", date(2024, 4, 11), 1, 2)]

    result = classify_source_seam_breaks(rows, actions, date(2026, 1, 1))

    assert len(result) == 1
    boundary_date, classification = result[0]
    assert boundary_date == date(2021, 6, 10)
    assert classification.treatment == "adjusted"
    assert classification.observed_ratio == pytest.approx(54.83 / 27.045)


def test_source_seam_non_ib_before_ib_after_uses_mirrored_target():
    """non-ib -> ib (existing data followed by a later IB fetch): the IB row
    is the one pre-adjusted for the later split, so adjusted_target =
    f_gap*f_later = 0.5 here (f_gap=1, no gap-group split) — NOT 1/f_later
    (=2.0), which is what the un-mirrored formula used to compare against
    and would have called this seam ambiguous.
    """
    rows = [
        _existing_row(date(2021, 6, 10), 27.10),
        {**_row(date(2021, 6, 11), 13.55), "source": "ib"},
    ]
    actions = [_split("later-2024", date(2024, 4, 11), 1, 2)]

    result = classify_source_seam_breaks(rows, actions, date(2026, 1, 1))

    assert len(result) == 1
    boundary_date, classification = result[0]
    assert boundary_date == date(2021, 6, 10)
    assert classification.treatment == "adjusted"
    assert classification.observed_ratio == pytest.approx(13.55 / 27.10, rel=0.001)
