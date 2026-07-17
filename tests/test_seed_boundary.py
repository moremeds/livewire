"""Unit tests for the seed-boundary detector.

Fixtures are real closes for real tickers, read from production bronze
(/Volumes/DATA_LAKE/livewire/data-lake) on 2026-07-17 and frozen. No network.
"""

from datetime import UTC, date, datetime

import pytest

from clients.corporate_action_store import CorporateAction
from clients.seed_boundary import (
    SeedBoundaryBreak,
    check_seed_boundary,
    classify_seed_boundary,
    measure_boundary_jump,
    predict_boundary_fold,
)


def _split(symbol: str, ex_date: str, split_from: float, split_to: float, status: str = "active") -> CorporateAction:
    return CorporateAction(
        action_id=f"{symbol}-{ex_date}-split",
        provider="massive",
        provider_event_id=f"{symbol}-{ex_date}",
        event_revision=1,
        supersedes_action_id=None,
        symbol=symbol,
        action_type="split",
        ex_date=date.fromisoformat(ex_date),
        split_from=split_from,
        split_to=split_to,
        cash_amount=None,
        currency=None,
        declaration_date=None,
        record_date=None,
        pay_date=None,
        status=status,
        fetched_at=datetime(2026, 7, 17, tzinfo=UTC),
        payload_hash=f"{symbol}{ex_date}",
    )


def _rows(pairs: list[tuple[str, float]]) -> list[dict]:
    return [{"trade_date": d, "close": c} for d, c in pairs]


# FUBO — still corrupt in bronze today: the 2021-06-18 straggler bar was IB
# back-adjusted for the 2026-03-24 12:1 REVERSE split (29.53 * 12 ~= 354) while its
# neighbours stayed raw. The only large-fold seed case left unrepaired, so every
# close below is a direct read rather than a reconstruction.
FUBO_ROWS = _rows(
    [
        ("2021-06-15", 27.90),
        ("2021-06-16", 28.62),
        ("2021-06-17", 29.53),
        ("2021-06-18", 351.1214),
        ("2021-06-21", 31.62),
    ]
)
FUBO_SPLITS = [_split("FUBO", "2026-03-24", 12, 1)]

# NVDA's real split history — used for fold arithmetic only. Its bronze rows were
# repaired on 2026-07-17 (source='ib', true raw), so the pre-repair corrupt closes
# no longer exist anywhere readable and are deliberately not reconstructed here.
NVDA_SPLITS = [_split("NVDA", "2021-07-20", 1, 4), _split("NVDA", "2024-06-10", 1, 10)]

# APH — the 2x case the 6.0 gate cannot see. Real 2024-06-12 1:2 split; still corrupt.
APH_ROWS = _rows([("2021-06-09", 34.07), ("2021-06-10", 34.13), ("2021-06-11", 68.45), ("2021-06-14", 68.31)])
APH_SPLITS = [_split("APH", "2024-06-12", 1, 2)]

# AAPL — no post-window split, flat boundary.
AAPL_ROWS = _rows([("2021-06-09", 127.13), ("2021-06-10", 126.11), ("2021-06-11", 127.35), ("2021-06-14", 130.48)])

# KLAC — post-window split (P=10) but genuinely raw: flat boundary.
KLAC_ROWS = _rows([("2021-06-09", 314.16), ("2021-06-10", 319.31), ("2021-06-11", 320.11), ("2021-06-14", 325.21)])
KLAC_SPLITS = [_split("KLAC", "2026-06-12", 1, 10)]


def test_predict_fold_multiplies_post_window_splits():
    assert predict_boundary_fold(NVDA_SPLITS) == pytest.approx(40.0)


def test_predict_fold_ignores_splits_inside_or_before_the_window():
    assert predict_boundary_fold([_split("NVDA", "2007-09-11", 2, 3), _split("NVDA", "2021-07-20", 1, 4)]) == (
        pytest.approx(4.0)
    )


def test_predict_fold_reverse_split_reports_magnitude():
    assert predict_boundary_fold([_split("ADV", "2026-03-27", 25, 1)]) == pytest.approx(25.0)


def test_predict_fold_no_post_window_split_is_one():
    assert predict_boundary_fold([]) == pytest.approx(1.0)


def test_predict_fold_ignores_cancelled_split():
    assert predict_boundary_fold([_split("NVDA", "2021-07-20", 1, 4, status="cancelled")]) == pytest.approx(1.0)


def test_measure_jump_finds_the_boundary_step():
    assert measure_boundary_jump(FUBO_ROWS) == ("2021-06-18", pytest.approx(11.89, rel=0.01))


def test_measure_jump_returns_none_when_no_rows_in_window():
    assert measure_boundary_jump(_rows([("2024-01-02", 10.5), ("2024-01-03", 10.7)])) is None


def test_fubo_is_corrupt_observed_matches_predicted():
    with pytest.raises(SeedBoundaryBreak) as exc:
        check_seed_boundary(FUBO_ROWS, FUBO_SPLITS)
    assert exc.value.date == "2021-06-18"
    assert exc.value.predicted == pytest.approx(12.0)


def test_aph_2x_is_corrupt_even_though_the_6x_gate_misses_it():
    with pytest.raises(SeedBoundaryBreak):
        check_seed_boundary(APH_ROWS, APH_SPLITS)


def test_aapl_no_post_window_split_is_clean():
    assert check_seed_boundary(AAPL_ROWS, []) is None


def test_klac_predicted_fold_but_flat_boundary_is_clean():
    assert check_seed_boundary(KLAC_ROWS, KLAC_SPLITS) is None


def test_low_fold_is_inconclusive_not_corrupt():
    rows = _rows([("2021-06-10", 40.00), ("2021-06-11", 50.00)])
    actions = [_split("CENTA", "2026-02-06", 4, 5)]
    assert classify_seed_boundary(rows, actions)["verdict"] == "inconclusive"
    assert check_seed_boundary(rows, actions) is None


def test_classify_reports_measurements():
    assert classify_seed_boundary(AAPL_ROWS, [])["verdict"] == "clean"
    corrupt = classify_seed_boundary(FUBO_ROWS, FUBO_SPLITS)
    assert corrupt["verdict"] == "corrupt"
    assert corrupt["date"] == "2021-06-18"
    assert corrupt["fold"] == pytest.approx(12.0)


def test_non_positive_close_is_inconclusive_not_a_crash():
    assert (
        classify_seed_boundary(_rows([("2021-06-17", 0.0), ("2021-06-18", 351.1214)]), FUBO_SPLITS)["verdict"]
        == "inconclusive"
    )


def test_a_split_with_no_ratio_recorded_is_ignored_not_a_crash():
    """split_from/split_to are nullable in the CA store."""
    assert predict_boundary_fold([_split("FUBO", "2026-03-24", None, None)]) == pytest.approx(1.0)


def test_a_zero_ratio_split_cannot_produce_a_nonsense_fold():
    assert predict_boundary_fold([_split("FUBO", "2026-03-24", 0, 12)]) == pytest.approx(1.0)


def test_a_non_numeric_close_is_skipped_rather_than_crashing():
    rows = [{"trade_date": "2021-06-17", "close": "n/a"}, {"trade_date": "2021-06-18", "close": 351.1214}]
    assert measure_boundary_jump(rows) is None


def test_a_non_finite_close_is_skipped_rather_than_crashing():
    rows = [{"trade_date": "2021-06-17", "close": float("inf")}, {"trade_date": "2021-06-18", "close": 351.1214}]
    assert measure_boundary_jump(rows) is None
