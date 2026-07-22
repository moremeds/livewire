"""Unit tests for Yahoo raw-close reconstruction and split reconciliation.

Reconstruction uses REAL AMC split-adjusted closes across its 2023-08-24 1:10 reverse
split (frozen 2026-07-17); the reconciliation fixtures use IBM's REAL split history
(Yahoo: 1997-05-28 & 1999-05-27 2:1, both absent from our Massive-derived store). No
network — these are pure functions.
"""

from datetime import date

import pytest

from clients.yahoo_basis import classify_existing_basis, reconcile_splits, reconstruct_raw_closes
from clients.yahoo_client import YahooBar, YahooSplit


def _bars(pairs):
    return [YahooBar(date.fromisoformat(d), c, c) for d, c in pairs]


def test_reconstruct_reverse_split_recovers_known_raw():
    # Real AMC: split-adjusted close 19.60 on 2023-08-23 → raw ~1.96; 14.37 on ex-date.
    bars = _bars([("2023-08-23", 19.60), ("2023-08-24", 14.37), ("2023-08-25", 12.43)])
    splits = [YahooSplit(date(2023, 8, 24), 1.0, 10.0)]
    raw = reconstruct_raw_closes(bars, splits)
    assert raw[date(2023, 8, 23)] == pytest.approx(1.96)  # pre-split ×0.1
    assert raw[date(2023, 8, 24)] == pytest.approx(14.37)  # ex-date, no fold ahead
    assert raw[date(2023, 8, 25)] == pytest.approx(12.43)


def test_reconstruct_compounds_multiple_forward_splits():
    # A row before both IBM 2:1 splits carries a ×4 fold; between them ×2; after, ×1.
    bars = _bars([("1997-05-27", 100.0), ("1998-01-02", 100.0), ("1999-05-27", 100.0)])
    splits = [YahooSplit(date(1997, 5, 28), 2.0, 1.0), YahooSplit(date(1999, 5, 27), 2.0, 1.0)]
    raw = reconstruct_raw_closes(bars, splits)
    assert raw[date(1997, 5, 27)] == pytest.approx(400.0)  # both folds ahead
    assert raw[date(1998, 1, 2)] == pytest.approx(200.0)  # only the 1999 fold ahead
    assert raw[date(1999, 5, 27)] == pytest.approx(100.0)  # ex-date of the last split


def test_reconcile_flags_store_missing_real_splits():
    yahoo = [YahooSplit(date(1997, 5, 28), 2.0, 1.0), YahooSplit(date(1999, 5, 27), 2.0, 1.0)]
    store = [(date(1999, 5, 27), 2.0)]  # store has only the 1999 one, missing 1997
    result = reconcile_splits(yahoo, store)
    assert result.matched == [date(1999, 5, 27)]
    assert result.yahoo_only == [(date(1997, 5, 28), 2.0)]
    assert result.store_only == []
    assert not result.reconciled


def test_reconcile_flags_store_spurious_split():
    yahoo = [YahooSplit(date(1999, 5, 27), 2.0, 1.0)]
    store = [(date(1999, 5, 27), 2.0), (date(2020, 1, 3), 1.013)]  # 1.3% "split" Yahoo lacks
    result = reconcile_splits(yahoo, store)
    assert result.matched == [date(1999, 5, 27)]
    assert result.store_only == [(date(2020, 1, 3), 1.013)]
    assert result.yahoo_only == []


def test_reconcile_tolerates_a_one_day_ex_date_disagreement():
    yahoo = [YahooSplit(date(2021, 7, 20), 4.0, 1.0)]
    store = [(date(2021, 7, 21), 4.0)]  # provider stamped it a day later
    result = reconcile_splits(yahoo, store)
    assert result.matched == [date(2021, 7, 20)]
    assert result.reconciled


def test_reconcile_clean_match_is_reconciled():
    yahoo = [YahooSplit(date(2021, 7, 20), 4.0, 1.0)]
    store = [(date(2021, 7, 20), 4.0)]
    assert reconcile_splits(yahoo, store).reconciled


def test_reconcile_min_date_drops_prehistory_disagreements():
    # A split on/before the first stored row affects no stored row (reconstruct_raw_closes
    # and build_factor_intervals both apply only ex_date > bar_date), so a disagreement
    # there is immaterial. min_date bounds the reconciliation to in-history splits.
    yahoo = [YahooSplit(date(1997, 5, 28), 2.0, 1.0), YahooSplit(date(2023, 1, 3), 2.0, 1.0)]
    store = [
        (date(2003, 4, 4), 3.0),
        (date(2023, 1, 3), 2.0),
    ]  # prehistory split each side, Yahoo lacks store's, vice versa
    unbounded = reconcile_splits(yahoo, store)
    assert not unbounded.reconciled  # both prehistory splits show as divergences
    bounded = reconcile_splits(yahoo, store, min_date=date(2010, 1, 1))
    assert bounded.reconciled  # only the matching 2023 in-history split remains
    assert bounded.matched == [date(2023, 1, 3)]


def test_reconcile_min_date_keeps_in_history_disagreement():
    yahoo = [YahooSplit(date(2015, 6, 1), 2.0, 1.0)]  # in-history split store lacks
    store = []
    assert not reconcile_splits(yahoo, store, min_date=date(2010, 1, 1)).reconciled


# A 2:1 split on 1999-05-27: a 1998 row's raw close is 2× its split-adjusted close.
_YRAW = {date(1998, 1, 2): 200.0, date(2020, 1, 2): 50.0}
_YADJ = {date(1998, 1, 2): 100.0, date(2020, 1, 2): 50.0}  # no split after 2020 → raw == adjusted


def test_classify_row_already_raw_is_relabel():
    result = classify_existing_basis([{"trade_date": "1998-01-02", "close": 200.0}], _YRAW, _YADJ)
    assert result.relabel == [date(1998, 1, 2)] and result.clean


def test_classify_adjusted_row_is_rewrite():
    result = classify_existing_basis([{"trade_date": "1998-01-02", "close": 100.0}], _YRAW, _YADJ)
    assert result.rewrite == [date(1998, 1, 2)]


def test_classify_row_with_no_split_ahead_is_relabel():
    result = classify_existing_basis([{"trade_date": "2020-01-02", "close": 50.0}], _YRAW, _YADJ)
    assert result.relabel == [date(2020, 1, 2)] and not result.rewrite


def test_classify_neither_raw_nor_adjusted_is_mismatch():
    result = classify_existing_basis([{"trade_date": "1998-01-02", "close": 150.0}], _YRAW, _YADJ)
    assert not result.clean and result.mismatch[0][0] == date(1998, 1, 2)


def test_classify_date_absent_from_yahoo_is_unmatched():
    result = classify_existing_basis([{"trade_date": "1975-01-02", "close": 9.0}], _YRAW, _YADJ)
    assert result.unmatched == [date(1975, 1, 2)] and result.clean


def test_classify_penny_stock_within_a_cent_is_relabel_not_mismatch():
    # Real IBIO shape: bronze 0.245 vs Yahoo raw 0.25 is a 2% gap — a few tenths of a
    # cent — ordinary vendor close disagreement on a sub-dollar name, not a basis error.
    yraw = {date(2022, 8, 9): 0.25}
    yadj = {date(2022, 8, 9): 125.0}
    result = classify_existing_basis([{"trade_date": "2022-08-09", "close": 0.245}], yraw, yadj)
    assert result.relabel == [date(2022, 8, 9)] and result.clean


# --- IB anchor verdict (compare reconstruction to IB on the post-last-split window) ---

from clients.yahoo_basis import ib_anchor_verdict, last_split_ex_date  # noqa: E402

# Real frozen AMC raw closes across the 2023-08-24 1:10 reverse split.
_CORRECTED = [
    {"trade_date": date(2023, 8, 23), "close": 1.96},  # raw (pre-split, folded)
    {"trade_date": date(2023, 8, 24), "close": 14.37},  # ex-date
    {"trade_date": date(2023, 8, 25), "close": 12.43},  # post-split
    {"trade_date": date(2023, 8, 28), "close": 11.90},  # post-split
]
_LAST_SPLIT = date(2023, 8, 24)


def test_last_split_ex_date_picks_the_max():
    assert last_split_ex_date([(date(2020, 1, 2), 2.0), (date(2023, 8, 24), 0.1)]) == date(2023, 8, 24)
    assert last_split_ex_date([]) is None


def test_anchor_verified_when_ib_matches_post_split_window():
    ib = [
        {"trade_date": date(2023, 8, 25), "close": 12.43},
        {"trade_date": date(2023, 8, 28), "close": 11.90},
    ]
    v = ib_anchor_verdict(_CORRECTED, ib, last_split_ex=_LAST_SPLIT, min_overlap=2)
    assert v.verified and v.reason == "verified" and v.overlap == 2


def test_anchor_ignores_pre_split_rows_entirely():
    # IB carries a DIFFERENT (adjusted) pre-split value; the anchor must not look before the split.
    ib = [
        {"trade_date": date(2023, 8, 23), "close": 19.60},  # would mismatch if compared — but it is pre-split
        {"trade_date": date(2023, 8, 25), "close": 12.43},
        {"trade_date": date(2023, 8, 28), "close": 11.90},
    ]
    v = ib_anchor_verdict(_CORRECTED, ib, last_split_ex=_LAST_SPLIT, min_overlap=2)
    assert v.verified


def test_anchor_mismatch_when_ib_recent_close_disagrees():
    ib = [
        {"trade_date": date(2023, 8, 25), "close": 12.43},
        {"trade_date": date(2023, 8, 28), "close": 8.00},  # wrong entity / broken reconstruction
    ]
    v = ib_anchor_verdict(_CORRECTED, ib, last_split_ex=_LAST_SPLIT, min_overlap=2)
    assert not v.verified and v.reason == "ib_mismatch"
    assert v.mismatches == [(date(2023, 8, 28), 11.90, 8.00)]


def test_anchor_insufficient_overlap_fails_closed():
    ib = [{"trade_date": date(2023, 8, 25), "close": 12.43}]  # 1 < min_overlap
    v = ib_anchor_verdict(_CORRECTED, ib, last_split_ex=_LAST_SPLIT, min_overlap=5)
    assert not v.verified and v.reason == "ib_insufficient_overlap" and v.overlap == 1


def test_anchor_no_split_uses_full_overlap():
    corrected = [{"trade_date": date(2026, 7, d), "close": c} for d, c in [(13, 100.0), (14, 101.0), (15, 102.0)]]
    ib = [{"trade_date": date(2026, 7, d), "close": c} for d, c in [(13, 100.0), (14, 101.0), (15, 102.0)]]
    v = ib_anchor_verdict(corrected, ib, last_split_ex=None, min_overlap=3)
    assert v.verified and v.overlap == 3
