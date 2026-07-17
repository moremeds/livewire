"""Unit tests for Yahoo raw-close reconstruction and split reconciliation.

Reconstruction uses REAL AMC split-adjusted closes across its 2023-08-24 1:10 reverse
split (frozen 2026-07-17); the reconciliation fixtures use IBM's REAL split history
(Yahoo: 1997-05-28 & 1999-05-27 2:1, both absent from our Massive-derived store). No
network — these are pure functions.
"""

from datetime import date

import pytest

from clients.yahoo_basis import reconcile_splits, reconstruct_raw_closes
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
