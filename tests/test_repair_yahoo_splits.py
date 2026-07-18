"""Unit tests for the evidence-gated Yahoo -> action-store split repair.

Fixtures are hand-built to isolate each verdict; the Yahoo client is a fake returning
frozen bars/splits (no network). Store splits are seeded via the real store.
"""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveSplit
from clients.yahoo_client import YahooBar, YahooSplit
from livewire_scripts import repair_yahoo_splits

AS_OF = date(2026, 7, 17)
FETCHED_AT = datetime(2026, 1, 4, tzinfo=UTC)


class _FakeYahoo:
    def __init__(self, bars, splits):
        self._bars, self._splits = bars, splits

    def get_daily(self, symbol, start, end):
        return self._bars, self._splits


def _flat_bars(dates, close=10.0):
    return [YahooBar(date.fromisoformat(d), close, close) for d in dates]


def _seed_store_split(root, symbol, ex_date, split_from, split_to, event_id):
    CorporateActionStore(root).reconcile(
        symbol,
        [
            MassiveSplit(
                provider_event_id=event_id,
                ticker=symbol,
                execution_date=date.fromisoformat(ex_date),
                split_from=Decimal(str(split_from)),
                split_to=Decimal(str(split_to)),
                payload_hash=f"{event_id}-h",
            )
        ],
        FETCHED_AT,
    )


def _plan(tmp_path, bars, splits):
    store = CorporateActionStore(tmp_path)
    return repair_yahoo_splits.plan_symbol("TEST", store=store, yahoo=_FakeYahoo(bars, splits), as_of=AS_OF)


def test_plan_already_reconciled(tmp_path):
    _seed_store_split(tmp_path, "TEST", "2001-06-27", 1, 2, "s1")
    plan = _plan(tmp_path, _flat_bars(["2001-06-26", "2001-06-27"]), [YahooSplit(date(2001, 6, 27), 2.0, 1.0)])
    assert plan["status"] == "already_reconciled"


def test_plan_adds_missing_yahoo_split(tmp_path):
    # Store empty; Yahoo has a 2:1 split → ADD it, symbol would reconcile.
    plan = _plan(tmp_path, _flat_bars(["2001-06-26", "2001-06-27"]), [YahooSplit(date(2001, 6, 27), 2.0, 1.0)])
    assert plan["status"] == "would_reconcile"
    assert plan["adds"] == [{"ex_date": "2001-06-27", "split_from": 1.0, "split_to": 2.0, "ratio": 2.0}]
    assert plan["cancel_safe"] == [] and plan["kept_ambiguous"] == []


def test_plan_cancels_stock_dividend_ratio_near_one(tmp_path):
    # Store has a 1.03 "split" (scrip dividend); Yahoo has none → safe cancel.
    _seed_store_split(tmp_path, "TEST", "2016-05-23", 100, 103, "sd")
    plan = _plan(tmp_path, _flat_bars(["2016-05-20", "2016-05-23"]), [])
    assert plan["status"] == "would_reconcile"
    assert plan["cancel_safe"] == [{"ex_date": "2016-05-23", "ratio": 1.03, "reason": "stock_dividend"}]
    assert plan["cancel_phantom"] == [] and plan["kept_ambiguous"] == []


def test_plan_cancels_phantom_when_yahoo_raw_smooth(tmp_path):
    # Store claims a 10:1 reverse (ratio 0.1) Yahoo lacks; Yahoo's raw close is flat across
    # the ex-date → no real event → phantom → cancel.
    _seed_store_split(tmp_path, "TEST", "2004-05-10", 10, 1, "ph")
    plan = _plan(tmp_path, _flat_bars(["2004-05-07", "2004-05-10", "2004-05-11"]), [])
    assert plan["status"] == "would_reconcile"
    assert plan["cancel_phantom"] and plan["cancel_phantom"][0]["ex_date"] == "2004-05-10"
    assert plan["kept_ambiguous"] == []


def test_plan_keeps_ambiguous_when_yahoo_shows_fold(tmp_path):
    # Store claims a 2:1 (ratio 2.0) Yahoo lacks, BUT Yahoo's raw close actually halves at
    # the ex-date (a fold Yahoo didn't record) → could be a real split → keep, do not cancel.
    _seed_store_split(tmp_path, "TEST", "2010-03-15", 1, 2, "amb")
    bars = [
        YahooBar(date(2010, 3, 12), 100.0, 100.0),
        YahooBar(date(2010, 3, 15), 50.0, 50.0),
        YahooBar(date(2010, 3, 16), 50.0, 50.0),
    ]
    plan = repair_yahoo_splits.plan_symbol(
        "TEST", store=CorporateActionStore(tmp_path), yahoo=_FakeYahoo(bars, []), as_of=AS_OF
    )
    assert plan["status"] == "partial"
    assert plan["kept_ambiguous"] and plan["kept_ambiguous"][0]["ex_date"] == "2010-03-15"
    assert plan["cancel_phantom"] == []


def test_plan_keeps_when_boundary_outside_yahoo_coverage(tmp_path):
    # Spurious split predates every Yahoo bar → unverifiable → keep.
    _seed_store_split(tmp_path, "TEST", "1995-01-03", 10, 1, "old")
    plan = _plan(tmp_path, _flat_bars(["2004-05-07", "2004-05-10"]), [])
    assert plan["status"] == "partial"
    assert plan["kept_ambiguous"][0]["evidence"] == "no_yahoo_boundary"


def test_plan_yahoo_missing(tmp_path):
    class _Missing:
        def get_daily(self, *a):
            from clients.yahoo_client import YahooNotFound

            raise YahooNotFound("TEST")

    plan = repair_yahoo_splits.plan_symbol("TEST", store=CorporateActionStore(tmp_path), yahoo=_Missing(), as_of=AS_OF)
    assert plan["status"] == "yahoo_missing"


def test_apply_mutates_store_and_rollback_restores(tmp_path):
    # Store has a phantom 10:1 reverse; Yahoo has a real 2:1 the store lacks.
    _seed_store_split(tmp_path, "TEST", "2004-05-10", 10, 1, "ph")
    bars = _flat_bars(["2001-06-26", "2001-06-27", "2004-05-10", "2004-05-11"])
    splits = [YahooSplit(date(2001, 6, 27), 2.0, 1.0)]
    output_dir = tmp_path / "repair"
    store = CorporateActionStore(tmp_path)
    before = store.path_for("TEST").read_bytes()

    rc = repair_yahoo_splits.run(
        ["--tickers", "TEST", "--output", str(tmp_path / "m.json"), "--apply", "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        yahoo_factory=lambda: _FakeYahoo(bars, splits),
        as_of_date=AS_OF,
    )
    assert rc == 0
    active = {(a.ex_date, round(a.split_to / a.split_from, 4)) for a in store.latest_active("TEST")}
    assert (date(2001, 6, 27), 2.0) in active  # added
    assert not any(ex == date(2004, 5, 10) for ex, _ in active)  # phantom cancelled

    # rollback restores the original store bytes
    assert (
        repair_yahoo_splits.run(
            ["--rollback", "--output", str(tmp_path / "m2.json"), "--output-dir", str(output_dir)],
            data_lake_root=tmp_path,
        )
        == 0
    )
    assert store.path_for("TEST").read_bytes() == before


def test_apply_requires_output_dir(tmp_path):
    with pytest.raises(ValueError, match="output-dir"):
        repair_yahoo_splits.run(
            ["--tickers", "TEST", "--output", str(tmp_path / "m.json"), "--apply"],
            data_lake_root=tmp_path,
            yahoo_factory=lambda: _FakeYahoo([], []),
            as_of_date=AS_OF,
        )


def test_reads_symbols_file(tmp_path):
    _seed_store_split(tmp_path, "TEST", "2016-05-23", 100, 103, "sd")
    sf = tmp_path / "syms.json"
    sf.write_text('{"split_mismatch": ["TEST"]}')
    rc = repair_yahoo_splits.run(
        ["--symbols-file", str(sf), "--output", str(tmp_path / "m.json")],
        data_lake_root=tmp_path,
        yahoo_factory=lambda: _FakeYahoo(_flat_bars(["2016-05-20", "2016-05-23"]), []),
        as_of_date=AS_OF,
    )
    assert rc == 0
    import json

    assert json.loads((tmp_path / "m.json").read_text())["counts"]["would_reconcile"] == 1


def test_plan_yahoo_error(tmp_path):
    class _Err:
        def get_daily(self, *a):
            from clients.yahoo_client import YahooError

            raise YahooError("boom")

    plan = repair_yahoo_splits.plan_symbol("TEST", store=CorporateActionStore(tmp_path), yahoo=_Err(), as_of=AS_OF)
    assert plan["status"] == "yahoo_error"


def test_plan_yahoo_empty(tmp_path):
    assert _plan(tmp_path, [], [])["status"] == "yahoo_empty"


def test_plan_nonpositive_raw_is_kept(tmp_path):
    _seed_store_split(tmp_path, "TEST", "2004-05-10", 10, 1, "z")
    bars = [YahooBar(date(2004, 5, 7), 0.0, 0.0), YahooBar(date(2004, 5, 10), 0.0, 0.0)]
    plan = repair_yahoo_splits.plan_symbol(
        "TEST", store=CorporateActionStore(tmp_path), yahoo=_FakeYahoo(bars, []), as_of=AS_OF
    )
    assert plan["kept_ambiguous"][0]["evidence"] == "nonpositive_raw"


def test_run_survives_bad_symbol(tmp_path):
    class _Boom:
        def get_daily(self, *a):
            raise RuntimeError("x")

    rc = repair_yahoo_splits.run(
        ["--tickers", "TEST", "--output", str(tmp_path / "m.json")],
        data_lake_root=tmp_path,
        yahoo_factory=lambda: _Boom(),
        as_of_date=AS_OF,
    )
    assert rc == 0
    import json

    assert json.loads((tmp_path / "m.json").read_text())["counts"]["error"] == 1


def test_rollback_requires_output_dir(tmp_path):
    with pytest.raises(ValueError, match="rollback requires"):
        repair_yahoo_splits.run(["--rollback", "--output", str(tmp_path / "m.json")], data_lake_root=tmp_path)


def test_no_symbols_source_raises(tmp_path):
    with pytest.raises(ValueError, match="--tickers or --symbols-file"):
        repair_yahoo_splits.run(
            ["--output", str(tmp_path / "m.json")], data_lake_root=tmp_path, yahoo_factory=lambda: _FakeYahoo([], [])
        )
