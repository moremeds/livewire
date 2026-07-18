from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from clients.corporate_action_store import CorporateActionStore, SplitAddition
from clients.massive_client import MassiveDividend, MassiveSplit

FETCHED_AT = datetime(2026, 7, 13, 1, 2, 3, tzinfo=UTC)


def _split(**changes) -> MassiveSplit:
    event = MassiveSplit(
        provider_event_id="split-1",
        ticker="NVDA",
        execution_date=date(2024, 6, 10),
        split_from=Decimal("1"),
        split_to=Decimal("10"),
        payload_hash="hash-v1",
    )
    return replace(event, **changes)


def _dividend(**changes) -> MassiveDividend:
    event = MassiveDividend(
        provider_event_id="div-1",
        ticker="NVDA",
        ex_dividend_date=date(2024, 6, 11),
        cash_amount=Decimal("0.01"),
        currency="USD",
        declaration_date=None,
        record_date=date(2024, 6, 11),
        pay_date=date(2024, 6, 28),
        payload_hash="div-hash-v1",
    )
    return replace(event, **changes)


def test_first_reconcile_inserts_canonical_rows(tmp_path):
    store = CorporateActionStore(tmp_path)

    result = store.reconcile("NVDA", [_split(), _dividend()], FETCHED_AT)

    assert result.inserted == 2
    assert result.revised == result.cancelled == result.unchanged == 0
    active = store.latest_active("NVDA")
    assert [row.action_type for row in active] == ["split", "cash_dividend"]
    assert all(row.event_revision == 1 and row.status == "active" for row in active)
    path = tmp_path / "bronze/asset_class=corporate_action/symbol=NVDA/events.parquet"
    assert pq.read_schema(path).names == list(store.schema.names)


def test_unchanged_payload_is_noop(tmp_path):
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split()], FETCHED_AT)
    original = store.path_for("NVDA").read_bytes()

    result = store.reconcile("NVDA", [_split()], FETCHED_AT)

    assert result.unchanged == 1
    assert result.changed is False
    assert store.path_for("NVDA").read_bytes() == original


def test_dry_run_compares_without_creating_artifacts(tmp_path):
    store = CorporateActionStore(tmp_path)

    result = store.reconcile("NVDA", [_split()], FETCHED_AT, dry_run=True)

    assert result.inserted == 1
    assert not store.path_for("NVDA").parent.exists()


def test_corrected_payload_creates_revision_lineage(tmp_path):
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split()], FETCHED_AT)

    result = store.reconcile(
        "NVDA",
        [_split(split_to=Decimal("4"), payload_hash="hash-v2")],
        FETCHED_AT,
    )

    assert result.revised == 1
    rows = pq.ParquetFile(store.path_for("NVDA")).read().to_pylist()
    previous, current = sorted(rows, key=lambda row: row["event_revision"])
    assert [previous["event_revision"], current["event_revision"]] == [1, 2]
    assert previous["status"] == "corrected"
    assert current["status"] == "active"
    assert current["supersedes_action_id"] == previous["action_id"]
    assert current["action_id"] != previous["action_id"]


def test_full_reconcile_records_disappeared_event_as_cancelled(tmp_path):
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split()], FETCHED_AT)

    result = store.reconcile("NVDA", [], FETCHED_AT, full_reconcile=True)

    assert result.cancelled == 1
    assert store.latest_active("NVDA") == []
    rows = pq.ParquetFile(store.path_for("NVDA")).read().to_pylist()
    assert max(rows, key=lambda row: row["event_revision"])["status"] == "cancelled"


def test_targeted_reconcile_never_infers_cancellation(tmp_path):
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split()], FETCHED_AT)

    result = store.reconcile("NVDA", [], FETCHED_AT, full_reconcile=False)

    assert result.cancelled == 0
    assert store.latest_active("NVDA")[0].provider_event_id == "split-1"


def test_duplicate_provider_ids_are_rejected(tmp_path):
    store = CorporateActionStore(tmp_path)
    with pytest.raises(ValueError, match="duplicate provider event id"):
        store.reconcile("NVDA", [_split(), _split(payload_hash="other")], FETCHED_AT)


def test_event_ticker_must_match_reconciliation_symbol(tmp_path):
    store = CorporateActionStore(tmp_path)
    with pytest.raises(ValueError, match="ticker"):
        store.reconcile("AAPL", [_split()], FETCHED_AT)


def test_case_distinct_provider_symbols_publish_to_distinct_paths(tmp_path):
    store = CorporateActionStore(tmp_path)
    common = _split(ticker="BCPC", provider_event_id="common")
    preferred = _split(ticker="BCpC", provider_event_id="preferred")

    store.reconcile("BCPC", [common], FETCHED_AT)
    store.reconcile("BCpC", [preferred], FETCHED_AT)

    assert store.path_for("BCPC") != store.path_for("BCpC")
    assert store.latest_active("BCPC")[0].symbol == "BCPC"
    assert store.latest_active("BCpC")[0].symbol == "BCpC"


def test_repeated_full_reconcile_does_not_cancel_twice(tmp_path):
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split()], FETCHED_AT)
    store.reconcile("NVDA", [], FETCHED_AT, full_reconcile=True)

    result = store.reconcile("NVDA", [], FETCHED_AT, full_reconcile=True)

    assert result.changed is False
    assert result.cancelled == 0


def test_rows_publish_in_action_id_order_and_active_view_in_event_order(tmp_path):
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_dividend(), _split()], FETCHED_AT)

    rows = pq.ParquetFile(store.path_for("NVDA")).read().to_pylist()
    assert [row["action_id"] for row in rows] == sorted(row["action_id"] for row in rows)
    assert [row.action_type for row in store.latest_active("NVDA")] == ["split", "cash_dividend"]


def test_publish_failure_leaves_existing_file_unchanged(tmp_path, monkeypatch):
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split()], FETCHED_AT)
    original = store.path_for("NVDA").read_bytes()

    def fail_publish(*args, **kwargs):
        raise RuntimeError("publish failed")

    monkeypatch.setattr("clients.corporate_action_store.publish_parquet", fail_publish)
    with pytest.raises(RuntimeError, match="publish failed"):
        store.reconcile(
            "NVDA",
            [_split(split_to=Decimal("4"), payload_hash="hash-v2")],
            FETCHED_AT,
        )

    assert store.path_for("NVDA").read_bytes() == original


# --- apply_repairs (Yahoo split add / spurious cancel) -------------------------------

_FIXED_AT = datetime(2026, 7, 18, 0, 0, 0, tzinfo=UTC)


def test_apply_repairs_adds_reference_split(tmp_path):
    store = CorporateActionStore(tmp_path)
    result = store.apply_repairs(
        "NVDA",
        add_splits=[SplitAddition(date(2007, 9, 11), split_from=1.0, split_to=1.5)],
        cancel_ex_dates=[],
        fetched_at=_FIXED_AT,
    )
    assert result.added == 1 and result.cancelled == 0
    active = store.latest_active("NVDA")
    assert len(active) == 1
    added = active[0]
    assert added.provider == "yahoo" and added.action_type == "split"
    assert added.ex_date == date(2007, 9, 11)
    assert added.split_to / added.split_from == 1.5


def test_apply_repairs_cancels_spurious_split_but_keeps_lineage(tmp_path):
    store = CorporateActionStore(tmp_path)
    # A real Massive split at 2024-06-10 plus a spurious 1.03 stock-dividend-as-split.
    store.reconcile(
        "NVDA",
        [
            _split(provider_event_id="real", execution_date=date(2024, 6, 10)),
            _split(provider_event_id="spurious", execution_date=date(2023, 5, 5), split_from=Decimal("100"), split_to=Decimal("103"), payload_hash="spur"),
        ],
        FETCHED_AT,
    )
    result = store.apply_repairs("NVDA", add_splits=[], cancel_ex_dates=[date(2023, 5, 5)], fetched_at=_FIXED_AT)
    assert result.cancelled == 1 and result.added == 0
    active = store.latest_active("NVDA")
    assert [row.ex_date for row in active] == [date(2024, 6, 10)]  # spurious gone from active
    # lineage retained: the cancelled revision still exists on disk
    all_rows = pq.ParquetFile(store.path_for("NVDA")).read().to_pylist()
    assert any(r["status"] == "cancelled" and r["ex_date"] == date(2023, 5, 5) for r in all_rows)


def test_apply_repairs_add_and_cancel_in_one_mutation(tmp_path):
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split(provider_event_id="spur", execution_date=date(2019, 3, 1), split_from=Decimal("50"), split_to=Decimal("51"), payload_hash="s")], FETCHED_AT)
    result = store.apply_repairs(
        "NVDA",
        add_splits=[SplitAddition(date(2001, 6, 27), split_from=1.0, split_to=2.0)],
        cancel_ex_dates=[date(2019, 3, 1)],
        fetched_at=_FIXED_AT,
    )
    assert result.added == 1 and result.cancelled == 1
    assert [row.ex_date for row in store.latest_active("NVDA")] == [date(2001, 6, 27)]


def test_apply_repairs_dry_run_writes_nothing(tmp_path):
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split()], FETCHED_AT)
    before = store.path_for("NVDA").read_bytes()
    result = store.apply_repairs(
        "NVDA",
        add_splits=[SplitAddition(date(2000, 1, 3), split_from=1.0, split_to=2.0)],
        cancel_ex_dates=[date(2024, 6, 10)],
        fetched_at=_FIXED_AT,
        dry_run=True,
    )
    assert result.added == 1 and result.cancelled == 1
    assert store.path_for("NVDA").read_bytes() == before  # no mutation on dry-run


def test_apply_repairs_reinserting_active_split_is_noop(tmp_path):
    store = CorporateActionStore(tmp_path)
    add = [SplitAddition(date(2007, 9, 11), split_from=1.0, split_to=1.5)]
    store.apply_repairs("NVDA", add_splits=add, cancel_ex_dates=[], fetched_at=_FIXED_AT)
    second = store.apply_repairs("NVDA", add_splits=add, cancel_ex_dates=[], fetched_at=_FIXED_AT)
    assert second.added == 0  # already active → not re-added
    assert len(store.latest_active("NVDA")) == 1
