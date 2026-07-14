from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from clients.corporate_action_store import CorporateActionStore
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
