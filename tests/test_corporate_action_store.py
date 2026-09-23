from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from clients.corporate_action_store import CorporateAction, CorporateActionStore, DividendConversion, SplitAddition
from clients.massive_client import MassiveDividend, MassivePageEvidence, MassiveSplit

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


def test_reconcile_persists_raw_response_lineage(tmp_path):
    store = CorporateActionStore(tmp_path)
    event = _split(
        source_ref="artifact://massive/page-1",
        source_hash="a" * 64,
        source_fetched_at=FETCHED_AT,
        source_cursor_identity="sha256:" + "b" * 64,
    )

    store.reconcile("NVDA", [event], FETCHED_AT)

    row = store.history("NVDA")[0]
    assert row.source_ref == event.source_ref
    assert row.source_hash == event.source_hash
    assert row.source_fetched_at == FETCHED_AT
    assert row.source_cursor_identity == event.source_cursor_identity


def test_zero_event_fetch_retains_split_and_dividend_negative_evidence(tmp_path):
    store = CorporateActionStore(tmp_path)
    pages = [
        MassivePageEvidence("splits", "artifact://sha256/" + "a" * 64, "a" * 64, FETCHED_AT, "sha256:" + "1" * 64),
        MassivePageEvidence("dividends", "artifact://sha256/" + "b" * 64, "b" * 64, FETCHED_AT, "sha256:" + "2" * 64),
    ]

    receipt = store.record_fetch("NVDA", pages, FETCHED_AT, full_reconcile=True)

    assert receipt.resources == ("dividends", "splits")
    history = store.fetch_history("NVDA")
    assert len(history) == 1
    assert set(history[0].resources) == {"splits", "dividends"}
    assert history[0].full_reconcile is True


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
    assert previous["status"] == "active"  # append only: the superseded row is never rewritten
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
            _split(
                provider_event_id="spurious",
                execution_date=date(2023, 5, 5),
                split_from=Decimal("100"),
                split_to=Decimal("103"),
                payload_hash="spur",
            ),
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
    store.reconcile(
        "NVDA",
        [
            _split(
                provider_event_id="spur",
                execution_date=date(2019, 3, 1),
                split_from=Decimal("50"),
                split_to=Decimal("51"),
                payload_hash="s",
            )
        ],
        FETCHED_AT,
    )
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


def test_full_reconcile_leaves_another_provider_alone(tmp_path):
    """A Massive response says nothing about an event Massive was never asked for.

    The sweep used to cancel every active row whose id was absent from `events`,
    regardless of provider — so the Sunday `--full-reconcile` undid the yahoo
    splits `apply_repairs` had just added, every week. 507 of 1,014 yahoo splits
    were cancelled that way across 2026-07-19 (418) and 2026-07-26 (89).
    """
    store = CorporateActionStore(tmp_path)
    store.reconcile("NVDA", [_split()], FETCHED_AT)
    store.apply_repairs(
        "NVDA",
        add_splits=[SplitAddition(date(2007, 9, 11), split_from=1.0, split_to=1.5)],
        cancel_ex_dates=[],
        fetched_at=_FIXED_AT,
    )

    result = store.reconcile("NVDA", [], FETCHED_AT, full_reconcile=True)

    # The Massive row disappeared from the response and is cancelled; the yahoo
    # row was never in scope and survives.
    assert result.cancelled == 1
    survivors = store.latest_active("NVDA")
    assert [row.provider for row in survivors] == ["yahoo"]
    assert survivors[0].ex_date == date(2007, 9, 11)


# --- convert_dividends (eod_fx currency repair) -------------------------------------

# Real fixture values from grok_index/gaps/dividend_fx_conversion_applied.json[0]:
# ACR paid 0.41 CAD on 2008-06-26; USDCAD EOD close 1.0118999481201172 that day,
# method=divide -> 0.40517839808341516 USD.
_ACR_DIV = dict(
    provider_event_id="acr-div-1",
    ticker="ACR",
    ex_dividend_date=date(2008, 6, 26),
    cash_amount=Decimal("0.41"),
    currency="CAD",
    declaration_date=None,
    record_date=None,
    pay_date=None,
    payload_hash="acr-cad-v1",
)
_ACR_CONVERTED = 0.40517839808341516
_ACR_SOURCE_REF = "eod_fx:USDCAD@2008-06-26 rate=1.01189995 method=divide orig=0.41 CAD -> 0.40517840 USD"
_ACR_SOURCE_HASH = "f" * 64


def _acr_conversion(action_id: str, cash_amount: float = _ACR_CONVERTED) -> DividendConversion:
    return DividendConversion(
        action_id=action_id,
        cash_amount=cash_amount,
        currency="USD",
        source_ref=_ACR_SOURCE_REF,
        source_hash=_ACR_SOURCE_HASH,
    )


def _reconciled_acr(tmp_path) -> tuple[CorporateActionStore, CorporateAction]:
    store = CorporateActionStore(tmp_path)
    store.reconcile("ACR", [_dividend(**_ACR_DIV)], FETCHED_AT)
    return store, store.latest_active("ACR")[0]


def test_foreign_currency_dividends_lists_only_active_rows_in_another_currency(tmp_path):
    store = CorporateActionStore(tmp_path)
    store.reconcile(
        "ACR",
        [
            _dividend(**_ACR_DIV),
            _dividend(
                provider_event_id="acr-div-usd",
                ticker="ACR",
                ex_dividend_date=date(2008, 9, 25),
                cash_amount=Decimal("0.40"),
                currency="USD",
                payload_hash="acr-usd-v1",
            ),
            _split(provider_event_id="acr-split", ticker="ACR", payload_hash="acr-split-v1"),
        ],
        FETCHED_AT,
    )

    foreign = store.foreign_currency_dividends("ACR", "USD")
    assert [row.provider_event_id for row in foreign] == ["acr-div-1"]
    assert foreign[0].currency == "CAD" and foreign[0].cash_amount == 0.41

    # Once converted the row reads USD and is no longer foreign-currency.
    store.apply_repairs(
        "ACR",
        add_splits=[],
        cancel_ex_dates=[],
        convert_dividends=[_acr_conversion(foreign[0].action_id)],
        fetched_at=_FIXED_AT,
    )
    assert store.foreign_currency_dividends("ACR", "USD") == []


def test_convert_dividend_supersedes_with_provider_eod_fx(tmp_path):
    store, old = _reconciled_acr(tmp_path)

    result = store.apply_repairs(
        "ACR",
        add_splits=[],
        cancel_ex_dates=[],
        convert_dividends=[_acr_conversion(old.action_id)],
        fetched_at=_FIXED_AT,
    )

    assert result.converted == 1 and result.changed is True
    active = store.latest_active("ACR")
    assert len(active) == 1
    row = active[0]
    assert row.provider == "eod_fx"
    assert row.provider_event_id == old.provider_event_id
    assert row.event_revision == old.event_revision + 1
    assert row.supersedes_action_id == old.action_id
    assert row.cash_amount == _ACR_CONVERTED
    assert row.currency == "USD"
    assert row.source_ref == _ACR_SOURCE_REF
    assert row.source_hash == _ACR_SOURCE_HASH
    assert row.ex_date == date(2008, 6, 26)
    history = {r.action_id: r for r in store.history("ACR")}
    assert history[old.action_id] == old  # append only: the superseded head is never rewritten
    assert history[row.action_id].status == "active"


def test_convert_is_idempotent(tmp_path):
    store, old = _reconciled_acr(tmp_path)
    conv = _acr_conversion(old.action_id)
    store.apply_repairs("ACR", add_splits=[], cancel_ex_dates=[], convert_dividends=[conv], fetched_at=_FIXED_AT)
    rows_after_first = store.path_for("ACR").read_bytes()

    second = store.apply_repairs(
        "ACR",
        add_splits=[],
        cancel_ex_dates=[],
        convert_dividends=[conv, _acr_conversion("no-such-action")],
        fetched_at=_FIXED_AT,
    )
    assert second.converted == 0 and second.changed is False
    assert store.path_for("ACR").read_bytes() == rows_after_first
    assert len(store.latest_active("ACR")) == 1


def test_full_reconcile_after_conversion_leaves_the_eod_fx_row_active(tmp_path):
    """pm:2026-09-13-corporate-actions-mutated-outside-the-ledger — a Sunday
    --full-reconcile fed the original CAD event must not revert the conversion."""
    store, old = _reconciled_acr(tmp_path)
    store.apply_repairs(
        "ACR",
        add_splits=[],
        cancel_ex_dates=[],
        convert_dividends=[_acr_conversion(old.action_id)],
        fetched_at=_FIXED_AT,
    )
    rows_before = store.path_for("ACR").read_bytes()

    result = store.reconcile("ACR", [_dividend(**_ACR_DIV)], FETCHED_AT, full_reconcile=True)

    assert result.changed is False
    active = store.latest_active("ACR")
    assert len(active) == 1
    assert active[0].provider == "eod_fx" and active[0].cash_amount == _ACR_CONVERTED
    assert store.path_for("ACR").read_bytes() == rows_before  # nothing appended
