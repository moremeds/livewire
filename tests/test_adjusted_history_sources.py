from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from clients.corporate_action_store import CorporateAction
from clients.massive_client import MassiveDailyBar, MassiveDividend, MassiveSMAValue, MassiveSplit
from livewire_scripts.adjusted_history_sources import (
    CACHE_VERSION,
    fetch_ib_evidence,
    fetch_massive_action_evidence,
    fetch_massive_evidence,
    load_cached_evidence,
    write_cached_evidence,
)


def _row(trade_date: date, close: float) -> dict:
    return {
        "trade_date": trade_date,
        "symbol_id": 1,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "adj_close": close,
        "volume": 100,
        "source": "ib",
        "price_basis": "split_adjusted",
        "currency": "USD",
    }


def _action(action_id: str, action_type: str, ex_date: date, **changes) -> CorporateAction:
    values = {
        "action_id": action_id,
        "provider": "massive",
        "provider_event_id": action_id,
        "event_revision": 1,
        "supersedes_action_id": None,
        "symbol": "TEST",
        "action_type": action_type,
        "ex_date": ex_date,
        "split_from": None,
        "split_to": None,
        "cash_amount": None,
        "currency": None,
        "declaration_date": None,
        "record_date": None,
        "pay_date": None,
        "status": "active",
        "fetched_at": datetime(2024, 1, 1, tzinfo=UTC),
        "payload_hash": action_id,
    }
    values.update(changes)
    return CorporateAction(**values)


class _Massive:
    def get_daily_bars(self, ticker, start, end, *, adjusted):
        assert (ticker, adjusted) == ("TEST", True)
        return [MassiveDailyBar(date(2024, 1, 3), 10, 10, 10, 10, 100)]

    def get_sma(self, ticker, window, start, end):
        return [MassiveSMAValue(date(2024, 1, 3), float(window))]

    def get_splits(self, ticker):
        return [MassiveSplit("split", ticker, date(2024, 1, 3), Decimal("1"), Decimal("2"), "split")]

    def get_dividends(self, ticker):
        return [
            MassiveDividend(
                "div",
                ticker,
                date(2024, 1, 4),
                Decimal("1"),
                "USD",
                None,
                None,
                None,
                "div",
                Decimal("0.98"),
            )
        ]


def test_massive_evidence_preserves_partial_range_and_smas() -> None:
    evidence = fetch_massive_evidence(
        _Massive(),
        "TEST",
        date(2024, 1, 2),
        date(2024, 1, 5),
        windows=(20, 50),
    )

    assert evidence.status == "ok"
    assert evidence.requested_start == date(2024, 1, 2)
    assert evidence.actual_start == evidence.actual_end == date(2024, 1, 3)
    assert evidence.complete_range is False
    assert evidence.sma == {20: {date(2024, 1, 3): 20.0}, 50: {date(2024, 1, 3): 50.0}}


def test_massive_sma_error_does_not_discard_valid_adjusted_bars() -> None:
    class MassiveWithUnavailableSMA(_Massive):
        def get_sma(self, ticker, window, start, end):
            raise RuntimeError("indicator entitlement unavailable")

    evidence = fetch_massive_evidence(
        MassiveWithUnavailableSMA(),
        "TEST",
        date(2024, 1, 2),
        date(2024, 1, 5),
        windows=(20,),
    )

    assert evidence.status == "ok"
    assert len(evidence.rows) == 1
    assert evidence.sma == {}
    assert evidence.sma_errors == {20: "indicator entitlement unavailable"}


def test_ib_evidence_classifies_and_normalizes_adjusted_split_history() -> None:
    split = _action("split", "split", date(2024, 1, 3), split_from=1.0, split_to=2.0)
    requested = []

    def fetcher(symbol, start, end):
        requested.append((symbol, start, end))
        return [_row(date(2024, 1, 2), 50), _row(date(2024, 1, 3), 51)]

    evidence = fetch_ib_evidence(
        fetcher,
        "TEST",
        date(2024, 1, 2),
        date(2024, 1, 3),
        [split],
        date(2024, 1, 3),
    )

    assert requested == [("TEST", date(2023, 12, 20), date(2024, 1, 3))]
    assert evidence.status == "ok"
    assert [row["close"] for row in evidence.rows] == pytest.approx([50, 51])
    assert all(row["price_basis"] == "split_adjusted" for row in evidence.rows)


def test_ib_timeout_is_a_terminal_source_state() -> None:
    def fetcher(symbol, start, end):
        raise TimeoutError("paced out")

    evidence = fetch_ib_evidence(fetcher, "TEST", date(2024, 1, 2), date(2024, 1, 3), [], date(2024, 1, 3))

    assert evidence.status == "timeout"
    assert evidence.rows == ()
    assert "paced out" in evidence.error


def test_massive_action_evidence_compares_active_inventory_and_factors() -> None:
    local = [
        _action("split-local", "split", date(2024, 1, 3), provider_event_id="split", split_from=1.0, split_to=2.0),
        _action(
            "div-local",
            "cash_dividend",
            date(2024, 1, 4),
            provider_event_id="div",
            cash_amount=1.0,
            currency="USD",
        ),
    ]

    evidence = fetch_massive_action_evidence(_Massive(), "TEST", local, date(2024, 1, 5))

    assert evidence.status == "complete"
    assert evidence.missing_local_ids == ()
    assert evidence.unexpected_provider_ids == ()
    assert evidence.historical_adjustment_factors == {"div": "0.98"}


def test_cache_is_atomic_content_checked_and_contains_no_credentials(tmp_path) -> None:
    path = tmp_path / "evidence.json"
    identity = {"version": CACHE_VERSION, "symbol": "TEST", "as_of_date": "2024-01-05"}
    payload = {"status": "ok", "rows": [{"trade_date": "2024-01-03", "close": 10}]}

    write_cached_evidence(path, identity, payload)

    assert load_cached_evidence(path, identity) == payload
    text = path.read_text()
    assert "MASSIVE_API_KEY" not in text
    assert "Authorization" not in text
    assert not list(tmp_path.glob(".*.tmp"))

    envelope = json.loads(text)
    envelope["payload"]["status"] = "tampered"
    path.write_text(json.dumps(envelope))
    with pytest.raises(ValueError, match="hash"):
        load_cached_evidence(path, identity)


def test_cache_identity_mismatch_is_rejected(tmp_path) -> None:
    path = tmp_path / "evidence.json"
    identity = {"version": CACHE_VERSION, "symbol": "TEST"}
    write_cached_evidence(path, identity, {"status": "ok"})

    with pytest.raises(ValueError, match="identity"):
        load_cached_evidence(path, {"version": CACHE_VERSION, "symbol": "OTHER"})
