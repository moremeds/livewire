from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from livewire_scripts.run_ib_fetch_robust import OutcomeCategory, TickerOutcome
from livewire_scripts.shepherd_daily import fetch_daily_work_unit, plan_daily
from tests.test_shepherd_daily import _seed, _write


def _missing_unit(root: Path) -> dict:
    _seed(root, [("AAPL", datetime(2026, 8, 28, tzinfo=UTC), None)])
    return plan_daily("sp500", 1, date(2026, 8, 31), data_lake_root=root)["workUnits"][0]


def test_fetch_uses_single_forced_ib_attempt_and_names_only_challenge_sources(tmp_path: Path) -> None:
    unit = _missing_unit(tmp_path)
    calls: list[dict] = []

    def runner(**kwargs):
        calls.append(kwargs)
        _write(tmp_path, "AAPL", [date(2026, 8, 28), date(2026, 8, 31)])
        return TickerOutcome("AAPL", OutcomeCategory.OK, 1, 0.1, 0, 2, "rows +2")

    receipt, exit_code = fetch_daily_work_unit(
        unit,
        data_lake_root=tmp_path,
        operation_id="attempt-1",
        runner=runner,
    )

    assert exit_code == 0
    assert len(calls) == 1
    assert calls[0]["ticker"] == "AAPL"
    assert calls[0]["mode"] == "seed"
    assert calls[0]["source"] == "ib"
    assert calls[0]["max_attempts"] == 1
    assert calls[0]["cooldown"] == 0
    assert receipt["outcome"] == "completed"
    assert receipt["stateHint"] == "EVIDENCE_PENDING"
    assert receipt["summary"]["postFetchCoverageState"] == "VERIFIED"
    assert receipt["summary"]["deepHistoryAuthority"] == "ib"
    assert receipt["summary"]["challengeSources"] == ["massive", "yahoo"]
    assert receipt["summary"]["requestedInterval"] == {
        "start": "2026-08-28",
        "end": "2026-08-31",
    }


def test_fetch_maps_gateway_or_session_wait_to_typed_exit_75_without_retry(tmp_path: Path) -> None:
    unit = _missing_unit(tmp_path)
    calls = 0

    def runner(**kwargs):
        nonlocal calls
        calls += 1
        return TickerOutcome(
            "AAPL",
            OutcomeCategory.TEMPORARY_UNAVAILABLE,
            1,
            0.1,
            0,
            0,
            "ib-gateway-unavailable",
        )

    receipt, exit_code = fetch_daily_work_unit(
        unit,
        data_lake_root=tmp_path,
        operation_id="attempt-2",
        runner=runner,
    )

    assert calls == 1
    assert exit_code == 75
    assert receipt["outcome"] == "temporary-unavailable"
    assert receipt["stateHint"] == "AWAITING_USER"
    assert receipt["summary"]["reasonCode"] == "ib-gateway-unavailable"
    assert receipt["changedPaths"] == []


def test_session_loss_after_a_partial_write_is_quarantined_not_waiting(tmp_path: Path) -> None:
    unit = _missing_unit(tmp_path)

    def runner(**kwargs):
        _write(tmp_path, "AAPL", [date(2026, 8, 28)])
        return TickerOutcome(
            "AAPL",
            OutcomeCategory.TEMPORARY_UNAVAILABLE,
            1,
            0.1,
            0,
            1,
            "ib-session-lost",
        )

    receipt, exit_code = fetch_daily_work_unit(
        unit,
        data_lake_root=tmp_path,
        operation_id="attempt-partial",
        runner=runner,
    )

    assert exit_code == 1
    assert receipt["outcome"] == "unsafe"
    assert receipt["stateHint"] == "QUARANTINED"
    assert receipt["summary"]["reasonCode"] == "partial-write-before-ib-wait"
    assert len(receipt["changedPaths"]) == 1


def test_runner_success_without_verified_output_is_a_failure(tmp_path: Path) -> None:
    unit = _missing_unit(tmp_path)

    receipt, exit_code = fetch_daily_work_unit(
        unit,
        data_lake_root=tmp_path,
        operation_id="attempt-empty",
        runner=lambda **kwargs: TickerOutcome("AAPL", OutcomeCategory.OK, 1, 0.1, 0, 0, "exit 0"),
    )

    assert exit_code == 1
    assert receipt["outcome"] == "failed"
    assert receipt["stateHint"] == "UNRESOLVED"
    assert receipt["summary"]["postFetchCoverageState"] == "MISSING"


def test_fetch_revalidates_registered_scope_before_invoking_runner(tmp_path: Path) -> None:
    unit = _missing_unit(tmp_path)
    forged = dict(unit)
    forged["identityEventId"] = "forged"
    target_keys = {
        "indexId",
        "membershipRevision",
        "asOf",
        "securityId",
        "identityEventId",
        "symbol",
        "provider",
        "exchangeMic",
        "startDate",
        "endDate",
    }
    encoded = (
        __import__("json").dumps(
            {key: forged[key] for key in target_keys},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    forged["scopeHash"] = f"sha256:{hashlib.sha256(encoded).hexdigest()}"

    with pytest.raises(ValueError, match="registered identity interval"):
        fetch_daily_work_unit(
            forged,
            data_lake_root=tmp_path,
            operation_id="attempt-forged",
            runner=lambda **kwargs: (_ for _ in ()).throw(AssertionError("runner must not execute")),
        )


def test_prelisting_dates_are_not_added_to_the_fetch_scope(tmp_path: Path) -> None:
    unit = _missing_unit(tmp_path)
    assert unit["startDate"] == "2026-08-28"
    assert unit["gaps"] == [{"start": "2026-08-28", "end": "2026-08-31", "count": 2}]
