from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.index_membership_store import IndexMembershipStore, MembershipEvent
from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from livewire_scripts.shepherd_daily import plan_daily, verify_daily_work_unit

KNOWN = datetime(2026, 8, 31, 1, 0, tzinfo=UTC)
SECURITY = "sec_00000000000000000000000000000001"


def _seed(
    root: Path,
    intervals: list[tuple[str, datetime, datetime | None]],
    *,
    security_id: str = SECURITY,
    known_at: datetime = KNOWN,
    membership_effective: datetime | None = None,
) -> None:
    evidence_store = SourceEvidenceStore(root)
    artifact = evidence_store.persist_raw(b"verified identity and membership source")
    evidence_store.record(
        SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url="https://www.sec.gov/Archives/fixture",
            retrieved_at=known_at,
            publication_time=known_at,
            mediawiki_revision_id=None,
            mediawiki_revision_time=None,
            content_type="application/json",
        )
    )

    def verify(ref, digest):
        return hashlib.sha256(evidence_store.read(ref)).hexdigest() == digest

    master = SecurityMaster(root, evidence_verifier=verify)
    for revision, (symbol, start, end) in enumerate(intervals, start=1):
        master.append(
            SecurityIdentityEvent(
                event_id=f"identity-{security_id}-{revision}",
                security_id=security_id,
                revision=revision,
                symbol=symbol,
                provider="listing",
                exchange_mic="XNAS",
                currency="USD",
                effective_from=start,
                effective_to=end,
                known_at=known_at,
                issuer_name="Fixture Corp",
                cik="0000000001",
                composite_figi=None,
                share_class_figi=None,
                continuity_basis="regulator_filing",
                relationship_type=None,
                related_security_id=None,
                source_refs=(artifact.ref,),
                source_hashes=(artifact.sha256,),
                status="verified",
                supersedes=None,
            )
        )
    memberships = IndexMembershipStore(root, security_master=master, evidence_verifier=verify)
    memberships.append(
        MembershipEvent(
            event_id=f"membership-{security_id}",
            index_id="sp500",
            security_id=security_id,
            action="add",
            announced_at=known_at,
            effective_at=membership_effective or intervals[-1][1],
            known_at=known_at,
            source_refs=(artifact.ref,),
            source_hashes=(artifact.sha256,),
            revision=1,
            supersedes=None,
            status="verified",
        )
    )


def _write(root: Path, symbol: str, dates: list[date], *, bad_ohlc: bool = False) -> Path:
    target = root / "bronze" / "asset_class=equity" / f"symbol={symbol}" / "1d.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    high = [9.0 if bad_ohlc else 12.0 for _ in dates]
    table = pa.table(
        {
            "trade_date": dates,
            "symbol_id": [1] * len(dates),
            "open": [10.0] * len(dates),
            "high": high,
            "low": [9.0] * len(dates),
            "close": [11.0] * len(dates),
            "adj_close": [11.0] * len(dates),
            "volume": [100] * len(dates),
            "source": ["ib"] * len(dates),
            "price_basis": ["raw"] * len(dates),
        }
    )
    pq.write_table(table, target)
    return target


def test_plan_uses_verified_identity_interval_as_denominator_and_never_a_glob(tmp_path: Path, monkeypatch) -> None:
    _seed(tmp_path, [("AAPL", datetime(2026, 8, 28, tzinfo=UTC), None)])
    _write(tmp_path, "AAPL", [date(2026, 8, 28), date(2026, 8, 31)])
    calls = []
    from clients.duckdb_catalog import symbol_files as real_symbol_files

    def observed(view, symbols, **kwargs):
        calls.append((view, list(symbols)))
        return real_symbol_files(view, symbols, **kwargs)

    monkeypatch.setattr("livewire_scripts.shepherd_daily.symbol_files", observed)
    result = plan_daily("sp500", 1, date(2026, 8, 31), data_lake_root=tmp_path)

    assert calls == [("bronze_equity_1d", ["AAPL"])]
    unit = result["workUnits"][0]
    assert unit["coverageState"] == "VERIFIED"
    assert unit["expectedSessions"] == 2
    assert unit["firstDate"] == "2026-08-28"
    assert unit["lastDate"] == "2026-08-31"
    assert unit["gaps"] == []
    assert unit["provenanceMix"] == {"ib/raw": 2}
    assert unit["nextOperation"] == {"kind": "none"}
    assert result["mutated"] is False


def test_missing_file_and_post_listing_gap_have_exact_next_operations(tmp_path: Path) -> None:
    _seed(tmp_path, [("AAPL", datetime(2026, 8, 28, tzinfo=UTC), None)])
    missing = plan_daily("sp500", 1, date(2026, 8, 31), data_lake_root=tmp_path)["workUnits"][0]
    assert missing["coverageState"] == "MISSING"
    assert missing["nextOperation"]["kind"] == "fetch-deep-history"

    _write(tmp_path, "AAPL", [date(2026, 8, 28)])
    incomplete = plan_daily("sp500", 1, date(2026, 8, 31), data_lake_root=tmp_path)["workUnits"][0]
    assert incomplete["coverageState"] == "INCOMPLETE"
    assert incomplete["gaps"] == [{"start": "2026-08-31", "end": "2026-08-31", "count": 1}]
    assert incomplete["nextOperation"]["kind"] == "fetch-missing-daily"


def test_rename_produces_one_unit_per_identity_interval_and_prelisting_is_not_a_gap(tmp_path: Path) -> None:
    _seed(
        tmp_path,
        [
            ("FB", datetime(2022, 6, 7, tzinfo=UTC), datetime(2022, 6, 9, tzinfo=UTC)),
            ("META", datetime(2022, 6, 9, tzinfo=UTC), None),
        ],
        known_at=datetime(2022, 6, 7, tzinfo=UTC),
    )
    _write(tmp_path, "FB", [date(2022, 6, 7), date(2022, 6, 8)])
    _write(tmp_path, "META", [date(2022, 6, 9), date(2022, 6, 10)])

    result = plan_daily("sp500", 1, date(2022, 6, 10), data_lake_root=tmp_path)
    assert [(unit["symbol"], unit["expectedSessions"], unit["gaps"]) for unit in result["workUnits"]] == [
        ("FB", 2, []),
        ("META", 2, []),
    ]


@pytest.mark.parametrize("problem", ["post-delisting", "duplicate", "bad-ohlc", "ticker-reuse"])
def test_verify_rejects_rows_outside_identity_or_invalid_parquet(tmp_path: Path, problem: str) -> None:
    start = datetime(2026, 8, 28, tzinfo=UTC)
    end = datetime(2026, 9, 1, tzinfo=UTC)
    _seed(tmp_path, [("AAPL", start, end)])
    dates = [date(2026, 8, 28), date(2026, 8, 31)]
    if problem in {"post-delisting", "ticker-reuse"}:
        dates.append(date(2026, 9, 1))
    if problem == "duplicate":
        dates.append(date(2026, 8, 31))
    _write(tmp_path, "AAPL", dates, bad_ohlc=problem == "bad-ohlc")
    unit = plan_daily("sp500", 1, date(2026, 8, 31), data_lake_root=tmp_path)["workUnits"][0]

    receipt = verify_daily_work_unit(unit, data_lake_root=tmp_path)
    assert receipt["coverageState"] == "CORRUPT"
    assert receipt["nextOperation"]["kind"] == "quarantine-and-refetch"
    assert receipt["violations"]


def test_verify_detects_file_change_after_plan(tmp_path: Path) -> None:
    _seed(tmp_path, [("AAPL", datetime(2026, 8, 28, tzinfo=UTC), None)])
    _write(tmp_path, "AAPL", [date(2026, 8, 28)])
    unit = plan_daily("sp500", 1, date(2026, 8, 31), data_lake_root=tmp_path)["workUnits"][0]
    _write(tmp_path, "AAPL", [date(2026, 8, 28), date(2026, 8, 31)])

    receipt = verify_daily_work_unit(unit, data_lake_root=tmp_path)
    assert receipt["fileChangedSincePlan"] is True
    assert receipt["coverageState"] == "VERIFIED"
    assert receipt["changedPaths"] == []


def test_verify_detects_file_created_after_missing_plan(tmp_path: Path) -> None:
    _seed(tmp_path, [("AAPL", datetime(2026, 8, 28, tzinfo=UTC), None)])
    unit = plan_daily("sp500", 1, date(2026, 8, 31), data_lake_root=tmp_path)["workUnits"][0]
    assert unit["parquetHash"] is None
    _write(tmp_path, "AAPL", [date(2026, 8, 28), date(2026, 8, 31)])

    receipt = verify_daily_work_unit(unit, data_lake_root=tmp_path)
    assert receipt["fileChangedSincePlan"] is True
    assert receipt["coverageState"] == "VERIFIED"


def test_verify_rejects_self_consistent_but_unregistered_identity_scope(tmp_path: Path) -> None:
    _seed(tmp_path, [("AAPL", datetime(2026, 8, 28, tzinfo=UTC), None)])
    _write(tmp_path, "AAPL", [date(2026, 8, 28), date(2026, 8, 31)])
    unit = plan_daily("sp500", 1, date(2026, 8, 31), data_lake_root=tmp_path)["workUnits"][0]
    forged = dict(unit)
    forged["identityEventId"] = "forged-identity"
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
    target = {key: forged[key] for key in target_keys}
    encoded = (json.dumps(target, sort_keys=True, separators=(",", ":")) + "\n").encode()
    forged["scopeHash"] = f"sha256:{hashlib.sha256(encoded).hexdigest()}"

    with pytest.raises(ValueError, match="registered identity interval"):
        verify_daily_work_unit(forged, data_lake_root=tmp_path)


def test_verify_rechecks_membership_and_identity_raw_evidence(tmp_path: Path) -> None:
    _seed(tmp_path, [("AAPL", datetime(2026, 8, 28, tzinfo=UTC), None)])
    _write(tmp_path, "AAPL", [date(2026, 8, 28), date(2026, 8, 31)])
    unit = plan_daily("sp500", 1, date(2026, 8, 31), data_lake_root=tmp_path)["workUnits"][0]
    source = SourceEvidenceStore(tmp_path).list_verified()[0]
    SourceEvidenceStore(tmp_path).raw_path(source.sha256).write_bytes(b"tampered")

    with pytest.raises(ValueError, match="evidence"):
        verify_daily_work_unit(unit, data_lake_root=tmp_path)


def test_wrong_parquet_schema_is_corrupt_not_merely_incomplete(tmp_path: Path) -> None:
    _seed(tmp_path, [("AAPL", datetime(2026, 8, 28, tzinfo=UTC), None)])
    path = _write(tmp_path, "AAPL", [date(2026, 8, 28), date(2026, 8, 31)])
    table = pq.read_table(path).set_column(7, "volume", pa.array([1.5, 2.5], type=pa.float64()))
    pq.write_table(table, path)

    unit = plan_daily("sp500", 1, date(2026, 8, 31), data_lake_root=tmp_path)["workUnits"][0]
    assert unit["coverageState"] == "CORRUPT"
    assert any("schema type" in item for item in unit["violations"])
