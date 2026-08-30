#!/usr/bin/env python3
"""Plan and independently verify PIT-scoped current-member daily coverage."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from clients.bronze_client import EQUITY_SOURCES, PRICE_BASES
from clients.duckdb_catalog import symbol_files
from clients.index_membership_store import IndexMembershipStore
from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.source_evidence import SourceEvidenceStore
from clients.trading_calendar import trading_dates_in_range
from livewire_scripts.paths import data_lake_dir


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _verify_evidence(store: SourceEvidenceStore):
    def verify(ref: str, digest: str) -> bool:
        try:
            return _digest(store.read(ref)) == digest
        except (OSError, ValueError):
            return False

    return verify


def _as_of_timestamp(value: date) -> datetime:
    return datetime.combine(value, time.max, tzinfo=UTC)


def _active_events(events: list[Any]) -> list[Any]:
    superseded = {event.supersedes for event in events if event.supersedes is not None}
    return [event for event in events if event.event_id not in superseded]


def _current_members(root: Path, index_id: str, revision: int, as_of: datetime) -> set[str]:
    evidence = SourceEvidenceStore(root)
    verifier = _verify_evidence(evidence)
    master = SecurityMaster(root, evidence_verifier=verifier)
    store = IndexMembershipStore(root, security_master=master, evidence_verifier=verifier)
    events = store.events(index_id)
    if revision < 1 or revision > len(events):
        raise ValueError(f"membership revision must be between 1 and {len(events)}")
    prefix = events[:revision]
    for event in prefix:
        if any(not verifier(ref, digest) for ref, digest in zip(event.source_refs, event.source_hashes, strict=True)):
            raise ValueError("membership revision evidence is missing or corrupt")
    visible = _active_events([event for event in prefix if event.known_at <= as_of])
    applicable = sorted(
        (event for event in visible if event.status == "verified" and event.effective_at <= as_of),
        key=lambda event: (event.effective_at, event.known_at, event.revision, event.event_id),
    )
    members: set[str] = set()
    for event in applicable:
        if event.action == "add":
            members.add(event.security_id)
        else:
            members.discard(event.security_id)
    return {security_id for security_id in members if master.is_verified(security_id, as_of, as_of)}


def _identity_intervals(root: Path, members: set[str], as_of: datetime) -> list[SecurityIdentityEvent]:
    evidence = SourceEvidenceStore(root)
    verifier = _verify_evidence(evidence)
    master = SecurityMaster(root, evidence_verifier=verifier)
    events = _active_events(master.events(as_of=as_of))
    selected = sorted(
        (
            event
            for event in events
            if event.security_id in members
            and event.status == "verified"
            and event.effective_from <= as_of
            and (event.effective_to is None or event.effective_from < event.effective_to)
        ),
        key=lambda event: (event.security_id, event.effective_from, event.revision),
    )
    for event in selected:
        if any(not verifier(ref, digest) for ref, digest in zip(event.source_refs, event.source_hashes, strict=True)):
            raise ValueError("security identity evidence is missing or corrupt")
    return selected


def _gap_ranges(gaps: list[date]) -> list[dict[str, Any]]:
    if not gaps:
        return []
    sessions = sorted(gaps)
    result: list[dict[str, Any]] = []
    start = previous = sessions[0]
    count = 1
    for current in sessions[1:]:
        expected_next = trading_dates_in_range(previous + timedelta(days=1), current)
        if expected_next == [current]:
            previous = current
            count += 1
            continue
        result.append({"start": start.isoformat(), "end": previous.isoformat(), "count": count})
        start = previous = current
        count = 1
    result.append({"start": start.isoformat(), "end": previous.isoformat(), "count": count})
    return result


def _scope_target(
    index_id: str,
    revision: int,
    as_of: date,
    identity: SecurityIdentityEvent,
    start: date,
    end: date,
) -> dict[str, Any]:
    return {
        "indexId": index_id,
        "membershipRevision": revision,
        "asOf": as_of.isoformat(),
        "securityId": identity.security_id,
        "identityEventId": identity.event_id,
        "symbol": identity.symbol,
        "provider": identity.provider,
        "exchangeMic": identity.exchange_mic,
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
    }


def _evaluate(
    target: dict[str, Any],
    *,
    data_lake_root: Path,
    planned_hash: str | None = None,
    compare_to_plan: bool = False,
) -> dict[str, Any]:
    start = date.fromisoformat(target["startDate"])
    end = date.fromisoformat(target["endDate"])
    expected = trading_dates_in_range(start, end)
    resolved = symbol_files(
        "bronze_equity_1d",
        [target["symbol"]],
        lake_root=data_lake_root,
        missing_ok=True,
    )
    base: dict[str, Any] = {
        **target,
        "expectedSessions": len(expected),
        "parquetPath": None,
        "parquetHash": None,
        "firstDate": None,
        "lastDate": None,
        "gaps": _gap_ranges(expected),
        "provenanceMix": {},
        "violations": [],
    }
    if not resolved:
        return {
            **base,
            "coverageState": "MISSING",
            "nextOperation": {"kind": "fetch-deep-history", "target": target},
            "fileChangedSincePlan": compare_to_plan and planned_hash is not None,
        }
    path = Path(resolved[0])
    raw_hash = _digest(path.read_bytes())
    base.update(parquetPath=str(path), parquetHash=raw_hash)
    violations: list[str] = []
    try:
        parquet = pq.ParquetFile(path)
        required = {
            "trade_date": pa.date32(),
            "symbol_id": pa.int64(),
            "open": pa.float64(),
            "high": pa.float64(),
            "low": pa.float64(),
            "close": pa.float64(),
            "adj_close": pa.float64(),
            "volume": pa.int64(),
            "source": pa.string(),
            "price_basis": pa.string(),
        }
        missing_columns = sorted(set(required) - set(parquet.schema_arrow.names))
        if missing_columns:
            violations.append(f"missing columns: {','.join(missing_columns)}")
            rows: list[dict[str, Any]] = []
        else:
            for name, expected_type in required.items():
                actual_type = parquet.schema_arrow.field(name).type
                if actual_type != expected_type:
                    violations.append(f"invalid schema type for {name}: {actual_type}")
            rows = parquet.read(columns=sorted(required)).to_pylist()
    except Exception as exc:
        violations.append(f"invalid parquet: {type(exc).__name__}")
        rows = []

    if not rows:
        violations.append("empty parquet")

    dates: list[date] = []
    provenance: Counter[str] = Counter()
    for row in rows:
        trade_date = row.get("trade_date")
        if not isinstance(trade_date, date):
            violations.append("trade_date is not a date")
            continue
        dates.append(trade_date)
        if trade_date < start or trade_date > end:
            violations.append(f"row outside identity interval: {trade_date.isoformat()}")
        values = [row.get(name) for name in ("open", "high", "low", "close", "adj_close")]
        if any(not isinstance(value, (int, float)) for value in values):
            violations.append(f"invalid OHLCV types: {trade_date.isoformat()}")
        else:
            opening, high, low, close, adjusted = (float(value) for value in values)
            if (
                min(opening, high, low, close, adjusted) <= 0
                or high < max(opening, low, close)
                or low > min(opening, high, close)
            ):
                violations.append(f"invalid OHLC bounds: {trade_date.isoformat()}")
        volume = row.get("volume")
        if not isinstance(volume, int) or volume < 0:
            violations.append(f"invalid volume: {trade_date.isoformat()}")
        source, basis = row.get("source"), row.get("price_basis")
        if source not in EQUITY_SOURCES or basis not in PRICE_BASES:
            violations.append(f"invalid source/price_basis: {trade_date.isoformat()}")
        else:
            provenance[f"{source}/{basis}"] += 1
    if len(dates) != len(set(dates)):
        violations.append("duplicate trade_date rows")
    within = set(dates) & set(expected)
    gaps = [session for session in expected if session not in within]
    base.update(
        firstDate=None if not dates else min(dates).isoformat(),
        lastDate=None if not dates else max(dates).isoformat(),
        gaps=_gap_ranges(gaps),
        provenanceMix=dict(sorted(provenance.items())),
        violations=sorted(set(violations)),
    )
    if violations:
        state, operation = "CORRUPT", "quarantine-and-refetch"
    elif gaps:
        state, operation = "INCOMPLETE", "fetch-missing-daily"
    else:
        state, operation = "VERIFIED", "none"
    return {
        **base,
        "coverageState": state,
        "nextOperation": {"kind": operation} if operation == "none" else {"kind": operation, "target": target},
        "fileChangedSincePlan": compare_to_plan and planned_hash != raw_hash,
    }


def plan_daily(index_id: str, revision: int, as_of: date, *, data_lake_root: Path) -> dict[str, Any]:
    root = Path(data_lake_root).expanduser()
    timestamp = _as_of_timestamp(as_of)
    members = _current_members(root, index_id, revision, timestamp)
    identities = _identity_intervals(root, members, timestamp)
    units = []
    for identity in identities:
        start = identity.effective_from.date()
        interval_end = (
            as_of
            if identity.effective_to is None
            else min(as_of, (identity.effective_to - timedelta(microseconds=1)).date())
        )
        if interval_end < start:
            continue
        target = _scope_target(index_id, revision, as_of, identity, start, interval_end)
        scope_hash = f"sha256:{_digest(_canonical(target))}"
        evaluation = _evaluate(target, data_lake_root=root)
        units.append(
            {
                "version": 1,
                "workUnitId": f"daily-{scope_hash.removeprefix('sha256:')[:24]}",
                "scopeHash": scope_hash,
                **evaluation,
            }
        )
    return {
        "version": 1,
        "operation": "shepherd-daily-plan",
        "indexId": index_id,
        "membershipRevision": revision,
        "asOf": as_of.isoformat(),
        "memberCount": len(members),
        "workUnits": units,
        "mutated": False,
    }


def verify_daily_work_unit(unit: dict[str, Any], *, data_lake_root: Path) -> dict[str, Any]:
    required = {
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
        "scopeHash",
    }
    if not isinstance(unit, dict) or not required.issubset(unit):
        raise ValueError("daily work unit is incomplete")
    target = {key: unit[key] for key in required if key != "scopeHash"}
    expected_scope = f"sha256:{_digest(_canonical(target))}"
    if unit["scopeHash"] != expected_scope:
        raise ValueError("daily work-unit scope hash mismatch")
    root = Path(data_lake_root).expanduser()
    as_of_date = date.fromisoformat(target["asOf"])
    timestamp = _as_of_timestamp(as_of_date)
    members = _current_members(root, target["indexId"], target["membershipRevision"], timestamp)
    identities = _identity_intervals(root, members, timestamp)
    registered = False
    for identity in identities:
        if identity.event_id != target["identityEventId"]:
            continue
        start = identity.effective_from.date()
        end = (
            as_of_date
            if identity.effective_to is None
            else min(as_of_date, (identity.effective_to - timedelta(microseconds=1)).date())
        )
        registered = target == _scope_target(
            target["indexId"], target["membershipRevision"], as_of_date, identity, start, end
        )
        break
    if not registered:
        raise ValueError("daily work unit does not match a registered identity interval")
    evaluation = _evaluate(
        target,
        data_lake_root=root,
        planned_hash=unit.get("parquetHash"),
        compare_to_plan=True,
    )
    receipt = {
        "version": 1,
        "operation": "shepherd-daily-verify",
        "scopeHash": expected_scope,
        **evaluation,
        "changedPaths": [],
    }
    receipt["receiptHash"] = f"sha256:{_digest(_canonical(receipt))}"
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-lake-root", type=Path, default=data_lake_dir())
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan")
    plan.add_argument("--index", choices=("sp500", "ndx100"), required=True)
    plan.add_argument("--membership-revision", type=int, required=True)
    plan.add_argument("--as-of", type=date.fromisoformat, required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--manifest", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "plan":
        result = plan_daily(args.index, args.membership_revision, args.as_of, data_lake_root=args.data_lake_root)
    else:
        if not args.manifest.is_absolute():
            raise ValueError("daily work-unit manifest path must be absolute")
        result = verify_daily_work_unit(json.loads(args.manifest.read_bytes()), data_lake_root=args.data_lake_root)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
