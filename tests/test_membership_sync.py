"""Tests for livewire_scripts/membership_sync.py — grok events.jsonl → IndexMembershipStore."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from clients import ledger
from clients.index_membership_store import IndexMembershipStore
from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.source_evidence import SourceEvidenceStore
from livewire_scripts import membership_sync

NOW = datetime(2026, 9, 13, 1, 0, tzinfo=UTC)


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


@pytest.fixture(autouse=True)
def _ledger_root(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))


# Six lines in the grok shape: AAPL and MSFT resolve through the security
# master, AEOS does not (its add+remove stay status='unresolved').
EVENTS = [
    {"effective_date": "2004-01-01", "action": "add", "ticker": "AAPL", "kind": "bootstrap"},
    {"effective_date": "2004-01-01", "action": "add", "ticker": "AEOS", "kind": "bootstrap"},
    {"effective_date": "2004-01-01", "action": "add", "ticker": "MSFT", "kind": "bootstrap"},
    {"effective_date": "2007-01-01", "action": "remove", "ticker": "AEOS", "kind": "diff"},
    {"effective_date": "2010-01-01", "action": "remove", "ticker": "AAPL", "kind": "diff"},
    {"effective_date": "2015-01-01", "action": "add", "ticker": "AAPL", "kind": "diff"},
]

SOURCE_BYTES = b'{"note": "wikipedia snapshot evidence"}\n'
SOURCE_SHA = hashlib.sha256(SOURCE_BYTES).hexdigest()


def _events_file(tmp_path: Path) -> Path:
    path = tmp_path / "events.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in EVENTS))
    return path


def _source_file(tmp_path: Path) -> Path:
    path = tmp_path / "source.snapshot"
    path.write_bytes(SOURCE_BYTES)
    return path


def verifies(ref: str, digest: str) -> bool:
    return ref == f"artifact://sha256/{digest}"


def _verified_security(master: SecurityMaster, symbol: str, mic: str = "XNAS") -> str:
    security_id = master.new_security_id()
    master.append(
        SecurityIdentityEvent(
            event_id=f"identity-{symbol}-{mic}",
            security_id=security_id,
            revision=1,
            symbol=symbol,
            provider="massive",
            exchange_mic=mic,
            currency="USD",
            effective_from=dt("2000-01-01"),
            effective_to=None,
            known_at=dt("2000-01-01"),
            issuer_name=f"{symbol} issuer",
            cik="0000000001",
            composite_figi=None,
            share_class_figi=None,
            continuity_basis="provider_figi",
            relationship_type=None,
            related_security_id=None,
            source_refs=("artifact://sha256/" + "a" * 64,),
            source_hashes=("a" * 64,),
            status="verified",
            supersedes=None,
        )
    )
    return security_id


def _lake(tmp_path: Path) -> Path:
    lake = tmp_path / "lake"
    master = SecurityMaster(lake, evidence_verifier=verifies)
    if not master.events():
        _verified_security(master, "AAPL")
        _verified_security(master, "MSFT")
    return lake


def _store(lake: Path) -> IndexMembershipStore:
    return IndexMembershipStore(
        lake,
        security_master=SecurityMaster(lake, evidence_verifier=None),
        evidence_verifier=membership_sync._evidence_verifier(SourceEvidenceStore(lake)),
    )


def _import(tmp_path: Path, *, confidence: str = "B") -> dict:
    return membership_sync.import_events(
        index_id="sp500",
        events_path=_events_file(tmp_path),
        sources=[_source_file(tmp_path)],
        data_lake_root=_lake(tmp_path),
        now=NOW,
        confidence=confidence,
    )


def test_import_appends_events_with_resolution_and_provenance(tmp_path):
    result = _import(tmp_path)

    assert result["added"] == 4 and result["removed"] == 2
    assert result["unresolved"] == 1 and result["skipped"] == 0

    lake = tmp_path / "lake"
    master = SecurityMaster(lake, evidence_verifier=None)
    store = _store(lake)
    events = store.events("sp500")
    assert len(events) == 6

    aapl_id = master.resolve_symbol("massive", "AAPL", "XNAS", dt("2004-01-01"), NOW)
    aapl = [event for event in events if event.security_id == aapl_id]
    assert [event.revision for event in aapl] == [1, 2, 3]
    assert all(event.status == "verified" for event in aapl)

    aeos = [event for event in events if event.status == "unresolved"]
    assert len(aeos) == 2
    assert all(event.security_id == "unresolved:AEOS" for event in aeos)

    # PIT-honest: known_at is the import time, never the source's own clock.
    assert all(event.known_at == NOW and event.announced_at is None for event in events)
    assert all(
        event.effective_at == dt(f"{date}")
        for event, date in zip(events, [r["effective_date"] for r in EVENTS], strict=True)
    )

    # Every event carries the committed source artifact.
    assert all(
        event.source_refs == (f"artifact://sha256/{SOURCE_SHA}",) and event.source_hashes == (SOURCE_SHA,)
        for event in events
    )
    assert SourceEvidenceStore(lake).read(f"artifact://sha256/{SOURCE_SHA}") == SOURCE_BYTES


def test_import_emits_the_run_and_measurements(tmp_path):
    _import(tmp_path)

    runs = ledger.query("select verdict from runs where job = 'membership-sync' and ended is not null")
    assert {row["verdict"] for row in runs} == {"OK"}
    rows = ledger.query("select name, scope, value from measurements where name like 'membership_%'")
    by_name = {row["name"]: row["value"] for row in rows}
    assert by_name["membership_events_added"] == 4.0
    assert by_name["membership_events_removed"] == 2.0
    assert by_name["membership_unresolved"] == 1.0
    assert all(row["scope"] == "sp500" for row in rows)


def test_second_import_of_the_same_file_appends_nothing(tmp_path):
    first = _import(tmp_path)
    assert first["added"] == 4

    again = _import(tmp_path)
    assert again["added"] == 0 and again["removed"] == 0 and again["skipped"] == 6
    # The unresolved backlog is a standing measurement, not a per-run delta —
    # a no-op re-import still reports it so the status WARN cannot clear while
    # the hole is still in the store.
    assert again["unresolved"] == 1
    assert len(_store(tmp_path / "lake").events("sp500")) == 6


def test_confidence_c_marks_resolved_events_candidate(tmp_path):
    _import(tmp_path, confidence="C")
    events = _store(tmp_path / "lake").events("sp500")
    resolved = [event for event in events if event.status != "unresolved"]
    assert len(resolved) == 4 and all(event.status == "candidate" for event in resolved)


def test_import_cli_dispatches(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    monkeypatch.setenv("MDW_DATA_LAKE", str(lake))
    assert (
        membership_sync.main(
            [
                "import",
                "--index",
                "sp500",
                "--events",
                str(_events_file(tmp_path)),
                "--source",
                str(_source_file(tmp_path)),
            ]
        )
        == 0
    )
    assert len(_store(lake).events("sp500")) == 6
