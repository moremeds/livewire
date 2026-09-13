"""Tests for livewire_scripts/membership_sync.py — grok events.jsonl → IndexMembershipStore."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from clients import ledger
from clients.index_membership_store import IndexMembershipStore
from clients.mediawiki_client import MediaWikiSnapshot
from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.shepherd_repair import HashedRef
from clients.source_evidence import SourceEvidence, SourceEvidenceStore, canonical_bytes
from clients.universe_client import UniverseFetchError
from livewire_scripts import membership_sync

NOW = datetime(2026, 9, 13, 1, 0, tzinfo=UTC)
SYNC_NOW = datetime(2026, 9, 14, 1, 0, tzinfo=UTC)


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


def _fetched(lake: Path, tickers: set[str]) -> tuple[set[str], HashedRef]:
    artifact = SourceEvidenceStore(lake).persist_raw(b'{"kind": "live snapshot"}')
    return tickers, HashedRef(artifact.ref, artifact.sha256)


def _runner(sent: list[list[str]]):
    def run(command: list[str], *, timeout: int) -> subprocess.CompletedProcess:
        sent.append(command)
        return subprocess.CompletedProcess(command, 0, "")

    return run


def test_sync_appends_one_add_and_one_remove(tmp_path):
    lake = _lake(tmp_path)
    _import(tmp_path)  # members: AAPL, MSFT (AEOS was added then removed)

    code = membership_sync.sync(
        indexes=["sp500"],
        data_lake_root=lake,
        now=SYNC_NOW,
        fetch_fn=lambda index_id: _fetched(lake, {"AAPL", "ORCL"}),
    )

    assert code == 0
    events = [event for event in _store(lake).events("sp500") if event.known_at == SYNC_NOW]
    by_action = {event.action: event for event in events}
    msft_id = SecurityMaster(lake, evidence_verifier=None).resolve_symbol("massive", "MSFT", "XNAS", SYNC_NOW, SYNC_NOW)
    assert by_action["add"].security_id == "unresolved:ORCL"
    assert by_action["add"].status == "unresolved"
    assert by_action["remove"].security_id == msft_id
    assert by_action["remove"].status == "verified"
    assert all(event.effective_at == SYNC_NOW and event.announced_at is None for event in events)


def test_sync_unchanged_appends_nothing_but_still_reports_fetch_ok(tmp_path):
    lake = _lake(tmp_path)
    _import(tmp_path)

    code = membership_sync.sync(
        indexes=["sp500"],
        data_lake_root=lake,
        now=SYNC_NOW,
        fetch_fn=lambda index_id: _fetched(lake, {"AAPL", "MSFT"}),
    )

    assert code == 0
    assert len(_store(lake).events("sp500")) == 6
    rows = ledger.query(
        "select name, value from measurements where name like 'membership_%' "
        "and scope='sp500' and measured_at >= '2026-09-14'"
    )
    by_name = {row["name"]: row["value"] for row in rows}
    assert by_name["membership_source_fetch_ok"] == 1.0
    assert by_name["membership_events_added"] == 0.0
    assert by_name["membership_events_removed"] == 0.0


def test_sync_fetch_failure_pages_fails_closed_and_exits_3(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
    lake = _lake(tmp_path)
    sent: list[list[str]] = []

    def broken_fetch(index_id):
        raise UniverseFetchError("simulated source outage")

    code = membership_sync.sync(
        indexes=["sp500"],
        data_lake_root=lake,
        now=SYNC_NOW,
        fetch_fn=broken_fetch,
        runner=_runner(sent),
    )

    assert code == 3
    assert len(sent) == 1  # one page_for_lane notice went through notify.send
    rows = ledger.query("select name, value from measurements where name like 'membership_%' and scope='sp500'")
    by_name = {row["name"]: row["value"] for row in rows}
    assert by_name["membership_source_fetch_ok"] == 0.0
    terminal = ledger.query("select verdict, exit_code from runs where job='membership-sync' and ended is not null")
    assert {row["verdict"] for row in terminal} == {"FAILED"}
    assert {row["exit_code"] for row in terminal} == {3}
    notices = ledger.query("select exit_code from executions where script='notify'")
    assert {row["exit_code"] for row in notices} == {0}


def test_djia_fetch_failure_fails_closed_and_pages(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
    lake = _lake(tmp_path)

    def broken():
        raise UniverseFetchError("DJIA: 12 constituents parsed, below 30")

    monkeypatch.setattr(membership_sync, "fetch_djia", broken)
    sent: list[list[str]] = []

    code = membership_sync.sync(indexes=["djia"], data_lake_root=lake, now=SYNC_NOW, runner=_runner(sent))

    assert code == 3
    assert len(sent) == 1
    assert _store(lake).events("djia") == []
    rows = ledger.query("select value from measurements where name='membership_source_fetch_ok' and scope='djia'")
    assert {row["value"] for row in rows} == {0.0}


def test_djia_sync_commits_the_fetched_set_as_slickcharts_evidence(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    tickers = {"AAPL"} | {f"ZZ{i:02d}" for i in range(29)}
    monkeypatch.setattr(membership_sync, "fetch_djia", lambda: tickers)

    code = membership_sync.sync(indexes=["djia"], data_lake_root=lake, now=SYNC_NOW)

    assert code == 0
    events = _store(lake).events("djia")
    assert len(events) == 30  # AAPL verified, the rest unresolved placeholders
    aapl_id = SecurityMaster(lake, evidence_verifier=None).resolve_symbol("massive", "AAPL", "XNAS", SYNC_NOW, SYNC_NOW)
    assert [event.status for event in events if event.security_id == aapl_id] == ["verified"]
    assert sum(event.status == "unresolved" for event in events) == 29
    artifact = events[0].source_refs[0]
    assert SourceEvidenceStore(lake).read(artifact) == canonical_bytes(sorted(tickers))
    recorded = {item.ref: item for item in SourceEvidenceStore(lake).list_verified()}
    assert recorded[artifact].source_url == "https://www.slickcharts.com/dowjones"


def test_r2k_proxy_fetch_below_floor_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
    lake = _lake(tmp_path)
    monkeypatch.setattr(membership_sync, "fetch_r2k", lambda: {"A", "B", "C"})
    sent: list[list[str]] = []

    code = membership_sync.sync(indexes=["r2k-proxy"], data_lake_root=lake, now=SYNC_NOW, runner=_runner(sent))

    assert code == 3
    assert len(sent) == 1
    rows = ledger.query("select value from measurements where name='membership_source_fetch_ok' and scope='r2k-proxy'")
    assert {row["value"] for row in rows} == {0.0}


def test_r2k_proxy_resolved_members_are_candidate(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    monkeypatch.setattr(membership_sync, "_R2K_PROXY_MIN_MEMBERS", 3)
    monkeypatch.setattr(membership_sync, "fetch_r2k", lambda: {"AAPL", "ZZ1", "ZZ2"})

    code = membership_sync.sync(indexes=["r2k-proxy"], data_lake_root=lake, now=SYNC_NOW)

    assert code == 0
    events = _store(lake).events("r2k-proxy")
    assert len(events) == 3
    by_status = {}
    for event in events:
        by_status.setdefault(event.status, []).append(event)
    aapl_id = SecurityMaster(lake, evidence_verifier=None).resolve_symbol("massive", "AAPL", "XNAS", SYNC_NOW, SYNC_NOW)
    assert [event.security_id for event in by_status["candidate"]] == [aapl_id]
    assert sorted(event.security_id for event in by_status["unresolved"]) == ["unresolved:ZZ1", "unresolved:ZZ2"]


def test_sync_cli_dispatches_and_dry_run_appends_nothing(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    _import(tmp_path)
    monkeypatch.setenv("MDW_DATA_LAKE", str(lake))
    monkeypatch.setattr(
        membership_sync,
        "_default_fetch",
        lambda index_id, store, now: _fetched(lake, {"AAPL", "MSFT", "ORCL"}),
    )

    assert membership_sync.main(["--index", "sp500", "--dry-run"]) == 0
    assert len(_store(lake).events("sp500")) == 6  # diff computed, nothing appended

    assert membership_sync.main(["--index", "sp500"]) == 0
    events = _store(lake).events("sp500")
    assert len(events) == 7  # the ORCL add landed on the apply pass
    assert any(event.security_id == "unresolved:ORCL" for event in events)


def test_import_skips_blank_lines_and_emits_failed_run_on_error(tmp_path):
    lake = _lake(tmp_path)
    events = tmp_path / "events.jsonl"
    events.write_text("\n" + json.dumps(EVENTS[0]) + "\n\n")

    result = membership_sync.import_events(
        index_id="sp500",
        events_path=events,
        sources=[_source_file(tmp_path)],
        data_lake_root=lake,
        now=NOW,
        confidence="B",
    )
    assert result["added"] == 1

    with pytest.raises(FileNotFoundError):
        membership_sync.import_events(
            index_id="sp500",
            events_path=tmp_path / "missing.jsonl",
            sources=[_source_file(tmp_path)],
            data_lake_root=lake,
            now=NOW,
            confidence="B",
        )
    terminal = ledger.query("select verdict from runs where job='membership-sync' and ended is not null")
    assert "FAILED" in {row["verdict"] for row in terminal}


def test_evidence_verifier_rejects_an_absent_artifact(tmp_path):
    lake = _lake(tmp_path)
    verifier = membership_sync._evidence_verifier(SourceEvidenceStore(lake))
    assert verifier("artifact://sha256/" + "0" * 64, "0" * 64) is False


def test_wikipedia_fetch_returns_members_and_snapshot_evidence(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    raw = (
        b"<html><body><table class='wikitable' id='constituents'>"
        b"<thead><tr><th>Symbol</th><th>Security</th></tr></thead>"
        b"<tbody><tr><td>AAPL</td><td>Apple</td></tr><tr><td>MSFT</td><td>Microsoft</td></tr>"
        b"</tbody></table></body></html>"
    )
    artifact = SourceEvidenceStore(lake).persist_raw(raw)
    snapshot = MediaWikiSnapshot(
        title="List of S&P 500 companies",
        canonical_url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        revision_id=7,
        revision_time=NOW,
        content=raw.decode(),
        evidence=SourceEvidence(
            ref=artifact.ref,
            sha256=artifact.sha256,
            source_url="https://en.wikipedia.org/w/rest.php/v1/page/List_of_S%26P_500_companies/html",
            retrieved_at=NOW,
            publication_time=NOW,
            mediawiki_revision_id=7,
            mediawiki_revision_time=NOW,
            content_type="text/html",
        ),
    )
    monkeypatch.setattr(
        membership_sync, "MediaWikiClient", lambda *args, **kwargs: SimpleNamespace(snapshot=lambda title: snapshot)
    )

    members, ref = membership_sync._default_fetch("sp500", SourceEvidenceStore(lake), NOW)

    assert members == {"AAPL", "MSFT"}
    assert ref == HashedRef(artifact.ref, artifact.sha256)


def test_sync_failed_run_row_when_processing_raises(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    monkeypatch.setattr(
        membership_sync,
        "_resolve",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("master corrupt")),
    )

    with pytest.raises(RuntimeError, match="master corrupt"):
        membership_sync.sync(
            indexes=["sp500"],
            data_lake_root=lake,
            now=SYNC_NOW,
            fetch_fn=lambda index_id: _fetched(lake, {"AAPL"}),
        )
    terminal = ledger.query("select verdict, exit_code from runs where job='membership-sync' and ended is not null")
    assert {(row["verdict"], row["exit_code"]) for row in terminal} == {("FAILED", 1)}
