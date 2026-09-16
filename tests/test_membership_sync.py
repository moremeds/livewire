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
from clients.index_membership_store import IndexMembershipStore, MembershipEvent
from clients.mediawiki_client import MediaWikiSnapshot
from clients.pit_silver_revision import PitSilverRevisionPublisher
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


def _verified_security(master: SecurityMaster, symbol: str, mic: str = "XNAS", provider: str = "massive") -> str:
    security_id = master.new_security_id()
    master.append(
        SecurityIdentityEvent(
            event_id=f"identity-{symbol}-{mic}-{provider}",
            security_id=security_id,
            revision=1,
            symbol=symbol,
            provider=provider,
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


# --- reresolve -----------------------------------------------------------

RERESOLVE_NOW = datetime(2026, 9, 15, 3, 0, tzinfo=UTC)

# The two errors pit_silver_revision raises when a verified membership replay
# is unbalanced; the reresolve guard exists to keep both out of the store.
_REPLAY_ERRORS = {"duplicate open membership interval", "membership removal has no open interval"}


def _reresolve(tmp_path: Path, *, index_id: str = "sp500", confidence: str = "B", now=None) -> dict:
    return membership_sync.reresolve(
        index_id=index_id,
        data_lake_root=tmp_path / "lake",
        now=now or RERESOLVE_NOW,
        confidence=confidence,
    )


def _receipt(as_of: datetime) -> dict:
    """The read-only corporate-action receipt shape pit_silver_revision checks."""
    receipt = {
        "version": 1,
        "operation": "shepherd-actions-export",
        "asOf": as_of.isoformat(),
        "symbols": [],
        "summary": {"requested": 0, "verified": 0, "unresolved": 0},
        "mutated": False,
    }
    encoded = (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode()
    receipt["receiptHash"] = f"sha256:{hashlib.sha256(encoded).hexdigest()}"
    return receipt


def _pit_replay_error(lake: Path, as_of: datetime) -> str | None:
    """Run pit_silver_revision's own replay over the index and return its error.

    `_build_core` verifies membership evidence, then replays the verified
    events — the step under test — and only afterwards reaches the identity and
    Silver-revision inputs this fixture does not build. Anything the replay
    itself rejects surfaces as one of `_REPLAY_ERRORS`.
    """
    events = _store(lake).events("sp500")
    try:
        PitSilverRevisionPublisher(lake)._build_core("sp500", len(events), as_of, _receipt(as_of))
    except ValueError as exc:
        return str(exc)
    return None


def test_reresolve_replaces_the_placeholder_and_rejects_it(tmp_path):
    _import(tmp_path)
    lake = tmp_path / "lake"
    master = SecurityMaster(lake, evidence_verifier=verifies)
    aeos = _verified_security(master, "AEOS")

    result = _reresolve(tmp_path)

    assert result["resolved"] == 2  # AEOS add + remove
    events = _store(lake).events("sp500")
    placeholders = [e for e in events if e.security_id == "unresolved:AEOS"]
    assert {e.status for e in placeholders} == {"unresolved", "rejected"}
    resolved = [e for e in events if e.security_id == aeos]
    assert {e.action for e in resolved} == {"add", "remove"}
    assert all(e.status == "verified" for e in resolved)
    assert all(e.known_at == RERESOLVE_NOW for e in resolved)


def test_reresolve_emits_the_run_and_measurements(tmp_path):
    _import(tmp_path)
    _verified_security(SecurityMaster(tmp_path / "lake", evidence_verifier=verifies), "AEOS")

    _reresolve(tmp_path)

    runs = ledger.query("select verdict from runs where job='membership-reresolve' and ended is not null")
    assert {row["verdict"] for row in runs} == {"OK"}
    rows = ledger.query(
        "select name, scope, value from measurements "
        "where name in ('membership_reresolve_conflict', 'membership_unresolved') and scope='sp500' "
        "and measured_at >= '2026-09-15'"
    )
    by_name = {row["name"]: row["value"] for row in rows}
    assert by_name["membership_reresolve_conflict"] == 0.0
    assert by_name["membership_unresolved"] == 0.0


def test_a_second_reresolve_appends_nothing(tmp_path):
    _import(tmp_path)
    _verified_security(SecurityMaster(tmp_path / "lake", evidence_verifier=verifies), "AEOS")
    _reresolve(tmp_path)
    before = len(_store(tmp_path / "lake").events("sp500"))

    assert _reresolve(tmp_path)["resolved"] == 0
    assert len(_store(tmp_path / "lake").events("sp500")) == before


def test_pit_honesty_an_as_of_before_the_reresolve_still_sees_nothing(tmp_path):
    _import(tmp_path)
    lake = tmp_path / "lake"
    aeos = _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")
    _reresolve(tmp_path)
    store = _store(lake)

    at = dt("2005-01-01")
    assert aeos not in store.members_effective_at("sp500", at, NOW)
    assert aeos in store.members_effective_at("sp500", at, RERESOLVE_NOW)


def test_an_interrupted_pass_is_completed_by_the_retry_with_no_duplicate(tmp_path, monkeypatch):
    """A crash between the resolved append and the rejection leaves the
    placeholder current; the retry reuses the stored replacement as is."""
    _import(tmp_path)
    lake = tmp_path / "lake"
    aeos = _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")

    original = IndexMembershipStore.append
    calls = {"n": 0}

    def crash_after_first_resolved(self, item):
        if item.status != "rejected":
            calls["n"] += 1
            result = original(self, item)
            if calls["n"] == 1:
                raise RuntimeError("interrupted")
            return result
        return original(self, item)

    monkeypatch.setattr(IndexMembershipStore, "append", crash_after_first_resolved)
    with pytest.raises(RuntimeError):
        _reresolve(tmp_path)
    monkeypatch.undo()

    _reresolve(tmp_path)
    events = _store(lake).events("sp500")
    resolved_adds = [e for e in events if e.security_id == aeos and e.action == "add"]
    assert len(resolved_adds) == 1
    members = membership_sync._current_members(events)
    assert "unresolved:AEOS" not in members


def test_an_add_and_a_remove_on_the_delisting_date_both_resolve_to_one_id(tmp_path):
    """Master intervals are end-exclusive, so a remove effective on the
    delisting date would never resolve on its own and its add would become a
    permanent member the next sync removes a second time."""
    lake = tmp_path / "lake"
    _import(tmp_path)
    master = SecurityMaster(lake, evidence_verifier=verifies)
    security_id = master.new_security_id()
    master.append(
        SecurityIdentityEvent(
            event_id="identity-AEOS-closed",
            security_id=security_id,
            revision=1,
            symbol="AEOS",
            provider="massive",
            exchange_mic="XNAS",
            currency="USD",
            effective_from=dt("2000-01-01"),
            effective_to=dt("2007-01-01"),
            known_at=dt("2000-01-01"),
            issuer_name="AEOS issuer",
            cik="0000000009",
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

    _reresolve(tmp_path)

    events = _store(lake).events("sp500")
    resolved = [e for e in events if e.security_id == security_id]
    assert {e.action for e in resolved} == {"add", "remove"}
    assert security_id not in membership_sync._current_members(events)


def test_a_retry_after_the_add_pair_still_resolves_the_delisting_date_remove(tmp_path, monkeypatch):
    """Spec §3.3 rule 1: the add may come from an earlier pass. A crash after
    the add's pair (resolved + rejection) but before the remove leaves the
    add placeholder superseded; the retry must find the open add through the
    store, because the clock alone never resolves a remove on the delisting
    date (end-exclusive intervals)."""
    lake = tmp_path / "lake"
    _import(tmp_path)
    master = SecurityMaster(lake, evidence_verifier=verifies)
    security_id = master.new_security_id()
    master.append(
        SecurityIdentityEvent(
            event_id="identity-AEOS-closed",
            security_id=security_id,
            revision=1,
            symbol="AEOS",
            provider="massive",
            exchange_mic="XNAS",
            currency="USD",
            effective_from=dt("2000-01-01"),
            effective_to=dt("2007-01-01"),
            known_at=dt("2000-01-01"),
            issuer_name="AEOS issuer",
            cik="0000000009",
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

    original = IndexMembershipStore.append
    seen = {"rejected": 0}

    def crash_after_the_first_rejection(self, item):
        result = original(self, item)
        if item.status == "rejected":
            seen["rejected"] += 1
            if seen["rejected"] == 1:
                raise RuntimeError("interrupted")
        return result

    monkeypatch.setattr(IndexMembershipStore, "append", crash_after_the_first_rejection)
    with pytest.raises(RuntimeError):
        _reresolve(tmp_path)
    monkeypatch.undo()

    _reresolve(tmp_path)

    events = _store(lake).events("sp500")
    resolved = [e for e in events if e.security_id == security_id]
    assert {e.action for e in resolved} == {"add", "remove"}
    assert security_id not in membership_sync._current_members(events)
    assert "unresolved:AEOS" not in membership_sync._current_members(events)


def test_a_later_sync_add_for_the_same_id_makes_the_historical_add_fail_closed(tmp_path):
    """The case a check at effective_at alone misses: the nightly sync already
    opened a membership, later in time, for the id this add resolves to."""
    _import(tmp_path)
    lake = tmp_path / "lake"
    aeos = _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")
    store = _store(lake)
    store.append(
        MembershipEvent(
            event_id="sync-add-aeos",
            index_id="sp500",
            security_id=aeos,
            action="add",
            announced_at=None,
            effective_at=dt("2026-09-14"),
            known_at=dt("2026-09-14"),
            source_refs=("artifact://sha256/" + SOURCE_SHA,),
            source_hashes=(SOURCE_SHA,),
            revision=1,
            supersedes=None,
            status="verified",
        )
    )

    result = _reresolve(tmp_path)

    assert result["conflicts"] >= 1
    # the placeholder is untouched: no resolved add, no rejection
    events = store.events("sp500")
    assert not [e for e in events if e.security_id == aeos and e.effective_at == dt("2004-01-01")]
    assert [e for e in events if e.security_id == "unresolved:AEOS" and e.status == "rejected"] == []


def test_a_candidate_add_then_a_remove_at_confidence_b_fails_closed(tmp_path):
    """PIT Silver replays verified events only, so the guard filters to the
    status the proposed event will carry: a remove whose add is candidate must
    not be appended as verified."""
    _import(tmp_path, confidence="C")  # placeholders' resolved status will be candidate
    lake = tmp_path / "lake"
    aeos = _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")
    _reresolve(tmp_path, confidence="C")  # add + remove land as candidate

    # now a second, verified-confidence pass over a fresh placeholder remove
    store = _store(lake)
    store.append(
        MembershipEvent(
            event_id="placeholder-remove-only",
            index_id="sp500",
            security_id="unresolved:AEOS",
            action="remove",
            announced_at=None,
            effective_at=dt("2009-01-01"),
            known_at=dt("2026-09-14"),
            source_refs=("artifact://sha256/" + SOURCE_SHA,),
            source_hashes=(SOURCE_SHA,),
            revision=max(e.revision for e in store.events("sp500") if e.security_id == "unresolved:AEOS") + 1,
            supersedes=None,
            status="unresolved",
        )
    )

    result = _reresolve(tmp_path, confidence="B", now=RERESOLVE_NOW.replace(hour=4))

    assert result["conflicts"] >= 1
    assert not [e for e in store.events("sp500") if e.security_id == aeos and e.status == "verified"]


def test_pit_silver_replay_over_a_reresolved_index_raises_nothing(tmp_path):
    _import(tmp_path)
    lake = tmp_path / "lake"
    _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")
    _reresolve(tmp_path)

    assert _pit_replay_error(lake, RERESOLVE_NOW) not in _REPLAY_ERRORS


def test_the_pit_replay_probe_catches_an_unbalanced_index(tmp_path):
    """Negative control for the probe above: a second open add for one id is
    exactly what the reresolve guard refuses to write."""
    _import(tmp_path)
    lake = tmp_path / "lake"
    store = _store(lake)
    aapl = SecurityMaster(lake, evidence_verifier=None).resolve_symbol("massive", "AAPL", "XNAS", NOW, NOW)
    store.append(
        MembershipEvent(
            event_id="unbalanced-second-add",
            index_id="sp500",
            security_id=aapl,
            action="add",
            announced_at=None,
            effective_at=dt("2020-01-01"),
            known_at=dt("2026-09-14"),
            source_refs=("artifact://sha256/" + SOURCE_SHA,),
            source_hashes=(SOURCE_SHA,),
            revision=max(e.revision for e in store.events("sp500") if e.security_id == aapl) + 1,
            supersedes=None,
            status="verified",
        )
    )

    assert _pit_replay_error(lake, RERESOLVE_NOW) == "duplicate open membership interval"


def test_confidence_maps_to_status_and_r2k_proxy_is_always_candidate(tmp_path):
    _import(tmp_path)
    lake = tmp_path / "lake"
    aeos = _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")

    _reresolve(tmp_path, confidence="C")

    resolved = [e for e in _store(lake).events("sp500") if e.security_id == aeos]
    assert {e.status for e in resolved} == {"candidate"}


def test_the_backlog_measure_agrees_across_import_sync_and_reresolve(tmp_path):
    _import(tmp_path)
    lake = tmp_path / "lake"
    _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")
    after = _reresolve(tmp_path)["unresolved"]

    events = _store(lake).events("sp500")
    assert after == len(membership_sync.unresolved_backlog(events))
    # a rejected placeholder chain is out of the backlog, so a re-import and the
    # nightly sync report the same number rather than restoring the WARN
    assert _import(tmp_path)["unresolved"] == after
    assert (
        membership_sync.sync(
            indexes=["sp500"],
            data_lake_root=lake,
            now=SYNC_NOW,
            fetch_fn=lambda index_id: _fetched(lake, {"AAPL", "MSFT"}),
        )
        == 0
    )
    rows = ledger.query(
        "select value from measurements where name='membership_unresolved' and scope='sp500' "
        "and measured_at >= '2026-09-14' and measured_at < '2026-09-15'"
    )
    assert {row["value"] for row in rows} == {float(after)}


def test_xase_is_one_of_the_resolve_mics():
    """The Russell list carries 44 NYSE American names (measured 2026-09-15)."""
    assert membership_sync._RESOLVE_MICS == ("XNAS", "XNYS", "ARCX", "XASE")


def test_a_ticker_that_still_does_not_resolve_is_left_alone_and_counted(tmp_path):
    _import(tmp_path)
    result = _reresolve(tmp_path)
    assert result["resolved"] == 0
    assert result["unresolved"] == 1  # unresolved:AEOS


def test_resolve_also_checks_the_wikipedia_provider(tmp_path):
    """The 2026-09-16 one-off import writes `provider='wikipedia_sec_research'`,
    never `massive` (pm:2026-07-19-cancellation-inference-provider-scoped) — so
    `_resolve` must check both, or the import sits unused in the master."""
    lake = tmp_path / "lake"
    master = SecurityMaster(lake, evidence_verifier=verifies)
    _verified_security(master, "AEOS", mic="XNAS", provider="wikipedia_sec_research")
    assert membership_sync._resolve(master, "AEOS", dt("2000-06-01"), NOW) is not None


def test_resolve_refuses_when_massive_and_wikipedia_disagree(tmp_path):
    lake = tmp_path / "lake"
    master = SecurityMaster(lake, evidence_verifier=verifies)
    _verified_security(master, "DUPX", mic="XNAS", provider="massive")
    _verified_security(master, "DUPX", mic="XNYS", provider="wikipedia_sec_research")
    assert membership_sync._resolve(master, "DUPX", dt("2000-06-01"), NOW) is None


def test_reresolve_clears_a_placeholder_identified_only_by_wikipedia_research(tmp_path):
    _import(tmp_path)
    lake = tmp_path / "lake"
    master = SecurityMaster(lake, evidence_verifier=verifies)
    _verified_security(master, "AEOS", mic="XNAS", provider="wikipedia_sec_research")
    result = _reresolve(tmp_path)
    assert result["resolved"] == 2  # the add and the remove both resolve
    assert result["unresolved"] == 0


def test_reresolve_cli_dispatches(tmp_path, monkeypatch):
    _import(tmp_path)
    lake = tmp_path / "lake"
    aeos = _verified_security(SecurityMaster(lake, evidence_verifier=verifies), "AEOS")
    monkeypatch.setenv("MDW_DATA_LAKE", str(lake))

    assert membership_sync.main(["reresolve", "--index", "sp500", "--confidence", "B"]) == 0
    assert [e for e in _store(lake).events("sp500") if e.security_id == aeos]


def test_reresolve_emits_a_failed_run_row_when_the_pass_raises(tmp_path, monkeypatch):
    _import(tmp_path)
    _verified_security(SecurityMaster(tmp_path / "lake", evidence_verifier=verifies), "AEOS")
    monkeypatch.setattr(
        membership_sync,
        "_resolve",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("master corrupt")),
    )

    with pytest.raises(RuntimeError, match="master corrupt"):
        _reresolve(tmp_path)
    terminal = ledger.query(
        "select verdict, exit_code from runs where job='membership-reresolve' and ended is not null"
    )
    assert {(row["verdict"], row["exit_code"]) for row in terminal} == {("FAILED", 1)}
