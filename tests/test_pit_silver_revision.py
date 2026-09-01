from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from clients.index_membership_store import IndexMembershipStore, MembershipEvent
from clients.pit_silver_revision import PitSilverRevisionPublisher
from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.silver_client import PublishedArtifact
from clients.silver_revision import AffectedSymbol, SilverRevisionPublisher
from clients.source_evidence import SourceEvidenceStore
from livewire_scripts.shepherd_actions import export_actions
from tests.test_shepherd_actions import _verified_empty_fetch
from tests.test_shepherd_daily import _seed

AS_OF = datetime(2026, 8, 31, 23, 59, tzinfo=UTC)


def _silver(root: Path) -> tuple[Path, bytes]:
    silver = root / "silver"
    artifact = silver / "asset_class=equity" / "symbol=AAPL" / "1d.parquet"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"canonical silver bars")
    SilverRevisionPublisher(silver).publish(
        [PublishedArtifact(artifact, hashlib.sha256(artifact.read_bytes()).hexdigest(), 1)],
        [AffectedSymbol("AAPL", date(2026, 8, 28), ("1d",))],
        AS_OF,
        published_at=AS_OF,
    )
    current = silver / "revisions" / "current.json"
    return artifact, current.read_bytes()


def _actions(*symbols: str, state: str = "VERIFIED") -> dict:
    receipt = {
        "version": 1,
        "operation": "shepherd-actions-export",
        "asOf": AS_OF.isoformat(),
        "symbols": [{"symbol": symbol, "state": state, "fetch": {}, "actions": [], "issues": []} for symbol in symbols],
        "summary": {
            "requested": len(symbols),
            "verified": len(symbols) if state == "VERIFIED" else 0,
            "unresolved": 0 if state == "VERIFIED" else len(symbols),
        },
        "mutated": False,
    }
    encoded = (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode()
    receipt["receiptHash"] = f"sha256:{hashlib.sha256(encoded).hexdigest()}"
    return receipt


def _ready(root: Path) -> tuple[Path, bytes]:
    _seed(root, [("AAPL", datetime(2026, 8, 28, tzinfo=UTC), None)])
    artifact, current = _silver(root)
    _verified_empty_fetch(root, "AAPL")
    return artifact, current


def test_publish_binds_existing_silver_membership_identity_and_actions_without_copying_bars(tmp_path: Path) -> None:
    artifact, apex_current = _ready(tmp_path)

    revision = PitSilverRevisionPublisher(tmp_path).publish(
        index_id="sp500",
        membership_revision=1,
        as_of=AS_OF,
        actions_receipt=export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path),
    )

    assert revision.revision == 1
    manifest = json.loads((tmp_path / "silver/pit-revisions/current.json").read_text())
    assert manifest["silver_revision"] == 1
    assert manifest["membership_revision"] == 1
    assert manifest["members"][0]["symbol"] == "AAPL"
    assert manifest["inputs"]["silver_artifacts"][0]["sha256"] == hashlib.sha256(artifact.read_bytes()).hexdigest()
    assert (tmp_path / "silver/revisions/current.json").read_bytes() == apex_current
    assert list((tmp_path / "silver").rglob("1d.parquet")) == [artifact]


def test_identical_inputs_are_a_noop_and_tampered_silver_preserves_pointer(tmp_path: Path) -> None:
    artifact, _ = _ready(tmp_path)
    publisher = PitSilverRevisionPublisher(tmp_path)
    actions = export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path)
    first = publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)
    pointer = (tmp_path / "silver/pit-revisions/current.json").read_bytes()
    second = publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)
    assert second.revision == first.revision
    assert second.input_hash == first.input_hash
    assert second.manifest_path == first.manifest_path
    assert second.changed_paths == ()

    artifact.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="Silver artifact hash"):
        publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)
    assert (tmp_path / "silver/pit-revisions/current.json").read_bytes() == pointer


def test_actions_scope_must_name_exact_current_members_but_partial_state_is_preserved(tmp_path: Path) -> None:
    _ready(tmp_path)
    publisher = PitSilverRevisionPublisher(tmp_path)

    with pytest.raises(ValueError, match="action receipt symbol scope"):
        publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=_actions("MSFT"))

    forged = _actions("AAPL", state="UNRESOLVED")
    with pytest.raises(ValueError, match="local replay"):
        publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=forged)


def test_verifier_rejects_a_self_consistently_repointed_manifest_input(tmp_path: Path) -> None:
    _ready(tmp_path)
    publisher = PitSilverRevisionPublisher(tmp_path)
    actions = export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path)
    revision = publisher.publish(
        index_id="sp500",
        membership_revision=1,
        as_of=AS_OF,
        actions_receipt=actions,
    )
    current = tmp_path / "silver/pit-revisions/current.json"
    payload = json.loads(current.read_text())
    payload["inputs"]["silver_artifacts"][0]["path"] = "forged/1d.parquet"
    forged = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    current.write_bytes(forged)
    revision.manifest_path.write_bytes(forged)

    with pytest.raises(ValueError, match="input references"):
        publisher.verify()


def test_verifier_rejects_current_pointer_moved_back_to_an_older_revision(tmp_path: Path) -> None:
    _ready(tmp_path)
    publisher = PitSilverRevisionPublisher(tmp_path)
    actions = export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path)
    first = publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)
    publisher.publish(
        index_id="sp500",
        membership_revision=1,
        as_of=datetime(2026, 9, 1, 23, 59, tzinfo=UTC),
        actions_receipt=actions,
    )
    (tmp_path / "silver/pit-revisions/current.json").write_bytes(first.manifest_path.read_bytes())

    with pytest.raises(ValueError, match="not the latest"):
        publisher.verify()


def test_current_member_cannot_disappear_when_verified_identity_is_missing(tmp_path: Path) -> None:
    _ready(tmp_path)
    actions = export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path)
    (tmp_path / "security_master/events.parquet").unlink()

    with pytest.raises(ValueError, match="missing verified security identity"):
        PitSilverRevisionPublisher(tmp_path).publish(
            index_id="sp500",
            membership_revision=1,
            as_of=AS_OF,
            actions_receipt=actions,
        )


def test_later_appends_do_not_invalidate_the_frozen_membership_and_identity_prefix(tmp_path: Path) -> None:
    _ready(tmp_path)
    publisher = PitSilverRevisionPublisher(tmp_path)
    actions = export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path)
    published = publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)

    evidence = SourceEvidenceStore(tmp_path)
    artifact = evidence.persist_raw(b"later verified source")

    def verify(ref: str, digest: str) -> bool:
        return hashlib.sha256(evidence.read(ref)).hexdigest() == digest

    master = SecurityMaster(tmp_path, evidence_verifier=verify)
    security_id = "sec_00000000000000000000000000000002"
    master.append(
        SecurityIdentityEvent(
            event_id="identity-later",
            security_id=security_id,
            revision=1,
            symbol="MSFT",
            provider="massive",
            exchange_mic="XNAS",
            currency="USD",
            effective_from=datetime(2026, 9, 1, tzinfo=UTC),
            effective_to=None,
            known_at=datetime(2026, 9, 1, tzinfo=UTC),
            issuer_name="Microsoft Corp.",
            cik=None,
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
    IndexMembershipStore(tmp_path, security_master=master, evidence_verifier=verify).append(
        MembershipEvent(
            event_id="membership-later",
            index_id="sp500",
            security_id=security_id,
            action="add",
            announced_at=datetime(2026, 9, 1, tzinfo=UTC),
            effective_at=datetime(2026, 9, 1, tzinfo=UTC),
            known_at=datetime(2026, 9, 1, tzinfo=UTC),
            source_refs=(artifact.ref,),
            source_hashes=(artifact.sha256,),
            revision=1,
            supersedes=None,
            status="verified",
        )
    )

    assert publisher.verify(published.manifest_path)["status"] == "PROVEN"


def test_later_silver_revision_does_not_invalidate_frozen_pit_revision(tmp_path: Path) -> None:
    _ready(tmp_path)
    publisher = PitSilverRevisionPublisher(tmp_path)
    published = publisher.publish(
        index_id="sp500",
        membership_revision=1,
        as_of=AS_OF,
        actions_receipt=export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path),
    )
    extra = tmp_path / "silver/asset_class=equity/symbol=MSFT/1d.parquet"
    extra.parent.mkdir(parents=True)
    extra.write_bytes(b"later unrelated Silver bytes")
    SilverRevisionPublisher(tmp_path / "silver").publish(
        [PublishedArtifact(extra, hashlib.sha256(extra.read_bytes()).hexdigest(), 1)],
        [AffectedSymbol("MSFT", date(2026, 8, 31), ("1d",))],
        AS_OF,
    )

    assert publisher.verify(published.manifest_path)["silverRevision"] == 1


def test_verified_identity_must_cover_every_instant_of_membership_span(tmp_path: Path) -> None:
    _seed(
        tmp_path,
        [
            ("AAPL", datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 8, 10, tzinfo=UTC)),
            ("AAPL", datetime(2026, 8, 20, tzinfo=UTC), None),
        ],
        membership_effective=datetime(2026, 8, 1, tzinfo=UTC),
    )
    _silver(tmp_path)
    _verified_empty_fetch(tmp_path, "AAPL")

    with pytest.raises(ValueError, match="identity has a gap"):
        PitSilverRevisionPublisher(tmp_path).publish(
            index_id="sp500",
            membership_revision=1,
            as_of=AS_OF,
            actions_receipt=export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path),
        )


def test_future_known_correction_does_not_suppress_visible_membership(tmp_path: Path) -> None:
    _ready(tmp_path)
    evidence = SourceEvidenceStore(tmp_path)
    artifact = evidence.persist_raw(b"future correction")

    def verify(ref: str, digest: str) -> bool:
        return hashlib.sha256(evidence.read(ref)).hexdigest() == digest

    master = SecurityMaster(tmp_path, evidence_verifier=verify)
    IndexMembershipStore(tmp_path, security_master=master, evidence_verifier=verify).append(
        MembershipEvent(
            event_id="membership-future-correction",
            index_id="sp500",
            security_id="sec_00000000000000000000000000000001",
            action="add",
            announced_at=datetime(2026, 9, 2, tzinfo=UTC),
            effective_at=datetime(2026, 8, 28, tzinfo=UTC),
            known_at=datetime(2026, 9, 2, tzinfo=UTC),
            source_refs=(artifact.ref,),
            source_hashes=(artifact.sha256,),
            revision=2,
            supersedes="membership-sec_00000000000000000000000000000001",
            status="verified",
        )
    )

    actions = export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path)
    result = PitSilverRevisionPublisher(tmp_path).publish(
        index_id="sp500", membership_revision=2, as_of=AS_OF, actions_receipt=actions
    )

    manifest = json.loads(result.manifest_path.read_text())
    assert [item["symbol"] for item in manifest["members"]] == ["AAPL"]


def test_verifier_rejects_tampered_status_and_top_level_lineage(tmp_path: Path) -> None:
    _ready(tmp_path)
    publisher = PitSilverRevisionPublisher(tmp_path)
    revision = publisher.publish(
        index_id="sp500",
        membership_revision=1,
        as_of=AS_OF,
        actions_receipt=export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path),
    )
    current = tmp_path / "silver/pit-revisions/current.json"
    payload = json.loads(current.read_text())
    payload["status"] = "PARTIAL"
    payload["policy_version"] = "forged-policy"
    forged = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    current.write_bytes(forged)
    revision.manifest_path.write_bytes(forged)

    with pytest.raises(ValueError, match="lineage|status"):
        publisher.verify()


def test_noop_publish_reports_no_changed_paths(tmp_path: Path) -> None:
    _ready(tmp_path)
    actions = export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path)
    publisher = PitSilverRevisionPublisher(tmp_path)
    first = publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)
    second = publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)

    assert first.changed_paths
    assert second.changed_paths == ()


def test_valid_orphan_is_adopted_and_invalid_orphan_is_quarantined(tmp_path: Path) -> None:
    _ready(tmp_path)
    publisher = PitSilverRevisionPublisher(tmp_path)
    actions = export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path)
    first = publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)
    orphan_payload = json.loads(first.manifest_path.read_text())
    orphan_payload["revision"] = 2
    orphan_payload["generation_id"] = "crash-complete-2"
    orphan = tmp_path / "silver/pit-revisions/revision=2.json"
    orphan.write_bytes((json.dumps(orphan_payload, sort_keys=True, separators=(",", ":")) + "\n").encode())

    recovered = publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)
    assert recovered.revision == 2
    assert publisher.verify()["revision"] == 2

    invalid = tmp_path / "silver/pit-revisions/revision=3.json"
    invalid.write_bytes(b"torn")
    changed = publisher.publish(
        index_id="sp500",
        membership_revision=1,
        as_of=datetime(2026, 9, 1, 23, 59, tzinfo=UTC),
        actions_receipt=actions,
    )
    assert changed.revision == 3
    assert list((tmp_path / "silver/pit-revisions/quarantine").glob("revision=3.json.*.orphan"))


def test_orphan_filename_and_payload_revision_must_match(tmp_path: Path) -> None:
    _ready(tmp_path)
    publisher = PitSilverRevisionPublisher(tmp_path)
    actions = export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path)
    first = publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)
    (tmp_path / "silver/pit-revisions/revision=2.json").write_bytes(first.manifest_path.read_bytes())

    recovered = publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)
    assert recovered.revision == 1
    assert list((tmp_path / "silver/pit-revisions/quarantine").glob("revision=2.json.*.orphan"))

    changed = publisher.publish(
        index_id="sp500",
        membership_revision=1,
        as_of=datetime(2026, 9, 1, 23, 59, tzinfo=UTC),
        actions_receipt=actions,
    )
    assert changed.revision == 2


def test_changed_publish_rejects_a_tampered_current_pointer(tmp_path: Path) -> None:
    _ready(tmp_path)
    publisher = PitSilverRevisionPublisher(tmp_path)
    actions = export_actions(["AAPL"], AS_OF, data_lake_root=tmp_path)
    publisher.publish(index_id="sp500", membership_revision=1, as_of=AS_OF, actions_receipt=actions)
    current = tmp_path / "silver/pit-revisions/current.json"
    payload = json.loads(current.read_text())
    payload["revision"] = 999
    payload["input_hash"] = "sha256:" + "f" * 64
    current.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="current pointer"):
        publisher.publish(
            index_id="sp500",
            membership_revision=1,
            as_of=datetime(2026, 9, 1, 23, 59, tzinfo=UTC),
            actions_receipt=actions,
        )
