from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from clients.pit_silver_revision import PitSilverRevisionPublisher
from clients.security_master import SecurityMaster
from clients.source_evidence import SourceEvidenceStore
from livewire_scripts.shepherd_silver import publish_pit
from tests.test_pit_silver_revision import AS_OF, _silver
from tests.test_shepherd_actions import _verified_empty_fetch
from tests.test_shepherd_daily import _seed


def _fixture(root: Path) -> None:
    _seed(root, [("AAPL", datetime(2026, 8, 28, tzinfo=UTC), None)])
    _silver(root)
    _verified_empty_fetch(root, "AAPL")


def test_publish_uses_exact_current_member_action_scope_and_verifies_replay(tmp_path: Path) -> None:
    _fixture(tmp_path)

    receipt = publish_pit("sp500", 1, AS_OF, data_lake_root=tmp_path)
    verified = PitSilverRevisionPublisher(tmp_path).verify()

    assert receipt["status"] == "PROVEN"
    assert receipt["actionSummary"] == {"requested": 1, "verified": 1, "unresolved": 0}
    assert verified["revision"] == receipt["revision"]
    assert verified["inputHash"] == receipt["inputHash"]
    assert verified["changedPaths"] == []


def test_verify_rejects_membership_changed_after_publication(tmp_path: Path) -> None:
    _fixture(tmp_path)
    publish_pit("sp500", 1, AS_OF, data_lake_root=tmp_path)
    membership = tmp_path / "index_membership/sp500/events.parquet"
    membership.write_bytes(b"tampered")

    with pytest.raises(Exception, match="membership|Parquet|magic bytes"):
        PitSilverRevisionPublisher(tmp_path).verify()


def test_a_relabel_known_after_as_of_does_not_leak_into_the_action_scope(tmp_path: Path) -> None:
    # daily_bar_cutoff(AS_OF) is the 2026-08-31 session and plan_daily read identities up to that day's
    # end, so a relabel known 30s after as_of named META in the receipt while the publisher scoped FB.
    _seed(tmp_path, [("FB", datetime(2026, 8, 28, tzinfo=UTC), None)])
    _silver(tmp_path)
    _verified_empty_fetch(tmp_path, "FB")
    evidence = SourceEvidenceStore(tmp_path)
    master = SecurityMaster(
        tmp_path, evidence_verifier=lambda ref, digest: hashlib.sha256(evidence.read(ref)).hexdigest() == digest
    )
    first = master.events()[0]
    master.append(
        replace(
            first,
            event_id="identity-relabel",
            revision=2,
            symbol="META",
            known_at=AS_OF + timedelta(seconds=30),
            supersedes=first.event_id,
        )
    )

    receipt = publish_pit("sp500", 1, AS_OF, data_lake_root=tmp_path)

    assert receipt["status"] == "PROVEN"
    assert receipt["actionSummary"] == {"requested": 1, "verified": 1, "unresolved": 0}
