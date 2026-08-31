from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from clients.pit_silver_revision import PitSilverRevisionPublisher
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
