from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from clients.mediawiki_client import MediaWikiSnapshot
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from livewire_scripts.shepherd_universe import (
    decision_payload_hash,
    import_decision,
    scan_index,
    scan_receipt,
    scope_hash_for,
    verify_revision,
)

NOW = datetime(2026, 8, 31, 8, 0, tzinfo=UTC)
RETRIEVED = datetime(2026, 8, 30, 20, 0, tzinfo=UTC)


class FakeWiki:
    def __init__(self, snapshot: MediaWikiSnapshot) -> None:
        self._snapshot = snapshot

    def snapshot(self, title: str) -> MediaWikiSnapshot:
        assert title == "List of S&P 500 companies"
        return self._snapshot


def _wiki(store: SourceEvidenceStore, symbols: list[str], *, content: str | None = None) -> MediaWikiSnapshot:
    if content is None:
        rows = "".join(f"<tr><td>{symbol}</td></tr>" for symbol in symbols)
        content = (
            '<html about="//en.wikipedia.org/wiki/Special:Redirect/revision/123">'
            '<head><meta property="dc:modified" content="2026-08-30T19:00:00Z" /></head>'
            f'<body><table id="constituents"><thead><tr><th>Symbol</th><th>Security</th></tr></thead>'
            f"<tbody>{rows}</tbody></table></body></html>"
        )
    raw = content.encode()
    artifact = store.persist_raw(raw)
    evidence = SourceEvidence(
        ref=artifact.ref,
        sha256=artifact.sha256,
        source_url="https://en.wikipedia.org/w/rest.php/v1/page/List/html",
        retrieved_at=RETRIEVED,
        publication_time=datetime(2026, 8, 30, 19, 0, tzinfo=UTC),
        mediawiki_revision_id=123,
        mediawiki_revision_time=datetime(2026, 8, 30, 19, 0, tzinfo=UTC),
        content_type="text/html",
    )
    store.record(evidence)
    return MediaWikiSnapshot(
        title="List of S&P 500 companies",
        canonical_url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        revision_id=123,
        revision_time=datetime(2026, 8, 30, 19, 0, tzinfo=UTC),
        content=content,
        evidence=evidence,
    )


def _preset(path: Path, symbols: list[str]) -> None:
    path.write_text(json.dumps({"name": "sp500", "tickers": symbols}), encoding="utf-8")


def _scan(tmp_path: Path) -> tuple[SourceEvidenceStore, dict[str, object], Path]:
    store = SourceEvidenceStore(tmp_path)
    snapshot = _wiki(store, ["AAPL", "MSFT"])
    preset = tmp_path / "sp500.json"
    _preset(preset, ["AAPL", "NVDA"])
    result = scan_index("sp500", store=store, wiki=FakeWiki(snapshot), preset_path=preset, now=RETRIEVED)
    return store, result, preset


def _decision(tmp_path: Path) -> tuple[dict[str, object], Path, SourceEvidenceStore]:
    store, scan, _ = _scan(tmp_path)
    sources = scan["sources"]
    assert isinstance(sources, list)
    accepted = ["identity.AAPL", "membership.sp500.AAPL"]
    security_id = "sec_00000000000000000000000000000001"
    common_sources = [{"ref": row["ref"], "sha256": row["sha256"]} for row in sources]
    manifest: dict[str, object] = {
        "version": 1,
        "indexId": "sp500",
        "scopeHash": scan["scopeHash"],
        "sourceEvidence": [{"ref": row["ref"], "sha256": row["sha256"], "kind": row["kind"]} for row in sources],
        "verifier": {
            "identity": "helium:independent-verifier",
            "decision": "pass",
            "decidedAt": "2026-08-31T07:00:00Z",
            "acceptedClaimKeys": accepted,
        },
        "identityEvents": [
            {
                "claimKey": accepted[0],
                "event_id": "identity-aapl-v1",
                "security_id": security_id,
                "revision": 1,
                "symbol": "AAPL",
                "provider": "listing",
                "exchange_mic": "XNAS",
                "currency": "USD",
                "effective_from": "1980-12-12T14:30:00Z",
                "effective_to": None,
                "known_at": "2026-08-31T06:00:00Z",
                "issuer_name": "Apple Inc.",
                "cik": "0000320193",
                "composite_figi": None,
                "share_class_figi": None,
                "continuity_basis": "regulator_filing",
                "relationship_type": None,
                "related_security_id": None,
                "sources": common_sources,
                "status": "verified",
                "supersedes": None,
            }
        ],
        "membershipEvents": [
            {
                "claimKey": accepted[1],
                "event_id": "membership-aapl-v1",
                "index_id": "sp500",
                "security_id": security_id,
                "action": "add",
                "announced_at": "2026-08-30T19:00:00Z",
                "effective_at": "2026-08-30T19:00:00Z",
                "known_at": "2026-08-31T06:00:00Z",
                "sources": common_sources,
                "revision": 1,
                "supersedes": None,
                "status": "verified",
            }
        ],
    }
    _resign(manifest, store)
    path = tmp_path / "decision.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest, path, store


def _resign(manifest: dict[str, object], store: SourceEvidenceStore) -> None:
    verifier = manifest["verifier"]
    verifier_receipt = {
        "version": 1,
        "identity": verifier["identity"],
        "decision": "pass",
        "decidedAt": verifier["decidedAt"],
        "scopeHash": manifest["scopeHash"],
        "acceptedClaimKeys": verifier["acceptedClaimKeys"],
        "decisionPayloadHash": decision_payload_hash(manifest),
    }
    verifier_artifact = store.persist_raw(
        (json.dumps(verifier_receipt, sort_keys=True, separators=(",", ":")) + "\n").encode()
    )
    manifest["verifier"].update(evidenceRef=verifier_artifact.ref, evidenceHash=verifier_artifact.sha256)


def test_scan_preserves_preset_and_emits_evidence_bound_conflict_case(tmp_path: Path) -> None:
    store, result, preset = _scan(tmp_path)

    assert json.loads(preset.read_text())["tickers"] == ["AAPL", "NVDA"]
    assert result["mutated"] is False
    assert result["counts"] == {"wikipedia": 2, "preset": 2, "conflicts": 2}
    assert result["teamCase"]["variant"] == "source-conflict"
    claims = {row["symbol"]: row for row in result["claims"]}
    assert len(claims["AAPL"]["evidenceRefs"]) == 2
    assert len(claims["MSFT"]["evidenceRefs"]) == 1
    assert len(claims["NVDA"]["evidenceRefs"]) == 1
    assert not (tmp_path / "security_master").exists()
    assert not (tmp_path / "index_membership").exists()
    assert store.read(result["scanArtifact"]["ref"])

    receipt = scan_receipt(result)
    assert "claims" not in receipt
    assert receipt["scanArtifact"] == result["scanArtifact"]
    assert receipt["teamCase"] == result["teamCase"]


def test_scan_rejects_naive_clock(tmp_path: Path) -> None:
    store = SourceEvidenceStore(tmp_path)
    preset = tmp_path / "sp500.json"
    _preset(preset, ["AAPL"])
    with pytest.raises(ValueError, match="timezone-aware"):
        scan_index(
            "sp500",
            store=store,
            wiki=FakeWiki(_wiki(store, ["AAPL"])),
            preset_path=preset,
            now=datetime(2026, 8, 31),
        )


def test_scan_turns_unparseable_revision_into_a_case_without_fake_members(tmp_path: Path) -> None:
    store = SourceEvidenceStore(tmp_path)
    snapshot = _wiki(
        store,
        [],
        content="""<table class="wikitable"><tbody><tr><th>Category</th><th>All-Time Highs</th></tr>
        <tr><td>Closing</td><td>30,000</td></tr><tr><td>Intraday</td><td>31,000</td></tr></tbody></table>""",
    )
    preset = tmp_path / "sp500.json"
    _preset(preset, ["AAPL", "NVDA"])

    result = scan_index("sp500", store=store, wiki=FakeWiki(snapshot), preset_path=preset, now=RETRIEVED)

    assert result["counts"] == {"wikipedia": 0, "preset": 2, "conflicts": 2}
    assert result["teamCase"]["sourceStates"] == {"wikipedia": "unparseable", "preset": "parsed"}
    assert {row["symbol"] for row in result["claims"]} == {"AAPL", "NVDA"}
    assert "Closing" not in result["teamCase"]["symbols"]


def test_verified_decision_imports_idempotently_and_replays_revision(tmp_path: Path) -> None:
    _, path, _ = _decision(tmp_path)

    first = import_decision(path.resolve(), data_lake_root=tmp_path, now=NOW)
    second = import_decision(path.resolve(), data_lake_root=tmp_path, now=NOW)
    receipt = verify_revision("sp500", 1, data_lake_root=tmp_path, effective_at=NOW, as_of=NOW)

    assert first["identityEventsAppended"] == 1
    assert first["membershipEventsAppended"] == 1
    assert second["identityEventsAppended"] == 0
    assert second["membershipEventsAppended"] == 0
    assert receipt["revisionSemantics"] == "append-order-prefix"
    assert receipt["members"] == [{"securityId": "sec_00000000000000000000000000000001", "symbol": "AAPL"}]
    assert receipt["stateHint"] == "VERIFIED"
    assert receipt["changedPaths"] == []
    assert receipt["receiptHash"].startswith("sha256:")


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda body: body["sourceEvidence"][0].update(kind="livewire-preset"), "preset"),
        (lambda body: body["verifier"].update(decidedAt="2026-09-01T00:00:00Z"), "future"),
        (lambda body: body.update(scopeHash="sha256:" + "0" * 64), "scope hash"),
        (lambda body: body["sourceEvidence"][0].update(sha256="0" * 64), "missing or corrupt"),
        (
            lambda body: body["membershipEvents"][0].update(effective_at="2026-08-29T19:00:00Z"),
            "exact decision payload",
        ),
    ],
)
def test_import_rejects_adversarial_manifest_without_mutation(tmp_path: Path, mutate, message: str) -> None:
    manifest, path, _ = _decision(tmp_path)
    bad = copy.deepcopy(manifest)
    mutate(bad)
    path.write_text(json.dumps(bad), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        import_decision(path.resolve(), data_lake_root=tmp_path, now=NOW)
    assert not (tmp_path / "security_master" / "events.parquet").exists()
    assert not (tmp_path / "index_membership" / "sp500" / "events.parquet").exists()


def test_even_a_bound_verifier_cannot_import_an_unaccepted_event(tmp_path: Path) -> None:
    manifest, path, store = _decision(tmp_path)
    manifest["membershipEvents"][0]["claimKey"] = "not-accepted"
    _resign(manifest, store)
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="accepted claim"):
        import_decision(path.resolve(), data_lake_root=tmp_path, now=NOW)
    assert not (tmp_path / "security_master" / "events.parquet").exists()


def test_import_preflights_all_events_before_appending_anything(tmp_path: Path) -> None:
    manifest, path, store = _decision(tmp_path)
    manifest["membershipEvents"][0]["security_id"] = "sec_ffffffffffffffffffffffffffffffff"
    _resign(manifest, store)
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="verified security identity"):
        import_decision(path.resolve(), data_lake_root=tmp_path, now=NOW)
    assert not (tmp_path / "security_master" / "events.parquet").exists()
    assert not (tmp_path / "index_membership" / "sp500" / "events.parquet").exists()


def test_scope_hash_is_order_independent() -> None:
    sources = [
        {"ref": "artifact://sha256/" + "1" * 64, "sha256": "1" * 64},
        {"ref": "artifact://sha256/" + "2" * 64, "sha256": "2" * 64},
    ]
    assert scope_hash_for("sp500", sources) == scope_hash_for("sp500", list(reversed(sources)))


def test_verify_fails_closed_when_identity_evidence_is_tampered(tmp_path: Path) -> None:
    manifest, path, store = _decision(tmp_path)
    manifest["identityEvents"][0]["sources"] = [manifest["identityEvents"][0]["sources"][0]]
    manifest["membershipEvents"][0]["sources"] = [manifest["membershipEvents"][0]["sources"][1]]
    _resign(manifest, store)
    path.write_text(json.dumps(manifest), encoding="utf-8")
    import_decision(path.resolve(), data_lake_root=tmp_path, now=NOW)
    digest = manifest["sourceEvidence"][0]["sha256"]
    store.raw_path(digest).write_bytes(b"tampered")

    with pytest.raises(ValueError, match="hash mismatch|corrupt evidence"):
        verify_revision("sp500", 1, data_lake_root=tmp_path, effective_at=NOW, as_of=NOW)
