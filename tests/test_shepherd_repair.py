from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import pytest

from clients.bronze_client import BronzeClient
from clients.security_master import SecurityIdentityEvent, SecurityMaster
from clients.shepherd_repair import ShepherdRepair
from clients.source_evidence import SourceEvidenceStore

SECURITY = "sec_00000000000000000000000000000001"
VERIFY_AT = datetime(2026, 9, 1, tzinfo=UTC)


def _bind_scope(payload: dict) -> None:
    scope = {
        "kind": "security-interval",
        "securityId": payload["securityId"],
        "symbol": payload["symbol"],
        "symbolValidFrom": payload["symbolValidFrom"],
        "dateFrom": payload["dateFrom"],
        "dateTo": payload["dateTo"],
        "timeframe": payload["timeframe"],
        "layer": payload["layer"],
    }
    if payload["symbolValidTo"] is not None:
        scope["symbolValidTo"] = payload["symbolValidTo"]
    encoded = json.dumps(scope, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    payload["scopeHash"] = f"sha256:{hashlib.sha256(encoded).hexdigest()}"
    payload["workUnitId"] = f"lws-{payload['scopeHash'][len('sha256:') :][:32]}"


def _jsonable(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _row(day: str, close: float) -> dict:
    return {
        "trade_date": day,
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "adj_close": close,
        "volume": 100,
        "source": "massive",
        "price_basis": "raw",
    }


def _fixture(
    root: Path,
    *,
    patch_day: str = "2026-08-31",
    max_rows: int = 2,
    max_bytes: int = 1_000_000,
    expires_at: str = "2027-01-01T00:00:00Z",
    security_id: str = SECURITY,
    date_from: str = "2026-08-31",
    date_to: str = "2026-08-31",
    prior_day: str = "2026-08-28",
    identity_as_of: str = "2026-08-31T00:00:00Z",
) -> Path:
    target = root / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    BronzeClient(target.parent.parent).replace_ticker_rows("AAPL", [_row(prior_day, 100.0)])
    patch_root = root / "fixture-patch"
    BronzeClient(patch_root).replace_ticker_rows("AAPL", [_row(patch_day, 101.0)])
    patch = patch_root / "symbol=AAPL/1d.parquet"
    evidence = SourceEvidenceStore(root).persist_raw(patch.read_bytes())
    identity_evidence = SourceEvidenceStore(root).persist_raw(b"AAPL identity fixture")
    master = SecurityMaster(
        root,
        evidence_verifier=lambda ref, digest: hashlib.sha256(SourceEvidenceStore(root).read(ref)).hexdigest() == digest,
    )
    master.append(
        SecurityIdentityEvent(
            event_id="identity-aapl-v1",
            security_id=SECURITY,
            revision=1,
            symbol="AAPL",
            provider="massive",
            exchange_mic="XNAS",
            currency="USD",
            effective_from=datetime(2000, 1, 1, tzinfo=UTC),
            effective_to=None,
            known_at=datetime(2000, 1, 1, tzinfo=UTC),
            issuer_name="Apple Inc.",
            cik="0000320193",
            composite_figi="BBG000B9XRY4",
            share_class_figi=None,
            continuity_basis="provider_figi",
            relationship_type=None,
            related_security_id=None,
            source_refs=(identity_evidence.ref,),
            source_hashes=(identity_evidence.sha256,),
            status="verified",
            supersedes=None,
        )
    )
    payload = {
        "version": 1,
        "operationId": "repair-aapl-20260831",
        "workUnitId": "",
        "scopeHash": "",
        "dataLakeRoot": str(root.resolve()),
        "layer": "bronze",
        "securityId": security_id,
        "symbol": "AAPL",
        "symbolValidFrom": "2000-01-01T00:00:00Z",
        "symbolValidTo": None,
        "identityAsOf": identity_as_of,
        "securityMasterRevision": 1,
        "securityMasterSha256": hashlib.sha256(
            (json.dumps([_jsonable(asdict(master.events()[0]))], sort_keys=True, separators=(",", ":")) + "\n").encode()
        ).hexdigest(),
        "sessionPolicy": "XNYS-close-and-early-close-v2",
        "dateFrom": date_from,
        "dateTo": date_to,
        "timeframe": "1d",
        "priorArtifacts": [
            {
                "path": "bronze/asset_class=equity/symbol=AAPL/1d.parquet",
                "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
            }
        ],
        "sourceEvidence": [{"ref": evidence.ref, "sha256": evidence.sha256}],
        "maxRows": max_rows,
        "maxBytes": max_bytes,
        "expiresAt": expires_at,
        "operation": "daily-merge",
    }
    _bind_scope(payload)
    manifest = root / "repair.json"
    manifest.write_text(json.dumps(payload, sort_keys=True))
    return manifest


def _replace_patch_evidence(root: Path, manifest: Path, rows: list[dict]) -> None:
    patch_root = root / "replacement-patch"
    BronzeClient(patch_root).replace_ticker_rows("AAPL", rows)
    patch = patch_root / "symbol=AAPL/1d.parquet"
    evidence = SourceEvidenceStore(root).persist_raw(patch.read_bytes())
    payload = json.loads(manifest.read_text())
    payload["sourceEvidence"] = [{"ref": evidence.ref, "sha256": evidence.sha256}]
    manifest.write_text(json.dumps(payload, sort_keys=True))


def test_preflight_binds_root_target_prior_hash_and_source_evidence(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    repair = ShepherdRepair(tmp_path)

    receipt = repair.preflight(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))

    assert receipt["state"] == "PREFLIGHT_OK"
    assert receipt["changedPaths"] == []
    assert receipt["scopeHash"] == "sha256:116150a93507c94466250bcc05b456595c3c5bdca6917058071ac9bead41e4ef"
    assert receipt["targetPaths"] == [str(tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet")]


def test_scope_hash_matches_helium_golden_vector_with_unicode_and_interval_end() -> None:
    payload = {
        "securityId": SECURITY,
        "symbol": "台積電",
        "symbolValidFrom": "2000-01-01T00:00:00Z",
        "symbolValidTo": "2027-01-01T00:00:00Z",
        "dateFrom": "2026-08-31",
        "dateTo": "2026-09-01",
        "timeframe": "1d",
        "layer": "bronze",
    }

    _bind_scope(payload)

    assert payload["scopeHash"] == "sha256:dc8a01bb1b622396ba9b0018805a39344cfcadaf629fdce4d78d9590d6f12b21"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda payload, root: payload.update(expiresAt="2026-01-01T00:00:00Z"), "expired"),
        (lambda payload, root: payload.update(dataLakeRoot=str(root / "other")), "data-lake root"),
        (
            lambda payload, root: payload["priorArtifacts"][0].update(path="../escape.parquet"),
            "escape",
        ),
        (lambda payload, root: payload["priorArtifacts"][0].update(sha256="f" * 64), "prior artifact"),
        (lambda payload, root: payload["sourceEvidence"][0].update(sha256="e" * 64), "source evidence"),
        (lambda payload, root: payload.update(maxRows="2"), "must be integers"),
        (lambda payload, root: payload["sourceEvidence"][0].update(note="unsigned"), "fields are unsupported"),
        (lambda payload, root: payload.update(workUnitId="lws-" + "2" * 32), "does not match scope hash"),
        (lambda payload, root: payload.update(symbol="MSFT"), "scope hash does not match"),
        (lambda payload, root: payload.update(operation="silver-rebuild"), "unsupported repair operation"),
        (
            lambda payload, root: payload.update(symbolValidFrom="2000-01-01T00:00:00+00:00"),
            "shared UTC Z format",
        ),
        (lambda payload, root: payload.update(securityMasterSha256="f" * 64), "append-prefix hash"),
    ],
)
def test_preflight_rejects_unbound_or_stale_manifest(tmp_path: Path, mutation, message: str) -> None:
    manifest = _fixture(tmp_path)
    payload = json.loads(manifest.read_text())
    mutation(payload, tmp_path)
    manifest.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match=message):
        ShepherdRepair(tmp_path).preflight(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))


def test_preflight_rejects_a_symlinked_target(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    target = tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    real = tmp_path / "outside.parquet"
    real.write_bytes(target.read_bytes())
    target.unlink()
    target.symlink_to(real)

    with pytest.raises(ValueError, match="symlink"):
        ShepherdRepair(tmp_path).preflight(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))


def test_identity_prefix_ignores_later_appends_but_requires_exact_interval(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    evidence = SourceEvidenceStore(tmp_path)
    later = evidence.persist_raw(b"later identity evidence")
    master = SecurityMaster(
        tmp_path,
        evidence_verifier=lambda ref, digest: hashlib.sha256(evidence.read(ref)).hexdigest() == digest,
    )
    master.append(
        SecurityIdentityEvent(
            event_id="identity-msft-v1",
            security_id="sec_00000000000000000000000000000002",
            revision=1,
            symbol="MSFT",
            provider="massive",
            exchange_mic="XNAS",
            currency="USD",
            effective_from=datetime(1990, 1, 1, tzinfo=UTC),
            effective_to=None,
            known_at=datetime(1990, 1, 1, tzinfo=UTC),
            issuer_name="Microsoft Corp.",
            cik="0000789019",
            composite_figi="BBG000BPH459",
            share_class_figi=None,
            continuity_basis="provider_figi",
            relationship_type=None,
            related_security_id=None,
            source_refs=(later.ref,),
            source_hashes=(later.sha256,),
            status="verified",
            supersedes=None,
        )
    )

    assert (
        ShepherdRepair(tmp_path).preflight(
            manifest,
            now=datetime(2026, 8, 31, tzinfo=UTC),
        )["state"]
        == "PREFLIGHT_OK"
    )

    payload = json.loads(manifest.read_text())
    payload["symbolValidFrom"] = "1999-01-01T00:00:00Z"
    _bind_scope(payload)
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="identity is not uniquely verified"):
        ShepherdRepair(tmp_path).preflight(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))


def test_stage_is_noncanonical_and_rejects_wider_or_over_budget_patch(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    repair = ShepherdRepair(tmp_path)
    target = tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    original = target.read_bytes()

    staged = repair.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))

    assert target.read_bytes() == original
    assert Path(staged["candidates"][0]["path"]).is_file()
    assert staged["candidates"][0]["rows"] == 2
    assert staged["changedPaths"]

    wider = _fixture(tmp_path / "wider", patch_day="2026-09-01")
    with pytest.raises(ValueError, match="outside manifest date range"):
        ShepherdRepair(tmp_path / "wider").stage(wider, now=datetime(2026, 8, 31, tzinfo=UTC))

    tiny = _fixture(tmp_path / "tiny", max_bytes=1)
    with pytest.raises(ValueError, match="byte budget"):
        ShepherdRepair(tmp_path / "tiny").stage(tiny, now=datetime(2026, 8, 31, tzinfo=UTC))

    incomplete_root = tmp_path / "incomplete"
    incomplete = _fixture(incomplete_root)
    payload = json.loads(incomplete.read_text())
    payload["dateTo"] = "2026-09-01"
    _bind_scope(payload)
    incomplete.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="exactly cover"):
        ShepherdRepair(incomplete_root).stage(incomplete, now=datetime(2026, 8, 31, tzinfo=UTC))

    weekend_root = tmp_path / "weekend"
    weekend = _fixture(
        weekend_root,
        patch_day="2026-08-30",
        date_from="2026-08-30",
        date_to="2026-08-30",
    )
    with pytest.raises(ValueError, match="not a bound exchange session"):
        ShepherdRepair(weekend_root).stage(weekend, now=datetime(2026, 8, 31, tzinfo=UTC))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row.update(close=float("nan")),
        lambda row: row.update(high=row["close"] - 1),
    ],
)
def test_stage_rejects_nonfinite_or_inconsistent_patch_rows(tmp_path: Path, mutation) -> None:
    manifest = _fixture(tmp_path)
    row = _row("2026-08-31", 101.0)
    mutation(row)
    _replace_patch_evidence(tmp_path, manifest, [row])

    with pytest.raises(ValueError, match="OHLCV integrity"):
        ShepherdRepair(tmp_path).stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))


def test_publish_verify_and_rollback_restore_exact_prior_bytes(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    repair = ShepherdRepair(tmp_path)
    target = tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    original = target.read_bytes()
    staged = repair.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))

    published = repair.publish(manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC))
    verified = repair.verify(manifest, Path(published["receiptPath"]), now=VERIFY_AT)

    assert target.read_bytes() != original
    assert [row["trade_date"] for row in BronzeClient(target.parent.parent).read_symbol_rows("AAPL")] == [
        "2026-08-28",
        "2026-08-31",
    ]
    assert verified["state"] == "VERIFIED"
    assert verified["postconditions"]["coverage"]["tradingDates"] == ["2026-08-31"]
    assert verified["postconditions"]["identity"]["eventIds"] == ["identity-aapl-v1"]

    rolled_back = repair.rollback(manifest, Path(published["receiptPath"]))
    assert rolled_back["state"] == "ROLLED_BACK"
    assert target.read_bytes() == original
    with pytest.raises(ValueError, match="already terminal"):
        repair.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))


def test_verify_rejects_unproven_identity_and_pre_close_freshness(tmp_path: Path) -> None:
    wrong_root = tmp_path / "wrong-identity"
    wrong_manifest = _fixture(
        wrong_root,
        security_id="sec_00000000000000000000000000000002",
    )
    wrong = ShepherdRepair(wrong_root)
    with pytest.raises(ValueError, match="identity is not uniquely verified"):
        wrong.stage(wrong_manifest, now=datetime(2026, 8, 31, tzinfo=UTC))

    early_root = tmp_path / "early"
    early_manifest = _fixture(early_root)
    early = ShepherdRepair(early_root)
    early_staged = early.stage(early_manifest, now=datetime(2026, 8, 31, tzinfo=UTC))
    early_published = early.publish(
        early_manifest,
        Path(early_staged["receiptPath"]),
        now=datetime(2026, 8, 31, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="before the final session close"):
        early.verify(
            early_manifest,
            Path(early_published["receiptPath"]),
            now=datetime(2026, 8, 31, 19, 59, tzinfo=UTC),
        )


def test_verify_uses_the_bound_early_close_session(tmp_path: Path) -> None:
    manifest = _fixture(
        tmp_path,
        patch_day="2025-11-28",
        date_from="2025-11-28",
        date_to="2025-11-28",
        prior_day="2025-11-26",
        identity_as_of="2025-11-28T00:00:00Z",
    )
    after_close = datetime(2025, 11, 28, 18, 1, tzinfo=UTC)
    repair = ShepherdRepair(tmp_path)
    staged = repair.stage(manifest, now=after_close)
    published = repair.publish(manifest, Path(staged["receiptPath"]), now=after_close)

    verified = repair.verify(manifest, Path(published["receiptPath"]), now=after_close)

    assert verified["postconditions"]["freshness"] == {
        "state": "VERIFIED",
        "sessionThrough": "2025-11-28",
        "sessionClose": "2025-11-28T18:00:00+00:00",
        "sessionPolicy": "XNYS-close-and-early-close-v2",
    }


def test_publish_resumes_after_crash_between_replace_and_receipt(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    target = tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    repair = ShepherdRepair(
        tmp_path,
        failpoint=lambda point: (_ for _ in ()).throw(RuntimeError(point)) if point == "after-publish" else None,
    )
    staged = repair.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))

    with pytest.raises(RuntimeError, match="after-publish"):
        repair.publish(manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC))
    changed_hash = hashlib.sha256(target.read_bytes()).hexdigest()

    resumed = ShepherdRepair(tmp_path).publish(
        manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC)
    )
    assert resumed["state"] == "PUBLISHED"
    assert hashlib.sha256(target.read_bytes()).hexdigest() == changed_hash


def test_publish_recovery_survives_expiry_and_missing_candidate(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path, expires_at="2026-08-31T00:01:00Z")
    target = tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    crashing = ShepherdRepair(
        tmp_path,
        failpoint=lambda point: (_ for _ in ()).throw(RuntimeError(point)) if point == "after-publish" else None,
    )
    staged = crashing.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))
    with pytest.raises(RuntimeError, match="after-publish"):
        crashing.publish(manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC))
    published_bytes = target.read_bytes()
    Path(staged["candidates"][0]["path"]).unlink()
    source_hash = json.loads(manifest.read_text())["sourceEvidence"][0]["sha256"]
    crashing.evidence.raw_path(source_hash).unlink()

    resumed = ShepherdRepair(tmp_path).publish(
        manifest,
        Path(staged["receiptPath"]),
        now=datetime(2026, 8, 31, 0, 2, tzinfo=UTC),
    )

    assert resumed["state"] == "PUBLISHED"
    assert target.read_bytes() == published_bytes
    assert Path(staged["candidates"][0]["path"]).read_bytes() == published_bytes


def test_first_publish_requires_staged_source_evidence_to_remain_available(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    repair = ShepherdRepair(tmp_path)
    staged = repair.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))
    source_hash = json.loads(manifest.read_text())["sourceEvidence"][0]["sha256"]
    repair.evidence.raw_path(source_hash).unlink()

    with pytest.raises(ValueError, match="source evidence is missing"):
        repair.publish(manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC))


def test_first_publish_fails_closed_after_manifest_expiry(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path, expires_at="2026-08-31T00:01:00Z")
    repair = ShepherdRepair(tmp_path)
    staged = repair.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))

    with pytest.raises(ValueError, match="expired before canonical publish"):
        repair.publish(
            manifest,
            Path(staged["receiptPath"]),
            now=datetime(2026, 8, 31, 0, 2, tzinfo=UTC),
        )


def test_crash_resume_refuses_to_terminalize_without_the_exact_backup(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    repair = ShepherdRepair(
        tmp_path,
        failpoint=lambda point: (_ for _ in ()).throw(RuntimeError(point)) if point == "after-publish" else None,
    )
    staged = repair.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))
    with pytest.raises(RuntimeError, match="after-publish"):
        repair.publish(manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC))
    backup = next((tmp_path / "operations/shepherd-repairs/repair-aapl-20260831/backups").glob("*.bin"))
    backup.unlink()

    with pytest.raises(ValueError, match="evidence path or hash"):
        ShepherdRepair(tmp_path).publish(manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC))


def test_cached_verification_rechecks_the_live_target(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    repair = ShepherdRepair(tmp_path)
    staged = repair.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))
    published = repair.publish(manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC))
    repair.verify(manifest, Path(published["receiptPath"]), now=VERIFY_AT)
    target = tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    target.write_bytes(b"tampered after verification")

    with pytest.raises(ValueError, match="published target hash"):
        repair.verify(manifest, Path(published["receiptPath"]), now=VERIFY_AT)


def test_receipts_must_use_the_canonical_operation_path(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    repair = ShepherdRepair(tmp_path)
    staged = repair.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))
    forged = Path(staged["receiptPath"]).with_name("forged-stage.json")
    forged.write_bytes(Path(staged["receiptPath"]).read_bytes())

    with pytest.raises(ValueError, match="canonical operation receipt"):
        repair.publish(manifest, forged, now=datetime(2026, 8, 31, tzinfo=UTC))


@pytest.mark.parametrize("point", ["before-stage", "after-stage"])
def test_stage_crash_restarts_without_touching_canonical(tmp_path: Path, point: str) -> None:
    manifest = _fixture(tmp_path)
    target = tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    original = target.read_bytes()
    crashing = ShepherdRepair(
        tmp_path,
        failpoint=lambda current: (_ for _ in ()).throw(RuntimeError(current)) if current == point else None,
    )
    with pytest.raises(RuntimeError, match=point):
        crashing.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))
    assert target.read_bytes() == original

    resumed = ShepherdRepair(tmp_path).stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))
    assert resumed["state"] == "STAGED"
    assert target.read_bytes() == original


@pytest.mark.parametrize("point", ["after-backup", "before-publish"])
def test_pre_publish_crash_resumes_from_exact_prior_bytes(tmp_path: Path, point: str) -> None:
    manifest = _fixture(tmp_path)
    staged = ShepherdRepair(tmp_path).stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))
    crashing = ShepherdRepair(
        tmp_path,
        failpoint=lambda current: (_ for _ in ()).throw(RuntimeError(current)) if current == point else None,
    )
    with pytest.raises(RuntimeError, match=point):
        crashing.publish(manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC))

    resumed = ShepherdRepair(tmp_path).publish(
        manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC)
    )
    assert resumed["state"] == "PUBLISHED"


def test_verify_and_rollback_crashes_converge_without_duplicate_mutation(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    repair = ShepherdRepair(tmp_path)
    staged = repair.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))
    published = repair.publish(manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC))
    before_verify = ShepherdRepair(
        tmp_path,
        failpoint=lambda point: (_ for _ in ()).throw(RuntimeError(point)) if point == "before-verify" else None,
    )
    with pytest.raises(RuntimeError, match="before-verify"):
        before_verify.verify(manifest, Path(published["receiptPath"]), now=VERIFY_AT)
    assert (
        ShepherdRepair(tmp_path).verify(manifest, Path(published["receiptPath"]), now=VERIFY_AT)["state"] == "VERIFIED"
    )

    during_rollback = ShepherdRepair(
        tmp_path,
        failpoint=lambda point: (_ for _ in ()).throw(RuntimeError(point)) if point == "during-rollback" else None,
    )
    with pytest.raises(RuntimeError, match="during-rollback"):
        during_rollback.rollback(manifest, Path(published["receiptPath"]))
    assert ShepherdRepair(tmp_path).rollback(manifest, Path(published["receiptPath"]))["state"] == "ROLLED_BACK"


def test_rollback_does_not_depend_on_source_evidence_after_publish(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    repair = ShepherdRepair(tmp_path)
    target = tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    original = target.read_bytes()
    manifest_payload = json.loads(manifest.read_text())
    source_hash = manifest_payload["sourceEvidence"][0]["sha256"]
    staged = repair.stage(manifest, now=datetime(2026, 8, 31, tzinfo=UTC))
    published = repair.publish(manifest, Path(staged["receiptPath"]), now=datetime(2026, 8, 31, tzinfo=UTC))
    repair.evidence.raw_path(source_hash).unlink()

    rolled_back = repair.rollback(manifest, Path(published["receiptPath"]))

    assert rolled_back["state"] == "ROLLED_BACK"
    assert target.read_bytes() == original


def test_transaction_verifies_success_and_is_idempotent(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    repair = ShepherdRepair(tmp_path)

    first = repair.transaction(manifest, now=VERIFY_AT)
    second = repair.transaction(manifest, now=VERIFY_AT)

    assert first["state"] == "VERIFIED"
    assert second["state"] == "VERIFIED"
    assert first["verifyReceipt"]["receiptHash"] == second["verifyReceipt"]["receiptHash"]


def test_postcondition_is_read_only_before_action_and_rechecks_the_exact_terminal_transaction(tmp_path: Path) -> None:
    manifest = _fixture(tmp_path)
    repair = ShepherdRepair(tmp_path)
    state_root = tmp_path / "operations/shepherd-repairs/repair-aapl-20260831"

    before = repair.postcondition(manifest, now=VERIFY_AT)

    assert before["state"] == "NOT_VERIFIED"
    assert not state_root.exists()

    repair.transaction(manifest, now=VERIFY_AT)
    verified = repair.postcondition(manifest, now=VERIFY_AT)
    assert verified["state"] == "VERIFIED"
    assert verified["workUnitId"] == json.loads(manifest.read_text())["workUnitId"]

    target = tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    target.write_bytes(b"tampered after terminal verification")
    failed = repair.postcondition(manifest, now=VERIFY_AT)
    assert failed["state"] == "FAILED"
    assert "published target hash" in failed["reason"]


def test_transaction_rolls_back_when_independent_verification_fails(tmp_path: Path, monkeypatch) -> None:
    manifest = _fixture(tmp_path)
    target = tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    original = target.read_bytes()
    repair = ShepherdRepair(tmp_path)

    def fail_verify(*_args, **_kwargs):
        raise ValueError("forced independent postcondition failure")

    monkeypatch.setattr(repair, "verify", fail_verify)
    result = repair.transaction(manifest, now=VERIFY_AT)

    assert result["state"] == "ROLLED_BACK"
    assert result["error"] == "forced independent postcondition failure"
    assert result["rollbackReceipt"]["state"] == "ROLLED_BACK"
    assert target.read_bytes() == original
