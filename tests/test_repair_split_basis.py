from __future__ import annotations

import json
from pathlib import Path

import pytest

from clients.bronze_client import BronzeClient
from livewire_scripts import audit_split_basis, repair_split_basis
from tests.test_audit_split_basis import _seed


def _manifest(tmp_path):
    output = tmp_path / "audit.json"
    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(output)], data_lake_root=tmp_path) == 0
    return output


def _approve(path):
    payload = json.loads(path.read_text())
    payload["symbols"][0]["approved"] = True
    path.write_text(json.dumps(payload, sort_keys=True))


def _two_symbol_manifest(tmp_path):
    first = _seed(tmp_path)
    manifest = _manifest(tmp_path)
    client = BronzeClient(first.parents[1], "equity")
    client.replace_ticker_rows("MSFT", client.read_symbol_rows("AAPL"))
    second = client.symbol_path("MSFT")
    payload = json.loads(manifest.read_text())
    payload["symbols"][0]["approved"] = True
    item = {
        **payload["symbols"][0],
        "symbol": "MSFT",
        "path": str(second),
        "source_sha256": repair_split_basis.sha256_file(second),
    }
    payload["symbols"].append(item)
    manifest.write_text(json.dumps(payload))
    return manifest, [first, second]


def test_manifest_symbol_must_match_its_target(tmp_path):
    manifest, paths = _two_symbol_manifest(tmp_path)
    before = [path.read_bytes() for path in paths]
    payload = json.loads(manifest.read_text())
    payload["symbols"][0]["symbol"] = "MSFT"
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="target does not match symbol"):
        repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path)
    assert [path.read_bytes() for path in paths] == before


@pytest.mark.parametrize("rollback", [False, True])
@pytest.mark.parametrize("stop_item", [1, 2])
@pytest.mark.parametrize("after_replace", [False, True])
def test_partial_batch_resumes_from_durable_per_item_intent(tmp_path, monkeypatch, rollback, stop_item, after_replace):
    manifest, paths = _two_symbol_manifest(tmp_path)
    original_bytes = [path.read_bytes() for path in paths]
    args = ["--manifest", str(manifest)]
    if rollback:
        repair_split_basis.run(args, data_lake_root=tmp_path)
        args.append("--rollback")
    restore = repair_split_basis.restore_parquet_exact
    calls = 0

    def interrupted(backup, target, checksum):
        nonlocal calls
        if target in paths:
            calls += 1
            durable = json.loads(manifest.read_text())["symbols"][calls - 1]
            assert durable["repair_state"] == ("rolling_back" if rollback else "applying")
            assert Path(durable["backup_path"]).read_bytes() == original_bytes[calls - 1]
            assert checksum == durable["source_sha256" if rollback else "applied_sha256"]
            if calls == stop_item:
                if after_replace:
                    restore(backup, target, checksum)
                raise RuntimeError("interrupted batch")
        return restore(backup, target, checksum)

    monkeypatch.setattr(repair_split_basis, "restore_parquet_exact", interrupted)
    with pytest.raises(RuntimeError, match="interrupted batch"):
        repair_split_basis.run(args, data_lake_root=tmp_path)
    monkeypatch.setattr(repair_split_basis, "restore_parquet_exact", restore)
    repair_split_basis.run(args, data_lake_root=tmp_path)
    final = json.loads(manifest.read_text())
    assert {item["repair_state"] for item in final["symbols"]} == {"restored" if rollback else "applied"}
    for item, path, before in zip(final["symbols"], paths, original_bytes, strict=True):
        assert Path(item["backup_path"]).read_bytes() == before
        assert repair_split_basis.sha256_file(path) == item["source_sha256" if rollback else "applied_sha256"]
    # A complete retry is idempotent and never replaces the original undo bytes.
    repair_split_basis.run(args, data_lake_root=tmp_path)


def test_partial_apply_can_rollback_without_backup_for_untouched_symbol(tmp_path, monkeypatch):
    manifest, paths = _two_symbol_manifest(tmp_path)
    original = [p.read_bytes() for p in paths]
    restore = repair_split_basis.restore_parquet_exact

    def interrupted(backup, target, checksum):
        restore(backup, target, checksum)
        if target == paths[0]:
            raise RuntimeError("after first apply")

    monkeypatch.setattr(repair_split_basis, "restore_parquet_exact", interrupted)
    with pytest.raises(RuntimeError):
        repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path)
    monkeypatch.setattr(repair_split_basis, "restore_parquet_exact", restore)
    repair_split_basis.run(["--manifest", str(manifest), "--rollback"], data_lake_root=tmp_path)
    assert [p.read_bytes() for p in paths] == original


def test_unapproved_manifest_does_not_mutate_bronze(tmp_path):
    bronze_path = _seed(tmp_path)
    manifest = _manifest(tmp_path)
    before = bronze_path.read_bytes()

    with pytest.raises(ValueError, match="approved"):
        repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path)
    assert bronze_path.read_bytes() == before


def test_explicit_approve_flag_records_approval_and_applies(tmp_path):
    bronze_path = _seed(tmp_path)
    manifest = _manifest(tmp_path)

    assert repair_split_basis.run(["--manifest", str(manifest), "--approve"], data_lake_root=tmp_path) == 0

    payload = json.loads(manifest.read_text())
    assert payload["symbols"][0]["approved"] is True
    row = BronzeClient(bronze_path.parents[1], "equity").read_symbol_rows("AAPL")[0]
    assert row["price_basis"] == "raw"


def test_entire_manifest_is_preflighted_before_first_mutation(tmp_path):
    bronze_path = _seed(tmp_path)
    original = bronze_path.read_bytes()
    manifest = _manifest(tmp_path)
    payload = json.loads(manifest.read_text())
    payload["symbols"][0]["approved"] = True
    invalid = {**payload["symbols"][0], "symbol": "LATE", "eligible": False}
    payload["symbols"].append(invalid)
    manifest.write_text(json.dumps(payload, sort_keys=True))

    with pytest.raises(ValueError, match="LATE"):
        repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path)

    assert bronze_path.read_bytes() == original


def test_stale_manifest_is_rejected(tmp_path):
    bronze_path = _seed(tmp_path)
    manifest = _manifest(tmp_path)
    _approve(manifest)
    bronze_path.write_bytes(bronze_path.read_bytes() + b"stale")
    stale = bronze_path.read_bytes()

    with pytest.raises(ValueError, match="stale"):
        repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path)
    assert bronze_path.read_bytes() == stale


@pytest.mark.parametrize("rollback", [False, True])
def test_retry_rejects_later_canonical_drift(tmp_path, rollback):
    path = _seed(tmp_path)
    manifest = _manifest(tmp_path)
    repair_split_basis.run(["--manifest", str(manifest), "--approve"], data_lake_root=tmp_path)
    item = json.loads(manifest.read_text())["symbols"][0]
    backup_before = Path(item["backup_path"]).read_bytes()
    client = BronzeClient(path.parents[1], "equity")
    client.merge_ticker_rows("AAPL", [{**client.read_symbol_rows("AAPL")[-1], "trade_date": "2020-09-01"}])
    current = path.read_bytes()
    args = ["--manifest", str(manifest)] + (["--rollback"] if rollback else [])
    with pytest.raises(ValueError, match="stale"):
        repair_split_basis.run(args, data_lake_root=tmp_path)
    assert path.read_bytes() == current
    assert Path(item["backup_path"]).read_bytes() == backup_before


def test_retry_never_overwrites_a_tampered_original_backup(tmp_path):
    path = _seed(tmp_path)
    manifest = _manifest(tmp_path)
    repair_split_basis.run(["--manifest", str(manifest), "--approve"], data_lake_root=tmp_path)
    item = json.loads(manifest.read_text())["symbols"][0]
    backup = Path(item["backup_path"])
    backup.write_bytes(b"tampered")
    current = path.read_bytes()
    with pytest.raises(ValueError, match="original backup checksum"):
        repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path)
    assert path.read_bytes() == current
    assert backup.read_bytes() == b"tampered"


def test_manifest_for_different_data_lake_is_rejected(tmp_path):
    bronze_path = _seed(tmp_path)
    manifest = _manifest(tmp_path)
    _approve(manifest)
    payload = json.loads(manifest.read_text())
    payload["data_lake_root"] = str(tmp_path / "different-lake")
    manifest.write_text(json.dumps(payload, sort_keys=True))
    before = bronze_path.read_bytes()

    with pytest.raises(ValueError, match="data-lake root"):
        repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path)
    assert bronze_path.read_bytes() == before


def test_apply_and_rollback_restore_exact_bytes(tmp_path):
    bronze_path = _seed(tmp_path)
    original = bronze_path.read_bytes()
    manifest = _manifest(tmp_path)
    _approve(manifest)

    assert repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path) == 0

    row = BronzeClient(bronze_path.parents[1], "equity").read_symbol_rows("AAPL")[0]
    assert row["close"] == 100.0
    assert row["source"] == "legacy"
    assert row["price_basis"] == "raw"
    assert repair_split_basis.run(["--manifest", str(manifest), "--rollback"], data_lake_root=tmp_path) == 0
    assert bronze_path.read_bytes() == original


def test_rollback_validates_backup_before_overwriting_current(tmp_path):
    import hashlib
    from pathlib import Path

    bronze_path = _seed(tmp_path)
    manifest = _manifest(tmp_path)
    repair_split_basis.run(["--manifest", str(manifest), "--approve"], data_lake_root=tmp_path)
    current = bronze_path.read_bytes()
    payload = json.loads(manifest.read_text())
    item = payload["symbols"][0]
    Path(item["backup_path"]).write_bytes(b"invalid parquet")
    item["source_sha256"] = hashlib.sha256(b"invalid parquet").hexdigest()
    manifest.write_text(json.dumps(payload))
    with pytest.raises(Exception, match="Parquet"):
        repair_split_basis.run(["--manifest", str(manifest), "--rollback"], data_lake_root=tmp_path)
    assert bronze_path.read_bytes() == current


def test_manifest_can_be_reapplied_after_rollback(tmp_path):
    bronze_path = _seed(tmp_path)
    manifest = _manifest(tmp_path)
    _approve(manifest)

    assert repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path) == 0
    assert repair_split_basis.run(["--manifest", str(manifest), "--rollback"], data_lake_root=tmp_path) == 0
    assert repair_split_basis.run(["--manifest", str(manifest)], data_lake_root=tmp_path) == 0

    rows = BronzeClient(bronze_path.parents[1], "equity").read_symbol_rows("AAPL")
    assert {row["price_basis"] for row in rows} == {"raw"}
