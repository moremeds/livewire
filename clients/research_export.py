"""Local immutable GLD research capture; never writes to the source lake."""

from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from clients.parquet_io import fsync_directory
from clients.silver_snapshot import SilverSnapshot


def export_gld_research(silver_root: Path, bronze_path: Path, destination: Path) -> Path:
    """Freeze committed Silver and separately observed Bronze, without asserting lineage.

    Identical input bytes reuse a verified capture and its original receipt time.
    Provider availability/vintage are unknown: this is current-revision research.
    """
    snapshot = SilverSnapshot.pin(silver_root)
    manifest = json.loads(snapshot.manifest)
    files = {"silver-manifest.json": snapshot.manifest, "bronze-GLD.parquet": Path(bronze_path).read_bytes()}
    references = {"bronze-GLD.parquet": str(bronze_path)}
    for kind in ("1d", "factors"):
        selected = [a for a in snapshot.artifacts if a.symbol == "GLD" and a.kind == kind]
        if len(selected) != 1:
            raise ValueError(f"missing GLD {kind} artifact")
        artifact = selected[0]
        payload = artifact.path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != artifact.sha256:
            raise ValueError("Silver artifact checksum mismatch")
        name = f"silver-GLD-{kind}.parquet"
        files[name] = payload
        references[name] = str(artifact.path.relative_to(snapshot.root))
    tables = {
        name: pq.ParquetFile(io.BytesIO(data)).read() for name, data in files.items() if name.endswith(".parquet")
    }
    if any(table.num_rows == 0 for table in tables.values()):
        raise ValueError("empty research input")
    segments = []
    for row in sorted(tables["bronze-GLD.parquet"].to_pylist(), key=lambda row: row["trade_date"]):
        identity = {"provider": row.get("source"), "input_price_basis": row.get("price_basis")}
        day = row["trade_date"].isoformat()
        if not segments or any(segments[-1][key] != value for key, value in identity.items()):
            segments.append({**identity, "start": day, "end": day, "rows": 0})
        segments[-1]["end"] = day
        segments[-1]["rows"] += 1
    hashes = {name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())}
    identity = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
    metadata = {
        "schema_version": 1,
        "instrument": "GLD",
        "capture_id": identity,
        "received_at": datetime.now(UTC).isoformat(),
        "received_at_meaning": "local_export_capture_not_provider_receipt",
        "lake_revision": snapshot.revision,
        "generation_id": manifest.get("generation_id"),
        "lake_published_at": manifest.get("published_at"),
        "upstream_published_at": None,
        "available_at": None,
        "vintage": None,
        "upstream_time_precision": "unknown",
        "historical_pit": False,
        "research_mode": "current_revision_reconstruction",
        "silver_upstream_provider": None,
        "silver_input_price_basis": None,
        "silver_transform": "split_and_dividend_back_adjustment",
        "lineage_status": "bronze_observed_separately_not_proven_silver_input",
        "bronze_source_segments": segments,
        "source_references": references,
        "sha256": hashes,
    }
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / f"{identity}.zip"

    def verify_existing() -> None:
        with zipfile.ZipFile(target) as archive:
            saved = json.loads(archive.read("capture.json"))
            if saved["capture_id"] != identity or saved["sha256"] != hashes:
                raise ValueError("existing capture metadata mismatch")
            for name, checksum in hashes.items():
                if hashlib.sha256(archive.read(name)).hexdigest() != checksum:
                    raise ValueError("existing capture checksum mismatch")

    if target.exists():
        verify_existing()
        return target
    with tempfile.NamedTemporaryFile(dir=destination, suffix=".tmp") as staged:
        with zipfile.ZipFile(staged, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, payload in files.items():
                archive.writestr(name, payload)
            archive.writestr("capture.json", json.dumps(metadata, sort_keys=True, allow_nan=False))
        staged.flush()
        os.fsync(staged.fileno())
        try:
            os.link(staged.name, target)  # Exclusive publication; never replaces an existing capture.
        except FileExistsError:
            verify_existing()
        fsync_directory(destination)
    return target
