"""PIT index membership — import grok events.jsonl panels into the store.

The grok layer kept ~30 years of add/remove events keyed by ticker with no
`security_id`, no evidence hash, and no `known_at` — and a README claiming a
weekday auto-update no scheduler runs. `import_events` maps each jsonl line to
a `MembershipEvent` (spec §2.2): resolved through `SecurityMaster`, committed
source files as `HashedRef` evidence, `known_at` = import time because PIT is
honest — an `as_of` before the import returns nothing, because we did not
know it then.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Literal

from clients import ledger
from clients.index_membership_store import IndexMembershipStore, MembershipEvent
from clients.security_master import SecurityMaster
from clients.source_evidence import SourceEvidence, SourceEvidenceStore, digest_bytes
from livewire_scripts.paths import data_lake_dir

Confidence = Literal["B", "C", "D"]

# The security master records equity identities under provider `massive`; US
# index constituents list on NASDAQ, NYSE or NYSE Arca — resolve across all
# three rather than assume one venue.
_RESOLVE_PROVIDER = "massive"
_RESOLVE_MICS = ("XNAS", "XNYS", "ARCX")

_CONFIDENCE_STATUS = {"B": "verified", "C": "candidate", "D": "candidate"}


def _evidence_verifier(store: SourceEvidenceStore):
    def verify(ref: str, digest: str) -> bool:
        try:
            return digest_bytes(store.read(ref)) == digest
        except (OSError, ValueError):
            return False

    return verify


def _resolve(master: SecurityMaster, ticker: str, effective_at: datetime, as_of: datetime) -> str | None:
    matches = {
        security_id
        for mic in _RESOLVE_MICS
        if (security_id := master.resolve_symbol(_RESOLVE_PROVIDER, ticker, mic, effective_at, as_of)) is not None
    }
    if len(matches) == 1:
        return next(iter(matches))
    return None  # no identity, or the same ticker verified on two venues


def _event_id(index_id: str, key: str, action: str, effective_date: str, source_hashes: tuple[str, ...]) -> str:
    payload = "\x00".join([index_id, key, action, effective_date, *source_hashes])
    return hashlib.sha256(payload.encode()).hexdigest()


def import_events(
    *,
    index_id: str,
    events_path: Path,
    sources: list[Path],
    data_lake_root: Path,
    now: datetime,
    confidence: Confidence,
) -> dict:
    """Import one grok `events.jsonl` panel into `IndexMembershipStore`.

    Each line (`effective_date`, `action`, `ticker`, `kind`, optional `year` /
    `confidence`) becomes one `MembershipEvent`. Unresolvable tickers are kept
    as `status="unresolved"` events under a deterministic `unresolved:<ticker>`
    security_id so a later identity backfill can revise them; `members_effective_at`
    never counts them. Re-running the same import appends nothing — `event_id`
    is a content hash, and the store dedups on it.
    """
    root = Path(data_lake_root)
    evidence = SourceEvidenceStore(root)
    verifier = _evidence_verifier(evidence)
    master = SecurityMaster(root, evidence_verifier=None)
    store = IndexMembershipStore(root, security_master=master, evidence_verifier=verifier)

    artifacts = [evidence.persist_raw(Path(source).read_bytes()) for source in sources]
    source_refs = tuple(artifact.ref for artifact in artifacts)
    source_hashes = tuple(artifact.sha256 for artifact in artifacts)
    for source, artifact in zip(sources, artifacts, strict=True):
        evidence.record(
            SourceEvidence(
                ref=artifact.ref,
                sha256=artifact.sha256,
                source_url=Path(source).resolve().as_uri(),
                retrieved_at=now,
                publication_time=None,
                mediawiki_revision_id=None,
                mediawiki_revision_time=None,
                content_type="application/jsonl" if Path(source).suffix == ".jsonl" else "application/octet-stream",
            )
        )

    run_id = os.environ.get("LW_RUN_ID") or ledger.new_run_id("membership-sync")
    run_row = {
        "run_id": run_id,
        "job": "membership-sync",
        "host": socket.gethostname(),
        "release_sha": os.environ.get("LW_RELEASE_SHA"),
        "presets_sha": None,
        "registry_sha": None,
        "started": now,
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    ledger.emit("runs", [run_row], run_id=run_id)
    try:
        resolved_status = "candidate" if index_id == "r2k-proxy" else _CONFIDENCE_STATUS[confidence]
        existing = store.events(index_id)
        seen_ids = {event.event_id for event in existing}
        revisions: dict[str, int] = {}
        for event in existing:
            revisions[event.security_id] = max(revisions.get(event.security_id, 0), event.revision)
        # The measurement is the store's current unresolved backlog, not this
        # run's appends — a re-import that adds nothing must still report it,
        # or a standing WARN in status would silently clear.
        unresolved_ids = {event.security_id for event in existing if event.status == "unresolved"}

        added = removed = skipped = 0
        for line in events_path.read_text().splitlines():
            if not (line := line.strip()):
                continue
            row = json.loads(line)
            ticker, action = row["ticker"], row["action"]
            effective_at = datetime.combine(date.fromisoformat(row["effective_date"]), time.min, tzinfo=UTC)
            resolved = _resolve(master, ticker, effective_at, now)
            event_id = _event_id(index_id, resolved or ticker, action, row["effective_date"], source_hashes)
            if event_id in seen_ids:
                skipped += 1
                continue
            security_id = resolved or f"unresolved:{ticker}"
            revision = revisions.get(security_id, 0) + 1
            appended = store.append(
                MembershipEvent(
                    event_id=event_id,
                    index_id=index_id,
                    security_id=security_id,
                    action=action,
                    announced_at=None,
                    effective_at=effective_at,
                    known_at=now,
                    source_refs=source_refs,
                    source_hashes=source_hashes,
                    revision=revision,
                    supersedes=None,
                    status=resolved_status if resolved else "unresolved",
                )
            )
            if appended:
                seen_ids.add(event_id)
                revisions[security_id] = revision
                if action == "add":
                    added += 1
                else:
                    removed += 1
                if resolved is None:
                    unresolved_ids.add(security_id)

        ledger.emit(
            "measurements",
            [
                {
                    "name": name,
                    "scope": index_id,
                    "measured_at": now,
                    "value": float(value),
                    "unit": "count",
                    "source": "measured",
                    "run_id": run_id,
                }
                for name, value in (
                    ("membership_events_added", added),
                    ("membership_events_removed", removed),
                    ("membership_unresolved", len(unresolved_ids)),
                )
            ],
            run_id=run_id,
        )
    except Exception:
        ledger.emit(
            "runs",
            [run_row | {"ended": datetime.now(UTC), "exit_code": 1, "verdict": "FAILED"}],
            run_id=run_id,
        )
        raise
    ledger.emit(
        "runs",
        [run_row | {"ended": datetime.now(UTC), "exit_code": 0, "verdict": "OK"}],
        run_id=run_id,
    )
    return {
        "index": index_id,
        "added": added,
        "removed": removed,
        "unresolved": len(unresolved_ids),
        "skipped": skipped,
        "run_id": run_id,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="livewire_ingest.py membership-sync",
        description="Import and maintain point-in-time index membership",
    )
    subcommands = parser.add_subparsers(dest="subcommand", required=True)
    importer = subcommands.add_parser("import", help="One-time import of a grok events.jsonl panel")
    importer.add_argument("--index", required=True, help="Index id (sp500, ndx100, djia, r2k-proxy)")
    importer.add_argument("--events", type=Path, required=True, help="Path to the grok events.jsonl")
    importer.add_argument(
        "--source",
        type=Path,
        action="append",
        required=True,
        help="Provenance file committed to the evidence CAS (repeatable)",
    )
    importer.add_argument("--confidence", choices=["B", "C", "D"], default="B")
    args = parser.parse_args(argv)

    if args.subcommand == "import":
        print(
            json.dumps(
                import_events(
                    index_id=args.index,
                    events_path=args.events,
                    sources=args.source,
                    data_lake_root=data_lake_dir(),
                    now=datetime.now(UTC),
                    confidence=args.confidence,
                ),
                sort_keys=True,
            )
        )
        return 0
    raise AssertionError("unreachable")  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
