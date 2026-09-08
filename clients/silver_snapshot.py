"""Resolve Silver files from one committed manifest, never from directory globs."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from clients.symbol_paths import canonical_symbol, decode_symbol


@dataclass(frozen=True)
class SnapshotArtifact:
    symbol: str
    kind: str
    path: Path
    sha256: str


@dataclass(frozen=True)
class SilverSnapshot:
    root: Path
    revision: int
    manifest: bytes
    artifacts: tuple[SnapshotArtifact, ...]

    @classmethod
    def pin(cls, root: Path) -> SilverSnapshot:
        root = Path(root).resolve()
        payload = (root / "revisions/current.json").read_bytes()
        data = json.loads(payload)
        revision = data.get("revision")
        if data.get("schema_version") != 1 or type(revision) is not int or revision <= 0:
            raise ValueError("invalid Silver revision")
        if (root / "revisions" / f"revision={revision}.json").read_bytes() != payload:
            raise ValueError("Silver pointer differs from immutable manifest")
        symbols = [canonical_symbol(item["symbol"]) for item in data["affected"]]
        if len(symbols) != len(set(symbols)):
            raise ValueError("duplicate Silver affected symbol")
        artifacts: list[SnapshotArtifact] = []
        seen: set[tuple[str, str]] = set()
        for entry in data["artifacts"]:
            relative = Path(entry["path"])
            path = (root / relative).resolve()
            if relative.is_absolute() or ".." in relative.parts or not path.is_relative_to(root):
                raise ValueError("Silver artifact escapes root")
            parts = relative.parts
            if len(parts) < 3 or parts[-3] != "asset_class=equity" or not parts[-2].startswith("symbol="):
                raise ValueError("invalid Silver artifact path")
            if parts[-1] == "1d.parquet":
                kind = "1d"
            elif parts[-1] == "factors.parquet" and len(parts) >= 4 and parts[-4] == "adjustments":
                kind = "factors"
            else:
                raise ValueError("unknown Silver artifact kind")
            symbol = canonical_symbol(decode_symbol(parts[-2][7:]))
            key = (symbol, kind)
            checksum = entry["sha256"]
            if key in seen or not isinstance(checksum, str) or re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
                raise ValueError("duplicate Silver artifact or invalid checksum")
            seen.add(key)
            artifacts.append(SnapshotArtifact(symbol, kind, path, checksum))
        if seen != {(symbol, kind) for symbol in symbols for kind in ("1d", "factors")}:
            raise ValueError("Silver manifest requires one daily/factor pair per affected symbol")
        return cls(root, revision, payload, tuple(artifacts))

    def files(self, kind: str, symbols: set[str] | None = None) -> list[str]:
        """Verify only selected artifacts; missing/corrupt references fail closed."""
        selected = None if symbols is None else {canonical_symbol(symbol) for symbol in symbols}
        paths: list[str] = []
        for artifact in self.artifacts:
            if artifact.kind != kind or (selected is not None and artifact.symbol not in selected):
                continue
            with artifact.path.open("rb") as handle:
                checksum = hashlib.file_digest(handle, "sha256").hexdigest()
            if checksum != artifact.sha256:
                raise ValueError(f"Silver revision {self.revision}: checksum mismatch for {artifact.path}")
            paths.append(str(artifact.path))
        return paths
