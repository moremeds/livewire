from __future__ import annotations

import json
from pathlib import Path

import pytest

from livewire_scripts import shepherd_repair


@pytest.mark.parametrize(
    ("command", "receipt_flag", "state", "expected_code"),
    [
        ("preflight", None, "OK", 0),
        ("stage", None, "STAGED", 0),
        ("transaction", None, "VERIFIED", 0),
        ("postcondition", None, "NOT_VERIFIED", 1),
        ("publish", "--staged-receipt", "PUBLISHED", 0),
        ("verify", "--publish-receipt", "VERIFIED", 0),
        ("rollback", "--publish-receipt", "ROLLED_BACK", 1),
    ],
)
def test_cli_dispatches_each_repair_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    command: str,
    receipt_flag: str | None,
    state: str,
    expected_code: int,
) -> None:
    manifest = tmp_path / "repair.json"
    receipt = tmp_path / "receipt.json"
    calls: list[tuple[str, tuple[Path, ...]]] = []

    class FakeRepair:
        def __init__(self, root: Path) -> None:
            assert root == tmp_path

        def __getattr__(self, name: str):
            def run(*paths: Path) -> dict[str, str]:
                calls.append((name, paths))
                return {"state": state}

            return run

    monkeypatch.setattr(shepherd_repair, "ShepherdRepair", FakeRepair)
    argv = ["--data-lake-root", str(tmp_path), command, "--manifest", str(manifest)]
    if receipt_flag is not None:
        argv.extend([receipt_flag, str(receipt)])

    assert shepherd_repair.main(argv) == expected_code
    expected_paths = (manifest,) if receipt_flag is None else (manifest, receipt)
    assert calls == [(command, expected_paths)]
    assert json.loads(capsys.readouterr().out) == {"state": state}


def test_cli_rejects_relative_manifest(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="manifest path must be absolute"):
        shepherd_repair.main(["--data-lake-root", str(tmp_path), "preflight", "--manifest", "repair.json"])
