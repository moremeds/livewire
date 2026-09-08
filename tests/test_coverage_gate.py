from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_coverage_below_95_exits_nonzero_before_rounding_to_whole_percent(tmp_path: Path) -> None:
    module = tmp_path / "near_threshold.py"
    module.write_text("\n".join(f"def f_{i}():\n    return {i}" for i in range(352)) + "\n")
    test = tmp_path / "test_near_threshold.py"
    test.write_text(
        "import near_threshold\n\n"
        "def test_calls():\n"
        "    for i in range(316):\n"
        "        assert getattr(near_threshold, f'f_{i}')() == i\n"
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(test),
            "-q",
            "--cov=near_threshold",
            "--cov-fail-under=95",
            "--cov-report=term",
            f"--cov-config={Path(__file__).parents[1] / 'pyproject.toml'}",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )

    assert "94.89%" in result.stdout
    assert result.returncode != 0
