"""The launchd templates encode where production gets its code from.

A template that names a path no machine has is a job that silently never runs,
so these assert the two rules the templates exist to enforce: the warehouse job
plists run the immutable release, and no template hardcodes a home directory.

Two templates are deliberate exceptions to the release rule and both say so in
their own header comment: release-promote builds the artifact so it cannot run
from one, and universe-refresh writes `PROJECT_ROOT/presets/*.json` on every run,
which `release.freeze()`'s `chmod -R a-w` forbids. They are pinned by name, so
adding a third job that reads the repo has to be a deliberate edit here.
"""

from __future__ import annotations

import plistlib
from pathlib import Path

import pytest

LAUNCHD_DIR = Path(__file__).resolve().parent.parent / "launchd"
JOB_TEMPLATES = (
    "com.livewire.daily-update",
    "com.livewire.daily-update-watchdog",
    "com.livewire.intraday-catchup",
    "com.livewire.coverage",
)
REPO_TEMPLATES = ("com.livewire.release-promote", "com.livewire.universe-refresh")
ALL_TEMPLATES = (*JOB_TEMPLATES, *REPO_TEMPLATES)


def command(label: str) -> str:
    payload = plistlib.loads((LAUNCHD_DIR / f"{label}.plist.example").read_bytes())
    program = payload["ProgramArguments"]

    assert program[:2] == ["/bin/bash", "-c"]
    return program[2]


@pytest.mark.parametrize("label", ALL_TEMPLATES)
def test_no_template_hardcodes_a_home_directory(label):
    """`/Users/<someone>/…` in a template is a path the running machine need not have."""
    assert "/Users/" not in command(label)


@pytest.mark.parametrize("label", (*JOB_TEMPLATES, "com.livewire.release-promote"))
def test_every_template_uses_its_own_relative_venv(label):
    assert ".venv/bin/python" in command(label)
    assert "/path/to/warehouse/.venv/bin/python" not in command(label)


def test_universe_refresh_borrows_the_warehouse_venv_by_path():
    """The one template that cannot use a relative venv: it runs from the repo,
    which has no venv of its own, so it names the warehouse's."""
    assert "/path/to/warehouse/.venv/bin/python" in command("com.livewire.universe-refresh")


@pytest.mark.parametrize("label", JOB_TEMPLATES)
def test_the_scheduled_jobs_run_the_release_not_a_checkout(label):
    text = command(label)

    assert text.startswith("cd /path/to/warehouse/current &&")
    assert "/path/to/repo" not in text


@pytest.mark.parametrize("label", REPO_TEMPLATES)
def test_the_repo_reading_jobs_are_exactly_the_promoter_and_universe_refresh(label):
    # The promoter builds the artifact; universe-refresh writes presets/ back
    # into the tree, which the frozen release forbids. Nothing else may.
    text = command(label)

    assert text.startswith("cd /path/to/repo &&")
    assert "/path/to/warehouse/current" not in text


def test_no_other_template_reads_the_repo():
    for path in sorted(LAUNCHD_DIR.glob("*.plist.example")):
        label = path.name.removesuffix(".plist.example")
        assert label in ALL_TEMPLATES, f"{label} is untested — add it to JOB_TEMPLATES or REPO_TEMPLATES"
        if label not in REPO_TEMPLATES:
            assert "/path/to/repo" not in command(label)
