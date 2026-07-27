"""The launchd templates encode where production gets its code from.

A template that names a path no machine has is a job that silently never runs,
so these assert the two rules the templates exist to enforce: the three job
plists run the immutable release, and no template hardcodes a home directory.
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
)
ALL_TEMPLATES = (*JOB_TEMPLATES, "com.livewire.release-promote")


def command(label: str) -> str:
    payload = plistlib.loads((LAUNCHD_DIR / f"{label}.plist.example").read_bytes())
    program = payload["ProgramArguments"]

    assert program[:2] == ["/bin/bash", "-c"]
    return program[2]


@pytest.mark.parametrize("label", ALL_TEMPLATES)
def test_no_template_hardcodes_a_home_directory(label):
    """`/Users/<someone>/…` in a template is a path the running machine need not have."""
    assert "/Users/" not in command(label)


@pytest.mark.parametrize("label", ALL_TEMPLATES)
def test_every_template_uses_its_own_relative_venv(label):
    assert ".venv/bin/python" in command(label)


@pytest.mark.parametrize("label", JOB_TEMPLATES)
def test_the_scheduled_jobs_run_the_release_not_a_checkout(label):
    text = command(label)

    assert text.startswith("cd /path/to/warehouse/current &&")
    assert "/path/to/repo" not in text


def test_the_promoter_is_the_one_job_that_reads_the_repo():
    # It builds the artifact, so it cannot run from one.
    text = command("com.livewire.release-promote")

    assert text.startswith("cd /path/to/repo &&")
    assert "release promote" in text
