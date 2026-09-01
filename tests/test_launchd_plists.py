"""Every shipped plist must parse with a conforming XML parser.

`plutil -lint` accepts `--` inside an XML comment; the XML spec forbids it and
plistlib/expat rejects it. launchd's loader is the strict one that matters, so a
plist that only passes plutil would simply never load. This has been shipped
twice, so it is a test rather than a habit.
"""

import plistlib
from pathlib import Path

import pytest

PLISTS = sorted((Path(__file__).resolve().parents[1] / "launchd").glob("*.plist.example"))


def test_there_are_plists_to_check():
    assert PLISTS, "no launchd templates found — the glob or the directory moved"


@pytest.mark.parametrize("path", PLISTS, ids=lambda p: p.name)
def test_plist_parses_and_declares_a_label_and_program(path):
    parsed = plistlib.loads(path.read_bytes())
    assert parsed["Label"] == path.name.removesuffix(".plist.example")
    assert parsed["ProgramArguments"], "a job with no ProgramArguments runs nothing"
