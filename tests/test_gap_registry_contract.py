"""The shipped registry must not claim coverage that does not exist.

`load_registry` rejects a row with an empty `test`, but it cannot check that the
test is real without resolving a repo-relative path against an unknown cwd. That
check belongs here, where the repo root is known.
"""

from pathlib import Path

from clients.gap_registry import VALID_CHECKS, load_registry

REPO = Path(__file__).resolve().parents[1]


def _rows():
    return load_registry(REPO / "registry" / "gaps.json")


def test_every_row_names_a_test_file_that_exists():
    missing = [(row.id, row.test) for row in _rows() if not (REPO / row.test.split("::", 1)[0]).is_file()]
    assert not missing, f"registry rows naming a nonexistent test file: {missing}"


def test_every_row_names_a_test_function_that_exists():
    missing = []
    for row in _rows():
        file_part, _, func = row.test.partition("::")
        if func and func not in (REPO / file_part).read_text():
            missing.append((row.id, row.test))
    assert not missing, f"registry rows naming a nonexistent test function: {missing}"


def test_every_row_names_a_dispatchable_check():
    unknown = [(row.id, row.check) for row in _rows() if row.check not in VALID_CHECKS]
    assert not unknown, f"registry rows naming an undispatched check: {unknown}"


def test_every_row_resolves_to_a_nonempty_universe():
    """A row whose presets resolve to no symbols is a silent zero denominator."""
    from clients.ingestion_common import load_preset

    empty = []
    for row in _rows():
        symbols = set()
        for name in row.universe:
            symbols |= set(load_preset(REPO / "presets" / f"{name}.json")[1])
        if not symbols:
            empty.append((row.id, list(row.universe)))
    assert not empty, f"registry rows resolving to no symbols: {empty}"


def test_no_row_declares_a_gap_the_engine_no_longer_emits():
    # classify() emits G1, G3 and G14 and nothing else. A row naming G2 promises
    # a check that no longer performs.
    for row in _rows():
        assert set(row.gap) <= {"G1", "G3", "G14"}, row.id


def test_g14_is_declared_only_where_a_tape_exists_to_compute_it():
    # terminus_of reads the SIP raw traded sets. There is no such tape for rates,
    # fx, volatility, futures or cmdty, so a G14 there would be a claim with no
    # check behind it -- the registry-side version of the disk-glob failure.
    for row in _rows():
        assert ("G14" in row.gap) == (row.asset_class == "equity"), row.id
