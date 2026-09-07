from datetime import UTC, date, datetime

import pytest

from clients.timeutils import coerce_date, require_aware, utc_iso


def test_utc_iso_has_no_microseconds_and_a_z_suffix():
    stamp = utc_iso()

    assert stamp.endswith("Z")
    assert datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").year >= 2026


def test_coerce_date_passes_dates_through_and_parses_strings():
    assert coerce_date(date(2026, 9, 5)) == date(2026, 9, 5)
    assert coerce_date("2026-09-05") == date(2026, 9, 5)


def test_require_aware_names_the_field_it_rejected():
    require_aware(datetime(2026, 9, 5, tzinfo=UTC), "as_of")

    with pytest.raises(ValueError, match="as_of must be timezone-aware"):
        require_aware(datetime(2026, 9, 5), "as_of")


def test_no_module_hand_rolls_these_three():
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1]
    offenders = [
        f"{path.name}:{name}"
        for package in ("clients", "livewire_scripts")
        for path in sorted((root / package).glob("*.py"))
        for name in ("def _utc_iso(", "def _require_aware(")
        if name in path.read_text(encoding="utf-8")
    ]

    assert offenders == []
