from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from livewire_scripts.flatfile_planner import FlatfilePlan, capacity_path, discover_plan, require_capacity


def test_discover_plan_uses_listing(tmp_path):
    client = MagicMock()
    client.list_objects.return_value = [
        {"Key": "us_stocks_sip/minute_aggs_v1/2026/06/2026-06-05.csv.gz", "Size": 10},
        {"Key": "us_stocks_sip/minute_aggs_v1/2026/06/2026-06-04.csv.gz", "Size": 20},
    ]
    with patch("livewire_scripts.flatfile_planner.shutil.disk_usage", return_value=MagicMock(free=1000)):
        plan = discover_plan(client, tmp_path)
    assert str(plan.earliest) == "2026-06-04"
    assert str(plan.latest) == "2026-06-05"
    assert plan.compressed_bytes == 30
    assert plan.projected_bytes == 240


def test_capacity_is_measured_on_the_volume_that_receives_the_raw_files(tmp_path):
    # data-lake is a symlink to the external volume in production, so measuring the
    # warehouse root reports a filesystem the raw files never land on.
    lake = tmp_path / "lake"
    (lake / "raw").mkdir(parents=True)
    (tmp_path / "warehouse").mkdir()
    (tmp_path / "warehouse" / "data-lake").symlink_to(lake)
    client = MagicMock()
    client.list_objects.return_value = [{"Key": "us_stocks_sip/minute_aggs_v1/2026/06/2026-06-05.csv.gz", "Size": 10}]

    with patch("livewire_scripts.flatfile_planner.shutil.disk_usage", return_value=MagicMock(free=1000)) as usage:
        discover_plan(client, tmp_path / "warehouse")

    measured = Path(usage.call_args.args[0]).resolve()
    assert measured == (lake / "raw").resolve()


def test_capacity_path_falls_back_to_an_existing_ancestor(tmp_path):
    # A warehouse with no data-lake yet must still resolve to something disk_usage accepts.
    assert capacity_path(tmp_path).exists()


def test_capacity_gate_rejects_plan_that_would_cross_minimum_free_space():
    plan = FlatfilePlan((), compressed_bytes=1, free_bytes=100, projected_bytes=80, minimum_free_bytes=25)
    with pytest.raises(RuntimeError, match="Insufficient disk"):
        require_capacity(plan)


def test_discover_plan_rejects_empty_listing(tmp_path):
    client = MagicMock()
    client.list_objects.return_value = [{"Key": "README.txt", "Size": 1}]
    with pytest.raises(RuntimeError, match="no objects"):
        discover_plan(client, tmp_path)
