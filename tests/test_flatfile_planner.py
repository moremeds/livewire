from unittest.mock import MagicMock, patch

import pytest

from livewire_scripts.flatfile_planner import FlatfilePlan, discover_plan, require_capacity


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


def test_capacity_gate_rejects_plan_that_would_cross_minimum_free_space():
    plan = FlatfilePlan((), compressed_bytes=1, free_bytes=100, projected_bytes=80, minimum_free_bytes=25)
    with pytest.raises(RuntimeError, match="Insufficient disk"):
        require_capacity(plan)


def test_discover_plan_rejects_empty_listing(tmp_path):
    client = MagicMock()
    client.list_objects.return_value = [{"Key": "README.txt", "Size": 1}]
    with pytest.raises(RuntimeError, match="no objects"):
        discover_plan(client, tmp_path)
