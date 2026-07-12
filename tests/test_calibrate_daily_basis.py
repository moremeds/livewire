from __future__ import annotations

import json

from livewire_scripts import calibrate_daily_basis
from tests.test_audit_split_basis import _seed


def test_calibration_reports_split_hypotheses_without_mutation(tmp_path):
    bronze_path = _seed(tmp_path)
    before = bronze_path.read_bytes()
    output = tmp_path / "calibration.json"

    assert calibrate_daily_basis.run(
        [
            "--tickers",
            "AAPL",
            "--output",
            str(output),
            "--data-lake-root",
            str(tmp_path),
        ]
    ) == 0

    assert bronze_path.read_bytes() == before
    payload = json.loads(output.read_text())
    assert payload["passed"] is True
    event = payload["symbols"][0]["events"][0]
    assert event["treatment"] == "adjusted"
    assert event["observed_ratio"] == 26.0 / 25.0
    assert event["adjusted_error"] < event["raw_error"]
    assert event["confidence"] > 0
