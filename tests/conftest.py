"""Shared fixtures for the test suite."""

from __future__ import annotations

import pytest

from clients.bronze_client import BronzeClient


@pytest.fixture(autouse=True)
def _clear_alert_rate_limit():
    try:
        from clients import quality_flags

        quality_flags._RATE_LIMIT_CACHE.clear()
    except (ImportError, AttributeError):
        pass
    yield


@pytest.fixture(autouse=True)
def _isolate_reliability_artifact_paths(tmp_path, monkeypatch):
    """Keep reliability tests from appending to operator-facing live logs."""
    monkeypatch.setenv("MDW_QUALITY_AUDIT_PATH", str(tmp_path / "quality_audit.jsonl"))
    monkeypatch.setenv("MDW_TELEMETRY_PATH", str(tmp_path / "telemetry.jsonl"))
    monkeypatch.setenv("MDW_UNDELIVERED_DIR", str(tmp_path / "quality_alerts_undelivered"))
    monkeypatch.setenv("MDW_LOG_DIR", str(tmp_path / "logs"))


@pytest.fixture()
def tmp_bronze(tmp_path):
    """Create a temporary bronze parquet root."""
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    return bronze_dir


@pytest.fixture()
def bronze(tmp_bronze):
    """Provide a BronzeClient connected to a fresh temp bronze root."""
    client = BronzeClient(bronze_dir=tmp_bronze)
    yield client
    client.close()
