"""Tests for clients.ib_gateway_preflight."""

from __future__ import annotations

import socket
from pathlib import Path
from unittest.mock import patch

import pytest

from clients import ib_gateway_preflight as pf


class TestTCPReachable:
    def test_returns_true_when_socket_connects(self):
        with patch.object(pf.socket, "create_connection") as mock_conn:
            mock_conn.return_value.__enter__ = lambda self: self
            mock_conn.return_value.__exit__ = lambda *a: None
            assert pf._tcp_reachable("127.0.0.1", 4001, 1.0) is True
            mock_conn.assert_called_once_with(("127.0.0.1", 4001), timeout=1.0)

    def test_returns_false_when_socket_raises(self):
        with patch.object(pf.socket, "create_connection", side_effect=socket.timeout()):
            assert pf._tcp_reachable("127.0.0.1", 4001, 0.1) is False


class TestAssertGatewayUp:
    def test_noop_when_skip_env_set(self, monkeypatch):
        monkeypatch.setenv("LIVEWIRE_SKIP_IB_PREFLIGHT", "1")
        with patch.object(pf, "_tcp_reachable") as mock_check:
            pf.assert_gateway_up()
            mock_check.assert_not_called()

    def test_returns_silently_when_gateway_reachable(self, monkeypatch):
        monkeypatch.delenv("LIVEWIRE_SKIP_IB_PREFLIGHT", raising=False)
        monkeypatch.delenv("MDW_IB_HOST", raising=False)
        monkeypatch.delenv("MDW_IB_PORT", raising=False)
        with patch.object(pf, "_tcp_reachable", return_value=True) as mock_check:
            pf.assert_gateway_up()
            mock_check.assert_called_once_with("127.0.0.1", 4001, 1.0)

    def test_uses_env_overrides(self, monkeypatch):
        monkeypatch.delenv("LIVEWIRE_SKIP_IB_PREFLIGHT", raising=False)
        monkeypatch.setenv("MDW_IB_HOST", "ib-gateway")
        monkeypatch.setenv("MDW_IB_PORT", "7497")
        with patch.object(pf, "_tcp_reachable", return_value=True) as mock_check:
            pf.assert_gateway_up()
            mock_check.assert_called_once_with("ib-gateway", 7497, 1.0)

    def test_explicit_args_win_over_env(self, monkeypatch):
        monkeypatch.delenv("LIVEWIRE_SKIP_IB_PREFLIGHT", raising=False)
        monkeypatch.setenv("MDW_IB_HOST", "from-env")
        monkeypatch.setenv("MDW_IB_PORT", "9999")
        with patch.object(pf, "_tcp_reachable", return_value=True) as mock_check:
            pf.assert_gateway_up(host="explicit", port=12345, timeout=2.5)
            mock_check.assert_called_once_with("explicit", 12345, 2.5)

    def test_exits_with_diagnostics_when_status_script_present(
        self, monkeypatch, capsys, tmp_path
    ):
        monkeypatch.delenv("LIVEWIRE_SKIP_IB_PREFLIGHT", raising=False)
        script = tmp_path / "ibc_gateway_status.sh"
        script.write_text("#!/bin/bash\necho diag\n")
        with (
            patch.object(pf, "_tcp_reachable", return_value=False),
            patch.object(pf, "STATUS_SCRIPT", script),
            patch.object(pf, "RUNBOOK", Path("/fake/runbook.md")),
            patch.object(pf.subprocess, "run") as mock_run,
        ):
            with pytest.raises(SystemExit) as exc_info:
                pf.assert_gateway_up()
        assert exc_info.value.code == 2
        mock_run.assert_called_once_with(["bash", str(script)], check=False)
        err = capsys.readouterr().err
        assert "IB Gateway not reachable" in err
        assert str(script) in err
        assert "/fake/runbook.md" in err

    def test_exits_without_subprocess_when_status_script_missing(
        self, monkeypatch, capsys, tmp_path
    ):
        monkeypatch.delenv("LIVEWIRE_SKIP_IB_PREFLIGHT", raising=False)
        missing = tmp_path / "does_not_exist.sh"
        with (
            patch.object(pf, "_tcp_reachable", return_value=False),
            patch.object(pf, "STATUS_SCRIPT", missing),
            patch.object(pf.subprocess, "run") as mock_run,
        ):
            with pytest.raises(SystemExit) as exc_info:
                pf.assert_gateway_up()
        assert exc_info.value.code == 2
        mock_run.assert_not_called()
        err = capsys.readouterr().err
        assert "IB Gateway not reachable" in err
        assert "Runbook" in err
