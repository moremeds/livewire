"""Tests for IBClient.connect() — clientId 326 fallback behavior."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from clients.ib_client import IBClient, IBConnectionError


# ── helpers ───────────────────────────────────────────────────────────


def _make_client():
    """Return an IBClient wired to a MagicMock IB instance."""
    with patch("clients.ib_client.IB") as MockIB:
        mock_ib = MagicMock()
        MockIB.return_value = mock_ib
        client = IBClient()
    return client, mock_ib


# ══════════════════════════════════════════════════════════════════════
# connect — happy path
# ══════════════════════════════════════════════════════════════════════


class TestConnectSuccess:
    def test_connect_succeeds_on_first_try(self):
        client, mock_ib = _make_client()

        client.connect()

        mock_ib.connect.assert_called_once_with(
            "127.0.0.1", 4001, clientId=0, timeout=10
        )

    def test_connect_stores_last_client_id_on_success(self):
        client, mock_ib = _make_client()

        client.connect(client_id=5)

        assert client._last_client_id == 5


# ══════════════════════════════════════════════════════════════════════
# connect — clientId 326 fallback
# ══════════════════════════════════════════════════════════════════════


class TestConnectClientIdFallback:
    def test_retries_with_next_client_id_on_326(self):
        """Error 326 on clientId 0 → automatically retry with clientId 1."""
        client, mock_ib = _make_client()

        call_count = 0

        def connect_side_effect(host, port, clientId, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                client._last_error = (326, "client id already in use")
                raise TimeoutError()
            # Second call succeeds

        mock_ib.connect.side_effect = connect_side_effect

        client.connect()  # must not raise

        assert mock_ib.connect.call_count == 2
        calls = mock_ib.connect.call_args_list
        assert calls[0][1]["clientId"] == 0
        assert calls[1][1]["clientId"] == 1

    def test_updates_last_client_id_to_actual_connected_id(self):
        """_last_client_id reflects the clientId that actually connected."""
        client, mock_ib = _make_client()

        call_count = 0

        def connect_side_effect(host, port, clientId, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                client._last_error = (326, "client id already in use")
                raise TimeoutError()

        mock_ib.connect.side_effect = connect_side_effect

        client.connect()

        assert client._last_client_id == 1

    def test_raises_after_all_client_ids_exhausted(self):
        """If all 10 clientIds return 326, raise IBConnectionError."""
        client, mock_ib = _make_client()

        def connect_side_effect(host, port, clientId, timeout):
            client._last_error = (326, "client id already in use")
            raise TimeoutError()

        mock_ib.connect.side_effect = connect_side_effect

        with pytest.raises(IBConnectionError, match="all clientIds"):
            client.connect()

        assert mock_ib.connect.call_count == 10

    def test_logs_warning_on_326_retry(self, caplog):
        """A warning is emitted when falling back to the next clientId."""
        client, mock_ib = _make_client()

        call_count = 0

        def connect_side_effect(host, port, clientId, timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                client._last_error = (326, "client id already in use")
                raise TimeoutError()

        mock_ib.connect.side_effect = connect_side_effect

        with caplog.at_level(logging.WARNING, logger="ib_client"):
            client.connect()

        messages = " ".join(r.message for r in caplog.records)
        assert "326" in messages or "already in use" in messages.lower()

    def test_non_326_error_does_not_retry_client_ids(self):
        """A plain TimeoutError (no 326) respects max_retries, not clientId retries."""
        client, mock_ib = _make_client()

        mock_ib.connect.side_effect = TimeoutError("gateway unreachable")

        with pytest.raises(IBConnectionError):
            client.connect()

        # max_retries=1 default: tried once, no clientId escalation
        assert mock_ib.connect.call_count == 1

    def test_stale_326_error_does_not_trigger_retry(self):
        """A _last_error=326 left over from a previous session must not cause retry."""
        client, mock_ib = _make_client()

        # Simulate stale state from a prior crashed session
        client._last_error = (326, "stale error from last run")

        # connect() itself succeeds immediately
        client.connect()

        # Should only have been called once — stale 326 was cleared before attempt
        mock_ib.connect.assert_called_once()

    def test_multiple_326s_before_success(self):
        """clientIds 0, 1, 2 all in use — succeeds on clientId 3."""
        client, mock_ib = _make_client()

        call_count = 0

        def connect_side_effect(host, port, clientId, timeout):
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                client._last_error = (326, "client id already in use")
                raise TimeoutError()

        mock_ib.connect.side_effect = connect_side_effect

        client.connect()

        assert mock_ib.connect.call_count == 4
        assert mock_ib.connect.call_args_list[3][1]["clientId"] == 3
        assert client._last_client_id == 3


def test_connect_attaches_telemetry(monkeypatch, tmp_path):
    monkeypatch.setenv("MDW_TELEMETRY_PATH", str(tmp_path / "t.jsonl"))
    fake_ib = MagicMock()
    fake_ib.connect.return_value = None
    fake_ib.errorEvent = MagicMock()
    fake_ib.connectedEvent = MagicMock()
    fake_ib.disconnectedEvent = MagicMock()
    with patch("clients.ib_client.IB", return_value=fake_ib):
        client = IBClient()
        client.connect(host="127.0.0.1", port=4001, client_id=99)
    fake_ib.errorEvent.connect.assert_called_once()
    fake_ib.connectedEvent.connect.assert_called_once()


def test_disconnect_detaches_telemetry(monkeypatch, tmp_path):
    monkeypatch.setenv("MDW_TELEMETRY_PATH", str(tmp_path / "t.jsonl"))
    fake_ib = MagicMock()
    fake_ib.errorEvent = MagicMock()
    fake_ib.connectedEvent = MagicMock()
    fake_ib.disconnectedEvent = MagicMock()
    with patch("clients.ib_client.IB", return_value=fake_ib):
        client = IBClient()
        client.connect(host="127.0.0.1", port=4001, client_id=99)
        client.disconnect()
    fake_ib.errorEvent.disconnect.assert_called_once()
