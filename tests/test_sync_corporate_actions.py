from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from livewire_scripts import sync_corporate_actions


class _Client:
    def __init__(self, *, fail: str | None = None):
        self.fail = fail
        self.calls: list[tuple[str, str]] = []

    def get_splits(self, ticker):
        self.calls.append(("splits", ticker))
        if ticker == self.fail:
            raise RuntimeError("provider failed")
        return [SimpleNamespace(provider_event_id=f"{ticker}-split")]

    def get_dividends(self, ticker):
        self.calls.append(("dividends", ticker))
        return [SimpleNamespace(provider_event_id=f"{ticker}-div")]


class _Store:
    def __init__(self):
        self.calls = []

    def reconcile(self, symbol, events, fetched_at, **kwargs):
        self.calls.append((symbol, events, fetched_at, kwargs))
        return SimpleNamespace(inserted=1, revised=2, cancelled=3, unchanged=4)


def test_explicit_tickers_reconcile_sequentially_with_dry_run(capsys):
    client = _Client()
    store = _Store()

    assert sync_corporate_actions.run(["--tickers", "nvda", "AAPL", "--dry-run"], client=client, store=store) == 0

    assert client.calls == [
        ("splits", "NVDA"),
        ("dividends", "NVDA"),
        ("splits", "AAPL"),
        ("dividends", "AAPL"),
    ]
    assert [call[3] for call in store.calls] == [
        {"full_reconcile": False, "dry_run": True},
        {"full_reconcile": False, "dry_run": True},
    ]
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary == {"cancelled": 6, "failed": 0, "inserted": 2, "revised": 4, "unchanged": 8}


def test_full_reconcile_is_explicitly_forwarded():
    store = _Store()
    sync_corporate_actions.run(["--tickers", "NVDA", "--full-reconcile"], client=_Client(), store=store)
    assert store.calls[0][3] == {"full_reconcile": True, "dry_run": False}


def test_preset_resolves_tickers(tmp_path):
    preset = tmp_path / "preset.json"
    preset.write_text(json.dumps({"name": "test", "tickers": ["spy", "QQQ"]}))
    client = _Client()

    sync_corporate_actions.run(["--preset", str(preset)], client=client, store=_Store())

    assert client.calls[0] == ("splits", "SPY")
    assert client.calls[2] == ("splits", "QQQ")


def test_no_scope_discovers_equity_bronze_symbols(tmp_path):
    for symbol in ("AAPL", "BRK%2EB"):
        (tmp_path / "bronze/asset_class=equity" / f"symbol={symbol}").mkdir(parents=True)
    client = _Client()

    sync_corporate_actions.run([], client=client, store=_Store(), data_lake_root=tmp_path)

    assert [call for call in client.calls if call[0] == "splits"] == [
        ("splits", "AAPL"),
        ("splits", "BRK.B"),
    ]


def test_provider_failure_counts_symbol_and_continues(capsys):
    client = _Client(fail="AAPL")
    store = _Store()

    assert sync_corporate_actions.run(["--tickers", "AAPL", "MSFT"], client=client, store=store) == 1

    assert [call[0] for call in store.calls] == ["MSFT"]
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["failed"] == 1


def test_empty_discovered_scope_is_rejected(tmp_path):
    with pytest.raises(SystemExit, match="no tickers"):
        sync_corporate_actions.run([], client=_Client(), store=_Store(), data_lake_root=tmp_path)
