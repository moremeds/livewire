from __future__ import annotations

import json
import threading
from types import SimpleNamespace

import pytest

from clients.massive_client import MassiveAuthError
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
        self.threads = []

    def reconcile(self, symbol, events, fetched_at, **kwargs):
        self.threads.append(threading.current_thread().name)
        self.calls.append((symbol, events, fetched_at, kwargs))
        return SimpleNamespace(inserted=1, revised=2, cancelled=3, unchanged=4)


class _FactoryClient:
    def __init__(self, factory):
        self.factory = factory
        self.closed = False

    def get_splits(self, ticker):
        self.factory.record(ticker)
        if self.factory.auth_fail:
            raise MassiveAuthError("bad credentials", status_code=401)
        if ticker in self.factory.fail:
            raise RuntimeError("provider failed")
        return [SimpleNamespace(provider_event_id=f"{ticker}-split")]

    def get_dividends(self, ticker):
        return [SimpleNamespace(provider_event_id=f"{ticker}-div")]

    def close(self):
        self.closed = True


class _ClientFactory:
    def __init__(self, *, fail=(), auth_fail=False):
        self.fail = set(fail)
        self.auth_fail = auth_fail
        self.clients = []
        self.fetched_symbols = set()
        self.fetch_counts = {}
        self._lock = threading.Lock()

    def __call__(self):
        client = _FactoryClient(self)
        with self._lock:
            self.clients.append(client)
        return client

    def record(self, ticker):
        with self._lock:
            self.fetched_symbols.add(ticker)
            self.fetch_counts[ticker] = self.fetch_counts.get(ticker, 0) + 1


def test_explicit_tickers_reconcile_sequentially_with_dry_run(tmp_path, capsys):
    client = _Client()
    store = _Store()

    assert (
        sync_corporate_actions.run(
            ["--tickers", "nvda", "AAPL", "--dry-run"],
            client=client,
            store=store,
            data_lake_root=tmp_path,
        )
        == 0
    )

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
    assert summary == {
        "attempted": 2,
        "cancelled": 6,
        "completed": 2,
        "cursor": summary["cursor"],
        "failed": 0,
        "inserted": 2,
        "pending": 0,
        "requested": 2,
        "resumed": 0,
        "revised": 4,
        "unchanged": 8,
    }
    assert summary["cursor"].startswith(str(tmp_path))


def test_full_reconcile_is_explicitly_forwarded(tmp_path):
    store = _Store()
    sync_corporate_actions.run(
        ["--tickers", "NVDA", "--full-reconcile"],
        client=_Client(),
        store=store,
        data_lake_root=tmp_path,
    )
    assert store.calls[0][3] == {"full_reconcile": True, "dry_run": False}


def test_preset_resolves_tickers(tmp_path):
    preset = tmp_path / "preset.json"
    preset.write_text(json.dumps({"name": "test", "tickers": ["spy", "QQQ"]}))
    client = _Client()

    sync_corporate_actions.run(
        ["--preset", str(preset)], client=client, store=_Store(), data_lake_root=tmp_path / "lake"
    )

    assert client.calls[0] == ("splits", "SPY")
    assert client.calls[2] == ("splits", "QQQ")


def test_no_scope_discovers_equity_bronze_symbols(tmp_path):
    for symbol in ("AAPL", "BRK%2EB", "BCPC", "BC%70C"):
        (tmp_path / "bronze/asset_class=equity" / f"symbol={symbol}").mkdir(parents=True)
    client = _Client()

    sync_corporate_actions.run([], client=client, store=_Store(), data_lake_root=tmp_path)

    assert [call for call in client.calls if call[0] == "splits"] == [
        ("splits", "AAPL"),
        ("splits", "BCPC"),
        ("splits", "BCpC"),
        ("splits", "BRK.B"),
    ]


def test_provider_failure_counts_symbol_and_continues(tmp_path, capsys):
    client = _Client(fail="AAPL")
    store = _Store()

    assert (
        sync_corporate_actions.run(["--tickers", "AAPL", "MSFT"], client=client, store=store, data_lake_root=tmp_path)
        == 1
    )

    assert [call[0] for call in store.calls] == ["MSFT"]
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["failed"] == 1


def test_empty_discovered_scope_is_rejected(tmp_path):
    with pytest.raises(SystemExit, match="no tickers"):
        sync_corporate_actions.run([], client=_Client(), store=_Store(), data_lake_root=tmp_path)


def test_worker_range_is_validated():
    with pytest.raises(SystemExit):
        sync_corporate_actions.parse_args(["--tickers", "AAPL", "--workers", "0"])
    with pytest.raises(SystemExit):
        sync_corporate_actions.parse_args(["--tickers", "AAPL", "--workers", "17"])


def test_injected_client_rejects_explicit_parallel_workers(tmp_path):
    with pytest.raises(ValueError, match="supplied client"):
        sync_corporate_actions.run(
            ["--tickers", "AAPL", "--workers", "2"],
            client=_Client(),
            store=_Store(),
            data_lake_root=tmp_path,
        )


def test_four_workers_use_distinct_clients_and_close_them(tmp_path):
    factory = _ClientFactory()
    store = _Store()

    result = sync_corporate_actions.run(
        ["--tickers", "A", "B", "C", "D", "--workers", "4"],
        client_factory=factory,
        store=store,
        data_lake_root=tmp_path,
    )

    assert result == 0
    assert len(factory.clients) == 4
    assert all(client.closed for client in factory.clients)
    assert store.threads == ["MainThread"] * 4


def test_failed_symbol_is_not_checkpointed_and_resume_retries_it(tmp_path):
    cursor = tmp_path / "cursor.json"
    first = _ClientFactory(fail={"MSFT"})

    assert (
        sync_corporate_actions.run(
            ["--tickers", "AAPL", "MSFT", "--workers", "2", "--cursor", str(cursor)],
            client_factory=first,
            store=_Store(),
            data_lake_root=tmp_path,
        )
        == 1
    )
    assert json.loads(cursor.read_text())["completed"] == ["AAPL"]
    assert first.fetch_counts == {"AAPL": 1, "MSFT": 1}
    assert all(client.closed for client in first.clients)

    second = _ClientFactory()
    assert (
        sync_corporate_actions.run(
            [
                "--tickers",
                "AAPL",
                "MSFT",
                "--workers",
                "2",
                "--cursor",
                str(cursor),
                "--resume",
            ],
            client_factory=second,
            store=_Store(),
            data_lake_root=tmp_path,
        )
        == 0
    )
    assert second.fetched_symbols == {"MSFT"}


def test_auth_failure_stops_new_work_and_reports_pending(tmp_path, capsys):
    factory = _ClientFactory(auth_fail=True)

    assert (
        sync_corporate_actions.run(
            ["--tickers", *[f"T{i}" for i in range(20)], "--workers", "2"],
            client_factory=factory,
            store=_Store(),
            data_lake_root=tmp_path,
        )
        == 1
    )

    summary = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert summary["pending"] > 0
    assert summary["requested"] == summary["resumed"] + summary["attempted"] + summary["pending"]
