from __future__ import annotations

import json
import threading
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import responses

from clients import ledger
from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore, SplitAddition
from clients.ib_gateway_preflight import GATEWAY_DOWN_EXIT_CODE
from clients.massive_client import MassiveAuthError, MassiveDividend, MassiveResponseCapture
from clients.source_evidence import SourceEvidenceStore
from clients.telemetry import MassiveTelemetry
from clients.yahoo_client import YahooSplit
from livewire_scripts import sync_corporate_actions
from livewire_scripts.corporate_action_cursor import build_identity, open_cursor


@responses.activate
def test_default_client_persists_negative_fetch_evidence_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "fixture-token")
    for resource in ("splits", "dividends"):
        responses.add(
            responses.GET,
            f"https://api.massive.com/v3/reference/{resource}",
            json={"status": "OK", "results": []},
            status=200,
        )

    assert (
        sync_corporate_actions.run(
            ["--tickers", "AAPL", "--workers", "1"],
            data_lake_root=tmp_path,
        )
        == 0
    )

    fetch = CorporateActionStore(tmp_path).fetch_history("AAPL")[0]
    assert set(fetch.resources) == {"splits", "dividends"}
    assert len(fetch.source_refs) == 2
    assert all(SourceEvidenceStore(tmp_path).read(ref) for ref in fetch.source_refs)


class _Client:
    def __init__(self, *, fail=None):
        self.fail = {fail} if isinstance(fail, str) else set(fail or ())
        self.calls: list[tuple[str, str]] = []
        self.closed = False

    def close(self):
        self.closed = True

    def get_splits(self, ticker):
        self.calls.append(("splits", ticker))
        if ticker in self.fail:
            raise RuntimeError("provider failed")
        return [SimpleNamespace(provider_event_id=f"{ticker}-split")]

    def get_dividends(self, ticker):
        self.calls.append(("dividends", ticker))
        return [SimpleNamespace(provider_event_id=f"{ticker}-div")]


def test_response_recorder_persists_exact_bytes_without_request_credentials(tmp_path):
    capture = MassiveResponseCapture(
        body=b'{"status":"OK","results":[]}',
        source_url="https://api.massive.com/v3/reference/splits?ticker=AAPL",
        fetched_at=datetime(2026, 8, 31, 1, 0, tzinfo=UTC),
        content_type="application/json",
        cursor_identity="sha256:" + "b" * 64,
    )

    buffer = sync_corporate_actions._EvidenceBuffer(tmp_path)
    artifact = buffer.recorder()(capture)

    # Exact bytes are durable immediately; the manifest row lands on flush.
    assert SourceEvidenceStore(tmp_path).read(artifact.ref) == capture.body
    assert SourceEvidenceStore(tmp_path).list_verified() == []
    buffer.flush()
    row = SourceEvidenceStore(tmp_path).list_verified()[0]
    assert row.source_url == f"massive-response://sha256/{artifact.sha256}"
    assert "AAPL" not in row.source_url


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
        "cycles": 1,
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


def test_one_flaky_symbol_in_a_large_run_does_not_fail_the_run(tmp_path, capsys):
    """`run_daily_update_job` gates the Silver rebuild on this exit code.

    2026-08-02: `TGNA: Response ended prematurely` — 1 symbol of 14,577, 0.007% —
    exited 1 and Silver was skipped for the whole ~13K equity universe. That
    symbol just keeps the actions already in the store.
    """
    tickers = [f"T{i}" for i in range(200)]
    client = _Client(fail="T7")

    rc = sync_corporate_actions.run(["--tickers", *tickers], client=client, store=_Store(), data_lake_root=tmp_path)

    out = capsys.readouterr()
    assert rc == 0
    assert json.loads(out.out.strip().splitlines()[-1])["failed"] == 1
    # Never silent: exit 0 must still say a symbol was dropped.
    assert "1/200 symbols failed" in out.err


def test_a_systemic_failure_rate_still_fails_the_run(tmp_path):
    """Above the rate, the run is systemic and must block Silver."""
    tickers = [f"T{i}" for i in range(100)]
    client = _Client(fail={f"T{i}" for i in range(20)})

    assert (
        sync_corporate_actions.run(["--tickers", *tickers], client=client, store=_Store(), data_lake_root=tmp_path) == 1
    )


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
    # The resumed pass owes only MSFT; finishing it frees the rest of the
    # budget, so the same invocation opens a fresh pass over the whole list.
    assert second.fetch_counts == {"MSFT": 2, "AAPL": 1}


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


def _stub_endpoints(tickers):
    for _ in tickers:
        for resource in ("splits", "dividends"):
            responses.add(
                responses.GET,
                f"https://api.massive.com/v3/reference/{resource}",
                json={"status": "OK", "results": []},
                status=200,
            )


def _stub_distinct_endpoints(tickers):
    """Production shape: every response body differs, so nothing dedupes.

    `_stub_endpoints` returns one byte-identical empty body, which every write
    after the first answers from the in-process digest cache -- that is what hid
    the flat-directory cost until the lane timed out three nights running.
    """
    for ticker in tickers:
        for resource in ("splits", "dividends"):
            responses.add(
                responses.GET,
                f"https://api.massive.com/v3/reference/{resource}",
                json={"status": "OK", "request_id": f"{ticker}-{resource}", "results": []},
                status=200,
            )


class TestDistinctResponseBodies:
    @responses.activate
    def test_every_artifact_is_sharded_and_no_lock_file_is_left_behind(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "fixture-token")
        tickers = ["AAPL", "MSFT", "NVDA"]
        _stub_distinct_endpoints(tickers)

        assert sync_corporate_actions.run(["--tickers", *tickers, "--workers", "1"], data_lake_root=tmp_path) == 0

        store = SourceEvidenceStore(tmp_path)
        assert len(store.list_verified()) == 2 * len(tickers)
        assert list(store.raw_root.rglob(".*.lock")) == []
        assert [path for path in store.raw_root.iterdir() if path.is_file()] == []

    @responses.activate
    def test_the_manifest_is_committed_during_the_run_not_only_at_the_end(self, tmp_path, monkeypatch):
        """A lane SIGKILLed at its budget never reaches the `finally`."""
        monkeypatch.setenv("MASSIVE_API_KEY", "fixture-token")
        monkeypatch.setattr(sync_corporate_actions, "_EVIDENCE_FLUSH_EVERY", 1)
        tickers = ["AAPL", "MSFT", "NVDA"]
        _stub_distinct_endpoints(tickers)
        publishes = []
        real_publish = SourceEvidenceStore._publish_manifest
        monkeypatch.setattr(
            SourceEvidenceStore,
            "_publish_manifest",
            lambda self, rows: (publishes.append(len(rows)), real_publish(self, rows))[1],
        )

        assert sync_corporate_actions.run(["--tickers", *tickers, "--workers", "1"], data_lake_root=tmp_path) == 0

        assert publishes == [2, 4, 6], "one commit per ticker, each carrying the whole manifest"


class TestEvidenceIsCommittedOncePerRun:
    """The manifest is rewritten whole, so a per-response commit is O(N * manifest)."""

    @responses.activate
    def test_many_responses_publish_the_manifest_once(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "fixture-token")
        tickers = ["AAPL", "MSFT", "NVDA"]
        _stub_endpoints(tickers)
        publishes = []
        real_publish = SourceEvidenceStore._publish_manifest
        monkeypatch.setattr(
            SourceEvidenceStore,
            "_publish_manifest",
            lambda self, rows: (publishes.append(len(rows)), real_publish(self, rows))[1],
        )

        assert sync_corporate_actions.run(["--tickers", *tickers, "--workers", "1"], data_lake_root=tmp_path) == 0

        # Six responses, one identical empty body, one commit.
        assert len(responses.calls) == 2 * len(tickers)
        assert publishes == [1]

    @responses.activate
    def test_evidence_survives_a_run_that_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "fixture-token")
        _stub_endpoints(["AAPL"])

        class _Exploding(CorporateActionStore):
            def reconcile(self, *args, **kwargs):
                raise KeyboardInterrupt("operator stopped the run")

        with pytest.raises(KeyboardInterrupt):
            sync_corporate_actions.run(
                ["--tickers", "AAPL", "--workers", "1"],
                data_lake_root=tmp_path,
                store=_Exploding(tmp_path),
            )

        # The bytes were fetched, so they must be in the manifest, not only on disk.
        assert len(SourceEvidenceStore(tmp_path).list_verified()) == 1


class TestEvidenceKillSwitch:
    @pytest.mark.parametrize("value", ["off", "0", "false", "no", "OFF"])
    def test_recognized_off_values(self, monkeypatch, value):
        monkeypatch.setenv("MDW_SOURCE_EVIDENCE", value)
        assert sync_corporate_actions.evidence_enabled() is False

    @pytest.mark.parametrize("value", ["on", "1", "", "anything"])
    def test_anything_else_leaves_evidence_on(self, monkeypatch, value):
        monkeypatch.setenv("MDW_SOURCE_EVIDENCE", value)
        assert sync_corporate_actions.evidence_enabled() is True

    def test_unset_leaves_evidence_on(self, monkeypatch):
        monkeypatch.delenv("MDW_SOURCE_EVIDENCE", raising=False)
        assert sync_corporate_actions.evidence_enabled() is True

    @responses.activate
    def test_off_writes_no_evidence_and_still_reconciles(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "fixture-token")
        monkeypatch.setenv("MDW_SOURCE_EVIDENCE", "off")
        _stub_endpoints(["AAPL"])

        assert sync_corporate_actions.run(["--tickers", "AAPL", "--workers", "1"], data_lake_root=tmp_path) == 0

        assert not SourceEvidenceStore(tmp_path).manifest_path.exists()
        assert list(CorporateActionStore(tmp_path).fetch_history("AAPL")[0].source_refs) == []


def test_one_buffer_is_shared_by_every_worker(tmp_path):
    """Workers build their own client, so a per-client buffer would never merge."""
    buffer = sync_corporate_actions._EvidenceBuffer(tmp_path)
    barrier = threading.Barrier(4)

    def record(index: int) -> None:
        barrier.wait()
        buffer.recorder()(
            MassiveResponseCapture(
                body=f"payload-{index}".encode(),
                source_url="https://api.massive.com/v3/reference/splits",
                fetched_at=datetime(2026, 8, 31, 1, 0, tzinfo=UTC),
                content_type="application/json",
                cursor_identity="sha256:" + "c" * 64,
            )
        )

    threads = [threading.Thread(target=record, args=(index,)) for index in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    buffer.flush()

    assert len(SourceEvidenceStore(tmp_path).list_verified()) == 4


def test_provider_totals_reach_the_ledger_not_just_the_log(tmp_path, monkeypatch):
    """2026-09-03: the lane ran 2h15m and nothing on disk said whether it was
    rate-limited, timing out, or just slow — telemetry was never passed."""
    monkeypatch.setenv("LW_RUN_ID", "daily-update-20260903T060005Z-49009")
    telemetry = MassiveTelemetry(jsonl_path=None)
    telemetry.record_request(endpoint="/v3/reference/splits", status=200, dt_ms=120)
    telemetry.record_request(endpoint="/v3/reference/splits", status=429, dt_ms=90)
    telemetry.record_wait(4.25)

    rc = sync_corporate_actions.run(
        ["--tickers", "AAPL", "--workers", "1"],
        client=_Client(),
        store=_Store(),
        data_lake_root=tmp_path,
        telemetry=telemetry,
    )

    assert rc == 0
    rows = ledger.query(
        "select name, scope, source, unit, value from measurements "
        "where run_id = 'daily-update-20260903T060005Z-49009' order by name"
    )
    assert [row["name"] for row in rows] == [
        "provider_errors",
        "provider_latency_p95_ms",
        "provider_requests",
        "provider_throttled",
        "provider_wait_s",
    ]
    assert {row["scope"] for row in rows} == {"corporate-actions"}
    assert {row["source"] for row in rows} == {"measured"}
    by_name = {row["name"]: row["value"] for row in rows}
    assert by_name["provider_requests"] == 2.0
    assert by_name["provider_throttled"] == 1.0
    assert by_name["provider_wait_s"] == 4.25


def test_the_default_client_factory_actually_attaches_the_telemetry(tmp_path, monkeypatch):
    """The seam the injected-factory tests skip: production goes through
    default_client_factory, and that is the only place the wiring exists."""
    monkeypatch.setenv("MASSIVE_API_KEY", "fixture-token")
    built: list[dict] = []

    class _Recorder(_Client):
        def __init__(self, **kwargs):
            super().__init__()
            built.append(kwargs)

    monkeypatch.setattr(sync_corporate_actions, "MassiveClient", _Recorder)
    telemetry = MassiveTelemetry(jsonl_path=None)

    sync_corporate_actions.run(
        ["--tickers", "AAPL", "--workers", "1"],
        store=_Store(),
        data_lake_root=tmp_path,
        telemetry=telemetry,
    )

    assert built and built[0]["telemetry"] is telemetry


def test_a_run_that_measured_nothing_emits_nothing(tmp_path, monkeypatch):
    """ledger.emit refuses zero rows; a run that made no measured request
    must skip the emit rather than abort a lane that otherwise succeeded."""
    monkeypatch.setenv("LW_RUN_ID", "manual-20260903T000000Z-1")

    rc = sync_corporate_actions.run(["--tickers", "AAPL"], client=_Client(), store=_Store(), data_lake_root=tmp_path)

    assert rc == 0
    # Narrowed only past the dividend-fx facts the lane now always emits; a
    # provider or progress row still has to stay absent.
    assert ledger.query("select count(*) as n from measurements where name not like 'dividend_%'")[0]["n"] == 0


def _seed_cursor(tmp_path, path, tickers, done):
    """Write a real, compatible, incomplete cursor -- the subject under test."""
    identity = build_identity(tmp_path, tickers, full_reconcile=False, dry_run=False)
    cursor = open_cursor(path, identity, resume=False, now=datetime(2026, 9, 4, 6, tzinfo=UTC))
    for ticker in done:
        cursor.mark_completed(ticker, now=datetime(2026, 9, 4, 6, tzinfo=UTC))
    return cursor


def test_a_resumed_pass_finishes_its_tail_then_opens_a_new_cycle(tmp_path):
    """Last night's tail first, then this night's own full pass.

    Resuming alone would leave the head of the universe untouched on a night
    the previous pass was nearly done; restarting alone was the bug.
    """
    tickers = ["AAPL", "MSFT", "NVDA"]
    cursor_path = tmp_path / "cursor.json"
    _seed_cursor(tmp_path, cursor_path, tickers, ["AAPL", "MSFT"])
    client = _Client()

    assert (
        sync_corporate_actions.run(
            ["--tickers", *tickers, "--cursor", str(cursor_path), "--resume"],
            client=client,
            store=_Store(),
            data_lake_root=tmp_path,
        )
        == 0
    )

    assert [ticker for kind, ticker in client.calls if kind == "splits"] == [
        "NVDA",
        "AAPL",
        "MSFT",
        "NVDA",
    ]
    assert json.loads(cursor_path.read_text())["run_completed_at"] is not None
    # One conversion per cycle, each under its own run id: the second cycle
    # writes dividends of its own, and a conversion that ran only before it
    # would file a 0 mismatch that `status` then grades OK.
    fx_runs = {
        row["run_id"] for row in ledger.query("select run_id from runs where job = 'dividend-fx' and ended is not null")
    }
    assert len(fx_runs) == 2
    converted = ledger.query("select value from measurements where name = 'dividend_fx_converted'")
    assert len(converted) == 2


def test_a_resumed_pass_that_does_not_finish_stays_resumable(tmp_path, capsys):
    tickers = ["AAPL", "MSFT", "NVDA"]
    cursor_path = tmp_path / "cursor.json"
    _seed_cursor(tmp_path, cursor_path, tickers, ["AAPL"])

    assert (
        sync_corporate_actions.run(
            ["--tickers", *tickers, "--cursor", str(cursor_path), "--resume"],
            client=_Client(fail="NVDA"),
            store=_Store(),
            data_lake_root=tmp_path,
        )
        == 1
    )

    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["cycles"] == 1
    assert summary["resumed"] == 1
    assert json.loads(cursor_path.read_text())["run_completed_at"] is None
    assert json.loads(cursor_path.read_text())["completed"] == ["AAPL", "MSFT"]


def test_progress_heartbeats_to_the_ledger_at_every_flush(tmp_path, monkeypatch):
    """A lane SIGKILLed at its budget prints nothing; the ledger still says how far it got."""
    monkeypatch.setenv("LW_RUN_ID", "daily-update-20260905T060000Z-1")
    monkeypatch.setattr(sync_corporate_actions, "_EVIDENCE_FLUSH_EVERY", 2)
    tickers = [f"T{index}" for index in range(4)]

    assert (
        sync_corporate_actions.run(
            ["--tickers", *tickers, "--cursor", str(tmp_path / "cursor.json")],
            client=_Client(),
            store=_Store(),
            data_lake_root=tmp_path,
        )
        == 0
    )

    rows = ledger.query("select name, value, unit, run_id from measurements where scope = 'corporate-actions'")
    assert sorted(row["value"] for row in rows if row["name"] == "progress") == [2.0, 4.0]
    assert {row["value"] for row in rows if row["name"] == "progress_total"} == {4.0}
    assert {row["unit"] for row in rows} == {"symbols"}
    assert {row["run_id"] for row in rows} == {"daily-update-20260905T060000Z-1"}


# --- corporate-actions convert-dividend-currency -------------------------------------

_ACR_EX = date(2008, 6, 26)
_ACR_USDCAD = 1.0118999481201172
_ACR_CONVERTED = 0.40517839808341516  # 0.41 CAD / USDCAD 1.0118999481201172


def _fx_lake(root: Path, rows: list[dict]) -> None:
    path = root / "bronze" / "asset_class=fx" / "symbol=USDCAD" / "1d.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [
            {
                "trade_date": row["trade_date"],
                "symbol_id": 1,
                "open": row["close"],
                "high": row["close"],
                "low": row["close"],
                "close": row["close"],
                "adj_close": row["close"],
                "volume": 0,
            }
            for row in rows
        ]
    )
    pq.write_table(table, path)


_ACR_CAD_DIVIDEND = MassiveDividend(
    provider_event_id="acr-div-1",
    ticker="ACR",
    ex_dividend_date=_ACR_EX,
    cash_amount=Decimal("0.41"),
    currency="CAD",
    declaration_date=None,
    record_date=None,
    pay_date=None,
    payload_hash="acr-cad-v1",
)


def _cad_lake(tmp_path):
    """One ACR dividend in CAD + one USDCAD bar on its ex-date."""
    store = CorporateActionStore(tmp_path)
    store.reconcile(
        "ACR",
        [
            MassiveDividend(
                provider_event_id="acr-div-1",
                ticker="ACR",
                ex_dividend_date=_ACR_EX,
                cash_amount=Decimal("0.41"),
                currency="CAD",
                declaration_date=None,
                record_date=None,
                pay_date=None,
                payload_hash="acr-cad-v1",
            )
        ],
        datetime(2026, 9, 13, tzinfo=UTC),
    )
    _fx_lake(tmp_path, [{"trade_date": _ACR_EX, "close": _ACR_USDCAD}])
    return store


def test_convert_dividend_currency_dry_run_writes_nothing_to_the_store(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    store = _cad_lake(tmp_path)
    before = store.path_for("ACR").read_bytes()

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["ACR"], apply=False, output_dir=None, lake_root=tmp_path
    )

    assert summary["converted"] == 0 and summary["remaining"] == 1
    assert summary["skipped"] == []
    assert store.path_for("ACR").read_bytes() == before
    # Ledger facts land in dry-run too.
    names = {row["name"] for row in ledger.query("select name from measurements")}
    assert {"dividend_currency_mismatch", "dividend_fx_converted", "dividend_fx_skipped"} <= names
    assert ledger.query("select job, verdict from runs where job = 'dividend-fx'")


def test_convert_dividend_currency_apply_converts_and_emits(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    store = _cad_lake(tmp_path)
    out = tmp_path / "out"

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["ACR"], apply=True, output_dir=out, lake_root=tmp_path
    )

    assert summary["converted"] == 1 and summary["remaining"] == 0
    active = store.latest_active("ACR")
    assert len(active) == 1 and active[0].provider == "eod_fx"
    assert active[0].cash_amount == pytest.approx(_ACR_CONVERTED)
    assert active[0].currency == "USD"
    assert active[0].source_ref.startswith("eod_fx:USDCAD@2008-06-26 rate=1.01189995 method=divide")
    assert active[0].source_hash is not None
    manifest = json.loads((out / "dividend_fx_conversion_applied.json").read_text())
    row = manifest["applied"][0]
    assert row["symbol"] == "ACR" and row["orig_cash"] == 0.41 and row["orig_currency"] == "CAD"
    assert row["converted_cash"] == pytest.approx(_ACR_CONVERTED)
    assert row["fx_pair"] == "USDCAD" and row["fx_method"] == "divide"
    assert row["old_action_id"] and row["new_action_id"] == active[0].action_id
    assert manifest["skipped"] == []
    measurements = {
        row["name"]: row["value"]
        for row in ledger.query("select name, value from measurements where source = 'measured'")
    }
    assert measurements["dividend_currency_mismatch"] == 0.0
    assert measurements["dividend_fx_converted"] == 1.0
    assert measurements["dividend_fx_skipped"] == 0.0
    assert ledger.query("select evidence_hash from evidence where kind = 'fx_bar'")


def test_convert_dividend_currency_missing_fx_bar_is_skipped(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    store = CorporateActionStore(tmp_path)
    store.reconcile(
        "ACR",
        [
            MassiveDividend(
                provider_event_id="acr-div-1",
                ticker="ACR",
                ex_dividend_date=_ACR_EX,
                cash_amount=Decimal("0.41"),
                currency="CAD",
                declaration_date=None,
                record_date=None,
                pay_date=None,
                payload_hash="acr-cad-v1",
            )
        ],
        datetime(2026, 9, 13, tzinfo=UTC),
    )
    # No fx file at all.
    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["ACR"], apply=True, output_dir=tmp_path / "out", lake_root=tmp_path
    )

    assert summary["converted"] == 0
    assert summary["skipped"] == [{"symbol": "ACR", "ex_date": "2008-06-26", "reason": "no_fx_bar"}]
    assert store.latest_active("ACR")[0].currency == "CAD"
    skipped = ledger.query("select value from measurements where name = 'dividend_fx_skipped'")
    assert skipped[-1]["value"] == 1.0


def _sm_event(symbol: str, currency: str):
    from clients.security_master import SecurityIdentityEvent

    return SecurityIdentityEvent(
        event_id=f"ev-{symbol}-1",
        security_id="sec_" + "1" * 32,
        revision=1,
        symbol=symbol,
        provider="massive",
        exchange_mic="XNAS",
        currency=currency,
        effective_from=datetime(2020, 1, 1, tzinfo=UTC),
        effective_to=None,
        known_at=datetime(2020, 1, 1, tzinfo=UTC),
        issuer_name="Example",
        cik="0000000001",
        composite_figi="BBG000000001",
        share_class_figi=None,
        continuity_basis="provider_figi",
        relationship_type=None,
        related_security_id=None,
        source_refs=(f"artifact://sha256/{'a' * 64}",),
        source_hashes=("a" * 64,),
        status="verified",
        supersedes=None,
    )


def test_convert_dividend_currency_uses_security_master_currency(tmp_path, monkeypatch):
    """A GBP equity's USD dividend converts through GBPUSD (orig is quote → divide)."""
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    from clients.security_master import SecurityMaster

    master = SecurityMaster(tmp_path, evidence_verifier=lambda ref, digest: True)
    assert master.append(_sm_event("VOD", "GBP"))
    CorporateActionStore(tmp_path).reconcile(
        "VOD",
        [
            MassiveDividend(
                provider_event_id="vod-div-1",
                ticker="VOD",
                ex_dividend_date=_ACR_EX,
                cash_amount=Decimal("2.00"),
                currency="USD",
                declaration_date=None,
                record_date=None,
                pay_date=None,
                payload_hash="vod-usd-v1",
            )
        ],
        datetime(2026, 9, 13, tzinfo=UTC),
    )
    path = tmp_path / "bronze" / "asset_class=fx" / "symbol=GBPUSD" / "1d.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "trade_date": _ACR_EX,
                    "symbol_id": 1,
                    "open": 2.0,
                    "high": 2.0,
                    "low": 2.0,
                    "close": 2.0,
                    "adj_close": 2.0,
                    "volume": 0,
                }
            ]
        ),
        path,
    )

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["VOD"], apply=True, output_dir=tmp_path / "out", lake_root=tmp_path
    )

    assert summary["converted"] == 1
    row = CorporateActionStore(tmp_path).latest_active("VOD")[0]
    assert row.provider == "eod_fx" and row.currency == "GBP"
    assert row.cash_amount == pytest.approx(1.0)  # 2.00 USD / GBPUSD 2.0
    assert row.source_ref.startswith("eod_fx:GBPUSD@2008-06-26 rate=2.00000000 method=divide")
    manifest = json.loads((tmp_path / "out" / "dividend_fx_conversion_applied.json").read_text())
    assert manifest["applied"][0]["equity_currency_source"] == "security_master"


def test_convert_dividend_currency_multiply_direction(tmp_path, monkeypatch):
    """GBP dividend on a USD equity: GBPUSD exists → multiply."""
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    store = CorporateActionStore(tmp_path)
    store.reconcile(
        "AZN",
        [
            MassiveDividend(
                provider_event_id="azn-div-1",
                ticker="AZN",
                ex_dividend_date=_ACR_EX,
                cash_amount=Decimal("1.00"),
                currency="GBP",
                declaration_date=None,
                record_date=None,
                pay_date=None,
                payload_hash="azn-gbp-v1",
            )
        ],
        datetime(2026, 9, 13, tzinfo=UTC),
    )
    path = tmp_path / "bronze" / "asset_class=fx" / "symbol=GBPUSD" / "1d.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "trade_date": _ACR_EX,
                    "symbol_id": 1,
                    "open": 1.27,
                    "high": 1.27,
                    "low": 1.27,
                    "close": 1.27,
                    "adj_close": 1.27,
                    "volume": 0,
                }
            ]
        ),
        path,
    )

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["AZN"], apply=True, output_dir=tmp_path / "out", lake_root=tmp_path
    )

    assert summary["converted"] == 1
    row = store.latest_active("AZN")[0]
    assert row.cash_amount == pytest.approx(1.27) and row.currency == "USD"
    assert "method=multiply" in row.source_ref


def test_convert_dividend_currency_stale_bar_is_skipped(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    store = _cad_lake(tmp_path)
    # The only bar is >5 sessions before the ex-date → no usable rate.
    _fx_lake(tmp_path, [{"trade_date": date(2008, 6, 10), "close": _ACR_USDCAD}])

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["ACR"], apply=True, output_dir=tmp_path / "out", lake_root=tmp_path
    )

    assert summary["converted"] == 0
    assert summary["skipped"][0]["reason"] == "no_fx_bar"
    assert store.latest_active("ACR")[0].currency == "CAD"


def test_convert_dividend_currency_discovers_ca_store_symbols(tmp_path, monkeypatch):
    """tickers=None scans every symbol with a corporate_action events file."""
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _cad_lake(tmp_path)

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=None, apply=False, output_dir=None, lake_root=tmp_path
    )

    assert summary["tickers"] == 1 and summary["detected"] == 1


def test_convert_dividend_currency_second_apply_is_a_noop(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _cad_lake(tmp_path)
    out = tmp_path / "out"
    sync_corporate_actions.convert_dividend_currency(tickers=["ACR"], apply=True, output_dir=out, lake_root=tmp_path)

    again = sync_corporate_actions.convert_dividend_currency(
        tickers=["ACR"], apply=True, output_dir=out, lake_root=tmp_path
    )
    assert again["detected"] == 0 and again["converted"] == 0 and again["remaining"] == 0
    assert len(CorporateActionStore(tmp_path).latest_active("ACR")) == 1


def test_convert_dividend_currency_cli_dispatch(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    monkeypatch.setattr(sync_corporate_actions, "data_lake_dir", lambda: tmp_path)
    _cad_lake(tmp_path)

    # --apply without --output-dir is a usage error.
    with pytest.raises(SystemExit):
        sync_corporate_actions.main(["convert-dividend-currency", "--apply"])

    assert sync_corporate_actions.main(["convert-dividend-currency", "--tickers", "ACR"]) == 0
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["detected"] == 1 and summary["converted"] == 0


def test_convert_dividend_currency_failure_closes_run_failed(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _cad_lake(tmp_path)

    def boom(*args, **kwargs):
        raise RuntimeError("store exploded")

    monkeypatch.setattr(CorporateActionStore, "apply_repairs", boom)
    with pytest.raises(RuntimeError, match="store exploded"):
        sync_corporate_actions.convert_dividend_currency(
            tickers=["ACR"], apply=True, output_dir=tmp_path / "out", lake_root=tmp_path
        )
    verdicts = ledger.query("select verdict from runs where job = 'dividend-fx'")
    assert any(row["verdict"] == "FAILED" for row in verdicts)


def test_convert_dividend_currency_usd_peg_needs_no_fx_bar(tmp_path, monkeypatch):
    """NTB BMD 0.32 ex 2017-11-10: grok converted at 1:1 with fx_pair USD_PEG."""
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    store = CorporateActionStore(tmp_path)
    store.reconcile(
        "NTB",
        [
            MassiveDividend(
                provider_event_id="ntb-div-1",
                ticker="NTB",
                ex_dividend_date=date(2017, 11, 10),
                cash_amount=Decimal("0.32"),
                currency="BMD",
                declaration_date=None,
                record_date=None,
                pay_date=None,
                payload_hash="ntb-bmd-v1",
            )
        ],
        datetime(2026, 9, 13, tzinfo=UTC),
    )
    calls = []

    def spy(pair, on):
        calls.append((pair, on))
        return (on, 1.0)

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["NTB"], apply=True, output_dir=tmp_path / "out", lake_root=tmp_path, fx_close_fn=spy
    )

    assert calls == []  # the peg never consults the FX tape
    assert summary["converted"] == 1 and summary["remaining"] == 0
    row = store.latest_active("NTB")[0]
    assert row.provider == "eod_fx" and row.cash_amount == 0.32 and row.currency == "USD"
    assert row.source_ref == "eod_fx:USD_PEG@2017-11-10 rate=1.00000000 method=peg orig=0.32 BMD -> 0.32000000 USD"
    assert row.source_hash is None
    assert not ledger.query("select * from evidence")
    manifest = json.loads((tmp_path / "out" / "dividend_fx_conversion_applied.json").read_text())
    applied = manifest["applied"][0]
    assert applied["fx_pair"] == "USD_PEG" and applied["fx_method"] == "peg"
    assert applied["fx_rate"] == 1.0 and applied["fx_date"] == "2017-11-10"


def test_convert_dividend_currency_non_pegged_still_uses_fx_close_fn(tmp_path, monkeypatch):
    """A non-pegged currency goes through the FX tape exactly as before."""
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _cad_lake(tmp_path)
    calls = []

    def spy(pair, on):
        calls.append((pair, on))
        return (on, 2.0)

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["ACR"], apply=True, output_dir=tmp_path / "out", lake_root=tmp_path, fx_close_fn=spy
    )

    assert calls == [("USDCAD", _ACR_EX)]
    assert summary["converted"] == 1
    row = CorporateActionStore(tmp_path).latest_active("ACR")[0]
    assert row.cash_amount == pytest.approx(0.41 / 2.0) and row.currency == "USD"
    assert "method=divide" in row.source_ref


def test_convert_dividend_currency_usd_dividend_on_pegged_equity(tmp_path, monkeypatch):
    """USD dividend on a BMD equity converts at the inverse peg, still no FX tape."""
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    from clients.security_master import SecurityMaster

    master = SecurityMaster(tmp_path, evidence_verifier=lambda ref, digest: True)
    assert master.append(_sm_event("NTB", "BMD"))
    CorporateActionStore(tmp_path).reconcile(
        "NTB",
        [
            MassiveDividend(
                provider_event_id="ntb-div-2",
                ticker="NTB",
                ex_dividend_date=date(2017, 11, 10),
                cash_amount=Decimal("0.32"),
                currency="USD",
                declaration_date=None,
                record_date=None,
                pay_date=None,
                payload_hash="ntb-usd-v1",
            )
        ],
        datetime(2026, 9, 13, tzinfo=UTC),
    )
    calls = []

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["NTB"],
        apply=True,
        output_dir=tmp_path / "out",
        lake_root=tmp_path,
        fx_close_fn=lambda pair, on: calls.append((pair, on)) or (on, 1.0),
    )

    assert calls == []
    assert summary["converted"] == 1
    row = CorporateActionStore(tmp_path).latest_active("NTB")[0]
    assert row.cash_amount == pytest.approx(0.32) and row.currency == "BMD"
    assert "method=peg" in row.source_ref and row.source_hash is None


# --- the lane converts foreign-currency dividends itself ------------------------


def test_the_lane_converts_foreign_currency_dividends_and_emits_its_measurements(tmp_path, capsys, monkeypatch):
    """Through the real lane argv, not the manual sub-command.

    PR #128 shipped `convert-dividend-currency` wired to nothing: the scheduled
    `corporate-actions --resume` lane emitted no `dividend_fx_*` measurement and
    ~30 symbols failed Silver on a currency mismatch.
    """
    monkeypatch.setenv("LW_RUN_ID", "daily-update-20260914T050000Z-1")
    store = _cad_lake(tmp_path)

    assert (
        sync_corporate_actions.run(
            ["--tickers", "ACR", "--resume"],
            # The pass itself marks ACR: the lane converts what it just
            # reconciled in a foreign currency, not the whole store.
            client=_DividendClient({"ACR": [_ACR_CAD_DIVIDEND]}),
            store=store,
            data_lake_root=tmp_path,
        )
        == 0
    )

    active = store.latest_active("ACR")
    assert len(active) == 1 and active[0].provider == "eod_fx"
    # One summary line still, with the repair folded in.
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["dividend_fx"]["converted"] == 1 and summary["dividend_fx"]["remaining"] == 0
    measurements = {
        row["name"]: row["value"]
        for row in ledger.query("select name, value from measurements where name like 'dividend_%'")
    }
    assert measurements["dividend_fx_converted"] == 1.0
    assert measurements["dividend_currency_mismatch"] == 0.0
    # Its own runs row: sharing the lane's run id would file a closed row under
    # a still-open daily-update run.
    fx_runs = {row["run_id"] for row in ledger.query("select run_id from runs where job = 'dividend-fx'")}
    assert fx_runs == {"daily-update-20260914T050000Z-1-dividend-fx"}


def test_a_targeted_pass_does_not_file_its_repair_as_the_whole_scope(tmp_path, monkeypatch):
    """`--tickers` measures a handful of symbols, so it is not scope 'all'.

    `status` grades today's newest `dividend_currency_mismatch` row; an operator
    repairing one symbol in the afternoon would otherwise erase the night's
    whole-scope WARN.
    """
    _cad_lake(tmp_path)

    assert (
        sync_corporate_actions.run(
            ["--tickers", "ACR", "--resume"], client=_Client(), store=_Store(), data_lake_root=tmp_path
        )
        == 0
    )

    scopes = {row["scope"] for row in ledger.query("select scope from measurements where name like 'dividend_%'")}
    assert scopes == {"subset"}


def test_a_failing_dividend_conversion_cannot_fail_the_lane(tmp_path, monkeypatch, capsys):
    def boom(**kwargs):
        raise RuntimeError("fx bar unreadable")

    monkeypatch.setattr(sync_corporate_actions, "convert_dividend_currency", boom)

    assert (
        sync_corporate_actions.run(
            ["--tickers", "ACR", "--resume"],
            client=_Client(),
            store=_Store(),
            data_lake_root=tmp_path,
        )
        == 0
    )
    assert "fx bar unreadable" in capsys.readouterr().err


def test_a_dry_run_lane_does_not_convert(tmp_path):
    store = _cad_lake(tmp_path)
    before = store.path_for("ACR").read_bytes()

    sync_corporate_actions.run(
        ["--tickers", "ACR", "--dry-run"], client=_Client(), store=_Store(), data_lake_root=tmp_path
    )

    assert store.path_for("ACR").read_bytes() == before


def _cad_lake_on(tmp_path, ex_date: date, fx_date: date):
    """One ACR CAD dividend on `ex_date` with a USDCAD bar on `fx_date`."""
    store = CorporateActionStore(tmp_path)
    store.reconcile(
        "ACR",
        [
            MassiveDividend(
                provider_event_id="acr-div-1",
                ticker="ACR",
                ex_dividend_date=ex_date,
                cash_amount=Decimal("0.41"),
                currency="CAD",
                declaration_date=None,
                record_date=None,
                pay_date=None,
                payload_hash="acr-cad-v1",
            )
        ],
        datetime(2026, 9, 13, tzinfo=UTC),
    )
    _fx_lake(tmp_path, [{"trade_date": fx_date, "close": _ACR_USDCAD}])
    return store


def test_an_announced_future_ex_date_is_not_converted_at_todays_fx(tmp_path):
    """`_fx_bar` accepts a bar up to 7 days stale, so an ex-date announced a few
    days ahead would be priced at today's close and never recomputed -- after
    conversion its currency matches the equity's and it leaves the scan."""
    now = datetime(2026, 9, 14, 6, tzinfo=UTC)
    store = _cad_lake_on(tmp_path, now.date() + timedelta(days=3), now.date())
    before = store.path_for("ACR").read_bytes()

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["ACR"], apply=True, output_dir=tmp_path / "out", lake_root=tmp_path, now=now
    )

    assert summary["converted"] == 0
    assert summary["skipped"] == [
        {"symbol": "ACR", "ex_date": (now.date() + timedelta(days=3)).isoformat(), "reason": "ex_date_pending"}
    ]
    assert store.path_for("ACR").read_bytes() == before
    # Not a leftover: the next run converts it, so it must not WARN nightly.
    assert summary["remaining"] == 0
    mismatch = ledger.query(
        "select value from measurements where name = 'dividend_currency_mismatch' order by measured_at desc limit 1"
    )
    assert mismatch[0]["value"] == 0.0


def test_a_missing_fx_bar_still_counts_as_a_mismatch(tmp_path):
    """`no_fx_bar` is a real leftover; `ex_date_pending` is not."""
    now = datetime(2026, 9, 14, 6, tzinfo=UTC)
    store = _cad_lake_on(tmp_path, now.date() + timedelta(days=3), now.date())
    store.reconcile(
        "BCE",
        [
            MassiveDividend(
                provider_event_id="bce-div-1",
                ticker="BCE",
                ex_dividend_date=now.date() - timedelta(days=400),
                cash_amount=Decimal("0.99"),
                currency="CAD",
                declaration_date=None,
                record_date=None,
                pay_date=None,
                payload_hash="bce-cad-v1",
            )
        ],
        datetime(2026, 9, 13, tzinfo=UTC),
    )

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["ACR", "BCE"], apply=True, output_dir=tmp_path / "out", lake_root=tmp_path, now=now
    )

    assert summary["detected"] == 2 and summary["converted"] == 0
    assert {row["reason"] for row in summary["skipped"]} == {"ex_date_pending", "no_fx_bar"}
    assert summary["remaining"] == 1


def test_a_settled_ex_date_still_converts(tmp_path):
    now = datetime(2026, 9, 14, 6, tzinfo=UTC)
    store = _cad_lake_on(tmp_path, now.date() - timedelta(days=1), now.date() - timedelta(days=1))

    summary = sync_corporate_actions.convert_dividend_currency(
        tickers=["ACR"], apply=True, output_dir=tmp_path / "out", lake_root=tmp_path, now=now
    )

    assert summary["converted"] == 1 and summary["skipped"] == []
    assert store.latest_active("ACR")[0].currency == "USD"


def test_a_preset_pass_does_not_file_its_repair_as_the_whole_scope(tmp_path):
    """`--preset` is a scope restriction exactly like `--tickers`."""
    _cad_lake(tmp_path)
    preset = tmp_path / "preset.json"
    preset.write_text(json.dumps({"name": "test", "tickers": ["ACR"]}))

    assert (
        sync_corporate_actions.run(["--preset", str(preset)], client=_Client(), store=_Store(), data_lake_root=tmp_path)
        == 0
    )

    scopes = {row["scope"] for row in ledger.query("select scope from measurements where name like 'dividend_%'")}
    assert scopes == {"subset"}


def test_the_conversion_runs_even_when_the_second_cycle_aborts(tmp_path):
    """The tail finished, so the dividends it wrote are converted before the
    invocation opens a fresh cycle that the lane budget may kill."""
    tickers = ["AAPL", "MSFT", "NVDA"]
    cursor_path = tmp_path / "cursor.json"
    _seed_cursor(tmp_path, cursor_path, tickers, ["AAPL", "MSFT"])
    real_open_cursor = sync_corporate_actions.open_cursor
    calls: list[bool] = []

    def flaky_open_cursor(path, identity, *, resume, now):
        calls.append(resume)
        if not resume:
            raise RuntimeError("killed at the lane budget")
        return real_open_cursor(path, identity, resume=resume, now=now)

    sync_corporate_actions.open_cursor = flaky_open_cursor
    try:
        with pytest.raises(RuntimeError, match="killed at the lane budget"):
            sync_corporate_actions.run(
                ["--tickers", *tickers, "--cursor", str(cursor_path), "--resume"],
                client=_Client(),
                store=_Store(),
                data_lake_root=tmp_path,
            )
    finally:
        sync_corporate_actions.open_cursor = real_open_cursor

    assert calls == [True, False]
    assert len(ledger.query("select run_id from runs where job = 'dividend-fx' and ended is not null")) == 1


class _DividendClient:
    """A provider that returns real dividend events for named tickers."""

    def __init__(self, dividends: dict[str, list]):
        self.dividends = dividends
        self.calls: list[str] = []

    def get_splits(self, ticker):
        self.calls.append(ticker)
        return []

    def get_dividends(self, ticker):
        return list(self.dividends.get(ticker, []))

    def close(self):
        pass


def test_the_second_cycles_own_dividends_are_converted_the_same_night(tmp_path):
    """The real store, not a stub: a conversion that ran only once, before the
    second cycle, left that cycle's dividends in CAD while the mismatch count it
    had already filed read 0 -- `status` OK on a night Silver fails on them."""
    tickers = ["ACR", "ZZZ"]
    cursor_path = tmp_path / "cursor.json"
    _seed_cursor(tmp_path, cursor_path, tickers, ["ACR"])
    _fx_lake(tmp_path, [{"trade_date": _ACR_EX, "close": _ACR_USDCAD}])
    store = CorporateActionStore(tmp_path)
    client = _DividendClient(
        {
            "ACR": [
                MassiveDividend(
                    provider_event_id="acr-div-1",
                    ticker="ACR",
                    ex_dividend_date=_ACR_EX,
                    cash_amount=Decimal("0.41"),
                    currency="CAD",
                    declaration_date=None,
                    record_date=None,
                    pay_date=None,
                    payload_hash="acr-cad-v1",
                )
            ]
        }
    )

    assert (
        sync_corporate_actions.run(
            ["--tickers", *tickers, "--cursor", str(cursor_path), "--resume"],
            client=client,
            store=store,
            data_lake_root=tmp_path,
        )
        == 0
    )

    # Cycle one finished the tail (ZZZ); cycle two fetched ACR and wrote the CAD
    # dividend, and the conversion at the end of that cycle superseded it.
    active = store.latest_active("ACR")
    assert [row.provider for row in active] == ["eod_fx"]
    assert active[0].currency == "USD" and active[0].cash_amount == pytest.approx(_ACR_CONVERTED)
    newest = ledger.query(
        "select value from measurements where name = 'dividend_currency_mismatch' order by measured_at desc limit 1"
    )
    assert newest[0]["value"] == 0.0
    fx_runs = {row["run_id"] for row in ledger.query("select run_id from runs where job = 'dividend-fx'")}
    assert len(fx_runs) == 2


def test_a_failing_second_cycle_conversion_closes_its_run_row(tmp_path, monkeypatch, capsys):
    """The lane swallows the exception, so the closed `runs` row is the only
    evidence that cycle one's zero no longer describes the store."""
    tickers = ["ACR", "ZZZ"]
    cursor_path = tmp_path / "cursor.json"
    _seed_cursor(tmp_path, cursor_path, tickers, ["ACR"])
    _fx_lake(tmp_path, [{"trade_date": _ACR_EX, "close": _ACR_USDCAD}])
    client = _DividendClient(
        {
            "ACR": [
                MassiveDividend(
                    provider_event_id="acr-div-1",
                    ticker="ACR",
                    ex_dividend_date=_ACR_EX,
                    cash_amount=Decimal("0.41"),
                    currency="CAD",
                    declaration_date=None,
                    record_date=None,
                    pay_date=None,
                    payload_hash="acr-cad-v1",
                )
            ]
        }
    )

    def unwritable(*args, **kwargs):
        raise OSError("evidence CAS unwritable")

    # A dependency, not the wrapper, and one reached *after* the conversion
    # loop: the run row is really opened, the rows are really written, and only
    # cycle two gets there -- cycle one has no foreign dividend to price.
    monkeypatch.setattr(sync_corporate_actions.SourceEvidenceStore, "persist_raw", unwritable)

    assert (
        sync_corporate_actions.run(
            ["--tickers", *tickers, "--cursor", str(cursor_path), "--resume"],
            client=client,
            store=CorporateActionStore(tmp_path),
            data_lake_root=tmp_path,
        )
        == 0
    )

    assert "evidence CAS unwritable" in capsys.readouterr().err
    rows = ledger.query("select run_id, ended, exit_code from runs where job = 'dividend-fx' and ended is not null")
    assert len(rows) == 2
    assert [row["exit_code"] for row in sorted(rows, key=lambda r: r["run_id"])] == [0, 1]


def test_a_conversion_that_fails_before_its_run_row_still_files_an_error(tmp_path, monkeypatch, capsys):
    """The `runs` row cannot cover a failure raised before it is opened; the
    `dividend_fx_error` measurement is what `status` reads."""
    monkeypatch.setenv("LW_RUN_ID", "daily-update-20260914T050000Z-1")
    _cad_lake(tmp_path)
    (tmp_path / "bronze/asset_class=equity/symbol=ACR").mkdir(parents=True)

    def unreadable(*args, **kwargs):
        raise OSError("corporate-action store unreadable")

    # A dependency the conversion builds *before* it opens its run row, and one
    # the lane itself does not use (the lane's store is injected).
    monkeypatch.setattr(sync_corporate_actions, "CorporateActionStore", unreadable)

    assert sync_corporate_actions.run([], client=_Client(), store=_Store(), data_lake_root=tmp_path) == 0

    assert "corporate-action store unreadable" in capsys.readouterr().err
    rows = ledger.query("select scope, value, run_id from measurements where name = 'dividend_fx_error'")
    assert [(row["scope"], row["value"]) for row in rows] == [("all", 1.0)]
    assert rows[0]["run_id"] == "daily-update-20260914T050000Z-1-dividend-fx"
    assert ledger.query("select run_id from runs where job = 'dividend-fx'") == []


def test_a_measurement_write_failure_closes_the_run_and_never_reads_as_a_zero(tmp_path, monkeypatch, capsys):
    """A swallowed measurement write used to close the run OK with no count at
    all. It now fails the conversion, so the run row closes `exit_code=1`."""
    _cad_lake(tmp_path)
    real_emit = ledger.emit

    def no_measurements(table, rows, *, run_id):
        if table == "measurements":
            raise OSError("ledger volume full")
        return real_emit(table, rows, run_id=run_id)

    monkeypatch.setattr(sync_corporate_actions.ledger, "emit", no_measurements)

    assert (
        sync_corporate_actions.run(["--tickers", "ACR"], client=_Client(), store=_Store(), data_lake_root=tmp_path) == 0
    )

    assert "ledger volume full" in capsys.readouterr().err
    # `ledger.query` reads parquet directly, so it is unaffected by the patched
    # emit (and monkeypatch.undo() here would also undo the ledger-root fixture).
    closed = ledger.query("select ended, exit_code from runs where job = 'dividend-fx' and ended is not null")
    assert [row["exit_code"] for row in closed] == [1]
    # The error row goes through the same failing path, so nothing is filed:
    # the closed FAILED run row is the only record, and no zero was written
    # either -- the check reads UNKNOWN on an absent measurement. Known
    # residual: an earlier whole-scope zero from the same day would still
    # grade OK in this partial-ledger-outage case (tribunal ISSUE-8, disclosed).
    assert ledger.query("select name from measurements where name = 'dividend_fx_error'") == []


# --- the nightly conversion scope: mark non-USD, do not rescan the store ---------


def _never(root):
    raise AssertionError("the nightly lane must not scan every store file")


def test_only_the_tickers_with_a_non_usd_dividend_are_converted(tmp_path, monkeypatch):
    """A USD dividend cannot need conversion, so it never enters the scope."""
    monkeypatch.setattr(sync_corporate_actions, "_convertible_symbols", _never)
    _fx_lake(tmp_path, [{"trade_date": _ACR_EX, "close": _ACR_USDCAD}])
    store = CorporateActionStore(tmp_path)
    usd = MassiveDividend(
        provider_event_id="msft-div-1",
        ticker="MSFT",
        ex_dividend_date=_ACR_EX,
        cash_amount=Decimal("0.75"),
        currency="USD",
        declaration_date=None,
        record_date=None,
        pay_date=None,
        payload_hash="msft-usd-v1",
    )

    assert (
        sync_corporate_actions.run(
            ["--tickers", "ACR", "MSFT"],
            client=_DividendClient({"ACR": [_ACR_CAD_DIVIDEND], "MSFT": [usd]}),
            store=store,
            data_lake_root=tmp_path,
        )
        == 0
    )

    assert [row.provider for row in store.latest_active("ACR")] == ["eod_fx"]
    assert [row.provider for row in store.latest_active("MSFT")] == ["massive"]
    # One symbol in scope, not two and not the whole store.
    measurements = {
        row["name"]: row["value"]
        for row in ledger.query("select name, value from measurements where name like 'dividend_%'")
    }
    assert measurements["dividend_fx_converted"] == 1.0


def test_a_symbol_left_pending_last_night_is_converted_without_being_refetched(tmp_path, monkeypatch):
    """Its dividend was reconciled on an earlier night; until its next successful fetch re-marks it, pending.json is what keeps it in scope."""
    monkeypatch.setattr(sync_corporate_actions, "_convertible_symbols", _never)
    store = _cad_lake(tmp_path)  # ACR's CAD dividend is already in the store
    (tmp_path / "repairs" / "dividend_fx").mkdir(parents=True)
    (tmp_path / "repairs" / "dividend_fx" / "pending.json").write_text(json.dumps({"symbols": ["acr"]}))

    assert (
        sync_corporate_actions.run(
            ["--tickers", "ZZZ"],
            client=_DividendClient({}),
            store=store,
            data_lake_root=tmp_path,
        )
        == 0
    )

    assert [row.provider for row in store.latest_active("ACR")] == ["eod_fx"]
    # Converted, so nothing is owed tomorrow.
    assert json.loads((tmp_path / "repairs" / "dividend_fx" / "pending.json").read_text())["symbols"] == []


def test_a_night_with_no_foreign_dividend_measures_zero_without_scanning_the_store(tmp_path, monkeypatch):
    monkeypatch.setattr(sync_corporate_actions, "_convertible_symbols", _never)

    assert (
        sync_corporate_actions.run(["--tickers", "AAPL"], client=_Client(), store=_Store(), data_lake_root=tmp_path)
        == 0
    )

    measurements = {
        row["name"]: row["value"]
        for row in ledger.query("select name, value, scope from measurements where name like 'dividend_%'")
    }
    assert measurements["dividend_currency_mismatch"] == 0.0
    scopes = {row["scope"] for row in ledger.query("select scope from measurements where name like 'dividend_%'")}
    assert scopes == {"subset"}
    assert ledger.query("select run_id from runs where job = 'dividend-fx' and ended is not null")


def test_the_manual_subcommand_without_tickers_still_scans_the_store(tmp_path, monkeypatch):
    """The bootstrap/audit path: it is the only full pass over the CA store."""
    _cad_lake(tmp_path)
    monkeypatch.setattr(sync_corporate_actions, "data_lake_dir", lambda: tmp_path)

    assert (
        sync_corporate_actions.convert_dividend_currency_main(["--apply", "--output-dir", str(tmp_path / "out")]) == 0
    )

    assert [row.provider for row in CorporateActionStore(tmp_path).latest_active("ACR")] == ["eod_fx"]


def test_a_usd_dividend_on_a_pegged_equity_is_marked_by_the_lane(tmp_path, monkeypatch):
    """Marking compares against the *equity* currency, which is what the store's
    `foreign_currency_dividends` predicate compares against. "not USD" would miss
    a USD dividend on a BMD-denominated equity entirely."""
    monkeypatch.setattr(sync_corporate_actions, "_convertible_symbols", _never)
    from clients.security_master import SecurityMaster

    master = SecurityMaster(tmp_path, evidence_verifier=lambda ref, digest: True)
    assert master.append(_sm_event("NTB", "BMD"))
    store = CorporateActionStore(tmp_path)
    usd_on_bmd = MassiveDividend(
        provider_event_id="ntb-div-2",
        ticker="NTB",
        ex_dividend_date=date(2017, 11, 10),
        cash_amount=Decimal("0.32"),
        currency="USD",
        declaration_date=None,
        record_date=None,
        pay_date=None,
        payload_hash="ntb-usd-v1",
    )
    usd_on_usd = MassiveDividend(
        provider_event_id="msft-div-1",
        ticker="MSFT",
        ex_dividend_date=date(2017, 11, 10),
        cash_amount=Decimal("0.75"),
        currency="USD",
        declaration_date=None,
        record_date=None,
        pay_date=None,
        payload_hash="msft-usd-v1",
    )

    assert (
        sync_corporate_actions.run(
            ["--tickers", "NTB", "MSFT"],
            client=_DividendClient({"NTB": [usd_on_bmd], "MSFT": [usd_on_usd]}),
            store=store,
            data_lake_root=tmp_path,
        )
        == 0
    )

    converted = store.latest_active("NTB")[0]
    assert converted.provider == "eod_fx" and converted.currency == "BMD"
    assert [row.provider for row in store.latest_active("MSFT")] == ["massive"]


def test_an_unreadable_pending_list_warns_and_does_not_kill_the_lane(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sync_corporate_actions, "_convertible_symbols", _never)
    pending = tmp_path / "repairs" / "dividend_fx" / "pending.json"
    pending.parent.mkdir(parents=True)
    pending.write_text("{not json")

    assert (
        sync_corporate_actions.run(["--tickers", "AAPL"], client=_Client(), store=_Store(), data_lake_root=tmp_path)
        == 0
    )

    assert "unreadable dividend FX pending list" in capsys.readouterr().err
    # The conversion still ran and still measured.
    assert ledger.query("select run_id from runs where job = 'dividend-fx' and ended is not null")


def test_the_currency_resolver_picks_a_row_when_two_share_one_known_at(tmp_path):
    """One identity fetch can append several rows for one symbol with the same
    known_at. The resolver keeps the first row it encounters on an equal
    known_at, with no provider, MIC or interval filter — assert the choice, not
    just that the field survived."""
    from clients.security_master import SecurityIdentityEvent, SecurityMaster

    fixture = Path(__file__).parent / "fixtures" / "massive_reference" / "imo-active-2026-09-15.json"
    row = (json.loads(fixture.read_bytes()).get("results") or [])[0]
    symbol, currency, mic = row["ticker"], row["currency_name"].upper(), row["primary_exchange"]
    # All 13,181 active US stock listings (14 pages of
    # ?market=stocks&active=true&limit=1000, measured on the mini 2026-09-15)
    # are currency_name: usd — no real non-USD listing exists on this endpoint
    # (fixtures README F6). The equal-known_at tie-break is therefore exercised
    # with two real USD rows: this proves the resolver returns a currency and
    # does not raise, not which of two different currencies wins.

    root = tmp_path / "lake"
    master = SecurityMaster(root, evidence_verifier=lambda ref, digest: ref.endswith(digest))
    known_at = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    for index, (interval_start, interval_end) in enumerate(
        [
            (datetime(2000, 1, 3, tzinfo=UTC), datetime(2010, 1, 4, tzinfo=UTC)),
            (datetime(2010, 1, 4, tzinfo=UTC), None),
        ]
    ):
        master.append(
            SecurityIdentityEvent(
                event_id=f"currency-{index}",
                security_id=master.new_security_id(),
                revision=1,
                symbol=symbol,
                provider="massive",
                exchange_mic=mic,
                currency=currency,
                effective_from=interval_start,
                effective_to=interval_end,
                known_at=known_at,
                issuer_name=row["name"],
                cik=None,
                composite_figi=None,
                share_class_figi=None,
                continuity_basis="provider_figi",
                relationship_type=None,
                related_security_id=None,
                source_refs=("artifact://sha256/" + "d" * 64,),
                source_hashes=("d" * 64,),
                status="verified",
                supersedes=None,
            )
        )

    resolve = sync_corporate_actions._equity_currency_resolver(root, known_at)
    assert resolve(symbol) == (currency, "security_master")


# --- restore-yahoo-splits ----------------------------------------------------

from tests.test_yahoo_basis import _AMC_ADJUSTED, _AMC_STEP, _amc_bronze_raw, _rows  # noqa: E402

# IBM 2-for-1 split, 1968-04-23 (a yahoo row in the macmini store, checked 2026-09-23) -- pre-IB-floor (1993-01-29), restorable
# from Yahoo's listing alone.
_IBM_EX = date(1968, 4, 23)
# Real AMC 2023-08-24 10:1 reverse split -- on/after the IB floor, so it needs an
# IB-confirmed price step (frozen fixtures shared with test_yahoo_basis.py).
_AMC_EX = date(2023, 8, 24)


def _yahoo_chart_bytes(splits: list[YahooSplit]) -> bytes:
    events = {
        str(index): {
            "date": int(datetime.combine(split.ex_date, datetime.min.time(), tzinfo=UTC).timestamp()),
            "numerator": split.numerator,
            "denominator": split.denominator,
        }
        for index, split in enumerate(splits)
    }
    return json.dumps({"chart": {"result": [{"events": {"splits": events}}]}}).encode()


class _FakeYahooSplits:
    def __init__(self, splits_by_symbol: dict[str, list[YahooSplit]]):
        self._splits_by_symbol = splits_by_symbol

    def get_split_events(self, symbol: str) -> tuple[bytes, list[YahooSplit]]:
        splits = self._splits_by_symbol.get(symbol, [])
        return _yahoo_chart_bytes(splits), splits


class _FakeIB:
    def __init__(self, *, fails: bool = False):
        self.fails = fails
        self.disconnected = False

    def connect(self, **kwargs):
        if self.fails:
            raise ConnectionError("IB gateway unreachable")

    def disconnect(self):
        self.disconnected = True


def _window_fetcher(rows: list[dict]):
    def factory(client):
        def fetch(symbol, start, end):
            return [row for row in rows if start <= row["trade_date"] <= end]

        return fetch

    return factory


def _seed_cancelled_yahoo_split(root: Path, symbol: str, ex_date: date, split_from: float, split_to: float) -> None:
    fetched_at = datetime(2026, 7, 19, tzinfo=UTC)
    store = CorporateActionStore(root)
    store.apply_repairs(
        symbol, add_splits=[SplitAddition(ex_date, split_from, split_to)], cancel_ex_dates=[], fetched_at=fetched_at
    )
    store.apply_repairs(symbol, add_splits=[], cancel_ex_dates=[ex_date], fetched_at=fetched_at)


def _amc_ib_rows() -> list[dict]:
    return _rows(_AMC_ADJUSTED)


def test_restore_yahoo_splits_dry_run_writes_nothing_but_emits_the_run(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "IBM", _IBM_EX, 1.0, 2.0)
    store = CorporateActionStore(tmp_path)
    before = store.path_for("IBM").read_bytes()

    result = sync_corporate_actions.restore_yahoo_splits(
        tickers=["IBM"],
        apply=False,
        ib_verify=False,
        output_dir=tmp_path / "out",
        lake_root=tmp_path,
        now=datetime(2026, 9, 23, tzinfo=UTC),
        yahoo_factory=lambda: _FakeYahooSplits({"IBM": [YahooSplit(_IBM_EX, 2.0, 1.0)]}),
    )

    assert result["restored"] == 0
    assert result["counts"] == {"yahoo_only": 1}
    assert result["exit_code"] == 0
    assert store.path_for("IBM").read_bytes() == before  # no store mutation
    assert not (tmp_path / "raw" / "shepherd").exists()  # no CAS evidence written
    manifest = json.loads((tmp_path / "out" / "restore_yahoo_splits.json").read_text())
    assert manifest["candidates"][0]["outcome"] == "yahoo_only"
    assert manifest["candidates"][0]["applied"] is False
    runs = ledger.query(
        "select job, exit_code, verdict from runs where job = 'restore-yahoo-splits' and ended is not null"
    )
    assert len(runs) == 1 and runs[0]["exit_code"] == 0 and runs[0]["verdict"] == "OK"


def test_restore_yahoo_splits_restores_a_pre_floor_candidate_as_yahoo_only(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "IBM", _IBM_EX, 1.0, 2.0)

    result = sync_corporate_actions.restore_yahoo_splits(
        tickers=["IBM"],
        apply=True,
        ib_verify=True,
        output_dir=None,
        lake_root=tmp_path,
        now=datetime(2026, 9, 23, tzinfo=UTC),
        yahoo_factory=lambda: _FakeYahooSplits({"IBM": [YahooSplit(_IBM_EX, 2.0, 1.0)]}),
    )

    assert result["restored"] == 1
    active = CorporateActionStore(tmp_path).latest_active("IBM")
    assert len(active) == 1
    revived = active[0]
    assert revived.source_cursor_identity == "restore-yahoo-splits:yahoo_only"
    evidence = SourceEvidenceStore(tmp_path)
    envelope = json.loads(evidence.read(revived.source_ref))
    assert (
        envelope["kind"] == "yahoo-split-restoration" and envelope["grade"] == "yahoo_only" and envelope["ib"] is None
    )


def test_restore_yahoo_splits_restores_an_ib_verified_candidate(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "AMC", _AMC_EX, 10.0, 1.0)
    BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").replace_ticker_rows(
        "AMC",
        [
            {
                "trade_date": d,
                "symbol_id": 1,
                "open": c,
                "high": c,
                "low": c,
                "close": c,
                "adj_close": c,
                "volume": 1_000,
                "source": "legacy",
                "price_basis": "unknown",
            }
            for row in _amc_bronze_raw()
            for d, c in [(row["trade_date"].isoformat(), row["close"])]
        ],
    )

    result = sync_corporate_actions.restore_yahoo_splits(
        tickers=["AMC"],
        apply=True,
        ib_verify=True,
        output_dir=None,
        lake_root=tmp_path,
        now=datetime(2026, 9, 23, tzinfo=UTC),
        yahoo_factory=lambda: _FakeYahooSplits({"AMC": [YahooSplit(_AMC_EX, 1.0, 10.0)]}),
        ib_factory=lambda: _FakeIB(),
        ib_fetcher_factory=_window_fetcher(_amc_ib_rows()),
    )

    assert result["restored"] == 1
    revived = CorporateActionStore(tmp_path).latest_active("AMC")[0]
    assert revived.source_cursor_identity == "restore-yahoo-splits:ib_verified"
    envelope = json.loads(SourceEvidenceStore(tmp_path).read(revived.source_ref))
    assert envelope["grade"] == "ib_verified"
    assert envelope["ib"]["step"] == pytest.approx(_AMC_STEP)


def test_restore_yahoo_splits_ib_step_mismatch_stays_cancelled(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "AMC", _AMC_EX, 10.0, 1.0)
    bronze_rows = _amc_bronze_raw()
    BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").replace_ticker_rows(
        "AMC",
        [
            {
                "trade_date": row["trade_date"].isoformat(),
                "symbol_id": 1,
                "open": row["close"],
                "high": row["close"],
                "low": row["close"],
                "close": row["close"],
                "adj_close": row["close"],
                "volume": 1_000,
                "source": "legacy",
                "price_basis": "unknown",
            }
            for row in bronze_rows
        ],
    )

    result = sync_corporate_actions.restore_yahoo_splits(
        tickers=["AMC"],
        apply=True,
        ib_verify=True,
        output_dir=tmp_path / "out",
        lake_root=tmp_path,
        now=datetime(2026, 9, 23, tzinfo=UTC),
        yahoo_factory=lambda: _FakeYahooSplits({"AMC": [YahooSplit(_AMC_EX, 1.0, 10.0)]}),
        ib_factory=lambda: _FakeIB(),
        # IB never adjusted -- same series as bronze, so the measured step is ~1,
        # not the claimed 0.1.
        ib_fetcher_factory=_window_fetcher(bronze_rows),
    )

    assert result["restored"] == 0
    assert result["counts"] == {"left_cancelled:ib_step_mismatch": 1}
    assert CorporateActionStore(tmp_path).latest_active("AMC") == []
    manifest = json.loads((tmp_path / "out" / "restore_yahoo_splits.json").read_text())
    assert manifest["candidates"][0]["outcome"] == "left_cancelled:ib_step_mismatch"


def test_restore_yahoo_splits_ib_unavailable_exits_86_but_restores_pre_floor(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "IBM", _IBM_EX, 1.0, 2.0)
    _seed_cancelled_yahoo_split(tmp_path, "AMC", _AMC_EX, 10.0, 1.0)

    result = sync_corporate_actions.restore_yahoo_splits(
        tickers=["AMC", "IBM"],
        apply=True,
        ib_verify=True,
        output_dir=tmp_path / "out",
        lake_root=tmp_path,
        now=datetime(2026, 9, 23, tzinfo=UTC),
        yahoo_factory=lambda: _FakeYahooSplits(
            {"IBM": [YahooSplit(_IBM_EX, 2.0, 1.0)], "AMC": [YahooSplit(_AMC_EX, 1.0, 10.0)]}
        ),
        ib_factory=lambda: _FakeIB(fails=True),
        ib_fetcher_factory=_window_fetcher([]),
    )

    assert result["exit_code"] == GATEWAY_DOWN_EXIT_CODE
    assert result["restored"] == 1  # IBM (pre-floor) still restored
    runs = ledger.query("select verdict from runs where job = 'restore-yahoo-splits' and ended is not null")
    assert [row["verdict"] for row in runs] == ["DEGRADED"]  # IB down is a skipped source, not a failure
    assert CorporateActionStore(tmp_path).latest_active("IBM")
    assert CorporateActionStore(tmp_path).latest_active("AMC") == []
    manifest = {
        row["symbol"]: row["outcome"]
        for row in json.loads((tmp_path / "out" / "restore_yahoo_splits.json").read_text())["candidates"]
    }
    assert manifest["AMC"] == "left_cancelled:ib_unavailable"
    assert manifest["IBM"] == "yahoo_only"


def test_restore_yahoo_splits_yahoo_not_listing_stays_cancelled(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "IBM", _IBM_EX, 1.0, 2.0)

    result = sync_corporate_actions.restore_yahoo_splits(
        tickers=["IBM"],
        apply=True,
        ib_verify=True,
        output_dir=tmp_path / "out",
        lake_root=tmp_path,
        now=datetime(2026, 9, 23, tzinfo=UTC),
        yahoo_factory=lambda: _FakeYahooSplits({"IBM": []}),  # Yahoo does not list this split at all
    )

    assert result["restored"] == 0
    assert CorporateActionStore(tmp_path).latest_active("IBM") == []
    manifest = json.loads((tmp_path / "out" / "restore_yahoo_splits.json").read_text())
    assert manifest["candidates"][0]["outcome"] == "left_cancelled:yahoo_does_not_list"


def test_restore_yahoo_splits_apply_without_ib_verify_is_refused(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "IBM", _IBM_EX, 1.0, 2.0)
    with pytest.raises(ValueError, match="ib-verify"):
        sync_corporate_actions.restore_yahoo_splits(
            tickers=["IBM"],
            apply=True,
            ib_verify=False,
            output_dir=None,
            lake_root=tmp_path,
            yahoo_factory=lambda: _FakeYahooSplits({"IBM": [YahooSplit(_IBM_EX, 2.0, 1.0)]}),
        )


def test_restore_yahoo_splits_cli_dispatches_through_livewire_ingest(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "IBM", _IBM_EX, 1.0, 2.0)
    monkeypatch.setattr(sync_corporate_actions, "data_lake_dir", lambda: tmp_path)
    monkeypatch.setattr(
        sync_corporate_actions,
        "YahooClient",
        lambda: _FakeYahooSplits({"IBM": [YahooSplit(_IBM_EX, 2.0, 1.0)]}),
    )

    exit_code = sync_corporate_actions.main(
        ["restore-yahoo-splits", "--tickers", "IBM", "--output-dir", str(tmp_path / "out")]
    )

    assert exit_code == 0
    summary = json.loads(capsys.readouterr().out.strip())
    assert summary["counts"] == {"yahoo_only": 1}


def test_restore_yahoo_splits_abandons_a_dangling_predecessor_run(tmp_path, monkeypatch, capsys):
    import socket

    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    stale_run_id = "restore-yahoo-splits-stale"
    ledger.open_run(
        {
            "run_id": stale_run_id,
            "job": "restore-yahoo-splits",
            "host": socket.gethostname(),
            "release_sha": None,
            "presets_sha": None,
            "registry_sha": None,
            "started": datetime(2026, 9, 1, tzinfo=UTC),
            "ended": None,
            "exit_code": None,
            "verdict": None,
        }
    )

    result = sync_corporate_actions.restore_yahoo_splits(
        tickers=["NOPE"],  # no cancelled yahoo split candidates -- no Yahoo/IB call at all
        apply=False,
        ib_verify=False,
        output_dir=None,
        lake_root=tmp_path,
        now=datetime(2026, 9, 23, tzinfo=UTC),
        yahoo_factory=lambda: _FakeYahooSplits({}),
    )

    assert result["candidates"] == 0
    assert "abandoned 1 open run(s) of restore-yahoo-splits" in capsys.readouterr().out
    closed = ledger.query(f"select verdict from runs where run_id = '{stale_run_id}' and ended is not null")
    assert closed[0]["verdict"] == "ABANDONED"


def test_restore_yahoo_splits_yahoo_fetch_error_leaves_it_cancelled(tmp_path, monkeypatch):
    class _RaisingYahoo:
        def get_split_events(self, symbol):
            raise RuntimeError("yahoo request failed")

    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "IBM", _IBM_EX, 1.0, 2.0)

    result = sync_corporate_actions.restore_yahoo_splits(
        tickers=["IBM"],
        apply=False,
        ib_verify=False,
        output_dir=None,
        lake_root=tmp_path,
        now=datetime(2026, 9, 23, tzinfo=UTC),
        yahoo_factory=lambda: _RaisingYahoo(),
    )

    assert result["counts"] == {"left_cancelled:yahoo_error": 1}


def test_restore_yahoo_splits_reuses_one_ib_connection_across_candidates(tmp_path, monkeypatch):
    # Real IBM 2:1 splits, both on/after the IB floor -- two IB-window candidates for
    # one symbol, so a second `get_ib_fetcher()` call must hit the lazy-connect cache.
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "IBM", date(1997, 5, 28), 1.0, 2.0)
    _seed_cancelled_yahoo_split(tmp_path, "IBM", date(1999, 5, 27), 1.0, 2.0)
    connects = []

    class _CountingIB(_FakeIB):
        def connect(self, **kwargs):
            connects.append(1)
            super().connect(**kwargs)

    result = sync_corporate_actions.restore_yahoo_splits(
        tickers=["IBM"],
        apply=True,
        ib_verify=True,
        output_dir=None,
        lake_root=tmp_path,
        now=datetime(2026, 9, 23, tzinfo=UTC),
        yahoo_factory=lambda: _FakeYahooSplits(
            {"IBM": [YahooSplit(date(1997, 5, 28), 2.0, 1.0), YahooSplit(date(1999, 5, 27), 2.0, 1.0)]}
        ),
        ib_factory=lambda: _CountingIB(),
        # No bronze rows seeded for IBM -- both candidates grade insufficient overlap,
        # which is irrelevant here; the point is the connection is made exactly once.
        ib_fetcher_factory=_window_fetcher(_amc_ib_rows()),
    )

    assert len(connects) == 1
    assert result["counts"] == {"left_cancelled:ib_insufficient_overlap": 2}


def test_restore_yahoo_splits_ib_unavailable_skips_every_remaining_ib_window_candidate(tmp_path, monkeypatch):
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "IBM", date(1997, 5, 28), 1.0, 2.0)
    _seed_cancelled_yahoo_split(tmp_path, "IBM", date(1999, 5, 27), 1.0, 2.0)

    result = sync_corporate_actions.restore_yahoo_splits(
        tickers=["IBM"],
        apply=True,
        ib_verify=True,
        output_dir=None,
        lake_root=tmp_path,
        now=datetime(2026, 9, 23, tzinfo=UTC),
        yahoo_factory=lambda: _FakeYahooSplits(
            {"IBM": [YahooSplit(date(1997, 5, 28), 2.0, 1.0), YahooSplit(date(1999, 5, 27), 2.0, 1.0)]}
        ),
        ib_factory=lambda: _FakeIB(fails=True),
        ib_fetcher_factory=_window_fetcher([]),
    )

    assert result["exit_code"] == GATEWAY_DOWN_EXIT_CODE
    assert result["counts"] == {"left_cancelled:ib_unavailable": 2}


def test_restore_yahoo_splits_ib_fetch_error_leaves_it_cancelled(tmp_path, monkeypatch):
    def _raising_fetcher_factory(client):
        def fetch(symbol, start, end):
            raise RuntimeError("IB pacing violation")

        return fetch

    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    _seed_cancelled_yahoo_split(tmp_path, "AMC", _AMC_EX, 10.0, 1.0)

    result = sync_corporate_actions.restore_yahoo_splits(
        tickers=["AMC"],
        apply=True,
        ib_verify=True,
        output_dir=None,
        lake_root=tmp_path,
        now=datetime(2026, 9, 23, tzinfo=UTC),
        yahoo_factory=lambda: _FakeYahooSplits({"AMC": [YahooSplit(_AMC_EX, 1.0, 10.0)]}),
        ib_factory=lambda: _FakeIB(),
        ib_fetcher_factory=_raising_fetcher_factory,
    )

    assert result["counts"] == {"left_cancelled:ib_error": 1}


def test_main_dispatches_generic_argv_to_run(monkeypatch):
    monkeypatch.setattr(sync_corporate_actions, "run", lambda argv: 0)
    assert sync_corporate_actions.main(["--dry-run"]) == 0
