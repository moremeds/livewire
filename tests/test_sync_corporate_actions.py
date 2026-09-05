from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
import responses

from clients import ledger
from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveAuthError, MassiveResponseCapture
from clients.source_evidence import SourceEvidenceStore
from clients.telemetry import MassiveTelemetry
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
    assert ledger.query("select count(*) as n from measurements")[0]["n"] == 0


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
