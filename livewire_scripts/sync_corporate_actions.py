#!/usr/bin/env python3
"""Reconcile Massive split and dividend events into canonical bronze Parquet."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Lock

import pyarrow.parquet as pq

from clients import constants, ledger
from clients.corporate_action_store import (
    CorporateAction,
    CorporateActionStore,
    DividendConversion,
    ProviderEvent,
)
from clients.ingestion_common import load_preset
from clients.massive_client import MassiveAuthError, MassiveClient, MassivePageEvidence
from clients.source_evidence import SourceEvidence, SourceEvidenceStore, canonical_bytes, digest_bytes
from clients.symbol_paths import canonical_symbol, decode_symbol
from clients.telemetry import MassiveTelemetry
from livewire_scripts.corporate_action_cursor import build_identity, default_cursor_path, open_cursor
from livewire_scripts.job_runner_common import emit_progress
from livewire_scripts.paths import data_lake_dir

# Share of attempted symbols that may fail before the run counts as systemic.
FAILURE_RATE_TOLERANCE = constants.declared("failure_rate_tolerance")

# Symbols between evidence-manifest commits. A commit is O(manifest), so per
# response it cost 41 min a night; once per run it is free but a lane SIGKILLed
# at its budget (three nights running, 2026-09-03/04/05) loses every manifest row
# for bytes already on disk. 500 pays ~1/500 of the per-response cost and caps
# the loss at 500 rows.
_EVIDENCE_FLUSH_EVERY = 500

# Values of MDW_SOURCE_EVIDENCE that turn response-evidence collection off.
_EVIDENCE_OFF = frozenset({"0", "off", "false", "no"})


def evidence_enabled() -> bool:
    """Whether response evidence is collected. Off is an operator escape hatch."""
    return os.environ.get("MDW_SOURCE_EVIDENCE", "on").strip().casefold() not in _EVIDENCE_OFF


class _EvidenceBuffer:
    """Persist exact response bytes now; commit the manifest once at the end.

    ``persist_raw`` is content-addressed and takes only a per-artifact lock, so
    it stays inline -- a provider response that is not written before the run
    dies cannot be refetched. ``record`` is the expensive half: it rewrites the
    whole manifest under one global lock, so calling it per response serializes
    every worker behind an O(manifest) write. The buffer defers it to a single
    ``flush``.
    """

    def __init__(self, root: Path) -> None:
        self._store = SourceEvidenceStore(root)
        self._lock = Lock()
        # Workers share one buffer; first observation of a ref wins, which is
        # also what the store does when the ref is already in the manifest.
        self._pending: dict[str, SourceEvidence] = {}

    def recorder(self):
        def record(capture):
            artifact = self._store.persist_raw(capture.body)
            with self._lock:
                self._pending.setdefault(
                    artifact.ref,
                    SourceEvidence(
                        ref=artifact.ref,
                        sha256=artifact.sha256,
                        # Exact bytes are globally content-addressed. Request/cursor
                        # identity remains on each normalized provider event so the
                        # same empty response body can safely support many symbols.
                        source_url=f"massive-response://sha256/{artifact.sha256}",
                        retrieved_at=capture.fetched_at,
                        publication_time=None,
                        mediawiki_revision_id=None,
                        mediawiki_revision_time=None,
                        content_type=capture.content_type,
                    ),
                )
            return artifact

        return record

    def flush(self) -> None:
        with self._lock:
            pending = list(self._pending.values())
            self._pending.clear()
        self._store.record_many(pending)


@dataclass(frozen=True)
class _FetchResult:
    ticker: str | None
    events: list[ProviderEvent] | None = None
    pages: list[MassivePageEvidence] | None = None
    error: Exception | None = None


@dataclass(frozen=True)
class _WorkerDone:
    pass


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--tickers", nargs="+", help="Explicit ticker list")
    scope.add_argument("--preset", type=Path, help="Preset JSON containing a tickers array")
    parser.add_argument(
        "--full-reconcile",
        action="store_true",
        help="Treat absent provider events as cancellations (requires complete symbol fetches)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compare provider state without publishing")
    parser.add_argument(
        "--workers", type=int, choices=range(1, 17), help="Concurrent provider fetch workers (default: 4)"
    )
    parser.add_argument("--resume", action="store_true", help="Resume a compatible incomplete reconciliation")
    parser.add_argument("--cursor", type=Path, help="Override the scope-specific reconciliation cursor")
    return parser.parse_args(list(argv) if argv is not None else None)


def _discover_symbols(root: Path) -> list[str]:
    equity_root = root / "bronze" / "asset_class=equity"
    return sorted(
        decode_symbol(path.name.removeprefix("symbol=")) for path in equity_root.glob("symbol=*") if path.is_dir()
    )


def _resolve_tickers(args: argparse.Namespace, root: Path) -> list[str]:
    if args.tickers:
        tickers = args.tickers
    elif args.preset:
        _, tickers, _ = load_preset(args.preset)
    else:
        tickers = _discover_symbols(root)
    normalized = list(dict.fromkeys(canonical_symbol(str(ticker)) for ticker in tickers))
    if not normalized:
        raise SystemExit("no tickers found for corporate-action reconciliation")
    return normalized


def _fetch_events(client: MassiveClient, ticker: str) -> tuple[list[ProviderEvent], list[MassivePageEvidence]]:
    if hasattr(client, "get_splits_evidenced") and hasattr(client, "get_dividends_evidenced"):
        splits, split_pages = client.get_splits_evidenced(ticker)
        dividends, dividend_pages = client.get_dividends_evidenced(ticker)
        return [*splits, *dividends], [*split_pages, *dividend_pages]
    return [*client.get_splits(ticker), *client.get_dividends(ticker)], []


def _fetch_sequential(client: MassiveClient, tickers: list[str]) -> Iterator[_FetchResult]:
    for ticker in tickers:
        try:
            events, pages = _fetch_events(client, ticker)
            yield _FetchResult(ticker=ticker, events=events, pages=pages)
        except Exception as exc:
            yield _FetchResult(ticker=ticker, error=exc)
            if isinstance(exc, MassiveAuthError):
                return


def _fetch_parallel(
    tickers: list[str],
    *,
    workers: int,
    client_factory: Callable[[], MassiveClient],
) -> Iterator[_FetchResult]:
    clients: list[MassiveClient] = []
    try:
        for _ in range(workers):
            clients.append(client_factory())
    except Exception as exc:
        for client in clients:
            try:
                client.close()
            except Exception:
                pass
        yield _FetchResult(ticker=None, error=exc)
        return

    symbols: Queue[str] = Queue()
    for ticker in tickers:
        symbols.put(ticker)
    results: Queue[_FetchResult | _WorkerDone] = Queue(maxsize=workers)
    stop = Event()

    def fetch_loop(client: MassiveClient) -> None:
        try:
            while not stop.is_set():
                try:
                    ticker = symbols.get_nowait()
                except Empty:
                    return
                try:
                    events, pages = _fetch_events(client, ticker)
                except Exception as exc:
                    if isinstance(exc, MassiveAuthError):
                        stop.set()
                    results.put(_FetchResult(ticker=ticker, error=exc))
                else:
                    results.put(_FetchResult(ticker=ticker, events=events, pages=pages))
        finally:
            try:
                client.close()
            except Exception as exc:
                results.put(_FetchResult(ticker=None, error=exc))
            finally:
                results.put(_WorkerDone())

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="corporate-actions") as executor:
        futures = [executor.submit(fetch_loop, client) for client in clients]
        done = 0
        try:
            while done < workers:
                result = results.get()
                if isinstance(result, _WorkerDone):
                    done += 1
                else:
                    yield result
        finally:
            stop.set()
            while done < workers:
                result = results.get()
                if isinstance(result, _WorkerDone):
                    done += 1
        for future in futures:
            future.result()


def _worker_count(args: argparse.Namespace, *, injected_client: bool) -> int:
    if injected_client and args.workers is None:
        return 1
    workers = 4 if args.workers is None else args.workers
    if injected_client and workers > 1:
        raise ValueError("a supplied client requires --workers 1")
    return workers


def run(
    argv: Sequence[str] | None = None,
    *,
    client: MassiveClient | None = None,
    client_factory: Callable[[], MassiveClient] | None = None,
    store: CorporateActionStore | None = None,
    data_lake_root: Path | None = None,
    telemetry: MassiveTelemetry | None = None,
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else data_lake_dir()
    tickers = _resolve_tickers(args, root)
    workers = _worker_count(args, injected_client=client is not None)
    if client is not None and client_factory is not None:
        raise ValueError("client and client_factory are mutually exclusive")
    action_store = store or CorporateActionStore(root)

    evidence = _EvidenceBuffer(root) if evidence_enabled() else None
    # One telemetry for every worker: _fetch_parallel builds one client per
    # worker and the totals only mean anything summed across the lane. It is a
    # parameter rather than a local because default_client_factory is bypassed
    # whenever a caller injects a client or a factory — which is every test.
    telemetry = telemetry or MassiveTelemetry(jsonl_path=None)

    def default_client_factory() -> MassiveClient:
        return MassiveClient(
            response_evidence_recorder=None if evidence is None else evidence.recorder(),
            telemetry=telemetry,
        )

    identity = build_identity(
        root,
        tickers,
        full_reconcile=args.full_reconcile,
        dry_run=args.dry_run,
    )
    cursor_path = args.cursor or default_cursor_path(root, identity)
    cursor = open_cursor(cursor_path, identity, resume=args.resume, now=datetime.now(UTC))
    ticker_set = set(tickers)
    if not cursor.completed <= ticker_set:
        raise ValueError(f"corporate-action cursor contains symbols outside this run: {cursor_path}")
    resumed = len(cursor.completed)
    counters = {"inserted": 0, "revised": 0, "cancelled": 0, "unchanged": 0, "failed": 0}
    attempted = 0
    marked = 0
    cycles = 0
    cycle_failed = 0
    run_id = _lane_run_id()

    def open_fetches(pending: list[str]) -> tuple[Iterator[_FetchResult], MassiveClient | None]:
        active_workers = min(workers, len(pending))
        if active_workers == 1:
            massive = client
            if massive is None:
                try:
                    owned = (client_factory or default_client_factory)()
                except Exception as exc:
                    return iter((_FetchResult(ticker=None, error=exc),)), None
                return _fetch_sequential(owned, pending), owned
            return _fetch_sequential(massive, pending), None
        return (
            _fetch_parallel(
                pending,
                workers=active_workers,
                client_factory=client_factory or default_client_factory,
            ),
            None,
        )

    # `--preset` restricts the scope exactly like `--tickers` does.
    scope = "all" if not args.tickers and not args.preset else "subset"
    dividend_fx: dict | None = None
    fx_passes = 0
    # At most two passes: finish whatever last night left, then -- if there is
    # budget left -- run this night's own pass. A SIGKILL at the lane budget
    # simply leaves the current pass resumable tomorrow.
    while True:
        # A pass that inherited work is the tail of an earlier night; only that
        # kind of pass earns a second cycle in the same invocation.
        continues_an_earlier_night = bool(cursor.completed)
        pending_tickers = [ticker for ticker in tickers if ticker not in cursor.completed]
        cycle_failed = 0
        if pending_tickers:
            cycles += 1
            fetches, owned_client = open_fetches(pending_tickers)
            try:
                for fetched in fetches:
                    if fetched.ticker is None:
                        counters["failed"] += 1
                        cycle_failed += 1
                        print(f"provider: {fetched.error}", file=sys.stderr)
                        continue
                    attempted += 1
                    ticker = fetched.ticker
                    if fetched.error is not None:
                        counters["failed"] += 1
                        cycle_failed += 1
                        print(f"{ticker}: {fetched.error}", file=sys.stderr)
                        continue
                    try:
                        fetched_at = datetime.now(UTC)
                        result = action_store.reconcile(
                            ticker,
                            fetched.events or [],
                            fetched_at,
                            full_reconcile=args.full_reconcile,
                            dry_run=args.dry_run,
                        )
                        if hasattr(action_store, "record_fetch"):
                            action_store.record_fetch(
                                ticker,
                                fetched.pages or [],
                                fetched_at,
                                full_reconcile=args.full_reconcile,
                                dry_run=args.dry_run,
                            )
                    except Exception as exc:
                        counters["failed"] += 1
                        cycle_failed += 1
                        print(f"{ticker}: {exc}", file=sys.stderr)
                        continue
                    for key in ("inserted", "revised", "cancelled", "unchanged"):
                        counters[key] += int(getattr(result, key))
                    cursor.mark_completed(ticker, now=datetime.now(UTC))
                    marked += 1
                    if attempted % _EVIDENCE_FLUSH_EVERY == 0:
                        if evidence is not None:
                            evidence.flush()
                        emit_progress(
                            scope="corporate-actions", completed=resumed + marked, total=len(tickers), run_id=run_id
                        )
            finally:
                if owned_client is not None:
                    owned_client.close()
                # Commit whatever was collected even when the run aborted: the bytes are
                # already on disk and a provider response is not refetchable later.
                if evidence is not None:
                    evidence.flush()

        complete = len(cursor.completed) == len(tickers) and counters["failed"] == 0
        if complete:
            cursor.mark_run_completed(now=datetime.now(UTC))
        # At the end of every cycle, not once after the loop. Deferred to the
        # end it never ran at all on a night the second cycle was SIGKILLed at
        # the lane budget; run once before the second cycle it filed a 0
        # `dividend_currency_mismatch` and then let that cycle's own dividends
        # go unconverted -- `status` OK on a night Silver fails on them. Each
        # cycle's conversion gets its own run id: two open/close pairs under
        # one run_id are one run to every `group by run_id` reader.
        if not args.dry_run:
            fx_passes += 1
            fx_run_id = f"{run_id}-dividend-fx" + ("" if fx_passes == 1 else f"-{fx_passes}")
            dividend_fx = _convert_dividends_after_sync(tickers, root, fx_run_id, scope=scope)
        if not (complete and continues_an_earlier_night):
            break
        cursor = open_cursor(cursor_path, identity, resume=False, now=datetime.now(UTC))

    summary = {
        "attempted": attempted,
        **counters,
        "completed": len(cursor.completed),
        "cursor": str(cursor_path),
        "cycles": cycles,
        # Symbols this invocation never reached, in the pass it ends on.
        "pending": len(tickers) - len(cursor.completed) - cycle_failed,
        "requested": len(tickers),
        "resumed": resumed,
    }
    # The conversion runs inside the loop above, at the end of each cycle that
    # just wrote dividends, rather than as a lane or an orchestrator step of
    # its own: it repairs exactly what that pass reconciled, needs no schedule
    # entry, and cannot page. The summary carries the last cycle's result --
    # the newest scope='all' measurement is the one `status` grades.
    # PR #128 shipped `convert-dividend-currency` wired to nothing;
    # the scheduled lane emitted no `dividend_fx_*` measurement at all and ~30
    # symbols failed Silver on "dividend currency does not match bronze
    # currency". It never changes the exit code — the sync's own failure *rate*
    # is the only thing that fails this lane.
    if dividend_fx is not None:
        summary["dividend_fx"] = dividend_fx
    print(json.dumps(summary, sort_keys=True))
    _emit_provider_measurements(telemetry, run_id)

    # Rate, not a binary. `run_daily_update_job.main()` gates the Silver rebuild on
    # this lane (`silver_inputs_ok = action_code == 0`), so `1 if failed` meant a
    # single flaky provider response blocked the adjusted rebuild for the whole
    # ~13K equity universe. 2026-08-02: `TGNA: Response ended prematurely` — one
    # symbol of 14,577, 0.007% — and Silver was skipped. That symbol simply keeps
    # the actions already in the store, which stays perfectly usable.
    #
    # `daily_outcomes.resolve_exit_code` is the same idea for the equity lane but
    # does not fit here: its absolute `max(50, …)` floor is calibrated for a 13K
    # universe and would pass a targeted 2-ticker run that failed one of them.
    # The rate alone keeps small runs strict and large ones proportionate.
    failed = int(counters["failed"])
    if failed:
        print(
            f"WARNING: {failed}/{attempted} symbols failed; the cursor was not marked "
            "complete, so the next run re-asks them.",
            file=sys.stderr,
        )
    if failed and (attempted == failed or failed > FAILURE_RATE_TOLERANCE * attempted):
        return 1
    return 0


_PROVIDER_MEASUREMENTS = (
    ("provider_requests", "requests", "count"),
    ("provider_throttled", "throttled", "count"),
    ("provider_errors", "errors", "count"),
    ("provider_wait_s", "wait_s", "s"),
    ("provider_latency_p95_ms", "latency_p95_ms", "ms"),
)


def _convert_dividends_after_sync(tickers: list[str], root: Path, fx_run_id: str, *, scope: str) -> dict:
    """Repair foreign-currency dividends; never fail the lane on this step."""
    try:
        result = convert_dividend_currency(
            tickers=tickers,
            apply=True,
            output_dir=root / "repairs" / "dividend_fx",
            lake_root=root,
            run_id=fx_run_id,
            scope=scope,
        )
    except Exception as exc:
        print(f"WARNING: dividend FX conversion failed: {exc}", file=sys.stderr)
        # One signal, not a gate per failure mode: the lane swallows this
        # exception, and it can be raised before the `runs` row is opened or
        # after it closed OK. `status` reads the newest whole-scope row of the
        # day, so this row stands in front of an earlier cycle's stale zero.
        try:
            ledger.emit(
                "measurements",
                [
                    {
                        "name": "dividend_fx_error",
                        "scope": scope,
                        "measured_at": datetime.now(UTC),
                        "value": 1.0,
                        "unit": "count",
                        "source": "measured",
                        "run_id": fx_run_id,
                    }
                ],
                run_id=fx_run_id,
            )
        except Exception as ledger_exc:  # pragma: no cover - the ledger is the last resort
            print(f"WARNING: could not record the dividend FX failure: {ledger_exc}", file=sys.stderr)
        return {"error": str(exc)}
    return {
        "converted": result["converted"],
        "remaining": result["remaining"],
        "run_id": result["run_id"],
        "skipped": len(result["skipped"]),
    }


def _lane_run_id() -> str:
    """The run this lane's ledger rows belong to; the orchestrator supplies it."""
    return os.environ.get("LW_RUN_ID") or ledger.new_run_id("corporate-actions")


def _emit_measurements(rows: list[dict], run_id: str, *, strict: bool = False) -> None:
    """Telemetry must not fail a good run -- unless the rows *are* the answer.

    `strict=True` for the conversion's own counts: a swallowed write leaves the
    run closing OK with no measurement, and `status` then grades an earlier
    cycle's zero. The provider totals are telemetry and keep swallowing.
    """
    try:
        ledger.emit("measurements", rows, run_id=run_id)
    except Exception as exc:
        if strict:
            raise
        print(f"WARNING: could not write measurements: {exc}", file=sys.stderr)


def _emit_provider_measurements(telemetry: MassiveTelemetry, run_id: str) -> None:
    """Publish what the provider cost this lane. Never aborts the run.

    2026-09-03: corporate-actions ran 2h15m of its 3h budget and nothing
    durable recorded whether it was throttled, timing out, or simply slow,
    because the client was built with telemetry=None.
    """
    totals = telemetry.summary()
    if not totals["requests"]:
        return
    now = datetime.now(UTC)
    _emit_measurements(
        [
            {
                "name": name,
                "scope": "corporate-actions",
                "measured_at": now,
                "value": float(totals[key]),
                "unit": unit,
                "source": "measured",
                "run_id": run_id,
            }
            for name, key, unit in _PROVIDER_MEASUREMENTS
        ],
        run_id,
    )


# --- convert-dividend-currency --------------------------------------------------

# FX pairs trade Sun–Fri (~6 sessions a week), so "within 5 sessions" is a
# 7-calendar-day window on the file's own date column.
_FX_STALE_DAYS = 7

# Currencies hard-pegged to the USD convert at the peg — there is no FX bar to
# look up because there is no market. Bermudian dollar, 1:1 since 1970.
USD_PEGGED = {"BMD": 1.0}


def _fx_bar_path(root: Path, pair: str) -> Path:
    return root / "bronze" / "asset_class=fx" / f"symbol={pair}" / "1d.parquet"


def _fx_pair_for(orig_ccy: str, equity_ccy: str, root: Path) -> tuple[str, str]:
    """``<eq><orig>`` exists → divide; ``<orig><eq>`` exists → multiply.

    CAD→USD resolves to USDCAD/divide; GBP→USD to GBPUSD/multiply. When neither
    file exists the ``<eq><orig>``/divide guess is returned only to name the
    pair in the skip record — ``fx_close_fn`` then reports ``no_fx_bar``.
    """
    if _fx_bar_path(root, f"{equity_ccy}{orig_ccy}").exists():
        return f"{equity_ccy}{orig_ccy}", "divide"
    if _fx_bar_path(root, f"{orig_ccy}{equity_ccy}").exists():
        return f"{orig_ccy}{equity_ccy}", "multiply"
    return f"{equity_ccy}{orig_ccy}", "divide"


def _fx_bar(root: Path, pair: str, on: date) -> dict | None:
    """The FX 1d bar for ``pair`` on ``on`` — the previous session's on a holiday."""
    path = _fx_bar_path(root, pair)
    if not path.exists():
        return None
    rows = pq.read_table(path, columns=["trade_date", "close"]).to_pylist()
    eligible = [row for row in rows if row["trade_date"] <= on]
    if not eligible:
        return None
    bar = max(eligible, key=lambda row: row["trade_date"])
    if (on - bar["trade_date"]).days > _FX_STALE_DAYS:
        return None
    return bar


def _equity_currency_resolver(root: Path, now: datetime) -> Callable[[str], tuple[str, str]]:
    """symbol -> (equity bronze currency, source), reading security_master once.

    Per symbol this re-read and re-deserialized the whole identity log. That was
    free for the operator sub-command's one `--tickers` symbol and is a lane
    cost now that the nightly corporate-actions pass calls it for every symbol
    of the universe (~15k full parquet reads on the exFAT lake, inside a killable
    LANE_BUDGET_S). One read, one dict.
    """
    verified: dict[str, tuple[str, datetime]] = {}
    if (root / "security_master" / "events.parquet").exists():
        try:
            from clients.security_master import SecurityMaster

            for event in SecurityMaster(root, evidence_verifier=None).events(as_of=now):
                if event.status != "verified":
                    continue
                known = verified.get(event.symbol)
                if known is None or event.known_at > known[1]:
                    verified[event.symbol] = (event.currency, event.known_at)
        except Exception:
            verified = {}

    def resolve(symbol: str) -> tuple[str, str]:
        known = verified.get(symbol)
        return ("USD", "default") if known is None else (known[0], "security_master")

    return resolve


def _convertible_symbols(root: Path) -> list[str]:
    action_root = root / "bronze" / "asset_class=corporate_action"
    return sorted(
        decode_symbol(path.name.removeprefix("symbol="))
        for path in action_root.glob("symbol=*")
        if (path / "events.parquet").exists()
    )


def convert_dividend_currency(
    *,
    tickers: list[str] | None,
    apply: bool,
    output_dir: Path | None,
    lake_root: Path,
    now: datetime | None = None,
    fx_close_fn: Callable[[str, date], tuple[date, float] | None] | None = None,
    run_id: str | None = None,
    scope: str = "all",
) -> dict:
    """Supersede foreign-currency dividends with `eod_fx` rows in the equity currency.

    Dry-run by default: detects and measures, writes no store rows and no CAS
    evidence. ``--apply`` writes the superseding rows and the manifest (the same
    JSON shape grok produced by hand). ``runs``/``measurements`` are emitted
    either way — the remaining count is a fact either way.
    """
    root = Path(lake_root)
    now = now or datetime.now(UTC)
    store = CorporateActionStore(root)
    symbols = [canonical_symbol(t) for t in tickers] if tickers else _convertible_symbols(root)
    if fx_close_fn is None:
        fx_close_fn = lambda pair, on: (  # noqa: E731
            (bar["trade_date"], float(bar["close"])) if (bar := _fx_bar(root, pair, on)) else None
        )
    evidence_store = SourceEvidenceStore(root) if apply else None
    currency_of = _equity_currency_resolver(root, now)

    # A caller inside a lane passes its own id: sharing the orchestrator's
    # LW_RUN_ID would file a *closed* `runs` row under the still-open
    # daily-update run, and "Daily update finished" would read finished.
    run_id = run_id or os.environ.get("LW_RUN_ID") or ledger.new_run_id("dividend-fx")
    run_row = {
        "run_id": run_id,
        "job": "dividend-fx",
        "host": socket.gethostname(),
        "release_sha": os.environ.get("LW_RELEASE_SHA"),
        "presets_sha": None,
        "registry_sha": None,
        "started": now,
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    ledger.emit("runs", [run_row], run_id=run_id)

    applied: list[dict] = []
    skipped: list[dict] = []
    detected = 0
    fx_evidence: list[tuple[str, str, dict]] = []  # (pair, sha256, row)
    try:
        for symbol in symbols:
            equity_ccy, equity_source = currency_of(symbol)
            conversions: list[DividendConversion] = []
            pending: list[tuple[CorporateAction, dict]] = []
            for row in store.foreign_currency_dividends(symbol, equity_ccy):
                detected += 1
                # An ex-date that has not closed yet: `_fx_bar` would price it
                # at a bar up to _FX_STALE_DAYS old, and after conversion the
                # row matches the equity currency and is never re-examined.
                # Strictly >=: the ex-date session's FX close is only final by
                # the next morning's run.
                if row.ex_date >= now.date():
                    skipped.append({"symbol": symbol, "ex_date": row.ex_date.isoformat(), "reason": "ex_date_pending"})
                    continue
                peg_rate: float | None = None
                if equity_ccy == "USD" and row.currency in USD_PEGGED:
                    peg_rate = USD_PEGGED[row.currency]
                elif row.currency == "USD" and equity_ccy in USD_PEGGED:
                    peg_rate = 1.0 / USD_PEGGED[equity_ccy]
                if peg_rate is not None:
                    pair, method = "USD_PEG", "peg"
                    fx_date, rate = row.ex_date, peg_rate
                else:
                    pair, method = _fx_pair_for(row.currency, equity_ccy, root)
                    quote = fx_close_fn(pair, row.ex_date)
                    if quote is None:
                        skipped.append({"symbol": symbol, "ex_date": row.ex_date.isoformat(), "reason": "no_fx_bar"})
                        continue
                    fx_date, rate = quote
                converted = row.cash_amount / rate if method == "divide" else row.cash_amount * rate
                source_ref = (
                    f"eod_fx:{pair}@{fx_date.isoformat()} rate={rate:.8f} method={method} "
                    f"orig={row.cash_amount} {row.currency} -> {converted:.8f} {equity_ccy}"
                )
                source_hash = None
                bar = None if method == "peg" else _fx_bar(root, pair, fx_date)
                if bar is not None:
                    source_hash = digest_bytes(canonical_bytes(bar, default=str))
                    if evidence_store is not None:
                        fx_evidence.append((pair, source_hash, bar))
                conversions.append(
                    DividendConversion(
                        action_id=row.action_id,
                        cash_amount=converted,
                        currency=equity_ccy,
                        source_ref=source_ref,
                        source_hash=source_hash,
                    )
                )
                pending.append((row, {"pair": pair, "method": method, "fx_date": fx_date, "rate": rate}))
            if not conversions:
                continue
            store.apply_repairs(
                symbol,
                add_splits=[],
                cancel_ex_dates=[],
                convert_dividends=conversions,
                fetched_at=now,
                dry_run=not apply,
            )
            if not apply:
                continue
            active_by_supersedes = {row.supersedes_action_id: row.action_id for row in store.latest_active(symbol)}
            for (old, aux), conversion in zip(pending, conversions, strict=True):
                if old.action_id not in active_by_supersedes:
                    continue  # idempotent no-op: eod_fx row already had this amount
                applied.append(
                    {
                        "symbol": symbol,
                        "ex_date": old.ex_date.isoformat(),
                        "orig_cash": old.cash_amount,
                        "orig_currency": old.currency,
                        "converted_cash": conversion.cash_amount,
                        "equity_currency": equity_ccy,
                        "equity_currency_source": equity_source,
                        "fx_pair": aux["pair"],
                        "fx_date": aux["fx_date"].isoformat(),
                        "fx_rate": aux["rate"],
                        "fx_method": aux["method"],
                        "source_ref": conversion.source_ref,
                        "old_action_id": old.action_id,
                        "new_action_id": active_by_supersedes[old.action_id],
                    }
                )

        # Commit each FX bar's exact bytes to the evidence CAS and mirror one
        # `evidence` row per HashedRef into the ledger — apply runs only.
        if evidence_store is not None and fx_evidence:
            seen: set[str] = set()
            for pair, sha, bar in fx_evidence:
                if sha in seen:
                    continue
                seen.add(sha)
                artifact = evidence_store.persist_raw(canonical_bytes(bar, default=str), expected_sha256=sha)
                evidence_store.record(
                    SourceEvidence(
                        ref=artifact.ref,
                        sha256=artifact.sha256,
                        source_url=f"bronze://fx/{pair}/1d.parquet#{bar['trade_date'].isoformat()}",
                        retrieved_at=now,
                        publication_time=None,
                        mediawiki_revision_id=None,
                        mediawiki_revision_time=None,
                        content_type="application/vnd.livewire.fx-bar+json",
                    )
                )
                ledger.emit(
                    "evidence",
                    [
                        {
                            "evidence_hash": artifact.sha256,
                            "kind": "fx_bar",
                            "subject": pair,
                            "payload_json": json.dumps(bar, default=str),
                            "source_url": f"bronze://fx/{pair}/1d.parquet#{bar['trade_date'].isoformat()}",
                            "fetched_at": now,
                            "proposer": "dividend-fx",
                            "run_id": run_id,
                        }
                    ],
                    run_id=run_id,
                )

        # A dividend whose ex-date has not closed yet is not a leftover: the
        # next run converts it. Counting it would WARN every night for every
        # announced dividend and send the operator to a remedy that re-skips
        # the same rows. `no_fx_bar` stays counted -- that one is real.
        pending_ex_dates = sum(1 for row in skipped if row["reason"] == "ex_date_pending")
        remaining = detected - len(applied) - pending_ex_dates
        _emit_measurements(
            [
                {
                    "name": name,
                    "scope": scope,
                    "measured_at": now,
                    "value": float(value),
                    "unit": "count",
                    "source": "measured",
                    "run_id": run_id,
                }
                for name, value in (
                    ("dividend_currency_mismatch", remaining),
                    ("dividend_fx_converted", len(applied)),
                    ("dividend_fx_skipped", len(skipped)),
                )
            ],
            run_id,
            strict=True,
        )
        ledger.emit(
            "runs",
            [run_row | {"ended": datetime.now(UTC), "exit_code": 0, "verdict": "OK"}],
            run_id=run_id,
        )
    except BaseException:
        # The whole body, not just the conversion loop: the lane wrapper
        # swallows this exception and files one `dividend_fx_error` row, which
        # is what `status` reads; the FAILED close here is the run's own record.
        ledger.emit(
            "runs",
            [run_row | {"ended": datetime.now(UTC), "exit_code": 1, "verdict": "FAILED"}],
            run_id=run_id,
        )
        raise

    manifest_path = None
    if apply and output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = output_dir / "dividend_fx_conversion_applied.json"
        manifest_path.write_text(json.dumps({"applied": applied, "skipped": skipped}, indent=1, sort_keys=True) + "\n")
    return {
        "tickers": len(symbols),
        "detected": detected,
        "converted": len(applied),
        "skipped": skipped,
        "remaining": remaining,
        "applied": applied,
        "manifest": None if manifest_path is None else str(manifest_path),
        "run_id": run_id,
    }


def _convert_dividend_currency_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="livewire_ingest.py corporate-actions convert-dividend-currency",
        description="Supersede foreign-currency dividends with eod_fx-converted rows",
    )
    parser.add_argument("--tickers", nargs="+", help="Symbols to scan (default: every CA-store symbol)")
    parser.add_argument("--apply", action="store_true", help="Write the superseding rows + manifest")
    parser.add_argument("--output-dir", type=Path, help="Where the apply manifest is written")
    return parser


def convert_dividend_currency_main(argv: Sequence[str]) -> int:
    args = _convert_dividend_currency_parser().parse_args(list(argv))
    if args.apply and args.output_dir is None:
        _convert_dividend_currency_parser().error("--apply requires --output-dir")
    summary = convert_dividend_currency(
        tickers=args.tickers,
        apply=args.apply,
        output_dir=args.output_dir,
        lake_root=data_lake_dir(),
        # A targeted repair measures a handful of symbols. Filing that under
        # scope 'all' let it overwrite the nightly whole-scope fact for the
        # rest of the day, and `status` grades today's newest row.
        scope="all" if args.tickers is None else "subset",
    )
    print(json.dumps(summary, sort_keys=True, default=str))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(argv) if argv is not None else sys.argv[1:]
    if argv[:1] == ["convert-dividend-currency"]:
        return convert_dividend_currency_main(argv[1:])
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
