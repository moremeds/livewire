#!/usr/bin/env python3
"""Reconcile Massive split and dividend events into canonical bronze Parquet."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Lock

from clients.corporate_action_store import CorporateActionStore, ProviderEvent
from clients.ingestion_common import load_preset
from clients.massive_client import MassiveAuthError, MassiveClient, MassivePageEvidence
from clients.source_evidence import SourceEvidence, SourceEvidenceStore
from clients.symbol_paths import canonical_symbol, decode_symbol
from livewire_scripts.corporate_action_cursor import build_identity, default_cursor_path, open_cursor
from livewire_scripts.paths import data_lake_dir

# Share of attempted symbols that may fail before the run counts as systemic.
FAILURE_RATE_TOLERANCE = 0.05

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
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else data_lake_dir()
    tickers = _resolve_tickers(args, root)
    workers = _worker_count(args, injected_client=client is not None)
    if client is not None and client_factory is not None:
        raise ValueError("client and client_factory are mutually exclusive")
    action_store = store or CorporateActionStore(root)

    evidence = _EvidenceBuffer(root) if evidence_enabled() else None

    def default_client_factory() -> MassiveClient:
        return MassiveClient(response_evidence_recorder=None if evidence is None else evidence.recorder())

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
    pending_tickers = [ticker for ticker in tickers if ticker not in cursor.completed]
    resumed = len(cursor.completed)
    counters = {"inserted": 0, "revised": 0, "cancelled": 0, "unchanged": 0, "failed": 0}
    attempted = 0

    owned_client: MassiveClient | None = None
    active_workers = min(workers, len(pending_tickers))
    if active_workers == 0:
        fetches = iter(())
    elif active_workers == 1:
        massive = client
        if massive is None:
            try:
                owned_client = (client_factory or default_client_factory)()
            except Exception as exc:
                fetches = iter((_FetchResult(ticker=None, error=exc),))
            else:
                massive = owned_client
                fetches = _fetch_sequential(massive, pending_tickers)
        else:
            fetches = _fetch_sequential(massive, pending_tickers)
    else:
        fetches = _fetch_parallel(
            pending_tickers,
            workers=active_workers,
            client_factory=client_factory or default_client_factory,
        )

    try:
        for fetched in fetches:
            if fetched.ticker is None:
                counters["failed"] += 1
                print(f"provider: {fetched.error}", file=sys.stderr)
                continue
            attempted += 1
            ticker = fetched.ticker
            if fetched.error is not None:
                counters["failed"] += 1
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
                print(f"{ticker}: {exc}", file=sys.stderr)
                continue
            for key in ("inserted", "revised", "cancelled", "unchanged"):
                counters[key] += int(getattr(result, key))
            cursor.mark_completed(ticker, now=datetime.now(UTC))
    finally:
        if owned_client is not None:
            owned_client.close()
        # Commit whatever was collected even when the run aborted: the bytes are
        # already on disk and a provider response is not refetchable later.
        if evidence is not None:
            evidence.flush()

    if len(cursor.completed) == len(tickers) and counters["failed"] == 0:
        cursor.mark_run_completed(now=datetime.now(UTC))
    summary = {
        "attempted": attempted,
        **counters,
        "completed": len(cursor.completed),
        "cursor": str(cursor_path),
        "pending": len(tickers) - resumed - attempted,
        "requested": len(tickers),
        "resumed": resumed,
    }
    print(json.dumps(summary, sort_keys=True))

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


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
