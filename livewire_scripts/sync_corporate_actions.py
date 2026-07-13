#!/usr/bin/env python3
"""Reconcile Massive split and dividend events into canonical bronze Parquet."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from queue import Empty, Queue
from threading import Event

from clients.corporate_action_store import CorporateActionStore, ProviderEvent
from clients.ingestion_common import load_preset
from clients.massive_client import MassiveAuthError, MassiveClient
from clients.symbol_paths import decode_symbol
from livewire_scripts.corporate_action_cursor import build_identity, default_cursor_path, open_cursor
from livewire_scripts.paths import data_lake_dir


@dataclass(frozen=True)
class _FetchResult:
    ticker: str | None
    events: list[ProviderEvent] | None = None
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
    normalized = list(dict.fromkeys(str(ticker).upper() for ticker in tickers))
    if not normalized:
        raise SystemExit("no tickers found for corporate-action reconciliation")
    return normalized


def _fetch_events(client: MassiveClient, ticker: str) -> list[ProviderEvent]:
    return [*client.get_splits(ticker), *client.get_dividends(ticker)]


def _fetch_sequential(client: MassiveClient, tickers: list[str]) -> Iterator[_FetchResult]:
    for ticker in tickers:
        try:
            yield _FetchResult(ticker=ticker, events=_fetch_events(client, ticker))
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
                    events = _fetch_events(client, ticker)
                except Exception as exc:
                    if isinstance(exc, MassiveAuthError):
                        stop.set()
                    results.put(_FetchResult(ticker=ticker, error=exc))
                else:
                    results.put(_FetchResult(ticker=ticker, events=events))
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
                owned_client = (client_factory or MassiveClient)()
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
            client_factory=client_factory or MassiveClient,
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
                result = action_store.reconcile(
                    ticker,
                    fetched.events or [],
                    datetime.now(UTC),
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
    return 1 if counters["failed"] else 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
