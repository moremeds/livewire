#!/usr/bin/env python3
"""Validate complete adjusted equity history against Massive and fresh IB."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Callable, Sequence
from contextlib import nullcontext
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from clients.adjusted_history_validation import (
    build_split_only_rows,
    build_total_return_rows,
    compare_series,
    find_mechanical_split_jumps,
    merge_reference_rows,
    rolling_sma,
)
from clients.corporate_action_store import CorporateActionStore
from clients.ib_client import IBClient
from clients.massive_client import MassiveClient
from clients.symbol_paths import decode_symbol, encode_symbol
from livewire_scripts.adjusted_history_sources import (
    IBHistoryFetcher,
    SourceEvidence,
    fetch_ib_evidence,
    fetch_massive_action_evidence,
    fetch_massive_evidence,
    load_cached_evidence,
    write_cached_evidence,
)
from livewire_scripts.paths import data_lake_dir

NEW_YORK = ZoneInfo("America/New_York")
VALIDATOR_VERSION = 2


class _UnavailableMassive:
    def __init__(self, error: Exception):
        self._message = str(error)

    def _raise(self):
        raise RuntimeError(self._message)

    def get_daily_bars(self, *args, **kwargs):
        return self._raise()

    def get_sma(self, *args, **kwargs):
        return self._raise()

    def get_splits(self, *args, **kwargs):
        return self._raise()

    def get_dividends(self, *args, **kwargs):
        return self._raise()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--tickers", nargs="+")
    scope.add_argument("--all-equities", action="store_true")
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--silver-root", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    parser.add_argument("--host", default=os.environ.get("MDW_IB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MDW_IB_PORT", "4001")))
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--warning-bps", type=float, default=1.0)
    parser.add_argument("--failure-bps", type=float, default=5.0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--no-ib-fallback", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _input_hashes(paths: dict[str, Path]) -> dict[str, str | None]:
    return {name: _sha256(path) if path.is_file() else None for name, path in paths.items()}


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(_jsonable(key)): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(_jsonable(payload), handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _assert_output_safe(output: Path, bronze: Path, silver: Path) -> None:
    resolved = output.resolve()
    if resolved.is_relative_to(bronze.resolve()) or resolved.is_relative_to(silver.resolve()):
        raise ValueError("validation output must be outside canonical Bronze and Silver roots")


def _symbols(bronze_root: Path, explicit: list[str] | None) -> list[str]:
    if explicit:
        return list(dict.fromkeys(item.upper() for item in explicit))
    return sorted(
        decode_symbol(path.name.removeprefix("symbol="))
        for path in bronze_root.glob("symbol=*")
        if path.is_dir() and (path / "1d.parquet").is_file()
    )


def _evidence_grade(bronze_rows: list[dict[str, Any]], sources: dict[date, str]) -> str:
    reference_providers = {value.split("+", 1)[0] for value in sources.values()}
    bronze_providers = {str(row.get("source", "unknown")) for row in bronze_rows}
    if reference_providers == {"ib"}:
        return "same_provider_replay"
    if "ib" in reference_providers or "massive" in bronze_providers:
        return "hybrid" if len(reference_providers) > 1 else "same_provider_replay"
    return "cross_provider"


def _massive_sma_diagnostics(
    evidence: SourceEvidence,
    *,
    warning_bps: float,
    failure_bps: float,
) -> tuple[dict[int, dict[str, Any]], bool]:
    reports: dict[int, dict[str, Any]] = {}
    failed = False
    for window, provider_values in evidence.sma.items():
        calculated = rolling_sma(evidence.rows, window)
        errors: list[tuple[date, float]] = []
        for trade_date in sorted(set(calculated) & set(provider_values)):
            reference = provider_values[trade_date]
            error = abs(calculated[trade_date] - reference) / abs(reference) * 10_000 if reference else float("inf")
            errors.append((trade_date, error))
        maximum = max((item[1] for item in errors), default=0.0)
        failures = sum(item[1] > failure_bps for item in errors)
        warnings = sum(warning_bps < item[1] <= failure_bps for item in errors)
        failed = failed or failures > 0
        reports[window] = {
            "comparison_count": len(errors),
            "warning_count": warnings,
            "failure_count": failures,
            "max_error_bps": maximum,
            "worst_date": max(errors, key=lambda item: item[1])[0] if errors else None,
            "calculated_count": len(calculated),
            "provider_count": len(provider_values),
        }
    return reports, failed


def _run_identity(args: argparse.Namespace, root: Path, silver: Path) -> dict[str, Any]:
    return {
        "schema_version": VALIDATOR_VERSION,
        "data_lake_root": str(root),
        "silver_root": str(silver),
        "as_of_date": args.as_of_date.isoformat(),
        "warning_bps": args.warning_bps,
        "failure_bps": args.failure_bps,
        "ib_fallback": not args.no_ib_fallback,
    }


def _load_cursor(path: Path, identity: dict[str, Any], resume: bool) -> dict[str, Any]:
    if not resume or not path.is_file():
        return {"identity": identity, "completed": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"identity": identity, "completed": {}}
    return payload if payload.get("identity") == identity else {"identity": identity, "completed": {}}


def _serialize_comparison(comparison) -> dict[str, Any]:
    return _jsonable(asdict(comparison))


def _source_payload(evidence: SourceEvidence) -> dict[str, Any]:
    return _jsonable(asdict(evidence))


def _source_from_payload(payload: dict[str, Any]) -> SourceEvidence:
    return SourceEvidence(
        provider=str(payload["provider"]),
        symbol=str(payload["symbol"]),
        requested_start=date.fromisoformat(payload["requested_start"]),
        requested_end=date.fromisoformat(payload["requested_end"]),
        actual_start=(date.fromisoformat(payload["actual_start"]) if payload.get("actual_start") else None),
        actual_end=(date.fromisoformat(payload["actual_end"]) if payload.get("actual_end") else None),
        complete_range=bool(payload["complete_range"]),
        rows=tuple(
            {
                **row,
                "trade_date": date.fromisoformat(row["trade_date"]),
            }
            for row in payload.get("rows", [])
        ),
        sma={
            int(window): {date.fromisoformat(item): float(value) for item, value in values.items()}
            for window, values in payload.get("sma", {}).items()
        },
        status=payload["status"],
        error=payload.get("error"),
        sma_errors={int(window): str(error) for window, error in (payload.get("sma_errors") or {}).items()},
    )


def _source_summary(evidence: SourceEvidence) -> dict[str, Any]:
    return {
        "provider": evidence.provider,
        "status": evidence.status,
        "error": evidence.error,
        "requested_start": evidence.requested_start,
        "requested_end": evidence.requested_end,
        "actual_start": evidence.actual_start,
        "actual_end": evidence.actual_end,
        "complete_range": evidence.complete_range,
        "row_count": len(evidence.rows),
        "sma_counts": {window: len(values) for window, values in evidence.sma.items()},
        "sma_errors": evidence.sma_errors or {},
    }


def _cache_identity(
    provider: str,
    symbol: str,
    start: date,
    end: date,
    as_of_date: date,
    *,
    dependency_hash: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": VALIDATOR_VERSION,
        "provider": provider,
        "symbol": symbol,
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "as_of_date": as_of_date.isoformat(),
        "dependency_hash": dependency_hash,
    }


def _acquire_cached_source(
    path: Path,
    identity: dict[str, Any],
    *,
    resume: bool,
    acquire: Callable[[], SourceEvidence],
) -> SourceEvidence:
    if resume and path.is_file():
        try:
            return _source_from_payload(load_cached_evidence(path, identity))
        except (OSError, KeyError, TypeError, ValueError):
            pass
    evidence = acquire()
    write_cached_evidence(path, identity, _source_payload(evidence))
    return evidence


def run(
    argv: Sequence[str] | None = None,
    *,
    massive_factory: Callable[[], Any] = MassiveClient,
    ib_factory: Callable[[], Any] = IBClient,
    ib_fetcher_factory: Callable[[Any], Callable[[str, date, date], list[dict[str, Any]]]] = IBHistoryFetcher,
) -> int:
    args = parse_args(argv)
    if args.workers != 1:
        raise ValueError("adjusted-history validation currently requires --workers 1 for provider pacing")
    root = (args.data_lake_root or data_lake_dir()).expanduser().resolve()
    bronze_root = root / "bronze/asset_class=equity"
    silver_root = (args.silver_root or root / "silver").expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    _assert_output_safe(output, root / "bronze", silver_root)
    output.mkdir(parents=True, exist_ok=True)
    effective_as_of = args.as_of_date or datetime.now(NEW_YORK).date()
    args.as_of_date = effective_as_of
    symbols = _symbols(bronze_root, args.tickers)
    action_store = CorporateActionStore(root)
    identity = _run_identity(args, root, silver_root)
    cursor_path = output / "cursor.json"
    cursor = _load_cursor(cursor_path, identity, args.resume)
    results: list[dict[str, Any]] = []
    ib_client = None
    ib_fetcher = None
    ib_connected = False
    ib_connection_error: str | None = None

    try:
        massive_resource = massive_factory()
    except Exception as exc:
        massive_resource = _UnavailableMassive(exc)
    massive_context = massive_resource if hasattr(massive_resource, "__enter__") else nullcontext(massive_resource)
    try:
        with massive_context as massive:
            for symbol in symbols:
                encoded = encode_symbol(symbol)
                paths = {
                    "bronze": bronze_root / f"symbol={encoded}" / "1d.parquet",
                    "silver": silver_root / "asset_class=equity" / f"symbol={encoded}" / "1d.parquet",
                    "actions": action_store.path_for(symbol),
                    "revision": silver_root / "revisions/current.json",
                }
                before = _input_hashes(paths)
                detail_path = output / "symbols" / f"{encoded}.json"
                checkpoint = cursor["completed"].get(symbol)
                if (
                    args.resume
                    and checkpoint
                    and checkpoint.get("input_hashes") == before
                    and detail_path.is_file()
                    and checkpoint.get("detail_sha256") == _sha256(detail_path)
                ):
                    results.append(json.loads(detail_path.read_text(encoding="utf-8")))
                    continue

                if not paths["bronze"].is_file() or not paths["silver"].is_file():
                    detail = {
                        "symbol": symbol,
                        "outcome": "provider-error",
                        "errors": ["missing Bronze or Silver daily artifact"],
                        "input_hashes": before,
                    }
                else:
                    bronze_rows = pq.ParquetFile(paths["bronze"]).read().to_pylist()
                    silver_rows = pq.ParquetFile(paths["silver"]).read().to_pylist()
                    actions = action_store.latest_active(symbol)
                    ordered_dates = sorted(row["trade_date"] for row in bronze_rows)
                    start, end = ordered_dates[0], ordered_dates[-1]
                    massive_cache = output / "cache/massive" / f"{encoded}.json"
                    massive_identity = _cache_identity("massive", symbol, start, end, effective_as_of)
                    massive_evidence = _acquire_cached_source(
                        massive_cache,
                        massive_identity,
                        resume=args.resume,
                        acquire=lambda massive=massive, symbol=symbol, start=start, end=end: fetch_massive_evidence(
                            massive, symbol, start, end
                        ),
                    )
                    initial_coverage = merge_reference_rows(ordered_dates, massive_evidence.rows, ())
                    ib_evidence = None
                    if initial_coverage.unresolved and not args.no_ib_fallback:
                        if ib_client is None:
                            ib_client = ib_factory()
                            try:
                                ib_client.connect(host=args.host, port=args.port)
                            except Exception as exc:
                                ib_connection_error = str(exc)
                            else:
                                ib_connected = True
                                ib_fetcher = ib_fetcher_factory(ib_client)
                        if ib_fetcher is None:
                            ib_evidence = SourceEvidence(
                                "ib",
                                symbol,
                                start,
                                end,
                                None,
                                None,
                                False,
                                (),
                                {},
                                "error",
                                ib_connection_error or "IB fetcher unavailable",
                            )
                        else:
                            ib_cache = output / "cache/ib" / f"{encoded}.json"
                            ib_identity = _cache_identity(
                                "ib",
                                symbol,
                                start,
                                end,
                                effective_as_of,
                                dependency_hash=before["actions"],
                            )
                            ib_evidence = _acquire_cached_source(
                                ib_cache,
                                ib_identity,
                                resume=args.resume,
                                acquire=lambda ib_fetcher=ib_fetcher, symbol=symbol, start=start, end=end, actions=actions: (
                                    fetch_ib_evidence(
                                        ib_fetcher,
                                        symbol,
                                        start,
                                        end,
                                        actions,
                                        effective_as_of,
                                    )
                                ),
                            )
                    coverage = merge_reference_rows(
                        ordered_dates,
                        massive_evidence.rows,
                        () if ib_evidence is None else ib_evidence.rows,
                    )
                    split_only = build_split_only_rows(bronze_rows, actions, effective_as_of)
                    total_return = build_total_return_rows(bronze_rows, actions, effective_as_of)
                    split_comparison = compare_series(
                        split_only,
                        coverage.rows,
                        warning_bps=args.warning_bps,
                        failure_bps=args.failure_bps,
                    )
                    total_comparison = compare_series(
                        silver_rows,
                        total_return,
                        warning_bps=args.warning_bps,
                        failure_bps=args.failure_bps,
                        exact_columns=(
                            "adj_close",
                            "volume",
                            "price_adjustment_factor",
                            "split_volume_factor",
                        ),
                    )
                    jumps = find_mechanical_split_jumps(silver_rows, actions, effective_as_of)
                    sma_diagnostics, sma_failed = _massive_sma_diagnostics(
                        massive_evidence,
                        warning_bps=args.warning_bps,
                        failure_bps=args.failure_bps,
                    )
                    action_evidence = fetch_massive_action_evidence(
                        massive,
                        symbol,
                        actions,
                        effective_as_of,
                        bronze_rows=bronze_rows,
                    )
                    after = _input_hashes(paths)
                    errors: list[str] = []
                    if coverage.unresolved:
                        errors.append("unresolved reference dates")
                    if not split_comparison.passed:
                        errors.append("split-only comparison failed")
                    if not total_comparison.passed:
                        errors.append("total-return comparison failed")
                    if jumps:
                        errors.append("mechanical split jump remains")
                    if sma_failed:
                        errors.append("Massive SMA endpoint disagreement")
                    if action_evidence.status == "partial":
                        errors.append("corporate-action inventory mismatch")
                    provider_states = {massive_evidence.status}
                    if ib_evidence is not None:
                        provider_states.add(ib_evidence.status)
                    if before != after:
                        outcome = "input-changed"
                    elif coverage.unresolved:
                        outcome = "provider-error" if provider_states <= {"error", "timeout", "empty"} else "unresolved"
                    elif errors:
                        outcome = "fail"
                    else:
                        outcome = "pass"
                    detail = {
                        "symbol": symbol,
                        "outcome": outcome,
                        "errors": errors,
                        "input_hashes": before,
                        "post_validation_hashes": after,
                        "price_evidence": _evidence_grade(bronze_rows, coverage.sources),
                        "action_reference_status": action_evidence.status,
                        "transformation_check": "pass"
                        if split_comparison.passed and total_comparison.passed
                        else "fail",
                        "independent_action_check": (
                            "pass"
                            if action_evidence.status == "complete"
                            else "fail"
                            if action_evidence.status == "partial"
                            else "unavailable"
                        ),
                        "coverage": {
                            "sources": {item.isoformat(): value for item, value in coverage.sources.items()},
                            "unresolved_dates": coverage.unresolved,
                            "extra_massive_dates": coverage.extra_massive_dates,
                            "extra_ib_dates": coverage.extra_ib_dates,
                        },
                        "massive": _jsonable(_source_summary(massive_evidence)),
                        "ib": None if ib_evidence is None else _jsonable(_source_summary(ib_evidence)),
                        "corporate_actions": _jsonable(asdict(action_evidence)),
                        "split_only_comparison": _serialize_comparison(split_comparison),
                        "total_return_comparison": _serialize_comparison(total_comparison),
                        "massive_sma": _jsonable(sma_diagnostics),
                        "mechanical_jumps": _jsonable([asdict(item) for item in jumps]),
                    }
                _write_json_atomic(detail_path, detail)
                cursor["completed"][symbol] = {
                    "input_hashes": before,
                    "detail_sha256": _sha256(detail_path),
                    "outcome": detail["outcome"],
                }
                _write_json_atomic(cursor_path, cursor)
                results.append(_jsonable(detail))
    finally:
        if ib_client is not None and ib_connected:
            ib_client.disconnect()

    passed = bool(results) and all(item["outcome"] == "pass" for item in results)
    manifest = {
        "schema_version": VALIDATOR_VERSION,
        "identity": identity,
        "passed": passed,
        "symbol_count": len(results),
        "outcomes": {
            status: sum(item["outcome"] == status for item in results)
            for status in sorted({item["outcome"] for item in results})
        },
        "symbols": [{"symbol": item["symbol"], "outcome": item["outcome"]} for item in results],
    }
    _write_json_atomic(output / "manifest.json", manifest)
    summary = ["# Adjusted History Validation", "", f"Overall: {'PASS' if passed else 'FAIL'}", ""]
    summary.extend(f"- {item['symbol']}: {str(item['outcome']).upper()}" for item in results)
    (output / "summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")
    print(json.dumps(_jsonable(manifest), sort_keys=True))
    return 0 if passed else 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
