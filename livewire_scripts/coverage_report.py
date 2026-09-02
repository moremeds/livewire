"""Daily coverage report + auto-recovery for Livewire.

For each tracked timeframe (1d, 1m, 1h, 5m), counts how many symbols have
bars current as-of the target trading day. If coverage drops below the
threshold (default 95%), triggers a targeted backfill via fetch_ib_historical
and re-checks. Sends an email alert when post-recovery coverage is still
incomplete; logs INFO only when recovery is fully successful.

Spec: docs/superpowers/specs/2026-04-06-multi-timeframe-design.md § 17 Layer 2.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from functools import partial
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(PROJECT_ROOT))

import pyarrow.parquet as pq
from rich.console import Console

from clients.corporate_action_store import CorporateActionStore
from clients.coverage_denominator import DUE_LAG_DAYS, build_denominator, session_due_at
from clients.gap_engine import (
    Finding,
    actual_sessions,
    classify,
    load_unresolved,
    massive_floor_for,
    suppress_unresolved,
)
from clients.gap_registry import RegistryError, load_registry
from clients.intraday_bronze_client import INTRADAY_PARQUET_FILENAME
from clients.symbol_paths import decode_symbol
from clients.terminus import (
    RAW_MINUTE_AGGS,
    TerminusVerdict,
    confirmed_terminus,
    raw_tape_covers,
    traded_by_session,
)
from clients.trading_calendar import trading_dates_in_range
from livewire_scripts.daily_update import _et_today, is_trading_day, previous_trading_day
from livewire_scripts.paths import data_lake_dir, log_dir

log = logging.getLogger(__name__)
console = Console()

_DATA_LAKE: Path | None = None
_LOG_DIR: Path | None = None
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent
_INGEST_SCRIPT = _REPO_ROOT / "scripts" / "livewire_ingest.py"
_OPS_SCRIPT = _REPO_ROOT / "scripts" / "livewire_ops.py"

TIMEFRAMES: tuple[str, ...] = ("1d", "1m", "1h", "5m", "30m")
DEFAULT_THRESHOLD = float(os.getenv("MDW_COVERAGE_ALERT_THRESHOLD", "0.95"))
DEFAULT_SAFETY_CAP = 100

# Threads for the per-file footer pass. Measured 2026-08-02 over the 13,270
# equity `1d` files, filesystem cache warm: 1 -> 154.0s, 8 -> 34.9s,
# 16 -> 29.2s, 32 -> 25.2s. 16 takes 5.3x off the wall clock; past that the
# curve flattens and only the file-descriptor pressure keeps growing.
#
# This is why coverage had to be parallel at all: five timeframes at ~150-300s
# each does not fit the 600s budget `_spawn_post_success_quality` gives it, and
# hasn't since the universe reached ~20K symbols. It timed out every night from
# 2026-07-07, so coverage logs stop at 2026-06-17 and every weekly report since
# has been an empty "No coverage logs found" stub.
FOOTER_READ_WORKERS = 16

# The window the 2026-09-01 measurement used. MIN_TERMINUS_SESSIONS decides
# inside it; this only bounds how far back a terminus can be dated.
TERMINUS_WINDOW_SESSIONS = 20


def _resolved_data_lake() -> Path:
    return _DATA_LAKE or data_lake_dir()


def _resolved_log_dir() -> Path:
    return _LOG_DIR or log_dir()


@dataclass
class CoverageResult:
    timeframe: str
    total: int
    present: int
    missing_symbols: list[str] = field(default_factory=list)
    # (symbol, terminus date) pairs. In NEITHER `present` nor `missing_symbols`,
    # and NOT in `total`: no source can supply bars for an instrument that
    # stopped printing, so counting these missing queues an impossible repair and
    # counting them present is what hid BK, EA, AVB and EQR.
    terminus_symbols: tuple[tuple[str, date], ...] = ()
    # A qualifying absence run the gates could not clear. Counted in `total` and
    # in `missing_symbols` -- NOT exempted as no-trade, which is what hid it.
    unconfirmed_terminus_symbols: tuple[str, ...] = ()
    # The session this result actually measured. Differs from the run's target
    # for a lane whose ingestion lag has not elapsed yet (rates is T+2).
    measured_session: date | None = None

    @property
    def ratio(self) -> float:
        return 1.0 if self.total == 0 else self.present / self.total


@dataclass
class RecoveryOutcome:
    timeframe: str
    attempted: list[str]
    recovered: int
    still_missing: list[str]
    aborted: bool = False
    reason: str = ""


def _filename_for(tf: str) -> str:
    return "1d.parquet" if tf == "1d" else INTRADAY_PARQUET_FILENAME[tf]


def _symbol_from_parquet_path(path: Path) -> str:
    return decode_symbol(path.parent.name.split("=", 1)[1])


def _raw_symbols_for_date(target_date: date, bronze_root: Path) -> set[str]:
    path = (
        bronze_root.parent
        / "raw"
        / "massive"
        / "us_stocks_sip"
        / "minute_aggs_v1"
        / f"date={target_date.isoformat()}"
        / "_symbols.parquet"
    )
    if not path.exists():
        return set()
    return set(pq.read_table(path, columns=["ticker"]).column("ticker").to_pylist())


def _latest_date_in_parquet(path: Path, column_name: str) -> date | None:
    """Return the latest date in *column_name*, or None if the file cannot be read.

    ⚠️ **One unreadable parquet must never kill the scan.** Coverage is a
    read-only detector over ~70K files; a single truncated footer used to raise
    out of `pool.map` and abort the whole run. It did, nine times: IGA's
    `5m.parquet` and VSLU's `1m.parquet` (`Parquet magic bytes not found in
    footer`) took the 11:00 UTC job down repeatedly and coverage logs stop for
    days at a time around each one. The lake lives on an external exFAT volume
    that bronze publishes to by `os.replace()`, so a torn file is a normal
    operating condition, not an exceptional one.

    Unreadable resolves to None, which is the **conservative** direction and
    needs no new plumbing: `present_symbols` requires `latest >= target_date`,
    so the symbol reads MISSING, enters the ratio, the `missing:` log line and
    the alert. This is the same argument the stale-cache-entry case already
    rests on — it over-reports gaps, it cannot hide one. It is emphatically not
    a swallowed warning: nothing is downgraded to silence, and the ERROR below
    names the exact path so the operator can quarantine or refetch it.

    The detector never repairs. Moving the file aside is
    `flatfile_publisher.quarantine_corrupt_parquet`'s job, on the write path
    that owns the data — a read-only scan that mutates bronze is a worse bug
    than the one it fixes.
    """
    try:
        return _read_latest_date_in_parquet(path, column_name)
    except Exception as exc:  # noqa: BLE001 - any single-file read failure, see above
        log.error("unreadable parquet, counted as missing: %s: %s", path, exc)
        return None


def _read_latest_date_in_parquet(path: Path, column_name: str) -> date | None:
    """Return the latest value in *column_name* using parquet footer statistics.

    Reads only the file's footer (row-group min/max stats) instead of the full
    column — the whole-column read is what made coverage take hours over ~13K
    symbols. Falls back to a full column read only when stats are unavailable.
    """
    md = pq.ParquetFile(path).metadata
    col_idx = next(
        (i for i in range(md.num_columns) if md.schema.column(i).path == column_name),
        None,
    )
    if col_idx is not None:
        max_value = None
        for rg_idx in range(md.num_row_groups):
            stats = md.row_group(rg_idx).column(col_idx).statistics
            if stats is None or not stats.has_min_max:
                max_value = None
                break
            candidate = stats.max
            if max_value is None or candidate > max_value:
                max_value = candidate
        if max_value is not None:
            if isinstance(max_value, datetime):
                return max_value.date()
            if isinstance(max_value, date):
                return max_value
            if isinstance(max_value, (str, bytes)):
                raw = max_value.decode() if isinstance(max_value, bytes) else max_value
                return date.fromisoformat(raw[:10])
    # Fallback: stats unavailable -> full column read (rare).
    table = pq.read_table(path, columns=[column_name])
    values = table.column(column_name).to_pylist()
    if not values:
        return None
    dates = [value.date() if isinstance(value, datetime) else value for value in values]
    return max(dates)


def _load_footer_cache(cache_path: Path | None) -> dict:
    """Return the persisted footer cache, or an empty one.

    A corrupt or unreadable cache is not an error: the worst case is one slow
    run, and failing the freshness detector because its optimisation file is
    malformed would be trading a real signal for a cosmetic one.
    """
    if cache_path is None:
        return {}
    try:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _save_footer_cache(cache_path: Path | None, cache: dict) -> None:
    if cache_path is None:
        return
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        # PID in the temp name: the launchd job and an operator running coverage
        # by hand would otherwise write the same tmp path, and the loser's
        # os.replace fails ENOENT after the winner moved it away — a spurious
        # warning for a benign race.
        tmp = cache_path.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_text(json.dumps(cache, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp, cache_path)
    except OSError as exc:  # pragma: no cover - logged but tolerated
        log.warning("could not persist the footer cache: %s", exc)


def _latest_date_with_cache(
    path: Path, column_name: str, cache: dict
) -> tuple[date | None, bool, tuple[float, int] | None]:
    """Return (latest_date, cache_hit, (mtime, size)). Reads `cache`; never writes it.

    A parquet whose mtime and size have not moved since the last run cannot
    have gained a later max date, so opening its footer is pure cost. On the
    external exFAT volume that cost is the entire runtime — 2858s cold on
    2026-08-09 against an 1800s budget, versus 29.2s warm for the same 1d pass.

    `size` is in the key on purpose: bronze publishes by `os.replace()` and
    exFAT stores mtime at 2-second granularity, so a republish landing inside
    that bucket leaves the timestamp unchanged. Any real republish that adds or
    removes a row also changes the size, and both come from the one `stat()`
    this makes anyway.

    `(mtime, size)` is not content identity — a republish inside the 2-second
    bucket that happens to compress to the identical byte length would serve a
    stale entry. That is accepted rather than hashed: hashing 13,270+ files
    costs far more than the footer reads this exists to avoid, and the failure
    is loud rather than silent. A stale entry can only hold an EARLIER date,
    and `present_symbols` requires `latest >= target_date`, so the symbol reads
    as MISSING and triggers recovery. It over-reports gaps; it cannot hide one.

    Read-only on purpose. This runs on 16 threads, and having each worker write
    into a shared dict would rest the whole cache's correctness on an argument
    about `dict.__setitem__` atomicity under the GIL — an argument that stops
    holding the day anyone runs this on a free-threaded build. `pool.map`
    preserves input order, so the caller reassembles the new cache
    single-threaded from the returned tuples and the question never arises.
    """
    key = str(path)
    try:
        stat = path.stat()
    except OSError:
        return None, False, None
    stamp = (stat.st_mtime, stat.st_size)
    entry = cache.get(key)
    if isinstance(entry, dict) and (entry.get("mtime"), entry.get("size")) == stamp:
        stored = entry.get("latest")
        try:
            return (date.fromisoformat(stored) if stored else None), True, stamp
        except (TypeError, ValueError):
            # A malformed entry is a cache miss, never an exception. This runs
            # inside pool.map, so raising here kills the whole coverage run —
            # and `_load_footer_cache` only guards against malformed JSON, not
            # malformed contents. The detector must not die of its own
            # optimisation file; the cost of a bad entry is one footer read.
            log.warning("ignoring malformed footer-cache entry for %s", key)
    return _latest_date_in_parquet(path, column_name), False, stamp


def _equity_preset_paths(registry_path: Path | None, presets_dir: Path) -> list[Path]:
    rows = load_registry(registry_path or Path("registry/gaps.json"))
    names: list[str] = []
    for row in rows:
        if row.asset_class == "equity" and row.timeframe == "1d":
            names.extend(row.universe)
    return [presets_dir / f"{name}.json" for name in dict.fromkeys(names)]


def compute_coverage(
    target_date: date,
    bronze_root: Path | None = None,
    cache_path: Path | None = None,
    registry_path: Path | None = None,
    presets_dir: Path | None = None,
    as_of: datetime | None = None,
) -> dict[str, CoverageResult]:
    """Return per-timeframe coverage as-of *target_date*.

    Denominator is the **active bronze universe for that timeframe** — the
    symbols we actually carry — not the full provider SIP set. A symbol counts
    as present if it is current through *target_date* OR it is absent from the
    day's raw traded set (it simply did not trade; no-trade is not missing).
    """
    bronze_root = bronze_root or _resolved_data_lake() / "bronze"
    # The real clock. NOT session_due_at(target_date): that makes the due filter
    # `session_due_at(d) <= as_of` tautologically true for a single-session
    # window, so the entire deadline rule would be inert on the one code path
    # production runs.
    as_of = as_of or datetime.now(UTC)
    presets_dir = presets_dir or Path("presets")
    results: dict[str, CoverageResult] = {}
    cache = _load_footer_cache(cache_path)
    fresh: dict = {}

    # The provider's exact target-day traded set, used only to exclude
    # instruments that did not trade from the "missing" count.
    traded_today = _raw_symbols_for_date(target_date, bronze_root)

    if not traded_today:
        # Be loud about it: with no raw partition the no-trade exemption
        # silently switches off and the same code reports a different, stricter
        # denominator with nothing in the log saying which one ran.
        log.warning(
            "No raw minute partition for %s — coverage runs without the no-trade "
            "exemption and intraday denominators fall back to files on disk.",
            target_date,
        )

    # One window, read once, shared by every timeframe. 40 calendar days is
    # comfortably more than 20 sessions including holidays; the slice, not the
    # span, defines the window. previous_trading_day(d) takes a single argument
    # and cannot step back N sessions, which is why this is a slice.
    window = trading_dates_in_range(target_date - timedelta(days=40), target_date)[-TERMINUS_WINDOW_SESSIONS:]
    raw_root = bronze_root.parent / RAW_MINUTE_AGGS
    tape = traded_by_session(raw_root, window)
    # The same three gates the classifier applies, via the same function. Reading
    # the tape and asking `terminus_of` alone -- which this did -- removes a symbol
    # from the DENOMINATOR on evidence the module itself calls insufficient, so a
    # store too stale to answer raised the coverage ratio instead of lowering it.
    tape_ok = raw_tape_covers(raw_root, target_date)
    action_store = CorporateActionStore(bronze_root.parent)

    for tf in TIMEFRAMES:
        parquet_paths = sorted((bronze_root / "asset_class=equity").glob(f"symbol=*/{_filename_for(tf)}"))
        on_disk = {_symbol_from_parquet_path(path) for path in parquet_paths}

        # For intraday, the provider's traded set is the honest denominator.
        # Globbing files on disk made the denominator self-defining: a symbol
        # with no 5m.parquet was not in the 5m universe, so a symbol that
        # silently stopped receiving intraday — or never got a file at all —
        # could never be counted missing, and coverage read 100% forever.
        registry_universe: set[str] = set()
        if tf == "1d":
            # The ingestion deadline, asked directly rather than inferred from an
            # empty denominator: "the registry resolves to no symbols" and "the
            # session is not due yet" are different facts, and only the second one
            # means zero of zero. Not due -> zero of zero, NOT one phantom tail gap
            # per symbol, which is what 497 of the first run's 501 findings were.
            if session_due_at(target_date) > as_of:
                results[tf] = CoverageResult(tf, 0, 0, [], ())
                # _save_footer_cache REPLACES the file with `fresh`, so returning
                # here without carrying the existing 1d entries forward deletes
                # ~13,270 of them -- and the next run pays a cold 1d footer walk,
                # the cost that made coverage a no-timeout job of its own. This
                # branch exists for exactly the pre-deadline run that would then
                # penalise the real one.
                fresh.update({key: cache[key] for path in parquet_paths if (key := str(path)) in cache})
                continue
            expected = build_denominator(
                _equity_preset_paths(registry_path, presets_dir),
                "equity",
                "1d",
                target_date,
                target_date,
                as_of=as_of,
            )
            # The registry, not the disk. A symbol that never landed has no file
            # to glob, which is why BK -- an sp500 member -- read as 100% healthy.
            # ponytail: a UNION, so ~96% of this denominator is still disk-derived.
            # Widening the registry row is a separate, measured change.
            registry_universe = {series.symbol for series in expected}
            universe = on_disk | registry_universe
        else:
            universe = on_disk if not traded_today else set(traded_today)

        # Scoped to the registry universe deliberately: the threshold is
        # calibrated on 515 liquid names and the on-disk tail legitimately does
        # not print for days.
        terminus: dict[str, date] = {}
        unconfirmed: set[str] = set()
        if tf == "1d":
            verdicts = {symbol: confirmed_terminus(tape, symbol, action_store, tape_ok) for symbol in registry_universe}
            terminus = {symbol: v.when for symbol, v in verdicts.items() if v.when is not None}
            unconfirmed = {symbol for symbol, v in verdicts.items() if v.withheld}

        column_name = "trade_date" if tf == "1d" else "bar_timestamp"
        # Threaded: the pass is one small footer read per file, so it is bound by
        # I/O rather than the GIL — pyarrow releases it for the read and the parse.
        started = time.monotonic()
        worker = partial(_latest_date_with_cache, column_name=column_name, cache=cache)
        with ThreadPoolExecutor(max_workers=FOOTER_READ_WORKERS) as pool:
            rows = list(pool.map(worker, parquet_paths))
        hits = sum(1 for _, cached, _ in rows if cached)
        # Rebuilt, not mutated: `fresh` ends up holding exactly the files that
        # exist right now, so a symbol archived to bronze-delisted/ drops out
        # instead of accumulating in the cache forever.
        for path, (latest, _, stamp) in zip(parquet_paths, rows, strict=True):
            if stamp is not None:
                fresh[str(path)] = {
                    "mtime": stamp[0],
                    "size": stamp[1],
                    "latest": latest.isoformat() if latest else None,
                }
        latest_by_symbol = {
            _symbol_from_parquet_path(path): latest
            for path, (latest, _, _) in zip(parquet_paths, rows, strict=True)
            if latest is not None
        }
        # Logged so the next time this outgrows its budget it is measurable rather
        # than a bare timeout. It outgrew the old one silently for four weeks, and
        # then outgrew the replacement too.
        log.info(
            "%s: %d files, %d cached, %d read, %.1fs",
            tf,
            len(parquet_paths),
            hits,
            len(parquet_paths) - hits,
            time.monotonic() - started,
        )
        # A terminus leaves the denominator ENTIRELY. Subtracting it from
        # `missing` alone is a no-op: the no-trade exemption below already counts
        # an absent symbol as present, so the symbol simply moves from one
        # counted bucket to the other and the ratio still reads green.
        countable = universe - set(terminus)
        # The no-trade exemption stays load-bearing for an ordinary one-session
        # absence -- without it the interior scan flags 96.6% of the universe.
        # It must NOT absorb a symbol whose absence run already qualified as a
        # terminus and was only withheld for lack of evidence: "we could not
        # check" reading as "it did not trade today" is precisely the mechanism
        # that hid EA/AVB/EQR for a month.
        present_symbols = {
            symbol
            for symbol in countable
            if (latest_by_symbol.get(symbol) or date.min) >= target_date
            or (tf == "1d" and traded_today and symbol not in traded_today and symbol not in unconfirmed)
        }
        missing = sorted(countable - present_symbols)
        results[tf] = CoverageResult(
            timeframe=tf,
            total=len(countable),
            present=len(present_symbols),
            missing_symbols=missing,
            terminus_symbols=tuple(sorted(terminus.items())),
            unconfirmed_terminus_symbols=tuple(sorted(unconfirmed & set(missing))),
            measured_session=target_date,
        )

    _save_footer_cache(cache_path, fresh)
    return results


def format_one_liner(target_date: date, results: dict[str, CoverageResult]) -> str:
    """Return the spec § 17 single-line summary."""
    parts = []
    for tf in TIMEFRAMES:
        r = results[tf]
        parts.append(f"{tf}={r.present}/{r.total} ({r.ratio:.2%})")
    return f"{target_date} coverage: " + " ".join(parts)


# The classifier window. gap_scan used 30 days; keeping it means the artifacts
# this produces are comparable to the ones it produced.
SCAN_WINDOW_DAYS = 30


def _urgency(finding: Finding) -> tuple[int, int]:
    """Sort key. `heal_by_days` is None when the repair source has no rolling
    window, i.e. nothing expires -- those sort last, never first."""
    if finding.heal_by_days is None:
        return (1, 0)
    return (0, finding.heal_by_days)


def _entry(finding: Finding) -> dict:
    return {
        "symbol": finding.symbol,
        "asset_class": finding.asset_class,
        "timeframe": finding.timeframe,
        "gap": finding.gap,
        "sessions": [session.isoformat() for session in finding.sessions],
        "heal_by_days": finding.heal_by_days,
        "source": finding.source,
    }


def _publish_json(path: Path, payload: object) -> None:
    """Write via temp + os.replace, the way bronze publishes.

    `write_text` truncates before it writes, so a reader that opens the file in
    that window gets valid JSON's worth of nothing. These two artifacts are the
    hand-off to Phase 2's repair executor; a torn read there would be a repair
    manifest that silently lost entries.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    os.replace(tmp, path)


def write_tier_a_manifest(findings: list[Finding], path: Path) -> None:
    repairs = [_entry(f) for f in sorted((f for f in findings if f.tier == "A"), key=_urgency)]
    _publish_json(path, {"repairs": repairs})


def write_decision_requests(findings: list[Finding], path: Path) -> None:
    """Tier B queue. Verdict vocabulary is triage_breaks.py's, not a new one."""
    requests = [
        # Spec section 10.8 added `terminus` as a fifth verdict precisely because
        # "we could not tell" and "the instrument stopped printing" are different
        # answers, and an operator triaging the queue acts differently on each.
        dict(_entry(f), verdict="terminus" if f.gap == "G14" else "inconclusive")
        for f in findings
        if f.tier == "B"
    ]
    _publish_json(path, requests)


def scan_findings(
    target_date: date,
    *,
    bronze_root: Path,
    registry_path: Path | None = None,
    presets_dir: Path | None = None,
    as_of: datetime | None = None,
    window_days: int = SCAN_WINDOW_DAYS,
) -> list[Finding]:
    """Every registry row, diffed over a trailing window and classified.

    This is the windowed classifier gap_scan.py used to be. It is here, and not
    in a fourth script, because it needs exactly what coverage already reads:
    the registry universe, the trading calendar, the on-disk bronze tree and the
    raw traded sets. Two detectors answering one question with two denominators
    is what spec section 11 criterion 7 forbids.
    """
    as_of = as_of or datetime.now(UTC)
    presets_dir = presets_dir or Path("presets")
    start = target_date - timedelta(days=window_days)
    # as_of, not target_date. The Massive floor rolls one day per day against the
    # WALL CLOCK; on a Monday run for Friday's session the two differ by three
    # days, which mislabels a session sitting just under the boundary as Tier A.
    floor = massive_floor_for(as_of.date())

    raw_root = bronze_root.parent / RAW_MINUTE_AGGS
    window = trading_dates_in_range(target_date - timedelta(days=40), target_date)[-TERMINUS_WINDOW_SESSIONS:]
    tape = traded_by_session(raw_root, window)
    # Criterion 8, first gate. A raw tape that stops short of the window makes
    # EVERY symbol look like a terminus at once -- one stalled flat-file lane
    # would page the whole universe. No G14 is emitted at all until the tape is
    # shown to cover the window.
    tape_ok = raw_tape_covers(raw_root, target_date)
    store = CorporateActionStore(bronze_root.parent)

    findings: list[Finding] = []
    for row in load_registry(registry_path or Path("registry/gaps.json")):
        expected = build_denominator(
            [presets_dir / f"{name}.json" for name in row.universe],
            row.asset_class,
            row.timeframe,
            start,
            target_date,
            as_of=as_of,
            lag_days=DUE_LAG_DAYS.get(row.asset_class, 1),
        )
        # A row that resolves to no symbols is a zero denominator: it reports
        # all-green for a reason that has nothing to do with the data. That is
        # exactly the disk-glob failure this engine replaces, reintroduced from
        # the registry side, so it fails the run rather than scoring perfect.
        if not expected:
            raise RegistryError(f"row {row.id}: universe {list(row.universe)} resolves to no symbols")
        for series in expected:
            if not series.sessions:
                continue
            # Terminus is an equity-tape fact. No other asset class has a tape to
            # ask, which is why only the equity registry row declares G14.
            # Criterion 8, all three gates, through the one function the coverage
            # denominator also calls. Any gate failing yields None, which drops the
            # symbol into the ordinary G1/G3 path -- a repairable gap, never a
            # delisting.
            verdict = (
                confirmed_terminus(tape, series.symbol, store, tape_ok)
                if row.asset_class == "equity"
                else TerminusVerdict(None, None)
            )
            findings.extend(
                classify(
                    series,
                    actual_sessions(bronze_root, series),
                    floor,
                    terminus=verdict.when,
                    unconfirmed=verdict.withheld,
                )
            )
    unresolved = load_unresolved(bronze_root.parent / "repairs" / "unresolved.json")
    return sorted(suppress_unresolved(findings, unresolved), key=_urgency)


def format_terminus_block(results: dict[str, CoverageResult]) -> list[str]:
    """One `  1d terminus:` line, or none.

    Deliberately NOT of the form `<tf>=<p>/<t>`: `status._coverage_section`
    selects the last line matching `coverage:` and parses timeframe terms, so a
    detail line that looked like a measurement would be read as one.
    """
    blocks = []
    pairs = results["1d"].terminus_symbols
    if pairs:
        listed = ", ".join(f"{symbol}@{when.isoformat()}" for symbol, when in pairs[:10])
        suffix = f", ... ({len(pairs)} total)" if len(pairs) > 10 else ""
        blocks.append(f"  1d terminus: {listed}{suffix}")
    # These ARE counted missing, and an operator triaging that list needs to know
    # which entries may not be fetchable at all: a symbol here stopped printing
    # for a full week and the corporate-action store could not say why.
    unconfirmed = results["1d"].unconfirmed_terminus_symbols
    if unconfirmed:
        listed = ", ".join(unconfirmed[:10])
        suffix = f", ... ({len(unconfirmed)} total)" if len(unconfirmed) > 10 else ""
        blocks.append(f"  1d unconfirmed terminus (counted missing): {listed}{suffix}")
    return blocks


def format_missing_blocks(results: dict[str, CoverageResult], max_listed: int = 10) -> list[str]:
    """Return per-timeframe 'missing:' lines for the log file."""
    blocks: list[str] = []
    for tf in TIMEFRAMES:
        r = results[tf]
        if not r.missing_symbols:
            continue
        head = ", ".join(r.missing_symbols[:max_listed])
        suffix = ""
        if len(r.missing_symbols) > max_listed:
            suffix = f", ... ({len(r.missing_symbols)} total)"
        blocks.append(f"  {tf} missing: {head}{suffix}")
    return blocks


# ponytail: derived from the registry, not written here. The hardcoded tuple
# ("volatility", "futures", "rates") omitted fx and cmdty, so a stale DXY was
# invisible -- and the omission was invisible too, because nothing compared the
# tuple to the asset classes the warehouse actually carries. There is no recovery
# path for these (CBOE/FRED/IB/Yahoo own them), so this reports and alerts only.
def _non_equity_rows(registry_path: Path | None):
    rows = load_registry(registry_path or Path("registry/gaps.json"))
    return [r for r in rows if r.asset_class != "equity" and r.timeframe == "1d"]


def _newest_due_session(target_date: date, lag_days: int, as_of: datetime) -> date | None:
    """Newest trading session at or before *target_date* whose filling job was due.

    Asking only about `target_date` is what made rates permanently invisible.
    The run targets the previous trading session S and rates are due at S+2
    10:00 UTC, but the job runs at S+1 11:00 UTC -- so rates scored 0/0 and, the
    next night, the target had already advanced to S+1. Session S was never
    revisited by any run. 0/0 maps to ratio 1.0, so it read green forever: a
    detector reporting perfect health because it enumerated nothing, which is
    the exact disease this engine was built to cure.
    """
    for session in reversed(trading_dates_in_range(target_date - timedelta(days=14), target_date)):
        if session_due_at(session, lag_days) <= as_of:
            return session
    return None


def compute_non_equity_coverage(
    target_date: date,
    bronze_root: Path | None = None,
    registry_path: Path | None = None,
    presets_dir: Path | None = None,
    as_of: datetime | None = None,
) -> dict[str, CoverageResult]:
    """Return per-asset-class 1d freshness for the non-equity universes.

    The denominator is the registry universe, never the files on disk: a symbol
    that never landed has to stay countable. No no-trade exemption -- these are
    small universes and a stale one is a real gap.

    ponytail: the calendar is XNYS for every class here, which is WRONG for fx
    (~24/5), CME futures and FRED -- see gap_registry.XNYS_CALENDAR_ASSET_CLASSES.
    A bar expected on an XNYS holiday is not expected at all, so its absence is
    invisible. Known, deferred, and carried forward deliberately; the fix is a
    per-class calendar, not a tweak here.
    """
    bronze_root = bronze_root or _resolved_data_lake() / "bronze"
    presets_dir = presets_dir or Path("presets")
    # Real wall clock, not the session's own due time: passing session_due_at
    # (target_date) here would make the due filter tautologically true and the
    # whole deadline rule inert.
    as_of = as_of or datetime.now(UTC)
    results: dict[str, CoverageResult] = {}
    for row in _non_equity_rows(registry_path):
        lag_days = DUE_LAG_DAYS.get(row.asset_class, 1)
        # Each class is graded against the newest session ITS lane owed, which
        # for rates is one session behind the run's target.
        session = _newest_due_session(target_date, lag_days, as_of)
        if session is None:
            results[row.asset_class] = CoverageResult(row.asset_class, 0, 0, [])
            continue
        expected = build_denominator(
            [presets_dir / f"{name}.json" for name in row.universe],
            row.asset_class,
            "1d",
            session,
            session,
            as_of=as_of,
            lag_days=lag_days,
        )
        universe = {series.symbol for series in expected if series.sessions}
        present = set()
        for symbol in universe:
            path = bronze_root / f"asset_class={row.asset_class}" / f"symbol={symbol}" / "1d.parquet"
            if not path.exists():
                continue
            latest = _latest_date_in_parquet(path, "trade_date")
            if latest is not None and latest >= session:
                present.add(symbol)
        results[row.asset_class] = CoverageResult(
            timeframe=row.asset_class,
            total=len(universe),
            present=len(present),
            missing_symbols=sorted(universe - present),
            measured_session=session,
        )
    return results


def format_non_equity_line(target_date: date, results: dict[str, CoverageResult]) -> str:
    """A class graded against an older session says so, rather than implying it
    measured the target. rates is T+2, so it is legitimately one session behind."""
    parts = []
    for ac in sorted(results):
        r = results[ac]
        stamp = f"@{r.measured_session}" if r.measured_session and r.measured_session != target_date else ""
        parts.append(f"{ac}={r.present}/{r.total}{stamp}")
    return f"{target_date} non-equity 1d: " + " ".join(parts)


MISSING_JSON_PREFIX = "MISSING_JSON "


def format_missing_json(results: dict[str, CoverageResult]) -> str:
    """One machine-readable line carrying the COMPLETE missing lists.

    The human `missing:` blocks are truncated to 10 names for readability, and
    the weekly report parsed only those — so a symbol whose name sorted after
    the first ten could never be detected as a persistent gap no matter how
    many consecutive days it was absent.
    """
    payload = {tf: results[tf].missing_symbols for tf in TIMEFRAMES if results[tf].missing_symbols}
    return MISSING_JSON_PREFIX + json.dumps(payload, separators=(",", ":"), sort_keys=True)


def parse_missing_json(text: str) -> dict[str, list[str]] | None:
    """Return the last well-formed MISSING_JSON payload in *text*, or None."""
    result = None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith(MISSING_JSON_PREFIX):
            continue
        try:
            result = json.loads(stripped[len(MISSING_JSON_PREFIX) :])
        except json.JSONDecodeError:
            continue
    return result


def write_coverage_log(
    target_date: date,
    line: str,
    missing_blocks: Iterable[str],
    results: dict[str, CoverageResult] | None = None,
) -> Path:
    resolved_log_dir = _resolved_log_dir()
    resolved_log_dir.mkdir(parents=True, exist_ok=True)
    log_path = resolved_log_dir / f"coverage_{target_date:%Y-%m-%d}.log"
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
        for block in missing_blocks:
            fh.write(block + "\n")
        if results is not None:
            fh.write(format_missing_json(results) + "\n")
    return log_path


def auto_recover(
    timeframe: str,
    missing_symbols: list[str],
    bronze_root: Path | None = None,
    target_date: date | None = None,
    safety_cap: int = DEFAULT_SAFETY_CAP,
    registry_path: Path | None = None,
    presets_dir: Path | None = None,
    as_of: datetime | None = None,
    withheld: tuple[str, ...] = (),
) -> RecoveryOutcome:
    """Trigger a targeted backfill subprocess and re-check coverage.

    The cap used to abort outright, which made it self-perpetuating: recovery
    only ran below the alert ratio AND below the cap, so a large outage landed
    in a dead zone where coverage was bad enough to alarm but too bad to act,
    and the same symbols were dropped and re-emailed every night forever.
    1d recovery now runs in cap-sized batches instead.

    The cap never applied to intraday in the first place: that branch passes no
    symbols at all — it republishes the whole day's flat file — so its cost is
    date-shaped, not symbol-shaped, and refusing to run because 101 symbols are
    missing was measuring the wrong quantity.
    """
    # A withheld terminus means we could not prove the symbol still trades:
    # the tape reached the session, it stopped printing, and the corporate-action
    # store could not explain it. Fetching one queues an instrument that may
    # never print again -- so it stays in `missing_symbols` (the ratio must not
    # lie) and it is still reported, but it is never attempted.
    unfetchable = [s for s in missing_symbols if s in set(withheld)]
    missing_symbols = [s for s in missing_symbols if s not in set(withheld)]
    if not missing_symbols:
        return RecoveryOutcome(timeframe=timeframe, attempted=[], recovered=0, still_missing=unfetchable)

    effective_target = target_date or datetime.now(UTC).date()

    if timeframe == "1d":
        batches = [missing_symbols[i : i + safety_cap] for i in range(0, len(missing_symbols), safety_cap)]
        if len(batches) > 1:
            console.print(
                f"[cyan]Auto-recover 1d: {len(missing_symbols)} symbols in {len(batches)} "
                f"batch(es) of up to {safety_cap}[/cyan]"
            )
        for batch in batches:
            subprocess.run(
                [
                    sys.executable,
                    str(_INGEST_SCRIPT),
                    "daily",
                    "--source",
                    "massive",
                    "--force",
                    "--target-date",
                    effective_target.isoformat(),
                    "--tickers",
                    *batch,
                ],
                check=False,
            )
    else:
        console.print(
            f"[cyan]Auto-recover {timeframe}: republishing {effective_target} "
            f"({len(missing_symbols)} symbols missing)[/cyan]"
        )
        subprocess.run(
            [
                sys.executable,
                str(_INGEST_SCRIPT),
                "flatfile-ingest",
                "repair",
                "--dates",
                effective_target.isoformat(),
            ],
            check=False,
        )

    # Deliberately uncached. Recovery just republished parquet and this
    # re-measures within the same run, and exFAT stores mtime at 2-second
    # granularity — a rewrite finishing inside that window leaves the stat
    # unchanged, so a cached re-check would report the gap recovery just
    # closed. Across the 24h between scheduled runs the granularity is
    # irrelevant; within one run it is exactly the failure mode. This reads
    # only the symbols recovery touched, so skipping the cache costs nothing.
    # Every argument, not just bronze_root. Dropping registry_path/presets_dir
    # would re-check with the DISK-GLOB denominator, so a registry-only symbol
    # like BK -- which has no file to glob -- vanishes from the universe and
    # reads as recovered by a fetch that could not have touched it.
    rechecked = compute_coverage(
        effective_target,
        bronze_root=bronze_root,
        registry_path=registry_path,
        presets_dir=presets_dir,
        as_of=as_of,
    )[timeframe]
    still_missing = [s for s in missing_symbols if s in rechecked.missing_symbols]
    recovered = len(missing_symbols) - len(still_missing)
    return RecoveryOutcome(
        timeframe=timeframe,
        attempted=list(missing_symbols),
        recovered=recovered,
        still_missing=still_missing + unfetchable,
    )


def _send_alert(
    target_date: date,
    outcomes: list[RecoveryOutcome],
    log_path: Path,
) -> None:
    """Send the coverage email via the existing failure-email script."""
    summary_lines = []
    for o in outcomes:
        if o.aborted:
            summary_lines.append(f"{o.timeframe}: ABORTED — {o.reason}; {len(o.still_missing)} missing")
        else:
            summary_lines.append(
                f"{o.timeframe}: recovered {o.recovered}/{len(o.attempted)}, {len(o.still_missing)} still missing"
            )
    error_summary = "coverage_report: " + "; ".join(summary_lines)
    cmd = [
        sys.executable,
        str(_OPS_SCRIPT),
        "send-alert",
        "--run-date",
        target_date.isoformat(),
        "--log-file",
        str(log_path),
        # One token. The two-token form breaks whenever the summary begins with
        # "--", which is how the 2026-08-08 page was lost.
        f"--error-summary={error_summary}",
        "--repo-root",
        str(_REPO_ROOT),
        "--job-name",
        "coverage_report",
    ]
    subprocess.run(cmd, check=False)


def _resolve_target_date(force: bool, override: date | None) -> date | None:
    """Resolve the session to measure: the most recently *completed* one.

    This used to be `datetime.now(UTC).date()`. The scheduled job runs at 06:00
    UTC — 02:00 ET — so on a weekday that resolved to a session that had not yet
    opened, and every symbol read as missing. `coverage_2026-06-17.log` is the
    artifact: `1d=0/20396 (0.00%)` on every timeframe. A 0% reading then fired
    auto-recovery across the whole universe, which is why `coverage report`
    exhausted its 600s budget night after night and no coverage log has been
    written since 2026-06-17 — leaving the digest's Coverage line permanently
    "(not found)".

    `daily_update._et_today` already encodes exactly this ET-close boundary for
    the ingest lane. Coverage measures what that lane ingested, so it must
    resolve to the same session rather than keep a second, wrong calendar.
    """
    if override is not None:
        return override
    target = _et_today()
    if is_trading_day(target):
        return target
    if force:
        return previous_trading_day(target)
    return None


def _scan_and_write_artifacts(target: date, as_of: datetime) -> str:
    """Run the windowed classifier and publish its two artifacts. Never raises.

    Returns the one log line describing the outcome. A scan failure degrades the
    run rather than failing it -- but it is loud in three places at once (ERROR in
    the job log, the line in the coverage log, and the artifacts simply not being
    rewritten), because a swallowed warning is how the last dead detector hid for
    four weeks.
    """
    try:
        findings = scan_findings(target, bronze_root=_resolved_data_lake() / "bronze", as_of=as_of)
        # Inside the boundary, not after it. A scan that succeeded and a WRITE
        # that failed (full disk, read-only release tree, a permission change)
        # escaped the "never raises" contract and aborted main() before
        # auto-recovery -- the same failure-domain leak, one line further down.
        repairs_dir = _resolved_data_lake() / "repairs"
        write_tier_a_manifest(findings, repairs_dir / f"tier_a_{target}.json")
        write_decision_requests(findings, repairs_dir / f"decisions_{target}.json")
    except Exception as exc:  # noqa: BLE001 - the coverage half must survive any scan failure
        log.error("scan failed for %s: %s", target, exc, exc_info=True)
        return f"  scan: FAILED ({type(exc).__name__}: {exc})"
    return (
        f"  scan: {len(findings)} findings "
        f"(tier A {sum(1 for f in findings if f.tier == 'A')}, "
        f"tier B {sum(1 for f in findings if f.tier == 'B')})"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily coverage report + auto-recovery")
    parser.add_argument(
        "--target-date",
        type=date.fromisoformat,
        help="Target trading day (YYYY-MM-DD). Defaults to today if a trading day.",
    )
    parser.add_argument(
        "--no-recover",
        action="store_true",
        help="Report coverage only — skip auto-recovery subprocess.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"Coverage ratio below which auto-recovery fires (default {DEFAULT_THRESHOLD}).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run on a non-trading day (uses the previous trading day).",
    )
    args = parser.parse_args()

    target = _resolve_target_date(args.force, args.target_date)
    if target is None:
        console.print(f"[yellow]{date.today()} is not a trading day. Use --force or --target-date.[/yellow]")
        return

    console.print(f"\n[bold]Coverage Report[/bold]  target_date={target}")
    # One clock for the whole run. Established here and passed to every consumer
    # so a single invocation cannot grade two different instants -- the equity
    # denominator, the non-equity denominator and the classifier must all agree
    # on whether the target session was due.
    as_of = datetime.now(UTC)
    # Cached across runs: an unchanged (mtime, size) cannot mean a later max
    # date, and the cold footer walk is what this job's runtime actually is.
    results = compute_coverage(
        target,
        cache_path=_resolved_log_dir() / "coverage_footer_cache.json",
        as_of=as_of,
    )
    line = format_one_liner(target, results)
    console.print(line)
    blocks = [*format_missing_blocks(results), *format_terminus_block(results)]
    for block in blocks:
        console.print(block)
    # Non-equity was in no denominator at all: a stale VIX, a stale DGS10 or a
    # stale futures contract could never register as missing.
    non_equity = compute_non_equity_coverage(target, as_of=as_of)
    non_equity_line = format_non_equity_line(target, non_equity)
    console.print(non_equity_line)
    stale_non_equity = {ac: r.missing_symbols for ac, r in non_equity.items() if r.missing_symbols}
    for asset_class, symbols in stale_non_equity.items():
        console.print(f"  [yellow]{asset_class} stale:[/yellow] {', '.join(symbols)}")

    # The windowed classifier, on the same clock and the same registry rows.
    # coverage answers "is this symbol current as of one session"; this answers
    # "which sessions in a 30-day window are absent, and what class and tier is
    # each run of them". Both artifacts land under <data-lake>/repairs/.
    #
    # ponytail: no timeout guard. com.livewire.coverage deliberately has none --
    # CLAUDE.md records four separate budgets that were guessed and expired. The
    # cost is bounded by iterating the ~515-symbol registry universe rather than
    # the ~13,270-file glob; Task 9 step 5 measures it.
    # The coverage measurement is durable BEFORE the scan runs. Consolidating two
    # jobs into one merged their failure domains too: a RegistryError in the scan
    # -- new, and consumed by nothing yet -- would otherwise take down the coverage
    # log, the auto-recovery and the alert, and leave `status` and the digest
    # reading a frozen log. That is the four-week blindness in CLAUDE.md, rebuilt.
    log_path = write_coverage_log(target, line, [*blocks, non_equity_line], results)

    scan_line = _scan_and_write_artifacts(target, as_of)
    console.print(scan_line)
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(scan_line + "\n")

    if args.no_recover:
        return

    # Decide which timeframes need recovery
    outcomes: list[RecoveryOutcome] = []
    for tf in TIMEFRAMES:
        r = results[tf]
        if r.ratio >= args.threshold:
            continue
        outcome = auto_recover(
            timeframe=tf,
            missing_symbols=r.missing_symbols,
            target_date=target,
            as_of=as_of,
            withheld=r.unconfirmed_terminus_symbols,
        )
        outcomes.append(outcome)

    if not outcomes:
        if stale_non_equity:
            # No recovery path exists for these — CBOE/FRED/IB own them — so
            # reporting is all we can honestly do, but silence was worse.
            _send_alert(
                target,
                [
                    RecoveryOutcome(
                        timeframe=asset_class,
                        attempted=symbols,
                        recovered=0,
                        still_missing=symbols,
                        aborted=True,
                        reason="no recovery path for this asset class",
                    )
                    for asset_class, symbols in stale_non_equity.items()
                ],
                log_path,
            )
            return
        log.info("Coverage above threshold for all timeframes — no recovery needed")
        return

    # Append recovery outcome lines to the same log
    with log_path.open("a", encoding="utf-8") as fh:
        for o in outcomes:
            if o.aborted:
                fh.write(f"  {o.timeframe} recovery ABORTED: {o.reason}\n")
            else:
                fh.write(
                    f"  {o.timeframe} recovery: recovered {o.recovered}/"
                    f"{len(o.attempted)}, still_missing={len(o.still_missing)}\n"
                )

    needs_email = any(o.aborted or o.still_missing for o in outcomes)
    if needs_email:
        _send_alert(target, outcomes, log_path)
    else:
        console.print("[green]All timeframes recovered — INFO log only, no email[/green]")


if __name__ == "__main__":
    main()
