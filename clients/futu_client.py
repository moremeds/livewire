"""Futu OpenD quote client: the only module that imports ``futu`` (and, through it, pandas).

Read-only quote calls for the option-chain capture. Every call returns plain Python rows,
never a DataFrame, so pandas stays inside this file (``tests/test_futu_containment.py``).

What each guard is for (docs/audits/2026-10-16-futu-opend-market-data-probe.md):

- A TCP probe before the SDK is touched: the SDK retries a refused OpenD forever
  (xenon PR #179 wedged xenon-api ~4h). A port that accepts TCP but never completes the
  handshake loops in the SDK constructor (``while True`` + 6 s sleep), so the constructor
  runs in a daemon thread with `futu_connect_timeout_s`, and every SDK thread is a daemon:
  an interrupted run must still let the process exit, or launchd never starts the next.
- Client-side pacing per endpoint: OpenD answers an over-limit request with an error,
  so whatever a throttled batch held is lost unless it is paced and retried.
- A failed snapshot batch is bisected: one code Futu no longer knows fails all 400. Only
  while OpenD is still logged in and the error is not pacing: a dropped session would
  otherwise turn 77 batches into ~30,000 single-code calls.
- ``'N/A'`` and empty strings become None; list cells are never returned.
"""

from __future__ import annotations

import math
import socket
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from clients import constants

#: Futu's US quote times are New York wall-clock strings.
NEW_YORK = ZoneInfo("America/New_York")
#: OpenD's answer to an over-limit request (seen for snapshot, chain and history calls).
_THROTTLED = "high frequency"
_WINDOW_S = 30.0


class FutuUnavailable(RuntimeError):
    """OpenD unreachable, not logged in, or without the quote right: a skipped run, exit 86."""


class FutuRequestError(RuntimeError):
    """A request OpenD rejected for a reason other than pacing."""


class RateLimiter:
    """At most `limit` calls in any rolling 30 s window."""

    def __init__(self, limit: float, *, clock: Callable[[], float] = time.monotonic, sleep=time.sleep):
        self.limit, self.clock, self.sleep = int(limit), clock, sleep
        self.calls: deque[float] = deque()

    def wait(self) -> None:
        now = self.clock()
        while self.calls and now - self.calls[0] >= _WINDOW_S:
            self.calls.popleft()
        if len(self.calls) >= self.limit:
            self.sleep(_WINDOW_S - (now - self.calls[0]) + 0.05)
            return self.wait()
        self.calls.append(self.clock())
        return None


def probe(host: str, port: int, timeout: float = 3.0) -> None:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as exc:
        raise FutuUnavailable(f"OpenD unreachable at {host}:{port}: {exc}") from exc


def _clean(value: Any) -> Any:
    if isinstance(value, str) and value.strip() in ("", "N/A"):
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (list, tuple, dict)):
        return None
    if hasattr(value, "item"):  # numpy scalar
        return value.item()
    return value


def _rows(frame: Any) -> list[dict[str, Any]]:
    return [{k: _clean(v) for k, v in row.items()} for row in frame.to_dict("records")]


def us_time(text: str | None) -> datetime | None:
    """A Futu US quote time (New York wall clock) as a UTC instant."""
    if not text:
        return None
    return datetime.fromisoformat(text).replace(tzinfo=NEW_YORK).astimezone(UTC)


class QuoteSession:
    """One OpenQuoteContext with per-endpoint pacing. Use as a context manager."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 11111,
        *,
        context_factory: Callable[..., Any] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.host, self.port = host, port
        self._factory = context_factory
        self._sleep = sleep
        self._limits = {
            "snapshot": RateLimiter(constants.declared("futu_calls_per_30s/snapshot"), clock=clock, sleep=sleep),
            "chain": RateLimiter(constants.declared("futu_calls_per_30s/option_chain"), clock=clock, sleep=sleep),
            "history_kline": RateLimiter(
                constants.declared("futu_calls_per_30s/history_kline"), clock=clock, sleep=sleep
            ),
        }
        self.throttled = 0
        self._ctx: Any = None

    def __enter__(self) -> QuoteSession:
        probe(self.host, self.port)
        factory = self._factory
        if factory is None:
            from futu import OpenQuoteContext, SysConfig

            SysConfig.set_all_thread_daemon(True)
            factory = OpenQuoteContext
        built: list[Any] = []

        def build() -> None:
            try:
                built.append(factory(host=self.host, port=self.port))
            except Exception as exc:  # surfaced below as FutuUnavailable
                built.append(exc)

        # ponytail: a constructor that outlives the timeout leaks one daemon context; the run exits 86 next.
        worker = threading.Thread(target=build, name="futu-connect", daemon=True)
        worker.start()
        timeout = constants.declared("futu_connect_timeout_s")
        worker.join(timeout)
        if not built or isinstance(built[0], Exception):
            cause = built[0] if built else None
            raise FutuUnavailable(f"OpenD at {self.host}:{self.port}: no session within {timeout:.0f}s ({cause})")
        self._ctx = built[0]
        return self

    def __exit__(self, *exc: object) -> None:
        if self._ctx is not None:
            self._ctx.close()

    def _call(self, endpoint: str, fn: Callable[[], tuple[int, Any]]) -> Any:
        for attempt in (1, 2):
            if endpoint in self._limits:
                self._limits[endpoint].wait()
            ret, data = fn()
            if ret == 0:
                return data
            if _THROTTLED in str(data) and attempt == 1:
                self.throttled += 1
                self._sleep(_WINDOW_S)
                continue
            raise FutuRequestError(f"{endpoint}: {data}")
        raise AssertionError("unreachable")  # pragma: no cover

    def require_logged_in(self) -> None:
        ret, state = self._ctx.get_global_state()
        if ret != 0 or state.get("qot_logined") not in (True, "1"):
            raise FutuUnavailable(f"OpenD quote session lost: {state}")

    def user_info(self) -> dict[str, Any]:
        ret, info = self._ctx.get_user_info()
        if ret != 0:
            raise FutuUnavailable(f"get_user_info: {info}")
        return {k: _clean(v) for k, v in info.items()}

    def expiries(self, underlying: str) -> list[date]:
        frame = self._call("chain", lambda: self._ctx.get_option_expiration_date(underlying))
        return sorted({date.fromisoformat(d) for d in frame["strike_time"]})

    def chain(self, underlying: str, start: date, end: date) -> list[dict[str, Any]]:
        frame = self._call(
            "chain", lambda: self._ctx.get_option_chain(underlying, start=start.isoformat(), end=end.isoformat())
        )
        return _rows(frame)

    def history_1m(self, code: str, start: date, end: date) -> list[dict[str, Any]]:
        """Regular-session 1m bars at raw prices, shaped as bronze rows.

        Futu labels a bar by its END in New York wall time, so one minute is subtracted to get
        the bar start Massive uses; it pads no-trade minutes with volume 0, which Massive omits,
        so those are dropped (docs/audits/2026-10-16-futu-opend-market-data-probe.md A3).
        Only a symbol's first page is rate-limited; follow-up pages are not.
        """
        from futu import AuType, KLType

        rows: list[dict[str, Any]] = []
        key = None
        while True:

            def page(key=key) -> tuple[int, Any]:
                ret, frame, next_key = self._ctx.request_history_kline(
                    code,
                    start=start.isoformat(),
                    end=end.isoformat(),
                    ktype=KLType.K_1M,
                    autype=AuType.NONE,
                    max_count=1000,
                    page_req_key=key,
                    extended_time=False,
                )
                return ret, (frame, next_key) if ret == 0 else frame

            frame, key = self._call("history_kline" if key is None else "history_page", page)
            for bar in _rows(frame):
                if not bar["volume"]:
                    continue
                end_at = datetime.fromisoformat(bar["time_key"]).replace(tzinfo=NEW_YORK)
                rows.append(
                    {
                        "bar_timestamp": (end_at - timedelta(minutes=1)).astimezone(UTC),
                        "open": float(bar["open"]),
                        "high": float(bar["high"]),
                        "low": float(bar["low"]),
                        "close": float(bar["close"]),
                        "volume": int(bar["volume"]),
                    }
                )
            if key is None:
                return rows

    def snapshot(self, codes: Iterable[str]) -> tuple[list[dict[str, Any]], list[str]]:
        """Snapshot rows for `codes`, in batches of the declared size; returns (rows, codes Futu refused).

        Each row carries `received_at`, the instant its batch came back: a 30,000-code chain
        spans ~45 s of paced calls, so one timestamp per capture is wrong by up to that much.
        """
        codes = list(codes)
        size = int(constants.declared("futu_codes_per_call/snapshot"))
        rows: list[dict[str, Any]] = []
        refused: list[str] = []
        for i in range(0, len(codes), size):
            self._snapshot_batch(codes[i : i + size], rows, refused)
        return rows, refused

    def _snapshot_batch(self, batch: list[str], rows: list[dict[str, Any]], refused: list[str]) -> bool:
        """True when any code in `batch` came back. Every code of a split batch failing is
        systemic, not a bad code: raise rather than bisect a service outage into single calls."""
        try:
            frame = self._call("snapshot", lambda: self._ctx.get_market_snapshot(batch))
        except FutuRequestError as exc:
            if _THROTTLED in str(exc):
                raise  # paced and retried once already; bisecting would only send more
            self.require_logged_in()
            if len(batch) == 1:
                refused += batch
                return False
            half = len(batch) // 2
            # `|`, not `or`: both halves always run. A pair of bad codes is refused, not systemic;
            # ponytail: three or more bad codes that fill one sub-batch read as systemic and fail the underlying.
            served = self._snapshot_batch(batch[:half], rows, refused) | self._snapshot_batch(
                batch[half:], rows, refused
            )
            if not served and len(batch) > 2:
                raise FutuRequestError(f"snapshot: no code of a {len(batch)}-code batch was served: {exc}") from exc
            return served
        received = datetime.now(UTC)
        rows += [row | {"received_at": received} for row in _rows(frame)]
        return True
