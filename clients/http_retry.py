"""The repo's one definition of a transient HTTP failure.

Before this module each provider decided for itself, and the decisions did not
agree: `MassiveClient` had exponential backoff honouring `Retry-After`, FRED got
a bounded retry only after a 502 cost a page
(pm:2026-09-14-fred-502-aborted-remaining-series), and CBOE had none at all —
it caught every exception per symbol and still exited 0, so the phase could not
fail no matter how much was missing
(pm:2026-09-16-cboe-could-not-fail).

The split that matters is not "error vs no error" but **whose problem it is**:

- a 5xx or a transport error is the server or the network, and asking again is
  the correct response;
- a 4xx is this request — a bad key, a retired series, a symbol that no longer
  exists — and asking again only makes the failure slower.

Callers keep their own policy for what an exhausted retry *means*: FRED fails
its phase, CBOE fails only if nothing else explains the gap. This module decides
when to retry, never what a failure costs.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import httpx


def is_transient(exc: BaseException) -> bool:
    """True for a failure worth repeating: a 5xx, a timeout, a reset."""
    if isinstance(exc, httpx.TransportError):
        return True
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500


def get_with_retry(
    send: Callable[[], httpx.Response],
    *,
    attempts: int,
    backoff_s: float,
    sleep: Callable[[float], None] | None = None,
) -> httpx.Response:
    """Call `send` until it returns a response whose status is not 5xx.

    Retries a transient failure `attempts` times with a linear backoff, then
    re-raises the last one. A 4xx leaves immediately, on the first attempt.
    `attempts` below 1 still sends exactly once; a negative backoff is treated
    as no wait, so a bad override degrades the pacing and never the request.

    `sleep` defaults to `time.sleep` resolved per call, not bound at import, so
    a test can patch `clients.http_retry.time.sleep` and see it take effect.
    """
    wait = sleep if sleep is not None else time.sleep
    attempts = max(1, int(attempts))
    backoff_s = max(0.0, float(backoff_s))
    last_error: BaseException | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = send()
            response.raise_for_status()
        except Exception as exc:
            if not is_transient(exc):
                raise
            last_error = exc
        else:
            return response
        if attempt < attempts:
            wait(backoff_s * attempt)

    assert last_error is not None  # only reachable after a transient failure
    raise last_error
