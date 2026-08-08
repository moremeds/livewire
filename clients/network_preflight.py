"""Preflight that outbound egress works, before a lane that depends on it.

Every provider client has a per-*request* timeout, which is the wrong granularity
when the network itself is down: a lane looping over thousands of tickers pays
that timeout thousands of times. On 2026-07-24 outbound egress was unavailable
for roughly a day and four phases of `daily-backfill` each burned their full
`MDW_SYNC_PHASE_TIMEOUT_SECONDS` budget — 24 hours of a wedged run for what a
three-second TCP connect would have reported immediately. The IB-backed phases in
the same run finished normally in seconds, because they dial 127.0.0.1.

This is the public-internet analogue of ``ib_gateway_preflight``: probe once, skip
the lanes that cannot work, and let everything else proceed.

Set LIVEWIRE_SKIP_NETWORK_PREFLIGHT=1 to bypass (offline tests, or an operator who
knows better).
"""

from __future__ import annotations

import os
import socket

#: Provider key -> the host its client actually dials. Kept in step with
#: massive_client, massive_flatfile_client, fred_client, fetch_cboe_volatility
#: and yahoo_client.
PROVIDER_HOSTS = {
    "massive_rest": "api.massive.com",
    "massive_flatfile": "files.massive.com",
    "cboe": "cdn.cboe.com",
    "fred": "api.stlouisfed.org",
    "yahoo": "query1.finance.yahoo.com",
}

#: Distinct from 1 (generic failure), 2 (argparse) and 86 (IB Gateway down), so a
#: wrapper can tell "the internet was unavailable" apart from "the lane failed".
#: Like a down Gateway, this is an expected operational state: not retried here,
#: and not counted as a data failure that should fail the whole run.
EGRESS_DOWN_EXIT_CODE = 87

DEFAULT_TIMEOUT = 3.0
HTTPS_PORT = 443


def _reachable(host: str, timeout: float) -> bool:
    """TCP-connect to the host's HTTPS port. Also surfaces DNS failure."""
    try:
        with socket.create_connection((host, HTTPS_PORT), timeout=timeout):
            return True
    except OSError:
        return False


def unreachable_providers(
    providers: object = None,
    *,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, str]:
    """Return ``{provider: host}`` for each requested provider that cannot be reached.

    ``providers`` defaults to every known provider. Unknown names are ignored
    rather than raised: a caller naming a provider this module has not learned
    about yet should not lose its lane over it.
    """
    if os.environ.get("LIVEWIRE_SKIP_NETWORK_PREFLIGHT"):
        return {}

    names = list(PROVIDER_HOSTS) if providers is None else list(providers)  # type: ignore[arg-type]
    checked: dict[str, bool] = {}
    down: dict[str, str] = {}
    for name in names:
        host = PROVIDER_HOSTS.get(name)
        if host is None:
            continue
        # One probe per host, not per provider: the two Massive lanes are
        # different hosts, but a caller may name the same provider twice.
        if host not in checked:
            checked[host] = _reachable(host, timeout)
        if not checked[host]:
            down[name] = host
    return down
