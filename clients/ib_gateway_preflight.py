"""Preflight check that IB Gateway is reachable before IB-backed work.

IB Gateway is owned by the trading-stack project at ~/trading-stack/.
This module surfaces a clear, fast failure with a runbook pointer instead
of letting ib_async burn its 4-minute connection timeout.

Set LIVEWIRE_SKIP_IB_PREFLIGHT=1 to bypass (e.g., for offline tests).
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 4001
STATUS_SCRIPT = Path.home() / "trading-stack" / "scripts" / "ibc_gateway_status.sh"
RUNBOOK = Path.home() / "runbooks" / "trading-stack" / "ib-gateway-ibc.md"


def _tcp_reachable(host: str, port: int, timeout: float) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def assert_gateway_up(
    host: str | None = None,
    port: int | None = None,
    timeout: float = 1.0,
) -> None:
    """Exit non-zero with diagnostics if IB Gateway is not reachable.

    Resolves host/port from args, then MDW_IB_HOST/MDW_IB_PORT, then defaults.
    Skip via LIVEWIRE_SKIP_IB_PREFLIGHT=1.
    """
    if os.environ.get("LIVEWIRE_SKIP_IB_PREFLIGHT"):
        return

    resolved_host = host or os.environ.get("MDW_IB_HOST", DEFAULT_HOST)
    resolved_port = int(port if port is not None else os.environ.get("MDW_IB_PORT", DEFAULT_PORT))

    if _tcp_reachable(resolved_host, resolved_port, timeout):
        return

    print(
        f"ERROR: IB Gateway not reachable on {resolved_host}:{resolved_port}",
        file=sys.stderr,
    )
    if STATUS_SCRIPT.is_file():
        print(f"--- {STATUS_SCRIPT} ---", file=sys.stderr)
        subprocess.run(["bash", str(STATUS_SCRIPT)], check=False)
    print(f"--- Runbook: {RUNBOOK} ---", file=sys.stderr)
    sys.exit(2)
