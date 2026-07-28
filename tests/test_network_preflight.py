"""Tests for the outbound-egress preflight.

No sockets are opened: `_reachable` is the single seam and it is patched.
"""

from __future__ import annotations

import pytest

from clients import network_preflight as np


@pytest.fixture(autouse=True)
def no_bypass(monkeypatch):
    monkeypatch.delenv("LIVEWIRE_SKIP_NETWORK_PREFLIGHT", raising=False)


def patch_reachable(monkeypatch, down_hosts=()):
    seen = []

    def fake(host, timeout):
        seen.append((host, timeout))
        return host not in set(down_hosts)

    monkeypatch.setattr(np, "_reachable", fake)
    return seen


def test_every_provider_reachable_reports_nothing(monkeypatch):
    patch_reachable(monkeypatch)

    assert np.unreachable_providers() == {}


def test_a_down_provider_is_reported_with_its_host(monkeypatch):
    patch_reachable(monkeypatch, down_hosts={"cdn.cboe.com"})

    assert np.unreachable_providers() == {"cboe": "cdn.cboe.com"}


def test_only_the_requested_providers_are_probed(monkeypatch):
    seen = patch_reachable(monkeypatch)
    np.unreachable_providers(("fred",))

    assert [host for host, _ in seen] == ["api.stlouisfed.org"]


def test_a_host_is_probed_once_however_often_it_is_named(monkeypatch):
    seen = patch_reachable(monkeypatch)
    np.unreachable_providers(("massive_flatfile", "massive_flatfile"))

    assert len(seen) == 1


def test_the_two_massive_lanes_are_different_hosts(monkeypatch):
    """A REST outage must not skip the flat-file lane, or the reverse."""
    patch_reachable(monkeypatch, down_hosts={"api.massive.com"})
    down = np.unreachable_providers(("massive_rest", "massive_flatfile"))

    assert down == {"massive_rest": "api.massive.com"}


def test_an_unknown_provider_never_loses_its_lane(monkeypatch):
    patch_reachable(monkeypatch, down_hosts=set(np.PROVIDER_HOSTS.values()))

    assert "not_a_provider" not in np.unreachable_providers(("not_a_provider",))


def test_the_bypass_env_var_reports_everything_reachable(monkeypatch):
    monkeypatch.setenv("LIVEWIRE_SKIP_NETWORK_PREFLIGHT", "1")
    patch_reachable(monkeypatch, down_hosts=set(np.PROVIDER_HOSTS.values()))

    assert np.unreachable_providers() == {}


def test_the_probe_timeout_is_seconds_not_minutes(monkeypatch):
    """The whole point is failing fast; a slow probe reintroduces the stall."""
    seen = patch_reachable(monkeypatch)
    np.unreachable_providers(("cboe",))

    assert seen[0][1] == np.DEFAULT_TIMEOUT <= 5.0


def test_reachable_reports_false_on_a_socket_error(monkeypatch):
    def boom(address, timeout):
        raise OSError("no route to host")

    monkeypatch.setattr(np.socket, "create_connection", boom)

    assert np._reachable("cdn.cboe.com", 1.0) is False


def test_reachable_reports_true_when_the_connection_opens(monkeypatch):
    class Sock:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(np.socket, "create_connection", lambda address, timeout: Sock())

    assert np._reachable("cdn.cboe.com", 1.0) is True


def test_the_exit_code_is_distinct_from_the_gateway_and_argparse_codes():
    from clients.ib_gateway_preflight import GATEWAY_DOWN_EXIT_CODE

    assert np.EGRESS_DOWN_EXIT_CODE not in {0, 1, 2, 124, GATEWAY_DOWN_EXIT_CODE}
