"""Run every test against a fresh, suite-owned GraphServer.

The server binds directly to an OS-assigned loopback port. Before the test
runs, both the environment inherited by child processes and ezmsg's imported
address constants are pointed at that server. Tests that pass an explicit
server address continue to use their own server.
"""

import pytest

from ezmsg.core import channelmanager, netprotocol
from ezmsg.core.graphserver import GraphService

assert netprotocol.GRAPHSERVER_ADDR_ENV == "EZMSG_GRAPHSERVER_ADDR", (
    "The env var this fixture sets no longer matches ezmsg's; update both together."
)


@pytest.fixture(autouse=True)
def hermetic_graph_server(monkeypatch: pytest.MonkeyPatch):
    """Provide a fresh GraphServer on a fresh loopback port for each test."""
    for _ in range(10):
        service = GraphService(address=("127.0.0.1", 0))
        server = service.create_server()
        if server.address.port != netprotocol.GRAPHSERVER_PORT_DEFAULT:
            break
        server.stop()
    else:
        raise RuntimeError("Could not allocate a non-default GraphServer port")

    address = str(server.address)
    monkeypatch.setenv(netprotocol.GRAPHSERVER_ADDR_ENV, address)
    monkeypatch.setattr(netprotocol, "GRAPHSERVER_ADDR", address)
    monkeypatch.setattr(channelmanager, "GRAPHSERVER_ADDR", address)

    try:
        yield server
    finally:
        server.stop()
        assert not server.is_alive(), "Hermetic GraphServer failed to stop"
