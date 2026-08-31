"""The hermetic-conftest contract: private defaults, fresh server per test."""

import os

import pytest

from ezmsg.core import channelmanager, netprotocol
from ezmsg.core.graphserver import GraphService
from ezmsg.core.netprotocol import close_stream_writer


class TestHermeticDefaults:
    def test_every_default_resolution_agrees_on_the_pinned_address(self):
        pinned = os.environ[netprotocol.GRAPHSERVER_ADDR_ENV]
        assert netprotocol.GRAPHSERVER_ADDR == pinned
        assert channelmanager.GRAPHSERVER_ADDR == pinned
        assert str(GraphService.default_address()) == pinned
        # The whole point: the pinned port is NOT the shared default one a
        # developer's live server may occupy.
        pinned_port = int(pinned.rsplit(":", 1)[1])
        assert pinned_port != netprotocol.GRAPHSERVER_PORT_DEFAULT

    @pytest.mark.asyncio
    async def test_default_clients_attach_to_the_per_test_server(self):
        service = GraphService()  # no address: resolves the pinned default
        started = await service.ensure()
        # Attach, not start: the autouse server is already listening there.
        assert started is None
        _reader, writer = await service.open_connection()
        await close_stream_writer(writer)

    @pytest.mark.asyncio
    async def test_implicit_auto_start_still_creates_a_private_server(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        # Exercise the real implicit-start decision without touching the
        # canonical port: port 0 lets the OS choose the server's address.
        monkeypatch.delenv(netprotocol.GRAPHSERVER_ADDR_ENV)
        monkeypatch.setattr(GraphService, "PORT_DEFAULT", 0)
        monkeypatch.setenv(netprotocol.SERVER_PORT_START_ENV, "0")

        service = GraphService()
        server = await service.ensure()
        assert server is not None
        try:
            assert service.address.port != 0
            _reader, writer = await service.open_connection()
            await close_stream_writer(writer)
        finally:
            server.stop()
            assert not server.is_alive()


_SERVERS_SEEN: list[object] = []


class TestPerTestFreshness:
    # Strong references keep ids unique for the comparison below.
    def test_server_is_fresh_per_test_first(self, hermetic_graph_server):
        _SERVERS_SEEN.append(hermetic_graph_server)

    def test_server_is_fresh_per_test_second(self, hermetic_graph_server):
        _SERVERS_SEEN.append(hermetic_graph_server)
        assert len({id(server) for server in _SERVERS_SEEN}) == len(_SERVERS_SEEN)
