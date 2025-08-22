import asyncio
import logging
import typing

from .netprotocol import AddressType, Address
from .graphserver import GraphServer, GraphService
from .pubclient import Publisher
from .subclient import Subscriber

from types import TracebackType

logger = logging.getLogger("ezmsg")


class GraphContext:
    """
    GraphContext maintains a list of created
    publishers, subscribers, and connections in the graph.
    Once the context is no-longer-needed, we can revert()
    changes in the graph which disconnects publishers and removes
    changes that this context made.
    """

    _clients: typing.Set[typing.Union[Publisher, Subscriber]]
    _edges: typing.Set[typing.Tuple[str, str]]

    _graph_address: AddressType | None
    _graph_server: typing.Optional[GraphServer]

    def __init__(
        self,
        graph_address: AddressType | None = None,
    ) -> None:
        self._clients = set()
        self._edges = set()
        self._graph_address = graph_address
        self._graph_server = None

    @property
    def graph_address(self) -> AddressType | None:
        if self._graph_server is not None:
            return self._graph_server.address
        else:
            return self._graph_address

    async def publisher(self, topic: str, **kwargs) -> Publisher:
        pub = await Publisher.create(
            topic, self.graph_address, **kwargs
        )
        self._clients.add(pub)
        return pub

    async def subscriber(self, topic: str, **kwargs) -> Subscriber:
        sub = await Subscriber.create(
            topic, self.graph_address, **kwargs
        )
        self._clients.add(sub)
        return sub

    async def connect(self, from_topic: str, to_topic: str) -> None:
        await GraphService(self.graph_address).connect(from_topic, to_topic)
        self._edges.add((from_topic, to_topic))

    async def disconnect(self, from_topic: str, to_topic: str) -> None:
        await GraphService(self.graph_address).disconnect(from_topic, to_topic)
        self._edges.discard((from_topic, to_topic))

    async def sync(self, timeout: typing.Optional[float] = None) -> None:
        await GraphService(self.graph_address).sync(timeout)

    async def pause(self) -> None:
        await GraphService(self.graph_address).pause()

    async def resume(self) -> None:
        await GraphService(self.graph_address).resume()

    async def _ensure_servers(self) -> None:
        self._graph_server = await GraphService(self.graph_address).ensure()

    async def _shutdown_servers(self) -> None:
        if self._graph_server is not None:
            self._graph_server.stop()
        self._graph_server = None

    async def __aenter__(self) -> "GraphContext":
        await self._ensure_servers()
        return self

    async def __aexit__(
        self,
        exc_t: typing.Optional[typing.Type[Exception]],
        exc_v: typing.Optional[typing.Any],
        exc_tb: typing.Optional[TracebackType],
    ) -> bool:
        await self.revert()
        await self._shutdown_servers()
        return False

    async def revert(self) -> None:
        for client in self._clients:
            client.close()

        wait = [c.wait_closed() for c in self._clients]
        for future in asyncio.as_completed(wait):
            await future

        for edge in self._edges:
            try:
                await GraphService(self.graph_address).disconnect(*edge)
            except (ConnectionRefusedError, BrokenPipeError, ConnectionResetError) as e:
                logger.warn(f"Could not remove edge {edge} from GraphServer: {e}")
