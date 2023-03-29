import asyncio
import logging
import typing

from .shmserver import SHMServer, SHMService
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
    It also maintains a context manager that ensures
    graph and SHMServer are up.
    """

    _clients: typing.Set[typing.Union[Publisher, Subscriber]]
    _edges: typing.Set[typing.Tuple[str, str]]

    _shm_service: SHMService
    _shm_server: typing.Optional[SHMServer]
    _graph_service: GraphService
    _graph_server: typing.Optional[GraphServer]

    def __init__(
        self,
        graph_service: typing.Optional[GraphService] = None,
        shm_service: typing.Optional[SHMService] = None,
    ) -> None:
        self._clients = set()
        self._edges = set()
        self._shm_service = shm_service if shm_service is not None else SHMService()
        self._shm_server = None
        self._graph_service = (
            graph_service if graph_service is not None else GraphService()
        )
        self._graph_server = None

    async def publisher(self, topic: str, **kwargs) -> Publisher:
        pub = await Publisher.create(
            topic, self._graph_service, self._shm_service, **kwargs
        )
        self._clients.add(pub)
        return pub

    async def subscriber(self, topic: str, **kwargs) -> Subscriber:
        sub = await Subscriber.create(
            topic, self._graph_service, self._shm_service, **kwargs
        )
        self._clients.add(sub)
        return sub

    async def connect(self, from_topic: str, to_topic: str) -> None:
        await self._graph_service.connect(from_topic, to_topic)
        self._edges.add((from_topic, to_topic))

    async def disconnect(self, from_topic: str, to_topic: str) -> None:
        await self._graph_service.disconnect(from_topic, to_topic)
        self._edges.discard((from_topic, to_topic))

    async def sync(self, timeout: typing.Optional[float] = None) -> None:
        await self._graph_service.sync(timeout)

    async def pause(self) -> None:
        await self._graph_service.pause()

    async def resume(self) -> None:
        await self._graph_service.resume()

    async def _ensure_servers(self) -> None:
        self._shm_server = await self._shm_service.ensure()
        self._graph_server = await self._graph_service.ensure()

    async def _shutdown_servers(self) -> None:
        if self._graph_server is not None:
            self._graph_server.stop()
        self._graph_server = None

        if self._shm_server is not None:
            self._shm_server.stop()
        self._shm_server = None

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
                await self._graph_service.disconnect(*edge)
            except (ConnectionRefusedError, BrokenPipeError, ConnectionResetError) as e:
                logger.warn(f"Could not remove edge {edge} from GraphServer: {e}")
