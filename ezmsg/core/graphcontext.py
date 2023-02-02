import asyncio
import logging

from .shmserver import SHMServer
from .graphserver import GraphServer
from .pubclient import Publisher
from .subclient import Subscriber
from .netprotocol import (
    AddressType,
    GRAPHSERVER_ADDR,
    Address
)

from types import TracebackType
from typing import Optional, Tuple, Set, Type, Any, Union

logger = logging.getLogger('ezmsg')

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

    _address: Address
    _clients: Set[Union[Publisher,Subscriber]]
    _edges: Set[Tuple[str, str]]

    _shm_server: Optional[SHMServer]
    _graph_server: Optional[GraphServer]
    _connection: GraphServer.Connection

    def __init__(self, address: AddressType = GRAPHSERVER_ADDR) -> None:
        self._address = Address(*address)
        self._clients = set()
        self._edges = set()
        self._shm_server = None
        self._graph_server = None
        self._connection = GraphServer.Connection(self._address)

    async def publisher(self, topic: str, **kwargs) -> Publisher:
        pub = await Publisher.create(topic, self._address, **kwargs)
        self._clients.add(pub)
        return pub

    async def subscriber(self, topic: str, **kwargs) -> Subscriber:
        sub = await Subscriber.create(topic, self._address, **kwargs)
        self._clients.add(sub)
        return sub

    async def connect(self, from_topic: str, to_topic: str) -> None:
        await self._connection.connect(from_topic, to_topic)
        self._edges.add((from_topic, to_topic))

    async def disconnect(self, from_topic: str, to_topic: str) -> None:
        await self._connection.disconnect(from_topic, to_topic)
        self._edges.discard((from_topic, to_topic))

    async def sync(self, timeout: Optional[float] = None) -> None:
        await self._connection.sync(timeout)

    async def pause(self) -> None:
        await self._connection.pause()

    async def resume(self) -> None:
        await self._connection.resume()

    async def _ensure_servers(self) -> None:
        self._shm_server = await SHMServer.ensure_running()
        self._graph_server = await GraphServer.ensure_running(self._address)

    async def _shutdown_servers(self) -> None:
        if self._graph_server is not None:
            logger.info( 'Terminating GraphServer' )
            self._graph_server.stop()
        self._graph_server = None

        if self._shm_server is not None:
            logger.info( 'Terminating SHMServer' )
            self._shm_server.stop()
        self._shm_server = None

    async def __aenter__(self) -> "GraphContext":
        await self._ensure_servers()
        return self

    async def __aexit__(self,
                        exc_t: Optional[Type[Exception]],
                        exc_v: Optional[Any],
                        exc_tb: Optional[TracebackType]
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
                await self._connection.disconnect(*edge)
            except (ConnectionRefusedError, BrokenPipeError, ConnectionResetError) as e:
                logger.warn(f'Could not remove edge {edge} from GraphServer: {e}')
