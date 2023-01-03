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

    _address: Address
    _clients: Set[Union[Publisher,Subscriber]]
    _edges: Set[Tuple[str, str]]

    _shm_server: Optional[SHMServer]
    _graph_server: Optional[GraphServer]

    def __init__(self, address: AddressType = GRAPHSERVER_ADDR) -> None:
        self._address = Address(*address)
        self._clients = set()
        self._edges = set()
        self._shm_server = None
        self._graph_server = None

    async def publisher(self, topic: str, **kwargs) -> Publisher:
        pub = await Publisher.create(topic, self._address, **kwargs)
        self._clients.add(pub)
        return pub

    async def subscriber(self, topic: str, **kwargs) -> Subscriber:
        sub = await Subscriber.create(topic, self._address, **kwargs)
        self._clients.add(sub)
        return sub

    async def connect(self, from_topic: str, to_topic: str) -> None:
        await GraphServer.connect(from_topic, to_topic, self._address)
        self._edges.add((from_topic, to_topic))

    async def disconnect(self, from_topic: str, to_topic: str) -> None:
        await GraphServer.disconnect(from_topic, to_topic, self._address)
        self._edges.discard((from_topic, to_topic))

    async def pause(self) -> None:
        await GraphServer.pause(self._address)

    async def sync(self, timeout: Optional[float] = None) -> None:
        await GraphServer.sync(timeout, self._address)

    async def resume(self) -> None:
        await GraphServer.resume(self._address)

    async def __aenter__(self):
        self._shm_server = await SHMServer.ensure_running()
        self._graph_server = await GraphServer.ensure_running(self._address)
        return self

    async def __aexit__(self,
                        exc_t: Optional[Type[Exception]],
                        exc_v: Optional[Any],
                        exc_tb: Optional[TracebackType]
                        ) -> bool:
        for client in self._clients:
            client.close()

        wait = [c.wait_closed() for c in self._clients]
        for future in asyncio.as_completed(wait):
            await future

        for edge in self._edges:
            try:
                await GraphServer.disconnect(*edge, address=self._address)
            except (ConnectionRefusedError, BrokenPipeError) as e:
                logger.warn(f'Could not remove edge from GraphServer: {e}')

        if self._graph_server is not None:
            logger.debug( 'Terminating GraphServer' )
            self._graph_server.shutdown()
            self._graph_server.join()

        if self._shm_server is not None:
            logger.debug( 'Terminating SHMServer' )
            self._shm_server.shutdown()
            self._shm_server.join()

        return False
