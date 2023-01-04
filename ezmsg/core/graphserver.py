import asyncio
import logging
import signal

from dataclasses import dataclass
from contextlib import suppress, asynccontextmanager
from multiprocessing import Process, Event
from multiprocessing.synchronize import Event as EventType

from uuid import UUID, uuid1, getnode

from .__version__ import __version__
from .dag import DAG, CyclicException
from .netprotocol import (
    Address,
    AddressType,
    encode_str,
    read_int,
    read_str,
    uint64_to_bytes,
    Command,
    GRAPHSERVER_ADDR,
    ClientInfo,
    SubscriberInfo,
    PublisherInfo
)

from typing import Dict, List, Tuple, Optional, AsyncGenerator

logger = logging.getLogger('ezmsg')

class VersionMismatch(Exception):
    ...

class GraphServer(Process):
    """ Pub-Sub Directed Acyclic Graph """

    address: Address
    graph: DAG
    clients: Dict[UUID, ClientInfo]
    _client_tasks: Dict[UUID, "asyncio.Task[None]"]
    _command_lock: asyncio.Lock
    _server_up: EventType

    def __init__(self, address: AddressType = GRAPHSERVER_ADDR) -> None:
        super().__init__(daemon=True)
        self.address = Address(*address)
        self.graph = DAG()
        self.clients = dict()
        self._client_tasks = dict()
        self._server_up = Event()
        self._shutdown = Event()

    @classmethod
    async def ensure_running(cls, address: AddressType = GRAPHSERVER_ADDR) -> Optional["GraphServer"]:
        graph_server = None
        address = Address(*address)
        try:
            await cls.open(address)
            logger.debug( f'GraphServer Exists: {address.host}:{address.port}' )
        except ConnectionRefusedError:
            graph_server = cls(address)
            graph_server.start()
            logger.info(f'Started GraphServer. PID:{graph_server.pid}@{address.host}:{address.port}')
        return graph_server

    def run(self) -> None:
        handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        asyncio.run(self._serve())
        signal.signal(signal.SIGINT, handler)

    async def _serve(self) -> None:
        loop = asyncio.get_running_loop()
        self._command_lock = asyncio.Lock()
        server = await asyncio.start_server(self.api, *self.address)

        async def monitor_shutdown() -> None:
            await loop.run_in_executor( None, self._shutdown.wait )
            server.close()

        monitor_task = loop.create_task( monitor_shutdown() )

        self._server_up.set()

        try:
            await server.serve_forever()

        except asyncio.CancelledError: # FIXME: Poor Form
            pass
        
        finally:
            monitor_task.cancel()
            with suppress( asyncio.CancelledError ):
                await monitor_task

    def start(self) -> None:
        super().start()
        self._server_up.wait()

    async def handle_client(self, id: UUID, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:

        logger.debug(f'Graph Server: Client connected: {id}')

        try:
            while True:
                req = await reader.read(1)

                if not req:
                    break

                if req == Command.COMPLETE.value:
                    self.clients[id].set_sync()

        except (ConnectionResetError, BrokenPipeError) as e:
            logger.debug(f'Client {id} disconnected from GraphServer: {e}')
        
        finally:
            self.clients[id].set_sync()
            del self.clients[id]
            writer.close()


    async def api(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            node = await read_int(reader)
            writer.write(encode_str(__version__))
            await writer.drain()

            while True:
                req = await reader.read(1)

                # Empty bytes object means EOF; Client disconnected
                # This happens frequently when future clients are just pinging
                # GraphServer to check if server is up
                if not req:
                    break

                if req == Command.SHUTDOWN.value:
                    self._shutdown.set()
                    break

                # We only want to handle one command at a time
                async with self._command_lock:

                    if req in [
                        Command.SUBSCRIBE.value, 
                        Command.PUBLISH.value 
                    ]:

                        id = uuid1(node=node)
                        writer.write(encode_str(str(id)))

                        pid = await read_int(reader)
                        topic = await read_str(reader)

                        if req == Command.SUBSCRIBE.value:
                            info = SubscriberInfo(id, writer, pid, topic)
                            self.clients[id] = info
                            self._client_tasks[id] = asyncio.create_task(self.handle_client(id, reader, writer))
                            await self._notify_subscriber(info)

                        elif req == Command.PUBLISH.value:
                            address = await Address.from_stream(reader)
                            info = PublisherInfo(id, writer, pid, topic, address)
                            self.clients[id] = info
                            self._client_tasks[id] = asyncio.create_task(self.handle_client(id, reader, writer))
                            for sub in self._downstream_subs(info.topic):
                                await self._notify_subscriber(sub)

                        writer.write(Command.COMPLETE.value)
                        await writer.drain()
                        break

                    elif req in [
                        Command.CONNECT.value,
                        Command.DISCONNECT.value
                    ]:
                        from_topic = await read_str(reader)
                        to_topic = await read_str(reader)

                        cmd = self.graph.add_edge
                        if req == Command.DISCONNECT.value:
                            cmd = self.graph.remove_edge

                        try:
                            cmd(from_topic, to_topic)
                            for sub in self._downstream_subs(to_topic):
                                await self._notify_subscriber(sub)
                            writer.write(Command.COMPLETE.value)
                        except CyclicException:
                            writer.write(Command.CYCLIC.value)

                        await writer.drain()

                    elif req == Command.SYNC.value:
                        for pub in self._publishers():
                            try:
                                async with pub.sync_writer() as pub_writer:
                                    pub_writer.write(Command.SYNC.value)
                            except (ConnectionResetError, BrokenPipeError):
                                continue

                        writer.write(Command.COMPLETE.value)
                        await writer.drain()

                    elif req == Command.PAUSE.value:
                        for pub in self._publishers():
                            pub.writer.write(Command.PAUSE.value)

                    elif req == Command.RESUME.value:
                        for pub in self._publishers():
                            pub.writer.write(Command.RESUME.value)

                    else:
                        logger.warn(f'GraphSerer received unknown command {req}')

        except (ConnectionResetError, BrokenPipeError):
            logger.debug('GraphServer connection fail mid-command')

    async def _notify_subscriber(self, sub: SubscriberInfo) -> None:
        try:
            notification = ''
            for pub in self._upstream_pubs(sub.topic):
                address = pub.address
                if address != None:
                    notification += f'{str(pub.id)}@{address.host}:{address.port},'

            async with sub.sync_writer() as writer:
                writer.write(Command.UPDATE.value)
                writer.write(encode_str(notification))

        except (ConnectionResetError, BrokenPipeError) as e:
            logger.debug(f'Failed to update Subscriber {sub.id}: {e}')

    def _publishers(self) -> List[PublisherInfo]:
        return [info for info in self.clients.values() if isinstance(info, PublisherInfo)]

    def _subscribers(self) -> List[SubscriberInfo]:
        return [info for info in self.clients.values() if isinstance(info, SubscriberInfo)]

    def _upstream_pubs(self, topic: str) -> List[PublisherInfo]:
        """ Given a topic, return a set of all publisher IDs upstream of that topic """
        upstream_topics = self.graph.upstream(topic)
        return [pub for pub in self._publishers() if pub.topic in upstream_topics]

    def _downstream_subs(self, topic: str) -> List[SubscriberInfo]:
        """ Given a topic, return a set of all subscriber IDs upstream of that topic """
        downstream_topics = self.graph.downstream(topic)
        return [sub for sub in self._subscribers() if sub.topic in downstream_topics]

    @staticmethod
    async def open(address: AddressType = GRAPHSERVER_ADDR) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection(*address)
        writer.write(uint64_to_bytes(getnode()))
        await writer.drain()
        server_version = await read_str(reader)
        if server_version != __version__:
            raise VersionMismatch(f"GraphServer@{server_version} != Client@{__version__}")
        return reader, writer

    @dataclass
    class Connection:

        reader: asyncio.StreamReader
        writer: asyncio.StreamWriter

        async def connect(self, from_topic: str, to_topic: str) -> None:
            self.writer.write(Command.CONNECT.value)
            self.writer.write(encode_str(from_topic))
            self.writer.write(encode_str(to_topic))
            await self.writer.drain()
            response = await self.reader.read(1)
            if response == Command.CYCLIC.value:
                raise CyclicException

        async def disconnect(self, from_topic: str, to_topic: str) -> None:
            self.writer.write(Command.DISCONNECT.value)
            self.writer.write(encode_str(from_topic))
            self.writer.write(encode_str(to_topic))
            await self.writer.drain()
            await self.reader.read(1) # Complete

        async def sync(self, timeout: Optional[float] = None) -> None:
            self.writer.write(Command.SYNC.value)
            await self.writer.drain()
            await asyncio.wait_for(self.reader.read(1), timeout=timeout) # Complete

        def pause(self) -> None:
            self.writer.write(Command.PAUSE.value)

        def resume(self) -> None:
            self.writer.write(Command.RESUME.value)

        def shutdown(self) -> None:
            self.writer.write(Command.SHUTDOWN.value)

    @classmethod
    @asynccontextmanager
    async def connection(cls, address: AddressType = GRAPHSERVER_ADDR) -> "AsyncGenerator[GraphServer.Connection, None]":
        reader, writer = await cls.open(address)
        try:
            yield cls.Connection(reader, writer)
        finally:
            writer.close()
