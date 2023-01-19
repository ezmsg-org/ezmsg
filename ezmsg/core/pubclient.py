import os
import asyncio
import logging
import socket

from uuid import UUID
from contextlib import suppress

from .backpressure import Backpressure
from .shmserver import SHMContext
from .graphserver import GraphServer
from .messagecache import MessageCache, Cache
from .messagemarshal import MessageMarshal, UndersizedMemory

from .netprotocol import (
    Address,
    AddressType,
    uint64_to_bytes,
    read_int,
    read_str,
    encode_str,
    Command,
    SubscriberInfo,
    GRAPHSERVER_ADDR,
    DEFAULT_SHM_SIZE,
    PUBLISHER_START_PORT,
)

from typing import Any, Dict, Optional

logger = logging.getLogger('ezmsg')

class Publisher:

    id: UUID
    pid: int
    topic: str

    _initialized: asyncio.Event
    _graph_task: "asyncio.Task[None]"
    _connection_task: "asyncio.Task[None]"
    _subscribers: Dict[UUID, SubscriberInfo]
    _subscriber_tasks: Dict[UUID, "asyncio.Task[None]"]
    _address: Address
    _backpressure: Backpressure
    _num_buffers: int
    _running: asyncio.Event
    _msg_id: int
    _shm: SHMContext
    _cache: Cache
    _force_tcp: bool

    @staticmethod
    def client_type() -> bytes:
        return Command.PUBLISH.value

    @classmethod
    async def create(
        cls, 
        topic: str, 
        address: AddressType = GRAPHSERVER_ADDR, 
        host: Optional[str] = None,
        port: Optional[int] = None,
        buf_size: int = DEFAULT_SHM_SIZE, 
        **kwargs
    ) -> "Publisher":

        reader, writer = await GraphServer.open(address)
        writer.write(Command.PUBLISH.value)
        id = UUID(await read_str(reader))
        pub = cls(id, topic, **kwargs)
        writer.write(uint64_to_bytes(pub.pid))
        writer.write(encode_str(pub.topic))
        pub._shm = await SHMContext.create(pub._num_buffers, buf_size)
        
        server = await asyncio.start_server(pub._on_connection, sock=_create_socket(host, port))
        pub._address = Address(*server.sockets[0].getsockname())
        pub._address.to_stream(writer)
        pub._graph_task = asyncio.create_task(pub._graph_connection(reader, writer))

        async def serve() -> None:
            try:
                await server.serve_forever()
            except asyncio.CancelledError: #FIXME: Poor form?
                pass

        pub._connection_task = asyncio.create_task(serve(), name=f'pub_{str(id)}')

        def on_done(_: asyncio.Future) -> None:
            server.close()

        pub._connection_task.add_done_callback(on_done)
        pub._cache = Cache(pub._num_buffers)
        MessageCache[id] = pub._cache
        await pub._initialized.wait()
        return pub

    def __init__(
        self, 
        id: UUID, 
        topic: str, 
        num_buffers: int = 32, 
        start_paused: bool = False, 
        force_tcp: bool = False
    ) -> None:
        self.id = id
        self.pid = os.getpid()
        self.topic = topic

        self._msg_id = 0
        self._subscribers = dict()
        self._subscriber_tasks = dict()
        self._running = asyncio.Event()
        if not start_paused:
            self._running.set()
        self._num_buffers = num_buffers
        self._backpressure = Backpressure(num_buffers)
        self._force_tcp = force_tcp
        self._initialized = asyncio.Event()

    def close(self) -> None:
        self._graph_task.cancel()
        self._shm.close()
        self._connection_task.cancel()
        for task in self._subscriber_tasks.values():
            task.cancel()

    async def wait_closed(self) -> None:
        await self._shm.wait_closed()
        with suppress(asyncio.CancelledError):
            await self._graph_task
        with suppress(asyncio.CancelledError):
            await self._connection_task
        for task in self._subscriber_tasks.values():
            with suppress(asyncio.CancelledError):
                await task

    async def _graph_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:

        try:
            while True:
                cmd = await reader.read(1)
                if not cmd:
                    break

                elif cmd == Command.COMPLETE.value:
                    self._initialized.set()

                elif cmd == Command.PAUSE.value:
                    self._running.clear()

                elif cmd == Command.RESUME.value:
                    self._running.set()

                elif cmd == Command.SYNC.value:
                    await self.sync()
                    writer.write(Command.COMPLETE.value)

                else:
                    logger.warn(f'Publisher {self.id} rx unknown command from GraphServer {cmd}')
                
                await writer.drain()
            
        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f'Publisher {self.id} lost connection to graph server')

        finally:
            writer.close()

    async def _on_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        id_str = await read_str(reader)
        id = UUID(id_str)
        pid = await read_int(reader)
        topic = await read_str(reader)

        writer.write(encode_str(str(self.id)))
        writer.write(uint64_to_bytes(self.pid))
        writer.write(encode_str(self.topic))
        writer.write(uint64_to_bytes(self._num_buffers))

        info = SubscriberInfo(id, writer, pid, topic)
        coro = self._handle_subscriber(info, reader)
        self._subscriber_tasks[id] = asyncio.create_task(coro)

        await writer.drain()

    async def _handle_subscriber(self, info: SubscriberInfo, reader: asyncio.StreamReader) -> None:

        self._subscribers[info.id] = info

        try:
            while True:
                msg = await reader.read(1)

                if len(msg) == 0:
                    break
                   
                elif msg == Command.RX_ACK.value:
                    msg_id = await read_int(reader)
                    self._backpressure.free(info.id, msg_id % self._num_buffers)

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f'Publisher {self.id}: Subscriber {id} connection fail')

        finally:
            self._backpressure.free(info.id)
            del self._subscribers[info.id]

    async def sync(self) -> None:
        """ Pause and drain backpressure """
        self._running.clear()
        await self._backpressure.sync()

    @property
    def running(self) -> bool:
        return self._running.is_set()

    def pause(self) -> None:
        self._running.clear()

    def resume(self) -> None:
        self._running.set()

    async def broadcast(self, obj: Any) -> None:

        await self._running.wait()

        buf_idx = self._msg_id % self._num_buffers
        msg_id_bytes = uint64_to_bytes(self._msg_id)

        if not self._backpressure.available(buf_idx):
            logger.warning(f'{self.topic} under subscriber backpressure!')
            await self._backpressure.wait(buf_idx)

        self._cache.put(self._msg_id, obj)

        for sub in list(self._subscribers.values()):
            if not self._force_tcp and sub.id.node == self.id.node:

                if sub.pid == self.pid:
                    sub.writer.write(Command.TX_LOCAL.value + msg_id_bytes)

                else:
                    try:
                        # Push cache to shm (if not already there)
                        self._cache.push(self._msg_id, self._shm)

                    except UndersizedMemory as e:
                        new_shm = await SHMContext.create(self._num_buffers, e.req_size * 2)
                        
                        for i in range(self._num_buffers):
                            with self._shm.buffer(i, readonly=True) as from_buf:
                                with new_shm.buffer(i) as to_buf:
                                    MessageMarshal.copy_obj(from_buf, to_buf)

                        self._shm.close()
                        self._shm = new_shm
                        self._cache.push(self._msg_id, self._shm)

                    sub.writer.write(Command.TX_SHM.value)
                    sub.writer.write(msg_id_bytes)
                    sub.writer.write(encode_str(self._shm.name))

            else:
                with MessageMarshal.serialize(self._msg_id, obj) as ser_obj:
                    total_size, header, buffers = ser_obj
                    total_size_bytes = uint64_to_bytes(total_size)

                    sub.writer.write(Command.TX_TCP.value)
                    sub.writer.write(msg_id_bytes)
                    sub.writer.write(total_size_bytes)
                    sub.writer.write(header)
                    for buffer in buffers:
                        sub.writer.write(buffer)

            try:
                await sub.writer.drain()
                self._backpressure.lease(sub.id, buf_idx)

            except (ConnectionResetError, BrokenPipeError):
                logger.debug(f'Publisher {self.id}: Subscriber {sub.id} connection fail')
                continue

        self._msg_id += 1

def _create_socket(
    host: Optional[str] = None, 
    port: Optional[int] = None, 
    start_port: int = PUBLISHER_START_PORT, 
    max_port: int = 65535
) -> socket.socket:

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    if host is None:
        host = '127.0.0.1'

    if port is not None:
        sock.bind((host,port))
        return sock

    port = start_port
    while port <= max_port:
        try:
            sock.bind((host, port))
            return sock
        except OSError:
            port += 1

    raise IOError('Failed to bind socket; no free ports')