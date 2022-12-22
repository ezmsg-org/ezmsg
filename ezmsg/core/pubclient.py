import asyncio
import logging

from uuid import UUID
from contextlib import suppress

from .backpressure import Backpressure
from .shmserver import SHMContext
from .graphserver import GraphServer
from .graphclient import GraphClient
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
    Response,
    client_socket,
    SubscriberInfo,
    GRAPHSERVER_ADDR
)

from typing import Any, Dict

logger = logging.getLogger('ezmsg')

class Publisher(GraphClient):

    _connection_task: asyncio.Task
    _subscribers: Dict[UUID, SubscriberInfo]
    _address: Address

    _backpressure: Backpressure
    _num_buffers: int
    _unpaused: asyncio.Event
    _msg_id: int = 0
    _shm: SHMContext
    _cache: Cache

    _force_tcp: bool = False
    # TODO: Modes -- auto (prefer shm), force_shm, force_tcp,

    @staticmethod
    def client_type() -> bytes:
        return Command.PUBLISH.value

    @classmethod
    async def create(cls, topic: str, address: AddressType = GRAPHSERVER_ADDR, **kwargs) -> "Publisher":

        reader, writer = await GraphServer.open(address)

        writer.write(Command.NEW_CLIENT.value)
        await writer.drain()
        id_str = await read_str(reader)
        id = UUID(id_str)

        pub = cls(id, topic, **kwargs)
        server = await asyncio.start_server(pub._subscriber_connected, sock=client_socket())
        pub._address = Address(*server.sockets[0].getsockname())
        await pub.connect_to_servers(Address(*address))

        async def serve() -> None:
            try:
                await server.serve_forever()
            except asyncio.CancelledError:
                pass

        pub._connection_task = asyncio.create_task(serve(), name=f'pub_{str(id)}')

        def on_done(_: asyncio.Future) -> None:
            server.close()

        pub._connection_task.add_done_callback(on_done)
        pub._shm = await SHMContext.create(pub._num_buffers)
        pub._cache = Cache(pub._num_buffers)
        MessageCache[id] = pub._cache

        response = await reader.read(1)
        return pub

    def __init__(self, id: UUID, topic: str, num_buffers: int = 32, start_paused: bool = False) -> None:
        super().__init__(id, topic)
        self._subscribers = dict()
        self._unpaused = asyncio.Event()
        if not start_paused:
            self._unpaused.set()
        self._num_buffers = num_buffers
        self._backpressure = Backpressure(num_buffers)

    def close(self) -> None:
        super().close()
        self._shm.close()
        self._connection_task.cancel()
        for sub in self._subscribers.values():
            if sub.task is not None:
                sub.task.cancel()

    async def wait_closed(self) -> None:
        await super().wait_closed()
        await self._shm.wait_closed()
        with suppress(asyncio.CancelledError):
            await self._connection_task
        for sub in self._subscribers.values():
            if sub.task is not None:
                with suppress(asyncio.CancelledError):
                    await sub.task

    async def _handle_graph_command(self, cmd: bytes, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:

        if cmd == Command.ADDRESS.value:
            self._address.to_stream(writer)
            await writer.drain()

        elif cmd == Command.PAUSE.value:
            self.set_paused(True)
            writer.write(Response.OK.value)
            await writer.drain()

        elif cmd == Command.RESUME.value:
            self.set_paused(False)
            writer.write(Response.OK.value)
            await writer.drain()

        elif cmd == Command.SYNC.value:
            await self.sync()
            writer.write(Response.OK.value)
            await writer.drain()

        else:
            logger.warn(f'Publisher {self.id} rx unknown command from GraphServer {cmd}')

    async def _subscriber_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:

        try:
            id_str = await read_str(reader)
            id = UUID(id_str)
            pid = await read_int(reader)
            topic = await read_str(reader)
            writer.write(encode_str(str(self.id)))
            writer.write(uint64_to_bytes(self.pid))
            writer.write(encode_str(self.topic))
            writer.write(uint64_to_bytes(self._num_buffers))
            await writer.drain()

        except BaseException: # FIXME: Poor form
            logger.warn('Subscriber failed to connect')
            raise

        self._subscribers[id] = SubscriberInfo(id, pid, topic, reader, writer, asyncio.current_task())

        try:
            while True:
                msg = await reader.read(1)

                if len(msg) == 0:
                    break
                   
                elif msg == Response.RX_ACK.value:
                    msg_id = await read_int(reader)
                    self._backpressure.free(id, msg_id % self._num_buffers)

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f'Publisher {self.id}: Subscriber {id} connection fail')

        except asyncio.CancelledError: # FIXME: Poor Form
            pass

        finally:
            self._backpressure.free(id)
            del self._subscribers[id]

    async def sync(self) -> None:
        self.set_paused(True)
        await self._backpressure.sync()

    def set_paused(self, paused: bool = True) -> None:
        if paused:
            self._unpaused.clear()
        else:
            self._unpaused.set()

    @property
    def running(self) -> bool:
        return self._unpaused.is_set()

    async def broadcast(self, obj: Any) -> None:

        await self._unpaused.wait()

        buf_idx = self._msg_id % self._num_buffers
        msg_id_bytes = uint64_to_bytes(self._msg_id)

        await self._backpressure.wait(buf_idx)
        self._cache.put(self._msg_id, obj)

        for sub in self._subscribers.values():
            if not self._force_tcp and sub.id.node == self.id.node:

                if sub.pid == self.pid:
                    sub.writer.write(Command.TX_LOCAL.value + msg_id_bytes)

                else:
                    try:
                        # Push to cache to shm (if not already there)
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

            except ConnectionResetError:
                logger.debug(f'Publisher {self.id}: Subscriber {sub.id} connection reset')
                continue

        self._msg_id += 1
