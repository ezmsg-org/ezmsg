import asyncio
import logging
import pickle

from uuid import UUID
from contextlib import suppress
from collections import defaultdict

from .shmserver import SHMContext
from .graphserver import GraphServer
from .graphclient import GraphClient
from .messagecache import MessageCache, Cache
from .messagemarshal import MessageMarshal, UndersizedMemory, UninitializedMemory

from .netprotocol import (
    Address,
    AddressType,
    UINT64_SIZE,
    uint64_to_bytes,
    bytes_to_uint,
    read_int,
    read_str,
    encode_str,
    Command,
    Response,
    client_socket,
    SubscriberInfo,
    GRAPHSERVER_ADDR
)

from typing import DefaultDict, Set, List, Union, Any, Dict, Type

logger = logging.getLogger('ezmsg')


class Publisher(GraphClient):

    _connection_task: asyncio.Task
    _subscribers: Dict[UUID, SubscriberInfo]
    _address: Address

    _backpressure: DefaultDict[int, Set[UUID]]
    _num_buffers: int
    _lowwater: asyncio.Event
    _unpaused: asyncio.Event
    _sync: asyncio.Event
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
        self._backpressure = defaultdict(set)
        self._lowwater = asyncio.Event()
        self._lowwater.set()
        self._unpaused = asyncio.Event()
        if not start_paused:
            self._unpaused.set()
        self._sync = asyncio.Event()
        self._num_buffers = num_buffers

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
            topic = await read_str(reader)
            writer.write(encode_str(self.topic))
            writer.write(uint64_to_bytes(self._num_buffers))
            await writer.drain()

        except BaseException: # FIXME: Poor form
            logger.warn('Subscriber failed to connect')
            raise

        self._subscribers[id] = SubscriberInfo(id, topic, reader, writer, asyncio.current_task())

        try:
            while True:
                msg = await reader.read(1)
                if len(msg) == 0:
                    break

                elif msg == Command.TRANSMIT.value:
                    # Subscriber had a CacheMiss, we need to transmit a message
                    msg_id_bytes = await reader.read(UINT64_SIZE)
                    msg_id = bytes_to_uint(msg_id_bytes)

                    if not self._force_tcp and id.node == self.id.node:
                        try:
                            self._cache.push(msg_id, self._shm)
                        except UndersizedMemory as e:
                            new_shm = await SHMContext.create(self._num_buffers, e.req_size * 2)
                            for buf_idx in range( self._num_buffers ):
                                with self._shm.buffer(buf_idx, readonly=True) as from_buf:
                                    try:
                                        msg_id = MessageMarshal.msg_id(from_buf)
                                        with MessageMarshal.obj_from_mem(from_buf) as obj:
                                            with new_shm.buffer(buf_idx) as to_buf:
                                                MessageMarshal.to_mem(msg_id, obj, to_buf)
                                    except UninitializedMemory:
                                        continue
                            self._shm.close()
                            self._shm = new_shm
                            self._cache.push(msg_id, self._shm)
                        finally:
                            writer.write(Command.TX_SHM.value)
                            writer.write(msg_id_bytes)
                            writer.write(encode_str(self._shm.name))

                    else:
                        with self._cache.get(msg_id) as obj:
                            with MessageMarshal.serialize(msg_id, obj) as ser_obj:
                                total_size, header, buffers = ser_obj
                                total_size_bytes = uint64_to_bytes(total_size)

                                writer.write(Command.TX_TCP.value)
                                writer.write(msg_id_bytes)
                                writer.write(total_size_bytes)
                                writer.write(header)
                                for buffer in buffers:
                                    writer.write(buffer)

                    await writer.drain()
                    
                elif msg == Response.RX_ACK.value:
                    idx = await read_int(reader)
                    self._relieve_backpressure(idx, id)

        except ConnectionResetError:
            logger.debug(f'Publisher {self.id}: Subscriber {id} connection reset')

        except BrokenPipeError:
            logger.debug(f'Publisher {self.id}: Subscriber {id} broken pipe')

        except asyncio.CancelledError:
            pass

        finally:
            for idx in list(self._backpressure):
                self._relieve_backpressure(idx, id)
            del self._subscribers[id]

    @property
    def backpressure(self) -> int:
        return sum(len(p) != 0 for p in self._backpressure.values())

    def _relieve_backpressure(self, idx: int, id: UUID) -> None:
        self._backpressure[idx].discard(id)
        backpressure = self.backpressure
        if backpressure < self._num_buffers // 4:
            self._lowwater.set()
        if backpressure == 0:
            self._sync.set()

    async def sync(self) -> None:
        self.set_paused(True)
        if self.backpressure != 0:
            self._sync.clear()
            await self._sync.wait()

    def set_paused(self, paused: bool = True) -> None:
        if paused:
            self._unpaused.clear()
        else:
            self._unpaused.set()

    @property
    def running(self) -> bool:
        return self._unpaused.is_set()

    async def broadcast(self, obj: Any) -> None:

        # Wait until channel is clear for more messages
        if self.backpressure >= self._num_buffers:
            self._lowwater.clear()
        await self._lowwater.wait()

        # Wait for unpaused state
        await self._unpaused.wait()

        buf_idx = self._msg_id % self._num_buffers
        msg_id_bytes = uint64_to_bytes(self._msg_id)

        # FIXME: wait for no outstanding reads on buf_idx

        self._cache.put(self._msg_id, obj)

        # Alert all subscribers
        for sub in self._subscribers.values():
            try:
                sub.writer.write(Command.MSG.value + msg_id_bytes)
                await sub.writer.drain()
                self._backpressure[buf_idx].add(sub.id)

            except ConnectionResetError:
                logger.debug(f'Publisher {self.id}: Subscriber {sub.id} connection reset')
                continue

        self._msg_id += 1
