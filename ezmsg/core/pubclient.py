import asyncio
import logging
import pickle

from uuid import UUID
from contextlib import suppress
from collections import defaultdict

from .shmserver import SHMContext
from .graphserver import GraphServer
from .graphclient import GraphClient

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

from typing import DefaultDict, Set, List, Union, Any, Dict

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
    _cur_msg: int = 0

    _shm: SHMContext

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
            await writer.drain()

        except BaseException:
            logger.warn('Subscriber failed to connect')
            raise

        self._subscribers[id] = SubscriberInfo(id, topic, reader, writer, asyncio.current_task())

        try:
            while True:
                msg = await reader.read(1)
                if len(msg) == 0:
                    break
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

        # Pickle object, but not its buffers
        obj_buffers: List[pickle.PickleBuffer] = list()
        ser_obj = pickle.dumps(obj, protocol=5, buffer_callback=obj_buffers.append)

        # Create a header and calculate buffer lengths
        buffers: List[Union[bytes, memoryview]] = [ser_obj] + [b.raw() for b in obj_buffers]
        header = uint64_to_bytes(len(buffers))
        buf_lengths = [len(buf) for buf in buffers]
        header_chunks = [header] + [uint64_to_bytes(b) for b in buf_lengths]
        header = b''.join(header_chunks)
        header_len = len(header)
        total_size = header_len + sum(buf_lengths)
        total_size_bytes = uint64_to_bytes(total_size)

        # Make sure message fits in SHM
        if self._shm.buf_size <= total_size:
            self._shm.close()
            await self._shm.wait_closed()
            self._shm = await SHMContext.create(self._num_buffers, total_size * 2)

        # Wait until channel is clear for more messages
        if self.backpressure >= self._num_buffers:
            self._lowwater.clear()
        await self._lowwater.wait()

        # Wait for unpaused state
        await self._unpaused.wait()

        # Copy data into SHM
        with self._shm.buffer(self._cur_msg) as mem:
            mem[:header_len] = header[:]
            start_idx = header_len
            for buf, buf_len in zip(buffers, buf_lengths):
                mem[start_idx: start_idx + buf_len] = buf[:]
                start_idx += buf_len

        # Transmit Payload
        idx_bytes = uint64_to_bytes(self._cur_msg)
        shm_name_bytes = encode_str(self._shm.name)
        for sub in self._subscribers.values():
            try:
                if not self._force_tcp and sub.id.node == self.id.node:
                    sub.writer.write(Command.TX_SHM.value)
                    sub.writer.write(idx_bytes)
                    sub.writer.write(shm_name_bytes)
                else:
                    sub.writer.write(Command.TX_TCP.value + idx_bytes + total_size_bytes + header)
                    for buffer in buffers:
                        sub.writer.write(buffer)
                await sub.writer.drain()
                self._backpressure[self._cur_msg].add(sub.id)

            except ConnectionResetError:
                logger.debug(f'Publisher {self.id}: Subscriber {sub.id} connection reset')
                continue

        self._cur_msg = (self._cur_msg + 1) % self._num_buffers
