import asyncio
import logging

from uuid import UUID
from contextlib import asynccontextmanager, suppress
from copy import deepcopy

from .shmserver import SHMContext
from .graphserver import GraphServer
from .graphclient import GraphClient
from .messagecache import MessageCache, CacheMiss, Cache
from .messagemarshal import MessageMarshal

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
    PublisherInfo,
    GRAPHSERVER_ADDR
)

from typing import Tuple, Dict, Any, AsyncGenerator, Set

logger = logging.getLogger('ezmsg')


class Subscriber(GraphClient):

    _publishers: Dict[UUID, PublisherInfo]
    _shms: Dict[UUID, SHMContext]
    _incoming: "asyncio.Queue[Tuple[UUID, int]]"
    _tasks: Set[asyncio.Task]

    @staticmethod
    def client_type() -> bytes:
        return Command.SUBSCRIBE.value

    @classmethod
    async def create(cls, topic: str, address: AddressType = GRAPHSERVER_ADDR, **kwargs) -> "Subscriber":
        reader, writer = await GraphServer.open(address)
        writer.write(Command.NEW_CLIENT.value)
        await writer.drain()
        id_str = await read_str(reader)
        id = UUID(id_str)
        sub = cls(id, topic, **kwargs)
        await sub.connect_to_servers(Address(*address))
        response = await reader.read(1)
        return sub

    def __init__(self, id: UUID, topic: str) -> None:
        super().__init__(id, topic)
        self._publishers = dict()
        self._shms = dict()
        self._incoming = asyncio.Queue()
        self._tasks = set()

    def close(self) -> None:
        super().close()
        for pub in self._publishers.values():
            if pub.task is not None:
                pub.task.cancel()
        for shm in self._shms.values():
            shm.close()

    async def wait_closed(self) -> None:
        await super().wait_closed()
        for pub in self._publishers.values():
            if pub.task is not None:
                with suppress(asyncio.CancelledError):
                    await pub.task
        for shm in self._shms.values():
            await shm.wait_closed()

    async def _handle_graph_command(self, cmd: bytes, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:

        if cmd == Command.UPDATE.value:

            pub_addresses: Dict[UUID, Address] = {}
            connections = await read_str(reader)
            connections = connections.strip(',')
            if len(connections):
                for connection in connections.split(','):
                    pub_id, pub_address = connection.split('@')
                    pub_id = UUID(pub_id)
                    pub_address = Address.from_string(pub_address)
                    pub_addresses[pub_id] = pub_address

            for id in set(pub_addresses.keys() - self._publishers.keys()):
                coro = self._handle_publisher(id, pub_addresses[id])
                task_name = f'sub{self.id}:_handle_publisher({id})'
                task = asyncio.create_task(coro, name = task_name)
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

            for id in set(self._publishers.keys() - pub_addresses.keys()):
                await self._disconnect_publisher(id)

            writer.write(Response.OK.value)
            await writer.drain()

        else:
            logger.warning(f'Subscriber {self.id} rx unknown command from GraphServer: {cmd}')

    async def _disconnect_publisher(self, id: UUID):
        if id in self._publishers:
            task = self._publishers[id].task
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    async def _handle_publisher(self, id: UUID, address: Address) -> None:

        reader, writer = await asyncio.open_connection(*address)
        writer.write(encode_str(str(self.id)))
        writer.write(encode_str(self.topic))
        await writer.drain()
        pub_topic = await read_str(reader)
        num_buffers = await read_int(reader)

        # NOTE: Not thread safe
        if id not in MessageCache:
            MessageCache[id] = Cache(num_buffers)

        self._publishers[id] = PublisherInfo(id, pub_topic, reader, writer, asyncio.current_task())

        try:
            while True:

                msg = await reader.read(1)
                if len(msg) == 0:
                    break

                msg_id_bytes = await reader.read( UINT64_SIZE )
                msg_id = bytes_to_uint(msg_id_bytes)

                if msg == Command.TX_SHM.value:
                    shm_name = await read_str(reader)

                    if id not in self._shms or self._shms[id].name != shm_name:
                        if id in self._shms:
                            self._shms[id].close()
                            await self._shms[id].wait_closed()
                        self._shms[id] = await SHMContext.attach(shm_name)

                if msg == Command.TX_TCP.value:
                    buf_size = await read_int(reader)
                    obj_bytes = await reader.readexactly(buf_size)

                    with MessageMarshal.obj_from_mem(memoryview(obj_bytes)) as obj:
                        MessageCache[id].put(msg_id, deepcopy(obj))

                self._incoming.put_nowait((id, msg_id))
                
        except (ConnectionResetError, BrokenPipeError):
            logger.info(f'connection fail: sub:{self.id} -> pub:{id}')

        finally:
            self._publishers[id].writer.close()
            del self._publishers[id]

    async def recv(self) -> Any:
        out_msg = None
        async with self.recv_zero_copy() as msg:
            out_msg = deepcopy(msg)
        return out_msg

    @asynccontextmanager
    async def recv_zero_copy(self) -> AsyncGenerator[Any, None]:

        while True:
            id, msg_id = await self._incoming.get()
            msg_id_bytes = uint64_to_bytes(msg_id)

            try:
                shm = self._shms.get(id, None)
                with MessageCache[id].get(msg_id, shm) as msg:
                    yield msg

                ack = Response.RX_ACK.value + msg_id_bytes
                self._publishers[id].writer.write(ack)
                break

            except CacheMiss:
                response = Command.TRANSMIT.value + msg_id_bytes
                self._publishers[id].writer.write(response)
                
            finally:
                try:
                    await self._publishers[id].writer.drain()
                except (BrokenPipeError, ConnectionResetError):
                    logger.info(f'connection fail: sub:{self.id} -> pub:{id}')


            
