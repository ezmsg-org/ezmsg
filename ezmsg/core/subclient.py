import asyncio
import logging
import pickle
import io

from uuid import UUID
from contextlib import asynccontextmanager, suppress
from copy import deepcopy

from .shmserver import SHMContext
from .graphserver import GraphServer
from .graphclient import GraphClient

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

from typing import Tuple, Dict, List, Any, AsyncGenerator, Optional

logger = logging.getLogger('ezmsg')


class Subscriber(GraphClient):

    _publishers: Dict[UUID, PublisherInfo]
    _shms: Dict[UUID, SHMContext]
    _incoming: "asyncio.Queue[ Tuple[ UUID, bytes, int, bytes ] ]"

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
                try:
                    pub_reader, pub_writer = await asyncio.open_connection(*pub_addresses[id])
                    pub_writer.write(encode_str(str(self.id)))
                    pub_writer.write(encode_str(self.topic))
                    await pub_writer.drain()
                    pub_topic = await read_str(pub_reader)
                except BaseException:
                    logger.warn(f'Error connecting subscriber {self.id} publisher {id}')
                    raise

                task = asyncio.create_task(self._handle_publisher(id, pub_reader))
                self._publishers[id] = PublisherInfo(id, pub_topic, pub_reader, pub_writer, task)

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

    async def _handle_publisher(self, id: UUID, reader: asyncio.StreamReader) -> None:
        try:
            while True:

                msg_type = await reader.read(1)
                if len(msg_type) == 0:
                    break

                idx = await read_int(reader)
                buf_size = await read_int(reader)
                obj_bytes = await reader.readexactly(buf_size)
                self._incoming.put_nowait((id, msg_type, idx, obj_bytes))

        except ConnectionResetError:
            logger.debug(f'Subscriber {self.id}: Publisher {id} connection reset')

        except BrokenPipeError:
            logger.debug(f'Subscriber {self.id}: Publisher {id} broken pipe')

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

        id, msg_type, idx, obj_bytes = await self._incoming.get()

        if msg_type == Command.TX_SHM.value:
            obj_stream = io.BytesIO(obj_bytes)
            shm_name = obj_bytes.decode('utf-8')

            if id not in self._shms or self._shms[id].name != shm_name:
                if id in self._shms:
                    self._shms[id].close()
                    await self._shms[id].wait_closed()
                self._shms[id] = await SHMContext.attach(shm_name)

            with self._shms[id].buffer(idx, readonly=True) as mem:
                # Read header info
                num_buffers = bytes_to_uint(mem[:UINT64_SIZE])

                # Grab buffer sizes
                start_idx: int = UINT64_SIZE
                buf_sizes: List[int] = list()
                for _ in range(num_buffers):
                    buf_sizes.append(bytes_to_uint(mem[start_idx: start_idx + UINT64_SIZE]))
                    start_idx += UINT64_SIZE

                obj: Any = None
                ser_obj: Optional[memoryview] = None
                mem_buffers: List[memoryview] = list()
                try:
                    # Get the pickled object representation
                    ser_obj = mem[start_idx: start_idx + buf_sizes[0]]
                    start_idx += buf_sizes[0]

                    # Extract readonly buffers from the memory view
                    for buf_len in buf_sizes[1:]:
                        buf_mem = mem[start_idx: start_idx + buf_len]
                        mem_buffers.append(buf_mem)
                        start_idx += buf_len

                    # Reconstitute the object
                    obj = pickle.loads(ser_obj, buffers=mem_buffers)
                    yield obj

                except IndexError:
                    logger.error(f"num_buffers: {num_buffers}")
                    logger.error(f"start_idx: {start_idx}, buf_sizes[0]: {buf_sizes[0]}")

                finally:
                    del obj
                    if ser_obj is not None:
                        ser_obj.release()
                    for buffer in mem_buffers:
                        buffer.release()
                    await self._acknowledge(id, idx)

        elif msg_type == Command.TX_TCP.value:
            obj_stream = io.BytesIO(obj_bytes)
            num_buffers = bytes_to_uint(obj_stream.read(UINT64_SIZE))
            buf_sizes = [bytes_to_uint(obj_stream.read(UINT64_SIZE)) for _ in range(num_buffers)]
            bytes_buffers = [obj_stream.read(buf_size) for buf_size in buf_sizes]
            obj = pickle.loads(bytes_buffers[0], buffers=bytes_buffers[1:])

            try:
                yield obj
            finally:
                del obj
                await self._acknowledge(id, idx)

    async def _acknowledge(self, id: UUID, idx: int) -> None:
        try:
            self._publishers[id].writer.write(Response.RX_ACK.value + uint64_to_bytes(idx))
            await self._publishers[id].writer.drain()
        except (BrokenPipeError, ConnectionResetError):
            # These might be totally normal/ok if our pub has concluded already
            logger.debug(f'Subscriber {self.id}: ack fail to Publisher {id}')
