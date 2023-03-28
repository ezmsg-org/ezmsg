import os
import asyncio
import logging
import typing

from uuid import UUID
from contextlib import asynccontextmanager, suppress
from copy import deepcopy

from .graphserver import GraphService
from .shmserver import SHMContext, SHMService
from .messagecache import MessageCache, Cache
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
    close_stream_writer,
    Command,
    PublisherInfo,
)


logger = logging.getLogger("ezmsg")


class Subscriber:
    id: UUID
    pid: int
    topic: str

    _initialized: asyncio.Event
    _graph_task: "asyncio.Task[None]"
    _publishers: typing.Dict[UUID, PublisherInfo]
    _publisher_tasks: typing.Dict[UUID, "asyncio.Task[None]"]
    _shms: typing.Dict[UUID, SHMContext]
    _incoming: "asyncio.Queue[typing.Tuple[UUID, int]]"

    _shm_service: SHMService

    @classmethod
    async def create(
        cls, topic: str, graph_service: GraphService, shm_service: SHMService, **kwargs
    ) -> "Subscriber":
        reader, writer = await graph_service.open_connection()
        writer.write(Command.SUBSCRIBE.value)
        id_str = await read_str(reader)
        sub = cls(UUID(id_str), topic, shm_service, **kwargs)
        writer.write(uint64_to_bytes(sub.pid))
        writer.write(encode_str(sub.topic))
        sub._graph_task = asyncio.create_task(sub._graph_connection(reader, writer))
        await sub._initialized.wait()
        return sub

    def __init__(self, id: UUID, topic: str, shm_service: SHMService, **kwargs) -> None:
        self.id = id
        self.pid = os.getpid()
        self.topic = topic

        self._publishers = dict()
        self._publisher_tasks = dict()
        self._shms = dict()
        self._incoming = asyncio.Queue()
        self._initialized = asyncio.Event()

        self._shm_service = shm_service

    def close(self) -> None:
        self._graph_task.cancel()
        for task in self._publisher_tasks.values():
            task.cancel()
        for shm in self._shms.values():
            shm.close()

    async def wait_closed(self) -> None:
        with suppress(asyncio.CancelledError):
            await self._graph_task
        for task in self._publisher_tasks.values():
            with suppress(asyncio.CancelledError):
                await task
        for shm in self._shms.values():
            await shm.wait_closed()

    async def _graph_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                cmd = await reader.read(1)
                if not cmd:
                    break

                elif cmd == Command.COMPLETE.value:
                    self._initialized.set()

                elif cmd == Command.UPDATE.value:
                    pub_addresses: typing.Dict[UUID, Address] = {}
                    connections = await read_str(reader)
                    connections = connections.strip(",")
                    if len(connections):
                        for connection in connections.split(","):
                            pub_id, pub_address = connection.split("@")
                            pub_id = UUID(pub_id)
                            pub_address = Address.from_string(pub_address)
                            pub_addresses[pub_id] = pub_address

                    for id in set(pub_addresses.keys() - self._publishers.keys()):
                        connected = asyncio.Event()
                        coro = self._handle_publisher(id, pub_addresses[id], connected)
                        task_name = f"sub{self.id}:_handle_publisher({id})"
                        self._publisher_tasks[id] = asyncio.create_task(
                            coro, name=task_name
                        )
                        await connected.wait()

                    for id in set(self._publishers.keys() - pub_addresses.keys()):
                        self._publisher_tasks[id].cancel()
                        with suppress(asyncio.CancelledError):
                            await self._publisher_tasks[id]

                    writer.write(Command.COMPLETE.value)
                    await writer.drain()

                else:
                    logger.warning(
                        f"Subscriber {self.id} rx unknown command from GraphServer: {cmd}"
                    )

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Subscriber {self.id} lost connection to graph server")

        finally:
            await close_stream_writer(writer)

    async def _handle_publisher(
        self, id: UUID, address: Address, connected: asyncio.Event
    ) -> None:
        reader, writer = await asyncio.open_connection(*address)
        writer.write(encode_str(str(self.id)))
        writer.write(uint64_to_bytes(self.pid))
        writer.write(encode_str(self.topic))
        await writer.drain()
        pub_id_str = await read_str(reader)
        pub_pid = await read_int(reader)
        pub_topic = await read_str(reader)
        num_buffers = await read_int(reader)

        if id != UUID(pub_id_str):
            raise ValueError("Unexpected Publisher ID")

        # NOTE: Not thread safe
        if id not in MessageCache:
            MessageCache[id] = Cache(num_buffers)

        self._publishers[id] = PublisherInfo(id, writer, pub_pid, pub_topic, address)

        connected.set()

        try:
            while True:
                msg = await reader.read(1)
                if not msg:
                    break

                msg_id_bytes = await reader.read(UINT64_SIZE)
                msg_id = bytes_to_uint(msg_id_bytes)

                if msg == Command.TX_SHM.value:
                    shm_name = await read_str(reader)

                    if id not in self._shms or self._shms[id].name != shm_name:
                        if id in self._shms:
                            self._shms[id].close()
                            await self._shms[id].wait_closed()
                        try:
                            self._shms[id] = await self._shm_service.attach(shm_name)
                        except ValueError:
                            logger.info(
                                "Invalid SHM received from publisher; may be dead"
                            )
                            raise

                # FIXME: TCP connections could be more efficient.
                # https://github.com/iscoe/ezmsg/issues/5
                elif msg == Command.TX_TCP.value:
                    buf_size = await read_int(reader)
                    obj_bytes = await reader.readexactly(buf_size)

                    with MessageMarshal.obj_from_mem(memoryview(obj_bytes)) as obj:
                        MessageCache[id].put(msg_id, obj)

                self._incoming.put_nowait((id, msg_id))

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"connection fail: sub:{self.id} -> pub:{id}")

        finally:
            await close_stream_writer(self._publishers[id].writer)
            del self._publishers[id]
            logger.debug(f"disconnected: sub:{self.id} -> pub:{id}")

    async def recv(self) -> typing.Any:
        out_msg = None
        async with self.recv_zero_copy() as msg:
            out_msg = deepcopy(msg)
        return out_msg

    @asynccontextmanager
    async def recv_zero_copy(self) -> typing.AsyncGenerator[typing.Any, None]:
        id, msg_id = await self._incoming.get()
        msg_id_bytes = uint64_to_bytes(msg_id)

        try:
            shm = self._shms.get(id, None)
            with MessageCache[id].get(msg_id, shm) as msg:
                yield msg

        finally:
            if id in self._publishers:
                try:
                    ack = Command.RX_ACK.value + msg_id_bytes
                    self._publishers[id].writer.write(ack)
                    await self._publishers[id].writer.drain()
                except (BrokenPipeError, ConnectionResetError):
                    logger.debug(f"ack fail: sub:{self.id} -> pub:{id}")
