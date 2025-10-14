import os
import asyncio
from collections.abc import AsyncGenerator
import logging
import typing

from uuid import UUID
from contextlib import asynccontextmanager, suppress
from copy import deepcopy

from .graphserver import GraphService
from .shm import SHMContext
from .messagecache import MessageCache, Cache
from .messagemarshal import MessageMarshal

from .netprotocol import (
    Address,
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
    """
    A subscriber client for receiving messages from publishers.
    
    Subscriber manages connections to multiple publishers, handles different
    transport methods (local, shared memory, TCP), and provides both copying
    and zero-copy message access patterns with automatic acknowledgment.
    """
    id: UUID
    pid: int
    topic: str

    _initialized: asyncio.Event
    _graph_task: "asyncio.Task[None]"
    _publishers: dict[UUID, PublisherInfo]
    _publisher_tasks: dict[UUID, "asyncio.Task[None]"]
    _shms: dict[UUID, SHMContext]
    _incoming: "asyncio.Queue[tuple[UUID, int]]"

    _graph_service: GraphService

    @classmethod
    async def create(
        cls, topic: str, graph_service: GraphService, **kwargs
    ) -> "Subscriber":
        """
        Create a new Subscriber instance and register it with the graph server.
        
        :param topic: The topic this subscriber will listen to.
        :type topic: str
        :param graph_service: Service for graph server communication.
        :type graph_service: GraphService
        :param shm_service: Service for shared memory management.
        :type shm_service: SHMService
        :param kwargs: Additional keyword arguments for Subscriber constructor.
        :return: Initialized and registered Subscriber instance.
        :rtype: Subscriber
        """
        reader, writer = await graph_service.open_connection()
        writer.write(Command.SUBSCRIBE.value)
        id_str = await read_str(reader)
        sub = cls(UUID(id_str), topic, graph_service, **kwargs)
        writer.write(uint64_to_bytes(sub.pid))
        writer.write(encode_str(sub.topic))
        sub._graph_task = asyncio.create_task(sub._graph_connection(reader, writer))
        await sub._initialized.wait()
        return sub

    def __init__(
        self, id: UUID, topic: str, graph_service: GraphService, **kwargs
    ) -> None:
        """
        Initialize a Subscriber instance.
        
        :param id: Unique identifier for this subscriber.
        :type id: UUID
        :param topic: The topic this subscriber listens to.
        :type topic: str
        :param graph_service: Service for graph operations.
        :type graph_service: GraphService
        :param kwargs: Additional keyword arguments (unused).
        """
        self.id = id
        self.pid = os.getpid()
        self.topic = topic

        self._publishers = dict()
        self._publisher_tasks = dict()
        self._shms = dict()
        self._incoming = asyncio.Queue()
        self._initialized = asyncio.Event()

        self._graph_service = graph_service

    def close(self) -> None:
        """
        Close the subscriber and cancel all associated tasks.
        
        Cancels graph connection, all publisher connection tasks,
        and closes all shared memory contexts.
        """
        self._graph_task.cancel()
        for task in self._publisher_tasks.values():
            task.cancel()
        for shm in self._shms.values():
            shm.close()

    async def wait_closed(self) -> None:
        """
        Wait for all subscriber resources to be fully closed.
        
        Waits for graph connection termination, all publisher connection
        tasks to complete, and all shared memory contexts to close.
        """
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
        """
        Handle communication with the graph server.
        
        Processes commands from the graph server including COMPLETE and UPDATE
        operations for managing publisher connections.
        
        :param reader: Stream reader for receiving commands from graph server.
        :type reader: asyncio.StreamReader
        :param writer: Stream writer for responding to graph server.
        :type writer: asyncio.StreamWriter
        """
        try:
            while True:
                cmd = await reader.read(1)
                if not cmd:
                    break

                elif cmd == Command.COMPLETE.value:
                    self._initialized.set()

                elif cmd == Command.UPDATE.value:
                    pub_addresses: dict[UUID, Address] = {}
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
        """
        Handle communication with a specific publisher.
        
        Establishes connection, exchanges identification, and processes
        incoming messages from the publisher using various transport methods.
        
        :param id: Unique identifier of the publisher.
        :type id: UUID
        :param address: Network address of the publisher.
        :type address: Address
        :param connected: Event to signal when connection is established.
        :type connected: asyncio.Event
        :raises ValueError: If publisher ID doesn't match expected ID.
        """
        reader, writer = await asyncio.open_connection(*address)
        writer.write(encode_str(str(self.id)))
        writer.write(uint64_to_bytes(self.pid))
        writer.write(encode_str(self.topic))
        await writer.drain()

        # Pub replies with current shm name
        # We attempt to attach and let pub know if we have SHM access
        shm_name = await read_str(reader)
        try:
            (await self._graph_service.attach_shm(shm_name)).close()
            writer.write(uint64_to_bytes(1))
        except (ValueError, OSError):
            writer.write(uint64_to_bytes(0))
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
                        try:
                            self._shms[id] = await self._graph_service.attach_shm(
                                shm_name
                            )
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
        """
        Receive the next message with a deep copy.
        
        This method creates a deep copy of the received message, allowing
        safe modification without affecting the original cached message.
        
        :return: Deep copy of the received message.
        :rtype: typing.Any
        """
        out_msg = None
        async with self.recv_zero_copy() as msg:
            out_msg = deepcopy(msg)
        return out_msg

    @asynccontextmanager
    async def recv_zero_copy(self) -> AsyncGenerator[typing.Any, None]:
        """
        Receive the next message with zero-copy access.
        
        This context manager provides direct access to the cached message
        without copying. The message should not be modified or stored beyond
        the context manager's scope.
        
        :return: Context manager yielding the received message.
        :rtype: collections.abc.AsyncGenerator[typing.Any, None]
        """
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
