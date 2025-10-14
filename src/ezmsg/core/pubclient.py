import os
import asyncio
import logging
import time

from uuid import UUID
from contextlib import suppress

from .backpressure import Backpressure
from .shm import SHMContext
from .graphserver import GraphService
from .messagecache import MessageCache, Cache
from .messagemarshal import MessageMarshal, UndersizedMemory

from .netprotocol import (
    Address,
    uint64_to_bytes,
    read_int,
    read_str,
    encode_str,
    close_stream_writer,
    close_server,
    Command,
    SubscriberInfo,
    create_socket,
    DEFAULT_SHM_SIZE,
    PUBLISHER_START_PORT_ENV,
    PUBLISHER_START_PORT_DEFAULT,
)

from typing import Any

logger = logging.getLogger("ezmsg")

BACKPRESSURE_WARNING = "EZMSG_DISABLE_BACKPRESSURE_WARNING" not in os.environ
BACKPRESSURE_REFRACTORY = 5.0  # sec


class Publisher:
    """
    A publisher client for broadcasting messages to subscribers.
    
    Publisher manages shared memory allocation, connection handling with subscribers,
    backpressure control, and supports both shared memory and TCP transport methods.
    Messages are broadcast to all connected subscribers with automatic cleanup
    and resource management.
    """
    id: UUID
    pid: int
    topic: str

    _initialized: asyncio.Event
    _graph_task: "asyncio.Task[None]"
    _connection_task: "asyncio.Task[None]"
    _subscribers: dict[UUID, SubscriberInfo]
    _subscriber_tasks: dict[UUID, "asyncio.Task[None]"]
    _address: Address
    _backpressure: Backpressure
    _num_buffers: int
    _running: asyncio.Event
    _msg_id: int
    _shm: SHMContext
    _force_tcp: bool
    _last_backpressure_event: float

    _graph_service: GraphService

    @staticmethod
    def client_type() -> bytes:
        """
        Get the client type identifier for publishers.
        
        :return: Command byte identifying this as a publisher client.
        :rtype: bytes
        """
        return Command.PUBLISH.value

    @classmethod
    async def create(
        cls,
        topic: str,
        graph_service: GraphService,
        host: str | None = None,
        port: int | None = None,
        buf_size: int = DEFAULT_SHM_SIZE,
        **kwargs,
    ) -> "Publisher":
        """
        Create a new Publisher instance and register it with the graph server.
        
        :param topic: The topic this publisher will broadcast to.
        :type topic: str
        :param graph_service: Service for graph server communication.
        :type graph_service: GraphService
        :param shm_service: Service for shared memory management.
        :type shm_service: SHMService
        :param host: Optional host address to bind to.
        :type host: str | None
        :param port: Optional port number to bind to.
        :type port: int | None
        :param buf_size: Size of shared memory buffers.
        :type buf_size: int
        :param kwargs: Additional keyword arguments for Publisher constructor.
        :return: Initialized and registered Publisher instance.
        :rtype: Publisher
        """
        reader, writer = await graph_service.open_connection()
        writer.write(Command.PUBLISH.value)
        id = UUID(await read_str(reader))
        pub = cls(id, topic, graph_service, **kwargs)
        writer.write(uint64_to_bytes(pub.pid))
        writer.write(encode_str(pub.topic))
        pub._shm = await graph_service.create_shm(pub._num_buffers, buf_size)

        start_port = int(
            os.getenv(PUBLISHER_START_PORT_ENV, PUBLISHER_START_PORT_DEFAULT)
        )
        sock = create_socket(host, port, start_port=start_port)
        server = await asyncio.start_server(pub._on_connection, sock=sock)
        pub._address = Address(*sock.getsockname())
        pub._address.to_stream(writer)
        pub._graph_task = asyncio.create_task(pub._graph_connection(reader, writer))

        async def serve() -> None:
            try:
                await server.serve_forever()
            except asyncio.CancelledError:  # FIXME: Poor form?
                logger.debug("pubclient serve is Cancelled...")
            finally:
                await close_server(server)

        pub._connection_task = asyncio.create_task(serve(), name=f"pub_{str(id)}")

        def on_done(_: asyncio.Future) -> None:
            logger.debug("Closing pub server task.")

        pub._connection_task.add_done_callback(on_done)
        MessageCache[id] = Cache(pub._num_buffers)
        await pub._initialized.wait()
        return pub

    def __init__(
        self,
        id: UUID,
        topic: str,
        graph_service: GraphService,
        num_buffers: int = 32,
        start_paused: bool = False,
        force_tcp: bool = False,
    ) -> None:
        """
        Initialize a Publisher instance.
        
        :param id: Unique identifier for this publisher.
        :type id: UUID
        :param topic: The topic this publisher broadcasts to.
        :type topic: str
        :param shm_service: Service for shared memory operations.
        :type shm_service: SHMService
        :param num_buffers: Number of buffers for message buffering.
        :type num_buffers: int
        :param start_paused: Whether to start in paused state.
        :type start_paused: bool
        :param force_tcp: Whether to force TCP transport instead of shared memory.
        :type force_tcp: bool
        """
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
        self._last_backpressure_event = -1

        self._graph_service = graph_service

    def close(self) -> None:
        """
        Close the publisher and cancel all associated tasks.
        
        Cancels graph connection, shared memory, connection server,
        and all subscriber handling tasks.
        """
        self._graph_task.cancel()
        self._shm.close()
        self._connection_task.cancel()
        for task in self._subscriber_tasks.values():
            task.cancel()

    async def wait_closed(self) -> None:
        """
        Wait for all publisher resources to be fully closed.
        
        Waits for shared memory cleanup, graph connection termination,
        connection server shutdown, and all subscriber tasks to complete.
        """
        await self._shm.wait_closed()
        with suppress(asyncio.CancelledError):
            await self._graph_task
        with suppress(asyncio.CancelledError):
            await self._connection_task
        for task in self._subscriber_tasks.values():
            with suppress(asyncio.CancelledError):
                await task

    async def _graph_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """
        Handle communication with the graph server.
        
        Processes commands from the graph server including COMPLETE, PAUSE,
        RESUME, and SYNC operations.
        
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

                elif cmd == Command.PAUSE.value:
                    self._running.clear()

                elif cmd == Command.RESUME.value:
                    self._running.set()

                elif cmd == Command.SYNC.value:
                    await self.sync()
                    writer.write(Command.COMPLETE.value)

                else:
                    logger.warning(
                        f"Publisher {self.id} rx unknown command from GraphServer {cmd}"
                    )

                await writer.drain()

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Publisher {self.id} lost connection to graph server")

        finally:
            await close_stream_writer(writer)

    async def _on_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """
        Handle new subscriber connections.
        
        Exchanges identification information with connecting subscribers
        and sets up subscriber handling tasks.
        
        :param reader: Stream reader for receiving subscriber info.
        :type reader: asyncio.StreamReader
        :param writer: Stream writer for sending publisher info.
        :type writer: asyncio.StreamWriter
        """
        id_str = await read_str(reader)
        id = UUID(id_str)
        pid = await read_int(reader)
        topic = await read_str(reader)

        # Subscriber determines if they have SHM access
        writer.write(encode_str(self._shm.name))
        shm_access = bool(await read_int(reader))

        writer.write(
            encode_str(str(self.id))
            + uint64_to_bytes(self.pid)
            + encode_str(self.topic)
            + uint64_to_bytes(self._num_buffers)
        )

        info = SubscriberInfo(id, writer, pid, topic, shm_access)
        coro = self._handle_subscriber(info, reader)
        self._subscriber_tasks[id] = asyncio.create_task(coro)

        await writer.drain()

    async def _handle_subscriber(
        self, info: SubscriberInfo, reader: asyncio.StreamReader
    ) -> None:
        """
        Handle communication with a specific subscriber.
        
        Processes acknowledgments from subscribers and manages backpressure
        control based on subscriber feedback.
        
        :param info: Information about the subscriber connection.
        :type info: SubscriberInfo
        :param reader: Stream reader for receiving subscriber messages.
        :type reader: asyncio.StreamReader
        """
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
            logger.debug(f"Publisher {self.id}: Subscriber {id} connection fail")

        finally:
            self._backpressure.free(info.id)
            await close_stream_writer(self._subscribers[info.id].writer)
            del self._subscribers[info.id]

    async def sync(self) -> None:
        """
        Pause and drain backpressure.
        
        Temporarily pauses the publisher and waits for all pending
        messages to be acknowledged by subscribers.
        """
        self._running.clear()
        await self._backpressure.sync()

    @property
    def running(self) -> bool:
        """
        Check if the publisher is currently running.
        
        :return: True if publisher is running and accepting broadcasts.
        :rtype: bool
        """
        return self._running.is_set()

    def pause(self) -> None:
        """
        Pause the publisher to stop broadcasting messages.
        
        Messages sent to broadcast() will block until resumed.
        """
        self._running.clear()

    def resume(self) -> None:
        """
        Resume the publisher to allow broadcasting messages.
        
        Unblocks any pending broadcast() calls.
        """
        self._running.set()

    async def broadcast(self, obj: Any) -> None:
        """
        Broadcast a message to all connected subscribers.
        
        Handles message serialization, shared memory management, transport
        selection (local/SHM/TCP), and backpressure control automatically.
        
        :param obj: The object/message to broadcast to subscribers.
        :type obj: Any
        """
        await self._running.wait()

        buf_idx = self._msg_id % self._num_buffers
        msg_id_bytes = uint64_to_bytes(self._msg_id)

        if not self._backpressure.available(buf_idx):
            delta = time.time() - self._last_backpressure_event
            if BACKPRESSURE_WARNING and (delta > BACKPRESSURE_REFRACTORY):
                logger.warning(f"{self.topic} under subscriber backpressure!")
            self._last_backpressure_event = time.time()
            await self._backpressure.wait(buf_idx)

        MessageCache[self.id].put(self._msg_id, obj)

        for sub in list(self._subscribers.values()):
            if not self._force_tcp and sub.shm_access:
                if sub.pid == self.pid:
                    sub.writer.write(Command.TX_LOCAL.value + msg_id_bytes)

                else:
                    try:
                        # Push cache to shm (if not already there)
                        MessageCache[self.id].push(self._msg_id, self._shm)

                    except UndersizedMemory as e:
                        new_shm = await self._graph_service.create_shm(
                            self._num_buffers, e.req_size * 2
                        )

                        for i in range(self._num_buffers):
                            with self._shm.buffer(i, readonly=True) as from_buf:
                                with new_shm.buffer(i) as to_buf:
                                    MessageMarshal.copy_obj(from_buf, to_buf)

                        self._shm.close()
                        self._shm = new_shm
                        MessageCache[self.id].push(self._msg_id, self._shm)

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
                logger.debug(
                    f"Publisher {self.id}: Subscriber {sub.id} connection fail"
                )
                continue

        self._msg_id += 1
