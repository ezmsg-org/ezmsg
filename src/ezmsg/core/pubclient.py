import os
import asyncio
import logging
import time

from uuid import UUID
from contextlib import suppress
from dataclasses import dataclass

from .backpressure import Backpressure
from .shm import SHMContext
from .graphserver import GraphService
from .channelmanager import CHANNELS
from .messagechannel import Channel
from .messagemarshal import MessageMarshal, UninitializedMemory

from .netprotocol import (
    Address,
    AddressType,
    uint64_to_bytes,
    read_int,
    read_str,
    encode_str,
    close_stream_writer,
    close_server,
    Command,
    ChannelInfo,
    create_socket,
    DEFAULT_SHM_SIZE,
    PUBLISHER_START_PORT_ENV,
    PUBLISHER_START_PORT_DEFAULT,
)

from typing import Any

logger = logging.getLogger("ezmsg")

BACKPRESSURE_WARNING = "EZMSG_DISABLE_BACKPRESSURE_WARNING" not in os.environ
BACKPRESSURE_REFRACTORY = 5.0  # sec


# Publisher needs a bit more information about connected channels
@dataclass
class PubChannelInfo(ChannelInfo):
    pid: int
    shm_ok: bool = False


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
    _channels: dict[UUID, PubChannelInfo]
    _channel_tasks: dict[UUID, "asyncio.Task[None]"]
    _local_channel: Channel
    _address: Address
    _backpressure: Backpressure
    _num_buffers: int
    _running: asyncio.Event
    _msg_id: int
    _shm: SHMContext
    _force_tcp: bool
    _last_backpressure_event: float

    _graph_address: AddressType | None

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
        graph_address: AddressType | None = None,
        host: str | None = None,
        port: int | None = None,
        buf_size: int = DEFAULT_SHM_SIZE,
        num_buffers: int = 32,
        start_paused: bool = False,
        force_tcp: bool = False,
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
        graph_service = GraphService(graph_address)
        reader, writer = await graph_service.open_connection()
        shm = await graph_service.create_shm(num_buffers, buf_size)

        writer.write(Command.PUBLISH.value)
        writer.write(encode_str(topic))

        pub_id = UUID(await read_str(reader))
        pub = cls(
            id=pub_id,
            topic=topic,
            shm=shm,
            graph_address=graph_address,
            num_buffers=num_buffers,
            start_paused=start_paused,
            force_tcp=force_tcp,
        )

        start_port = int(
            os.getenv(PUBLISHER_START_PORT_ENV, PUBLISHER_START_PORT_DEFAULT)
        )
        sock = create_socket(host, port, start_port=start_port)
        server = await asyncio.start_server(pub._channel_connect, sock=sock)
        pub._connection_task = asyncio.create_task(
            pub._serve_channels(server), name=f"pub-{pub.id}: {pub.topic}"
        )

        # Notify GraphServer that our server is up
        channel_server_address = Address(*sock.getsockname())
        channel_server_address.to_stream(writer)
        result = await reader.read(1)  # channels connect
        if result != Command.COMPLETE.value:
            logger.warning(f"Could not create publisher {topic=}")

        # Pass off graph connection keep-alive to publisher task
        pub._graph_task = asyncio.create_task(
            pub._graph_connection(reader, writer),
            name=f"pub-{pub.id}: _graph_connection",
        )

        pub._local_channel = await CHANNELS.register_local_pub(
            pub_id=pub.id,
            local_backpressure=pub._backpressure,
            graph_address=pub._graph_address,
        )

        logger.debug(f"created pub {pub.id=} {topic=} {channel_server_address=}")

        return pub

    async def _serve_channels(self, server: asyncio.Server) -> None:
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            logger.debug(f"{self.log_name} cancelled")
        finally:
            await close_server(server)
            await CHANNELS.unregister(self.id, self.id, self._graph_address)
            logger.debug(f"{self.log_name} done")

    def __init__(
        self,
        id: UUID,
        topic: str,
        shm: SHMContext,
        graph_address: AddressType | None = None,
        num_buffers: int = 32,
        start_paused: bool = False,
        force_tcp: bool = False,
    ) -> None:
        """
        Initialize a Publisher instance.
        DO NOT USE this constructor to make a Publisher; use `create` instead

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
        self._shm = shm
        self._msg_id = 0
        self._channels = dict()
        self._channel_tasks = dict()
        self._running = asyncio.Event()
        if not start_paused:
            self._running.set()
        self._num_buffers = num_buffers
        self._backpressure = Backpressure(num_buffers)
        self._force_tcp = force_tcp
        self._last_backpressure_event = -1
        self._graph_address = graph_address

    @property
    def log_name(self) -> str:
        return f"pub_{self.topic}{str(self.id)}"

    def close(self) -> None:
        """
        Close the publisher and cancel all associated tasks.

        Cancels graph connection, shared memory, connection server,
        and all subscriber handling tasks.
        """
        self._graph_task.cancel()
        self._shm.close()
        self._connection_task.cancel()
        for task in self._channel_tasks.values():
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
        for task in self._channel_tasks.values():
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

    async def _channel_connect(
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
        cmd = await reader.read(1)

        if len(cmd) == 0:
            return

        if cmd == Command.CHANNEL.value:
            channel_id_str = await read_str(reader)
            channel_id = UUID(channel_id_str)
            writer.write(encode_str(self._shm.name))
            shm_ok = await reader.read(1) == Command.SHM_OK.value
            pid = await read_int(reader)
            info = PubChannelInfo(channel_id, writer, self.id, pid, shm_ok)
            coro = self._handle_channel(info, reader)
            self._channel_tasks[channel_id] = asyncio.create_task(coro)
            writer.write(Command.COMPLETE.value + uint64_to_bytes(self._num_buffers))

        await writer.drain()

    async def _handle_channel(
        self, info: PubChannelInfo, reader: asyncio.StreamReader
    ) -> None:
        """
        Handle communication with a specific channel.

        Processes acknowledgments from channels and manages backpressure
        control based on channel feedback.

        :param info: Information about the channel connection.
        :type info: PubChannelInfo
        :param reader: Stream reader for receiving channel messages.
        :type reader: asyncio.StreamReader
        """
        self._channels[info.id] = info

        try:
            while True:
                msg = await reader.read(1)

                if len(msg) == 0:
                    break

                elif msg == Command.RX_ACK.value:
                    msg_id = await read_int(reader)
                    self._backpressure.free(info.id, msg_id % self._num_buffers)

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Publisher {self.id}: Channel {info.id} connection fail")

        finally:
            self._backpressure.free(info.id)
            await close_stream_writer(self._channels[info.id].writer)
            del self._channels[info.id]

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

        # Get local channel and put variable there for local tx
        self._local_channel.put_local(self._msg_id, obj)

        if self._force_tcp or any(
            ch.pid != self.pid or not ch.shm_ok for ch in self._channels.values()
        ):
            with MessageMarshal.serialize(self._msg_id, obj) as (
                total_size,
                header,
                buffers,
            ):
                total_size_bytes = uint64_to_bytes(total_size)

                if not self._force_tcp and any(
                    ch.pid != self.pid and ch.shm_ok for ch in self._channels.values()
                ):
                    if self._shm.buf_size < total_size:
                        new_shm = await GraphService(self._graph_address).create_shm(
                            self._num_buffers, total_size * 2
                        )

                        for i in range(self._num_buffers):
                            try:
                                with self._shm.buffer(i, readonly=True) as from_buf:
                                    with new_shm.buffer(i) as to_buf:
                                        MessageMarshal.copy_obj(from_buf, to_buf)
                            except UninitializedMemory:
                                pass

                        self._shm.close()
                        await self._shm.wait_closed()
                        self._shm = new_shm

                    with self._shm.buffer(buf_idx) as mem:
                        MessageMarshal._write(mem, header, buffers)

                for channel in self._channels.values():
                    msg: bytes = b""

                    if self.pid == channel.pid and channel.shm_ok:
                        continue  # Local transmission handled by channel.put

                    elif (
                        (not self._force_tcp)
                        and self.pid != channel.pid
                        and channel.shm_ok
                    ):
                        msg = (
                            Command.TX_SHM.value
                            + msg_id_bytes
                            + encode_str(self._shm.name)
                        )

                    else:
                        msg = (
                            Command.TX_TCP.value
                            + msg_id_bytes
                            + total_size_bytes
                            + header
                            + b"".join([buffer for buffer in buffers])
                        )

                    try:
                        channel.writer.write(msg)
                        await channel.writer.drain()
                        self._backpressure.lease(channel.id, buf_idx)

                    except (ConnectionResetError, BrokenPipeError):
                        logger.debug(
                            f"Publisher {self.id}: Channel {channel.id} connection fail"
                        )

        self._msg_id += 1
