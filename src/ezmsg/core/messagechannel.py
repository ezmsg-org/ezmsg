import os
import asyncio
import typing
import logging
import enum

from uuid import UUID
from contextlib import contextmanager, suppress

from .shm import SHMContext
from .messagemarshal import MessageMarshal, NO_MESSAGE
from .backpressure import Backpressure

from .graphserver import GraphService
from .netprotocol import (
    Command,
    Address,
    AddressType,
    read_str,
    uint64_to_bytes,
    encode_str,
    close_stream_writer,
    GRAPHSERVER_ADDR,
)

logger = logging.getLogger("ezmsg")


NotificationQueue = asyncio.Queue[typing.Tuple[UUID, int]]


class CacheMiss(Exception): ...


class PublisherProtocolState(enum.Enum):
    # Handshake states
    HANDSHAKE_SHM_NAME_LEN = enum.auto()
    HANDSHAKE_SHM_NAME_DATA = enum.auto()
    HANDSHAKE_COMPLETE = enum.auto()
    HANDSHAKE_NUM_BUFFERS = enum.auto()
    # Message states
    COMMAND = enum.auto()
    MSG_ID = enum.auto()
    SHM_NAME_LEN = enum.auto()
    SHM_NAME_DATA = enum.auto()
    TCP_SIZE = enum.auto()
    TCP_DATA = enum.auto()


class PublisherMessage(typing.NamedTuple):
    """Parsed message from publisher"""

    msg_id: int
    command: bytes
    shm_name: str | None = None
    tcp_data: bytes | None = None


class PublisherProtocol(asyncio.Protocol):
    """High-performance protocol for publisher message stream"""

    def __init__(
        self,
        channel_id: str,
        message_queue: asyncio.Queue[PublisherMessage],
    ):
        self.channel_id = channel_id
        self.message_queue = message_queue
        self.transport: asyncio.Transport | None = None

        # Pre-allocate buffer with reasonable size (1MB)
        # This avoids repeated reallocations during growth
        self.buffer = bytearray(1024 * 1024)
        self.buffer_size = 0  # Actual data in buffer
        self.buffer_offset = 0  # Read position

        # Handshake state
        self.handshake_complete = asyncio.Future()
        self.handshake_shm_received = asyncio.Event()
        self.handshake_shm_name: str | None = None
        self.num_buffers: int | None = None

        # Parser state machine - start in handshake mode
        self.state = PublisherProtocolState.HANDSHAKE_SHM_NAME_LEN
        self.expected_bytes = 8  # Expecting shm_name length

        # Message parsing state
        self.current_msg_id: int | None = None
        self.current_command: int | None = None  # Store as int, not bytes
        self.payload_size: int | None = None
        self.shm_name_len: int | None = None

    def connection_made(self, transport: asyncio.Transport) -> None:  # type: ignore
        assert transport is not None
        self.transport = transport
        logger.debug("PublisherProtocol connected")
        # Send initial handshake
        self.transport.write(Command.CHANNEL.value)
        self.transport.write(encode_str(self.channel_id))

    def send_handshake_response(self, shm_ok: bool, pid: int) -> None:
        """Send SHM attachment response after Channel attaches"""
        if shm_ok:
            self.transport.write(Command.SHM_OK.value)
        else:
            self.transport.write(Command.SHM_ATTACH_FAILED.value)
        self.transport.write(uint64_to_bytes(pid))
        # Resume parsing - might have buffered data
        while self._try_parse():
            pass

    def data_received(self, data: bytes) -> None:
        """Hot path - called directly by event loop"""
        data_len = len(data)

        # Ensure buffer has space
        if self.buffer_size + data_len > len(self.buffer):
            # Compact buffer if offset is large enough
            if self.buffer_offset > len(self.buffer) // 2:
                remaining = self.buffer_size - self.buffer_offset
                self.buffer[0:remaining] = self.buffer[
                    self.buffer_offset : self.buffer_size
                ]
                self.buffer_offset = 0
                self.buffer_size = remaining
            else:
                # Grow buffer
                new_size = max(len(self.buffer) * 2, self.buffer_size + data_len)
                self.buffer.extend(bytearray(new_size - len(self.buffer)))

        # Copy data into buffer
        self.buffer[self.buffer_size : self.buffer_size + data_len] = data
        self.buffer_size += data_len

        # Process as many complete messages as possible
        while self._try_parse():
            pass

    def _try_parse(self) -> bool:
        """State machine parser - returns True if made progress"""
        available = self.buffer_size - self.buffer_offset
        if available < self.expected_bytes:
            return False

        # Handshake states
        if self.state == PublisherProtocolState.HANDSHAKE_SHM_NAME_LEN:
            name_len = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.expected_bytes = name_len
            self.state = PublisherProtocolState.HANDSHAKE_SHM_NAME_DATA
            return True

        elif self.state == PublisherProtocolState.HANDSHAKE_SHM_NAME_DATA:
            shm_name = self.buffer[
                self.buffer_offset : self.buffer_offset + self.expected_bytes
            ].decode("utf-8")
            self.buffer_offset += self.expected_bytes
            self.handshake_shm_name = shm_name
            self.handshake_shm_received.set()
            self.state = PublisherProtocolState.HANDSHAKE_COMPLETE
            self.expected_bytes = 1
            return False  # Pause until handshake response is sent

        elif self.state == PublisherProtocolState.HANDSHAKE_COMPLETE:
            complete_byte = self.buffer[self.buffer_offset]
            self.buffer_offset += 1
            if complete_byte != Command.COMPLETE.value[0]:
                logger.error("Handshake failed: did not receive COMPLETE")
                if self.transport:
                    self.transport.close()
                return False
            self.state = PublisherProtocolState.HANDSHAKE_NUM_BUFFERS
            self.expected_bytes = 8
            return True

        elif self.state == PublisherProtocolState.HANDSHAKE_NUM_BUFFERS:
            num_buffers = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.num_buffers = num_buffers
            self.state = PublisherProtocolState.COMMAND
            self.expected_bytes = 1
            self.handshake_complete.set_result(num_buffers)
            return True

        # Message states
        elif self.state == PublisherProtocolState.COMMAND:
            cmd = self.buffer[self.buffer_offset]
            self.buffer_offset += 1
            self.current_command = cmd
            self.state = PublisherProtocolState.MSG_ID
            self.expected_bytes = 8  # uint64
            return True

        elif self.state == PublisherProtocolState.MSG_ID:
            msg_id = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.current_msg_id = msg_id

            if self.current_command == Command.TX_SHM.value[0]:
                self.state = PublisherProtocolState.SHM_NAME_LEN
                self.expected_bytes = 8
            elif self.current_command == Command.TX_TCP.value[0]:
                self.state = PublisherProtocolState.TCP_SIZE
                self.expected_bytes = 8
            else:
                # Unknown command - queue it anyway
                self.message_queue.put_nowait(
                    PublisherMessage(
                        msg_id=msg_id, command=bytes([self.current_command or 0])
                    )
                )
                self._reset_state()
            return True

        elif self.state == PublisherProtocolState.SHM_NAME_LEN:
            name_len = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.expected_bytes = name_len
            self.state = PublisherProtocolState.SHM_NAME_DATA
            return True

        elif self.state == PublisherProtocolState.SHM_NAME_DATA:
            shm_name = self.buffer[
                self.buffer_offset : self.buffer_offset + self.expected_bytes
            ].decode("utf-8")
            self.buffer_offset += self.expected_bytes
            if self.current_msg_id is not None and self.current_command is not None:
                self.message_queue.put_nowait(
                    PublisherMessage(
                        msg_id=self.current_msg_id,
                        command=bytes([self.current_command]),
                        shm_name=shm_name,
                    )
                )
            self._reset_state()
            return True

        elif self.state == PublisherProtocolState.TCP_SIZE:
            buf_size = int.from_bytes(
                self.buffer[self.buffer_offset : self.buffer_offset + 8], "little"
            )
            self.buffer_offset += 8
            self.expected_bytes = buf_size
            self.state = PublisherProtocolState.TCP_DATA
            return True

        elif self.state == PublisherProtocolState.TCP_DATA:
            # Use bytes() here - memoryview causes issues with pickle/marshal
            obj_bytes = bytes(
                self.buffer[
                    self.buffer_offset : self.buffer_offset + self.expected_bytes
                ]
            )
            self.buffer_offset += self.expected_bytes
            if self.current_msg_id is not None and self.current_command is not None:
                self.message_queue.put_nowait(
                    PublisherMessage(
                        msg_id=self.current_msg_id,
                        command=bytes([self.current_command]),
                        tcp_data=obj_bytes,
                    )
                )
            self._reset_state()
            return True

        return False

    def _reset_state(self) -> None:
        self.state = PublisherProtocolState.COMMAND
        self.expected_bytes = 1
        self.current_msg_id = None
        self.current_command = None
        self.payload_size = None
        self.shm_name_len = None

    def connection_lost(self, exc: Exception | None) -> None:
        if exc:
            logger.debug(f"PublisherProtocol connection lost: {exc}")
        else:
            logger.debug("PublisherProtocol disconnected")


class Channel:
    """cache-backed message channel for a particular publisher"""

    id: UUID
    pub_id: UUID
    pid: int
    topic: str

    num_buffers: int
    tcp_cache: typing.List[memoryview]
    cache: typing.List[typing.Any]
    cache_id: typing.List[int | None]
    shm: SHMContext | None
    clients: typing.Dict[UUID, NotificationQueue | None]
    backpressure: Backpressure

    _graph_task: asyncio.Task[None]
    _pub_protocol: PublisherProtocol
    _pub_transport: asyncio.Transport
    _pub_message_queue: asyncio.Queue[PublisherMessage]
    _pub_process_task: asyncio.Task[None]
    _graph_address: AddressType | None
    _local_backpressure: Backpressure | None

    def __init__(
        self,
        id: UUID,
        pub_id: UUID,
        num_buffers: int,
        shm: SHMContext | None,
        graph_address: AddressType | None,
    ) -> None:
        self.id = id
        self.pub_id = pub_id
        self.num_buffers = num_buffers
        self.shm = shm

        self.tcp_cache = [memoryview(NO_MESSAGE)] * self.num_buffers
        self.cache_id = [None] * self.num_buffers
        self.cache = [None] * self.num_buffers
        self.backpressure = Backpressure(self.num_buffers)
        self.clients = dict()
        self._graph_address = graph_address
        self._local_backpressure = None

    @classmethod
    async def create(
        cls,
        pub_id: UUID,
        graph_address: AddressType,
    ) -> "Channel":
        graph_service = GraphService(graph_address)

        graph_reader, graph_writer = await graph_service.open_connection()
        graph_writer.write(Command.CHANNEL.value)
        graph_writer.write(encode_str(str(pub_id)))

        response = await graph_reader.read(1)
        if response != Command.COMPLETE.value:
            # FIXME: This will happen if the channel requested connection
            # to a non-existent (or non-publisher) UUID.  Ideally GraphServer
            # would tell us what happened rather than drop connection
            raise ValueError(f"failed to create channel {pub_id=}")

        id_str = await read_str(graph_reader)
        pub_address = await Address.from_stream(graph_reader)

        # Create protocol and connect directly (no StreamReader!)
        loop = asyncio.get_running_loop()
        pub_message_queue: asyncio.Queue[PublisherMessage] = asyncio.Queue()
        transport, protocol = await loop.create_connection(
            lambda: PublisherProtocol(id_str, pub_message_queue),
            *pub_address,
        )

        # Wait for protocol to receive shm_name
        await protocol.handshake_shm_received.wait()
        assert protocol.handshake_shm_name is not None

        # Attach SHM
        shm = None
        shm_ok = False
        try:
            shm = await graph_service.attach_shm(protocol.handshake_shm_name)
            shm_ok = True
        except (ValueError, OSError):
            pass

        # Tell protocol to send handshake response
        protocol.send_handshake_response(shm_ok, os.getpid())

        # Wait for handshake to complete (protocol receives COMPLETE + num_buffers)
        num_buffers = await protocol.handshake_complete

        chan = cls(UUID(id_str), pub_id, num_buffers, shm, graph_address)

        chan._graph_task = asyncio.create_task(
            chan._graph_connection(graph_reader, graph_writer),
            name=f"chan-{chan.id}: _graph_connection",
        )

        # Set up publisher connection (protocol already connected!)
        chan._pub_message_queue = pub_message_queue
        chan._pub_protocol = protocol
        chan._pub_transport = transport

        # Single task to process messages from protocol
        chan._pub_process_task = asyncio.create_task(
            chan._process_publisher_messages(),
            name=f"chan-{chan.id}: _process_messages",
        )

        logger.debug(f"created channel {chan.id=} {pub_id=} {pub_address=}")

        return chan

    def close(self) -> None:
        if self.shm is not None:
            self.shm.close()
        self._graph_task.cancel()
        self._pub_transport.close()
        self._pub_process_task.cancel()

    async def wait_closed(self) -> None:
        if self.shm is not None:
            await self.shm.wait_closed()
        with suppress(asyncio.CancelledError):
            await self._graph_task
        with suppress(asyncio.CancelledError):
            await self._pub_process_task

    async def _graph_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                cmd = await reader.read(1)

                if not cmd:
                    break

                else:
                    logger.warning(
                        f"Channel {self.id} rx unknown command from GraphServer: {cmd}"
                    )
        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Channel {self.id} lost connection to graph server")

        finally:
            await close_stream_writer(writer)

    async def _process_publisher_messages(self) -> None:
        """Process messages from PublisherProtocol queue"""
        while True:
            msg = await self._pub_message_queue.get()
            buf_idx = msg.msg_id % self.num_buffers

            if msg.command == Command.TX_SHM.value and msg.shm_name is not None:
                # Handle SHM message
                needs_attach = self.shm is None or self.shm.name != msg.shm_name

                if needs_attach:
                    if self.shm is not None:
                        self.shm.close()
                        await self.shm.wait_closed()
                    try:
                        self.shm = await GraphService(self._graph_address).attach_shm(
                            msg.shm_name
                        )
                    except ValueError:
                        logger.info("Invalid SHM received from publisher; may be dead")
                        self._pub_transport.close()
                        return

            elif msg.command == Command.TX_TCP.value and msg.tcp_data is not None:
                # Handle TCP message
                self.tcp_cache[buf_idx] = memoryview(msg.tcp_data).toreadonly()

            # Notify clients
            if not self._notify_clients(msg.msg_id):
                # Nobody is listening; need to ack!
                self._acknowledge(msg.msg_id)

    def _notify_clients(self, msg_id: int) -> bool:
        """notify interested clients and return true if any were notified"""
        buf_idx = msg_id % self.num_buffers
        for client_id, queue in self.clients.items():
            if queue is None:
                continue  # queue is none if this is the pub
            self.backpressure.lease(client_id, buf_idx)
            queue.put_nowait((self.pub_id, msg_id))
        return not self.backpressure.available(buf_idx)

    def put_local(self, msg_id: int, msg: typing.Any) -> None:
        """
        put an object into cache (should only be used by Publishers)
        returns true if any clients were notified
        """
        if self._local_backpressure is None:
            raise ValueError(
                "cannot put_local without access to publisher backpressure (is publisher in same process?)"
            )

        buf_idx = msg_id % self.num_buffers
        if self._notify_clients(msg_id):
            self.cache_id[buf_idx] = msg_id
            self.cache[buf_idx] = msg
            self._local_backpressure.lease(self.id, buf_idx)

    @contextmanager
    def get(
        self, msg_id: int, client_id: UUID
    ) -> typing.Generator[typing.Any, None, None]:
        """get object from cache; if not in cache and shm provided -- get from shm"""

        dispatched = False

        buf_idx = msg_id % self.num_buffers
        if self.cache_id[buf_idx] == msg_id:
            dispatched = True
            yield self.cache[buf_idx]

        elif self.shm is not None:
            with self.shm.buffer(buf_idx, readonly=True) as mem:
                if MessageMarshal.msg_id(mem) == msg_id:
                    with MessageMarshal.obj_from_mem(mem) as obj:
                        dispatched = True
                        yield obj

        if not dispatched:
            tcp_mem = self.tcp_cache[buf_idx]
            if MessageMarshal.msg_id(tcp_mem) == msg_id:
                with MessageMarshal.obj_from_mem(tcp_mem) as obj:
                    dispatched = True
                    yield obj
            else:
                raise CacheMiss

        self.backpressure.free(client_id, buf_idx)
        if self.backpressure.buffers[buf_idx].is_empty:
            # If pub is in same process as this channel, avoid TCP
            if self._local_backpressure is not None:
                self._local_backpressure.free(self.id, buf_idx)
                return

            self._acknowledge(msg_id)

    def _acknowledge(self, msg_id: int) -> None:
        try:
            ack = Command.RX_ACK.value + uint64_to_bytes(msg_id)
            self._pub_transport.write(ack)
        except (BrokenPipeError, ConnectionResetError):
            logger.info(f"ack fail: channel:{self.id} -> pub:{self.pub_id}")

    def register_client(
        self,
        client_id: UUID,
        queue: NotificationQueue | None = None,
        local_backpressure: Backpressure | None = None,
    ) -> None:
        self.clients[client_id] = queue
        if client_id == self.pub_id:
            self._local_backpressure = local_backpressure

    def unregister_client(self, client_id: UUID) -> None:
        queue = self.clients[client_id]

        # queue is only 'None' if this client is a local publisher
        if queue is not None:
            for _ in range(queue.qsize()):
                pub_id, msg_id = queue.get_nowait()
                if pub_id != self.pub_id:
                    queue.put_nowait((pub_id, msg_id))

            self.backpressure.free(client_id)

        elif client_id == self.pub_id and self._local_backpressure is not None:
            self._local_backpressure.free(self.id)
            self._local_backpressure = None

        del self.clients[client_id]

    def clear_cache(self):
        self.cache_id = [None] * self.num_buffers
        self.cache = [None] * self.num_buffers


def _ensure_address(address: AddressType | None) -> Address:
    if address is None:
        return Address.from_string(GRAPHSERVER_ADDR)

    elif not isinstance(address, Address):
        return Address(*address)

    return address


class ChannelManager:
    _registry: typing.Dict[Address, typing.Dict[UUID, Channel]]

    def __init__(self):
        default_address = Address.from_string(GRAPHSERVER_ADDR)
        self._registry = {default_address: dict()}
        self._lock = asyncio.Lock()

    async def get(
        self,
        pub_id: UUID,
        graph_address: AddressType | None = None,
        create: bool = False,
    ) -> Channel:
        graph_address = _ensure_address(graph_address)
        channel = self._registry.get(graph_address, dict()).get(pub_id, None)
        if create and channel is None:
            channel = await Channel.create(pub_id, graph_address)
            channels = self._registry.get(graph_address, dict())
            channels[pub_id] = channel
            self._registry[graph_address] = channels
        if channel is None:
            raise KeyError(f"channel {pub_id=} {graph_address=} does not exist")
        return channel

    async def register(
        self,
        pub_id: UUID,
        client_id: UUID,
        queue: NotificationQueue,
        graph_address: AddressType | None = None,
    ) -> Channel:
        return await self._register(pub_id, client_id, queue, graph_address, None)

    async def register_local_pub(
        self,
        pub_id: UUID,
        local_backpressure: Backpressure | None = None,
        graph_address: AddressType | None = None,
    ) -> Channel:
        return await self._register(
            pub_id, pub_id, None, graph_address, local_backpressure
        )

    async def _register(
        self,
        pub_id: UUID,
        client_id: UUID,
        queue: NotificationQueue | None = None,
        graph_address: AddressType | None = None,
        local_backpressure: Backpressure | None = None,
    ) -> Channel:
        channel = await self.get(pub_id, graph_address, create=True)
        channel.register_client(client_id, queue, local_backpressure)
        return channel

    async def unregister(
        self, pub_id: UUID, client_id: UUID, graph_address: AddressType | None = None
    ) -> None:
        channel = await self.get(pub_id, graph_address)
        channel.unregister_client(client_id)

        if len(channel.clients) == 0:
            channel.close()
            await channel.wait_closed()
            graph_address = _ensure_address(graph_address)
            registry = self._registry[graph_address]
            del registry[pub_id]
            logger.debug(f"closed channel {pub_id}: no clients")


CHANNELS = ChannelManager()
