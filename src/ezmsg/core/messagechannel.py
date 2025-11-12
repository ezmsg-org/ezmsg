import os
import asyncio
import typing
import logging

from uuid import UUID
from contextlib import contextmanager, suppress, ExitStack

from .shm import SHMContext
from .messagemarshal import MessageMarshal
from .backpressure import Backpressure
from .publisherprotocol import PublisherProtocol, PublisherMessage
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


class Channel:
    """cache-backed message channel for a particular publisher"""

    id: UUID
    pub_id: UUID
    pid: int
    topic: str

    num_buffers: int
    contexts: typing.List[typing.ContextManager | None]
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

        self.contexts = [None] * self.num_buffers
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
        self._graph_task.cancel()
        self._pub_transport.close()
        self._pub_process_task.cancel()

    async def wait_closed(self) -> None:
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
        try:
            while True:
                msg = await self._pub_message_queue.get()
                buf_idx = msg.msg_id % self.num_buffers
                
                ctx: typing.ContextManager | None = None
                value: typing.Any = None

                if msg.command == Command.TX_SHM.value:
                    if (
                        self.shm is not None and
                        self.shm.name != msg.shm_name and
                        msg.shm_name is not None
                    ):

                        shm_entries = []
                        for i, c in enumerate(self.contexts):
                            if isinstance(c, ExitStack):
                                shm_entries.append(i)
                                c.close()
                            
                        self.shm.close()
                        await self.shm.wait_closed()
                        try:
                            self.shm = await GraphService(self._graph_address).attach_shm(msg.shm_name)
                        except ValueError:
                            logger.info(
                                "Invalid SHM received from publisher; may be dead"
                            )
                            raise

                        for i in shm_entries:
                            stack = ExitStack()
                            view = stack.enter_context(self.shm.buffer(i))
                            assert MessageMarshal.msg_id(view) == self.cache_id[i]
                            self.cache[i] = stack.enter_context(MessageMarshal.obj_from_mem(view))
                            self.contexts[i] = stack
                    
                    assert self.shm is not None

                    ctx = ExitStack()
                    view = ctx.enter_context(self.shm.buffer(buf_idx))
                    assert MessageMarshal.msg_id(view) == msg.msg_id
                    value = ctx.enter_context(MessageMarshal.obj_from_mem(view))

                elif msg.command == Command.TX_TCP.value and msg.tcp_data is not None:
                    view = memoryview(msg.tcp_data).toreadonly()
                    ctx = MessageMarshal.obj_from_mem(view)
                    value = ctx.__enter__()

                else:
                    raise ValueError(f"unimplemented data telemetry: {msg}")

                if self._notify_clients(msg.msg_id):
                    self.cache_id[buf_idx] = msg.msg_id
                    self.cache[buf_idx] = value
                    self.contexts[buf_idx] = ctx
                else:
                    # Nobody is listening; need to ack!
                    self._acknowledge(msg.msg_id)
                    ctx.__exit__(None, None, None)
        finally:
            for i in range(self.num_buffers):
                obj = self.cache[i]
                self.cache[i] = None
                del obj
                
                self.cache_id[i] = None
                ctx = self.contexts[i]
                if ctx is not None:
                    ctx.__exit__(None, None, None)
                self.contexts[i] = None

            if self.shm is not None:
                self.shm.close()
            self.shm = None

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

        buf_idx = msg_id % self.num_buffers
        if self.cache_id[buf_idx] != msg_id:
            raise CacheMiss
        
        try:
            yield self.cache[buf_idx]
        finally:
            self.backpressure.free(client_id, buf_idx)
            if self.backpressure.buffers[buf_idx].is_empty:

                # If pub is in same process as this channel, avoid TCP
                if self._local_backpressure is not None:
                    self._local_backpressure.free(self.id, buf_idx)
                else:
                    self._acknowledge(msg_id)

                # Free cache and release contexts
                self.cache[buf_idx] = None
                self.cache_id[buf_idx] = None
                ctx = self.contexts[buf_idx]
                if ctx is not None:
                    ctx.__exit__(None, None, None)
                self.contexts[buf_idx] = None

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
