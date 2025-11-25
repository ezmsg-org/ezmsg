import os
import asyncio
import typing
import logging

from uuid import UUID
from contextlib import contextmanager, suppress

from .shm import SHMContext
from .messagemarshal import MessageMarshal
from .backpressure import Backpressure
from .publisherprotocol import PublisherClientProtocol, PublisherMessage
from .messagecache import MessageCache
from .graphserver import GraphService
from .netprotocol import (
    Command,
    Address,
    AddressType,
    read_str,
    read_int,
    uint64_to_bytes,
    encode_str,
    close_stream_writer,
    GRAPHSERVER_ADDR,
)

logger = logging.getLogger("ezmsg")


NotificationQueue = asyncio.Queue[typing.Tuple[UUID, int]]


class Channel:
    """
    Channel is a "middle-man" that receives messages from a particular Publisher,
    maintains the message in a MessageCache, and pushes notifications to interested
    Subscribers in this process.

    Channel primarily exists to reduce redundant message serialization and telemetry.

    .. note::
    The Channel constructor should not be called directly, instead use Channel.create(...)
    """

    _SENTINEL = object()

    id: UUID
    pub_id: UUID
    pid: int
    topic: str

    num_buffers: int
    cache: MessageCache
    shm: SHMContext | None
    clients: dict[UUID, NotificationQueue | None]
    backpressure: Backpressure

    _graph_task: asyncio.Task[None]
    _pub_protocol: PublisherClientProtocol
    _pub_transport: asyncio.Transport
    _pub_message_queue: asyncio.Queue[PublisherMessage]
    _pub_process_task: asyncio.Task[None]
    _graph_address: AddressType | None

    def __init__(
        self,
        id: UUID,
        pub_id: UUID,
        num_buffers: int,
        shm: SHMContext | None,
        graph_address: AddressType | None,
        _guard = None,
    ) -> None:
        if _guard is not self._SENTINEL:
            raise TypeError(
                "Channel cannot be instantiated directly."
                "Use 'await CHANNELS.register(...)' instead."
            )
        
        self.id = id
        self.pub_id = pub_id
        self.num_buffers = num_buffers
        self.shm = shm

        self.cache = MessageCache(self.num_buffers)
        self.backpressure = Backpressure(self.num_buffers)
        self.clients = dict()
        self._graph_address = graph_address
        self._local_backpressure = None

    @classmethod
    async def create(
        cls,
        pub_id: UUID,
        graph_address: AddressType,
        is_local: bool = False,
    ) -> "Channel":
        """
        Create a channel for a particular Publisher managed by a GraphServer at graph_address

        :param pub_id: The Publisher's UUID on the GraphServer
        :type pub_id: UUID
        :param graph_address: The address the GraphServer is hosted on.
        :type graph_address: AddressType
        :return: a configured and connected Channel for messages from the Publisher
        :rtype: Channel

        .. note:: This is typically called by ChannelManager as interested Subscribers register.
        """
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
            lambda: PublisherClientProtocol(id_str, pub_message_queue),
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
        assert num_buffers > 0, "publisher reports invalid num_buffers"

        channel_cls: type[Channel]
        if is_local and shm is not None:
            channel_cls = LocalChannel
        elif shm_ok and shm is not None:
            channel_cls = SHMChannel
        else:
            channel_cls = TCPChannel

        chan = channel_cls(UUID(id_str), pub_id, num_buffers, shm, graph_address, _guard=cls._SENTINEL)

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
        """
        Wait until the Channel has properly shutdown and its resources have been deallocated.
        """
        with suppress(asyncio.CancelledError):
            await self._pub_process_task
        with suppress(asyncio.CancelledError):
            await self._graph_task
        if self.shm is not None:
            await self.shm.wait_closed()

    async def _graph_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """
        The task that handles communication between the GraphServer and the Publisher.
        """
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

                if msg.command == Command.TX_SHM.value:

                    if (
                        self.shm is not None and 
                        msg.shm_name is not None and 
                        self.shm.name != msg.shm_name
                    ):
                        shm_entries = self.cache.keys()
                        self.cache.clear()
                        self.shm.close()
                        await self.shm.wait_closed()

                        try:
                            self.shm = await GraphService(
                                self._graph_address
                            ).attach_shm(msg.shm_name)
                        except ValueError:
                            logger.info(
                                "Invalid SHM received from publisher; may be dead"
                            )
                            raise

                        for id in shm_entries:
                            self.cache.put_from_mem(self.shm[id % self.num_buffers])

                    assert self.shm is not None
                    assert MessageMarshal.msg_id(self.shm[buf_idx]) == msg.msg_id
                    self.cache.put_from_mem(self.shm[buf_idx])

                elif msg.command == Command.TX_TCP.value:
                    assert msg.tcp_data is not None
                    assert MessageMarshal.msg_id(msg.tcp_data) == msg.msg_id
                    self.cache.put_from_mem(memoryview(msg.tcp_data).toreadonly())


                if not self._notify_clients(msg.msg_id):
                    # Nobody is listening; need to ack!
                    self.cache.release(msg.msg_id)
                    self._acknowledge(msg.msg_id)

        finally:
            self.cache.clear()
            if self.shm is not None:
                self.shm.close()

            logger.debug(f"disconnected: channel:{self.id} -> pub:{self.pub_id}")

    def _notify_clients(self, msg_id: int) -> bool:
        """notify interested clients and return true if any were notified"""
        buf_idx = msg_id % self.num_buffers
        for client_id, queue in self.clients.items():
            if queue is None:
                continue  # queue is none if this is the pub
            self.backpressure.lease(client_id, buf_idx)
            queue.put_nowait((self.pub_id, msg_id))
        return not self.backpressure.available(buf_idx)

    @contextmanager
    def get(
        self, msg_id: int, client_id: UUID
    ) -> typing.Generator[typing.Any, None, None]:
        """
        Get a message via a ContextManager

        :param msg_id: Message ID to retreive
        :type msg_id: int
        :param client_id: UUID of client retreiving this message for backpressure purposes
        :type client_id: UUID
        :raises CacheMiss: If this msg_id does not exist in the cache.
        :return: A ContextManager for the message (type: Any)
        :rtype: Generator[Any]
        """

        try:
            yield self.cache[msg_id]
        finally:
            buf_idx = msg_id % self.num_buffers
            self.backpressure.free(client_id, buf_idx)
            if self.backpressure.buffers[buf_idx].is_empty:
                self.cache.release(msg_id)

                # If pub is in same process as this channel, avoid TCP
                if self._local_backpressure is not None:
                    self._local_backpressure.free(self.id, buf_idx)
                else:
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
        """
        Register an interested client and provide a queue for incoming message notifications.

        :param client_id: The UUID of the subscribing client
        :type client_id: UUID
        :param queue: The notification queue for the subscribing client
        :type queue: asyncio.Queue[tuple[UUID, int]] | None
        :param local_backpressure: The backpressure object for the Publisher if it is in the same process
        :type local_backpressure: Backpressure
        """
        self.clients[client_id] = queue
        if client_id == self.pub_id:
            self._local_backpressure = local_backpressure

    def unregister_client(self, client_id: UUID) -> None:
        """
        Unregister a subscribed client

        :param client_id: The UUID of the subscribing client
        :type client_id: UUID
        """
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


class SHMChannel(Channel):
    """
    Channel specialized for SHM telemetry from publisher.
    Prefers SHM telemetry but will handle TCP if publisher falls back.
    """

    async def _process_publisher_messages(self) -> None:
        try:
            while True:
                msg = await self._pub_message_queue.get()
                buf_idx = msg.msg_id % self.num_buffers

                if msg.command == Command.TX_TCP.value and msg.tcp_data is not None:
                    assert MessageMarshal.msg_id(msg.tcp_data) == msg.msg_id
                    self.cache.put_from_mem(memoryview(msg.tcp_data).toreadonly())
                else:
                    if msg.command != Command.TX_SHM.value:
                        logger.warning(
                            f"Channel {self.id} expected TX_SHM, received {msg.command}"
                        )
                    if (
                        self.shm is not None
                        and msg.shm_name is not None
                        and self.shm.name != msg.shm_name
                    ):
                        shm_entries = self.cache.keys()
                        self.cache.clear()
                        self.shm.close()
                        await self.shm.wait_closed()

                        try:
                            self.shm = await GraphService(
                                self._graph_address
                            ).attach_shm(msg.shm_name)
                        except ValueError:
                            logger.info("Invalid SHM received from publisher; may be dead")
                            raise

                        for id in shm_entries:
                            self.cache.put_from_mem(self.shm[id % self.num_buffers])

                    assert self.shm is not None
                    assert MessageMarshal.msg_id(self.shm[buf_idx]) == msg.msg_id
                    self.cache.put_from_mem(self.shm[buf_idx])

                if not self._notify_clients(msg.msg_id):
                    self.cache.release(msg.msg_id)
                    self._acknowledge(msg.msg_id)

        finally:
            self.cache.clear()
            if self.shm is not None:
                self.shm.close()

            logger.debug(f"disconnected: channel:{self.id} -> pub:{self.pub_id}")


class LocalChannel(SHMChannel):
    """
    Channel specialized for local publisher communication.
    No network telemetry expected; messages arrive via put_local.
    """

    _local_backpressure: Backpressure

    async def _process_publisher_messages(self) -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            pass
        finally:
            self.cache.clear()
            if self.shm is not None:
                self.shm.close()

            logger.debug(f"disconnected: channel:{self.id} -> pub:{self.pub_id}")

    def put(self, msg_id: int, msg: typing.Any) -> None:
        """
        Put a message DIRECTLY into cache and notify all clients.
        """
        buf_idx = msg_id % self.num_buffers
        if self._notify_clients(msg_id):
            self.cache.put_local(msg, msg_id)
            self._local_backpressure.lease(self.id, buf_idx)


class TCPChannel(Channel):
    """
    Channel specialized for TCP telemetry from publisher.
    Expects only TX_TCP messages after handshake.
    """

    async def _process_publisher_messages(self) -> None:
        try:
            while True:
                msg = await self._pub_message_queue.get()
                buf_idx = msg.msg_id % self.num_buffers

                if msg.command != Command.TX_TCP.value or msg.tcp_data is None:
                    logger.error(
                        f"Channel {self.id} expected TX_TCP, received {msg.command}"
                    )
                    continue

                assert MessageMarshal.msg_id(msg.tcp_data) == msg.msg_id
                self.cache.put_from_mem(memoryview(msg.tcp_data).toreadonly())

                if not self._notify_clients(msg.msg_id):
                    self.cache.release(msg.msg_id)
                    self._acknowledge(msg.msg_id)

        finally:
            self.cache.clear()
            if self.shm is not None:
                self.shm.close()

            logger.debug(f"disconnected: channel:{self.id} -> pub:{self.pub_id}")
