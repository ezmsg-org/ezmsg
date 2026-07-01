import os
import asyncio
import typing
import logging

from uuid import UUID
from contextlib import contextmanager, suppress

from .shm import SHMContext
from .messagemarshal import MessageMarshal
from .backpressure import Backpressure
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
)
from .graphmeta import ProfileChannelType

logger = logging.getLogger("ezmsg")


class LeakyQueue(asyncio.Queue[typing.Tuple[UUID, int]]):
    """
    An asyncio.Queue that drops oldest items when full.

    When putting a new item into a full queue, the oldest item is
    dropped to make room.

    :param maxsize: Maximum queue size (must be positive)
    :param on_drop: Optional callback called with dropped item when dropping
    """

    def __init__(
        self,
        maxsize: int,
        on_drop: typing.Callable[[typing.Any], None] | None = None,
    ):
        super().__init__(maxsize=maxsize)
        self._on_drop = on_drop

    def _drop_oldest(self) -> None:
        """Drop the oldest item from the queue, calling on_drop if set."""
        try:
            dropped = self.get_nowait()
            if self._on_drop is not None:
                self._on_drop(dropped)
        except asyncio.QueueEmpty:
            pass

    async def put(self, item: typing.Tuple[UUID, int]) -> None:
        """Put an item into the queue, dropping oldest if full."""
        if self.full():
            self._drop_oldest()
        await super().put(item)

    def put_nowait(self, item: typing.Tuple[UUID, int]) -> None:
        """Put an item without blocking, dropping oldest if full."""
        if self.full():
            self._drop_oldest()
        super().put_nowait(item)


NotificationQueue = asyncio.Queue[typing.Tuple[UUID, int]] | LeakyQueue


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
    _pub_task: asyncio.Task[None]
    _pub_writer: asyncio.StreamWriter
    _graph_address: AddressType | None
    _local_backpressure: Backpressure | None
    _channel_kind: ProfileChannelType

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
        self._channel_kind = ProfileChannelType.UNKNOWN

    @classmethod
    async def create(
        cls,
        pub_id: UUID,
        graph_address: AddressType,
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

        reader, writer = await asyncio.open_connection(*pub_address)
        writer.write(Command.CHANNEL.value)
        writer.write(encode_str(id_str))

        topic = await read_str(reader)

        shm = None
        shm_name = await read_str(reader)
        try:
            shm = await graph_service.attach_shm(shm_name)
            writer.write(Command.SHM_OK.value)
        except (ValueError, OSError):
            shm = None
            writer.write(Command.SHM_ATTACH_FAILED.value)
        writer.write(uint64_to_bytes(os.getpid()))

        result = await reader.read(1)
        if result != Command.COMPLETE.value:
            # NOTE: The only reason this would happen is if the
            # publisher's writer is closed due to a crash or shutdown
            raise ValueError(f"failed to create channel {pub_id=}")

        num_buffers = await read_int(reader)
        assert num_buffers > 0, "publisher reports invalid num_buffers"

        chan = cls(UUID(id_str), pub_id, num_buffers, shm, graph_address, _guard=cls._SENTINEL)
        chan.topic = topic

        chan._graph_task = asyncio.create_task(
            chan._graph_connection(graph_reader, graph_writer),
            name=f"chan-{chan.id}: _graph_connection",
        )

        chan._pub_writer = writer
        chan._pub_task = asyncio.create_task(
            chan._publisher_connection(reader),
            name=f"chan-{chan.id}: _publisher_connection",
        )

        logger.debug(f"created channel {chan.id=} {pub_id=} {pub_address=}")

        return chan

    def close(self) -> None:
        """
        Mark the Channel for shutdown and resource deallocation
        """
        self._pub_task.cancel()
        self._graph_task.cancel()

    async def wait_closed(self) -> None:
        """
        Wait until the Channel has properly shutdown and its resources have been deallocated.
        """
        with suppress(asyncio.CancelledError):
            await self._pub_task
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

    async def _publisher_connection(self, reader: asyncio.StreamReader) -> None:
        """
        The task that handles communication between the Channel and the Publisher it receives messages from.
        """
        try:
            while True:
                msg = await reader.read(1)

                if not msg:
                    break

                msg_id = await read_int(reader)
                buf_idx = msg_id % self.num_buffers
                channel_kind = ProfileChannelType.UNKNOWN

                if msg == Command.TX_SHM.value:
                    channel_kind = ProfileChannelType.SHM
                    shm_name = await read_str(reader)

                    if self.shm is not None and self.shm.name != shm_name:
                        shm_entries = self.cache.keys()
                        self.cache.clear()
                        self.shm.close()
                        await self.shm.wait_closed()

                        try:
                            self.shm = await GraphService(
                                self._graph_address
                            ).attach_shm(shm_name)
                        except ValueError:
                            logger.info(
                                "Invalid SHM received from publisher; may be dead"
                            )
                            raise

                        for id in shm_entries:
                            self.cache.put_from_mem(self.shm[id % self.num_buffers])

                    assert self.shm is not None
                    assert MessageMarshal.msg_id(self.shm[buf_idx]) == msg_id
                    self.cache.put_from_mem(self.shm[buf_idx])

                elif msg == Command.TX_TCP.value:
                    channel_kind = ProfileChannelType.TCP
                    buf_size = await read_int(reader)
                    obj_bytes = await reader.readexactly(buf_size)
                    assert MessageMarshal.msg_id(obj_bytes) == msg_id
                    self.cache.put_from_mem(memoryview(obj_bytes).toreadonly())

                else:
                    raise ValueError(f"unimplemented data telemetry: {msg}")

                self._set_channel_kind(channel_kind)

                if not self._notify_clients(msg_id):
                    # Nobody is listening; need to ack!
                    self.cache.release(msg_id)
                    self._acknowledge(msg_id)

        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            logger.debug(f"connection fail: channel:{self.id} - pub:{self.pub_id}")

        finally:
            self.cache.clear()
            if self.shm is not None:
                self.shm.close()

            await close_stream_writer(self._pub_writer)

            logger.debug(f"disconnected: channel:{self.id} -> pub:{self.pub_id}")

    def _set_channel_kind(self, kind: ProfileChannelType) -> None:
        if self._channel_kind == ProfileChannelType.UNKNOWN:
            self._channel_kind = kind
        elif self._channel_kind != kind:
            logger.warning(
                "Channel %s observed channel kind change: %s -> %s",
                self.id,
                self._channel_kind.value,
                kind.value,
            )
            self._channel_kind = kind

    @property
    def channel_kind(self) -> ProfileChannelType:
        return self._channel_kind

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
        Put a message DIRECTLY into cache and notify all clients.
        .. note:: This command should ONLY be used by Publishers that are in the same process as this Channel.
        """
        if self._local_backpressure is None:
            raise ValueError(
                "cannot put_local without access to publisher backpressure (is publisher in same process?)"
            )

        buf_idx = msg_id % self.num_buffers
        self._set_channel_kind(ProfileChannelType.LOCAL)
        if self._notify_clients(msg_id):
            self.cache.put_local(msg, msg_id)
            self._local_backpressure.lease(self.id, buf_idx)

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
            self._release_backpressure(msg_id, client_id)

    def release_without_get(self, msg_id: int, client_id: UUID) -> None:
        """
        Release backpressure for a message without retrieving it.

        Used by leaky subscribers when dropping notifications to ensure
        backpressure is properly released for messages that will never be read.

        :param msg_id: Message ID to release
        :type msg_id: int
        :param client_id: UUID of client releasing this message
        :type client_id: UUID
        """
        self._release_backpressure(msg_id, client_id)

    def _release_backpressure(self, msg_id: int, client_id: UUID) -> None:
        """
        Internal method to release backpressure for a message.

        :param msg_id: Message ID to release
        :type msg_id: int
        :param client_id: UUID of client releasing this message
        :type client_id: UUID
        """
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
            self._pub_writer.write(ack)
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
