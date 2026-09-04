import os
import asyncio
import typing
import logging

from uuid import UUID
from contextlib import contextmanager, suppress

from .shm import SHMContext
from .messagemarshal import MessageMarshal, UninitializedMemory
from .backpressure import Backpressure
from .messagecache import MessageCache, CacheMiss
from .graphserver import GraphService
from .frameproto import FramedProtocol
from .netprotocol import (
    Command,
    Address,
    AddressType,
    BYTEORDER,
    read_str,
    uint64_to_bytes,
    encode_str,
    close_stream_writer,
)
from .graphmeta import ProfileChannelType

# TX_SHM and TX_TCP share a fixed prefix: command byte, msg_id, then a length
# that covers the rest of the frame (the SHM segment name, or the serialized
# message respectively).
_FRAME_PREFIX = 1 + 8 + 8

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


class ChannelProtocol(FramedProtocol):
    """
    Receives message notifications from a Publisher.

    During dispatch, :meth:`frames_available` runs synchronously in the
    transport's read callback, so an incoming message is cached and its
    subscribers notified without the extra task wakeup the streams API costs.
    """

    def __init__(self) -> None:
        super().__init__()
        self._channel: "Channel | None" = None

    def bind(self, channel: "Channel") -> None:
        self._channel = channel

    def connection_lost(self, exc: BaseException | None) -> None:
        super().connection_lost(exc)
        if self._channel is not None:
            self._channel._on_disconnected()

    def frames_available(self) -> None:
        chan = self._channel
        assert chan is not None, "protocol dispatching before bind()"
        buf = self._buffer

        while True:
            if len(buf) < _FRAME_PREFIX:
                return
            cmd = bytes(buf[0:1])
            msg_id = int.from_bytes(buf[1:9], BYTEORDER)
            tail = int.from_bytes(buf[9:_FRAME_PREFIX], BYTEORDER)
            end = _FRAME_PREFIX + tail
            if len(buf) < end:
                return

            if cmd == Command.TX_SHM.value:
                shm_name = bytes(buf[_FRAME_PREFIX:end]).decode("utf-8")
                if chan.shm is None or chan.shm.name != shm_name:
                    # Attaching is async and cannot happen in a read callback.
                    # Stop reading, leave this frame buffered, and hand off.
                    self.pause_reading()
                    asyncio.get_running_loop().create_task(
                        self._reattach_shm(shm_name, end, msg_id),
                        name=f"chan-{chan.id}: reattach_shm",
                    )
                    return
                del buf[:end]
                chan._deliver_from_shm(msg_id)

            elif cmd == Command.TX_TCP.value:
                payload = bytes(buf[_FRAME_PREFIX:end])
                del buf[:end]
                chan._deliver_from_tcp(msg_id, payload)

            else:
                raise ValueError(f"unimplemented data telemetry: {cmd!r}")

    async def _reattach_shm(self, shm_name: str, frame_end: int, msg_id: int) -> None:
        """
        Swap to a new SHM generation, then resume dispatch.

        The triggering frame is still buffered: on success we re-dispatch it
        against the new segment, on failure we drop it and release its
        backpressure so the publisher is not stalled by it.
        """
        chan = self._channel
        assert chan is not None

        try:
            preserved = chan._snapshot_cached_messages()
            chan.cache.clear()
            for preserved_msg in preserved:
                chan.cache.put_from_mem(preserved_msg)

            if chan.shm is not None:
                old_shm = chan.shm
                chan.shm = None
                old_shm.close()
                await old_shm.wait_closed()

            try:
                chan.shm = await GraphService(chan._graph_address).attach_shm(shm_name)
            except ValueError:
                logger.warning(
                    "Channel %s received stale SHM %s for publisher %s; waiting for next valid SHM",
                    chan.id,
                    shm_name,
                    chan.pub_id,
                )
                chan.shm = None

            if chan.shm is None:
                # Drop the frame we parked, otherwise dispatch would retry the
                # attach against the same name forever.
                logger.warning(
                    "Channel %s dropping message %s from publisher %s because its SHM generation is stale",
                    chan.id,
                    msg_id,
                    chan.pub_id,
                )
                del self._buffer[:frame_end]
                chan._release_backpressure(msg_id, chan.id)
        except Exception:
            logger.exception("Channel %s failed to reattach SHM", chan.id)
            self.close()
            return

        self.resume_reading()
        self._drain_frames()


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
    _proto: ChannelProtocol
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

        # The per-message path uses a Protocol rather than the streams API so
        # that an incoming notification is handled in the transport's read
        # callback instead of costing an extra task wakeup. The handshake below
        # is sequential and runs once, so it reads in the ordinary awaiting way.
        loop = asyncio.get_running_loop()
        _, proto = await loop.create_connection(ChannelProtocol, *pub_address)
        proto.write(Command.CHANNEL.value + encode_str(id_str))

        topic = await proto.read_str()

        shm = None
        shm_name = await proto.read_str()
        try:
            shm = await graph_service.attach_shm(shm_name)
            proto.write(Command.SHM_OK.value)
        except (ValueError, OSError):
            shm = None
            proto.write(Command.SHM_ATTACH_FAILED.value)
        proto.write(uint64_to_bytes(os.getpid()))

        result = await proto.read_exactly(1)
        if result != Command.COMPLETE.value:
            # NOTE: The only reason this would happen is if the
            # publisher's writer is closed due to a crash or shutdown
            proto.close()
            raise ValueError(f"failed to create channel {pub_id=}")

        num_buffers = await proto.read_uint64()
        if num_buffers <= 0:
            proto.close()
            raise ValueError("publisher reports invalid num_buffers")

        chan = cls(UUID(id_str), pub_id, num_buffers, shm, graph_address, _guard=cls._SENTINEL)
        chan.topic = topic

        chan._graph_task = asyncio.create_task(
            chan._graph_connection(graph_reader, graph_writer),
            name=f"chan-{chan.id}: _graph_connection",
        )

        chan._proto = proto
        proto.bind(chan)
        # Anything the publisher sent between the handshake and here is already
        # buffered; start_dispatch drains it before returning.
        proto.start_dispatch()

        logger.debug(f"created channel {chan.id=} {pub_id=} {pub_address=}")

        return chan

    def close(self) -> None:
        """
        Mark the Channel for shutdown and resource deallocation
        """
        self._proto.close()
        self._graph_task.cancel()

    async def wait_closed(self) -> None:
        """
        Wait until the Channel has properly shutdown and its resources have been deallocated.
        """
        await self._proto.wait_closed()
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

    def _deliver_from_shm(self, msg_id: int) -> None:
        """
        Cache the message sitting in our SHM slot and notify clients.

        Called inline from :meth:`ChannelProtocol.frames_available`, so the
        caller has already established that ``self.shm`` matches the segment the
        publisher named.
        """
        assert self.shm is not None
        shm_buf = self.shm[msg_id % self.num_buffers]
        # The slot for this msg_id may be uninitialized after a mid-stream
        # resize; msg_id() raises UninitializedMemory in that case. Treat it as
        # a mismatch (drop + release) rather than letting it kill the channel.
        try:
            slot_msg_id = MessageMarshal.msg_id(shm_buf)
        except UninitializedMemory:
            slot_msg_id = None
        if slot_msg_id != msg_id:
            logger.warning(
                "Channel %s skipping stale SHM contents for message %s from publisher %s; will use next valid SHM generation",
                self.id,
                msg_id,
                self.pub_id,
            )
            self._release_backpressure(msg_id, self.id)
            return

        self.cache.put_from_mem(shm_buf)
        self._set_channel_kind(ProfileChannelType.SHM)
        self._finish_delivery(msg_id)

    def _deliver_from_tcp(self, msg_id: int, obj_bytes: bytes) -> None:
        """
        Cache a message that arrived inline over TCP and notify clients.

        Called inline from :meth:`ChannelProtocol.frames_available`.
        """
        assert MessageMarshal.msg_id(obj_bytes) == msg_id
        self.cache.put_from_mem(memoryview(obj_bytes).toreadonly())
        self._set_channel_kind(ProfileChannelType.TCP)
        self._finish_delivery(msg_id)

    def _finish_delivery(self, msg_id: int) -> None:
        if not self._notify_clients(msg_id):
            # Nobody is listening; need to ack!
            self.cache.release(msg_id)
            self._acknowledge(msg_id)

    def _on_disconnected(self) -> None:
        """
        Release per-connection resources. Invoked from the protocol's
        ``connection_lost``, which replaces the old task's ``finally`` block.
        """
        self.cache.clear()
        if self.shm is not None:
            self.shm.close()
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

    def _snapshot_cached_messages(self) -> list[memoryview]:
        if self.shm is None:
            return []

        preserved: list[memoryview] = []
        for msg_id in self.cache.keys():
            shm_buf = self.shm[msg_id % self.num_buffers]
            try:
                if MessageMarshal.msg_id(shm_buf) == msg_id:
                    preserved.append(memoryview(bytes(shm_buf)).toreadonly())
            except UninitializedMemory:
                pass

        return preserved

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
            try:
                self.cache.release(msg_id)
            except CacheMiss:
                logger.debug(
                    "Channel %s observed cache miss while releasing msg_id=%s from publisher %s; continuing backpressure release",
                    self.id,
                    msg_id,
                    self.pub_id,
                )

            # If pub is in same process as this channel, avoid TCP
            if self._local_backpressure is not None:
                self._local_backpressure.free(self.id, buf_idx)
            else:
                self._acknowledge(msg_id)

    def _acknowledge(self, msg_id: int) -> None:
        try:
            ack = Command.RX_ACK.value + uint64_to_bytes(msg_id)
            self._proto.write(ack)
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
