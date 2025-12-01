import os
import asyncio
import typing
import logging

from uuid import UUID
from contextlib import contextmanager, suppress

from .shm import SHMContext
from .messagemarshal import MessageMarshal
from .backpressure import Backpressure
from .publisherprotocol import PublisherClientProtocol, SHMMessage, TCPMessage
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

T = typing.TypeVar('T', SHMMessage, TCPMessage)

class Channel(typing.Generic[T]):
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
    clients: dict[UUID, asyncio.Queue[tuple[UUID, int]] | None]
    backpressure: Backpressure

    _graph_task: asyncio.Task[None]
    _graph_address: AddressType | None

    def __init__(
        self,
        id: UUID,
        pub_id: UUID,
        num_buffers: int,
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
        self.cache = MessageCache(self.num_buffers)
        self.clients = dict()
        self.backpressure = Backpressure(self.num_buffers)
        self._graph_address = graph_address

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

        # Create a connection to the publisher
        loop = asyncio.get_running_loop()
        pub_message_queue: asyncio.Queue[T] = asyncio.Queue()
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
        num_buffers, mode = await protocol.handshake_complete
        assert num_buffers > 0, "publisher reports invalid num_buffers"

        # WE do all of this here, and make the proper channel; closing the publisher connection if Local
        chan: Channel | None = None
        if protocol.mode == Command.TX_LOCAL.value:
            chan = LocalChannel(UUID(id_str), pub_id, num_buffers, graph_address, cls._SENTINEL)
        else:
            if protocol.mode == Command.TX_SHM.value:
                assert shm is not None, "publisher will transmit via SHM, but channel failed to attach"
                chan = SHMChannel(UUID(id_str), pub_id, num_buffers, graph_address, shm, cls._SENTINEL)
            elif protocol.mode == Command.TX_TCP.value:
                chan = TCPChannel(UUID(id_str), pub_id, num_buffers, graph_address, cls._SENTINEL)

        if chan is None:
            raise ValueError(f"publisher negotiated for unknown channel type: {protocol.mode=}")

        chan._graph_task = asyncio.create_task(
            chan._graph_connection(graph_reader, graph_writer),
            name=f"chan-{chan.id}: _graph_connection",
        )

        if not isinstance(chan, SHMChannel) and shm is not None:
            shm.close()
            await shm.wait_closed()

        if isinstance(chan, RemoteChannel):
            # Set up publisher connection (protocol already connected!)
            chan._pub_message_queue = pub_message_queue
            chan._pub_protocol = protocol
            chan._pub_transport = transport

            # Single task to process messages from protocol
            chan._pub_process_task = asyncio.create_task(
                chan._process_publisher_messages(),
                name=f"chan-{chan.id}: _process_messages",
            )

        logger.debug(f"created {type(chan).__name__}({chan.id}) {pub_id=} {pub_address=}")

        return chan

    def close(self) -> None:
        self._graph_task.cancel()

    async def wait_closed(self) -> None:
        """
        Wait until the Channel has properly shutdown and its resources have been deallocated.
        """
        with suppress(asyncio.CancelledError):
            await self._graph_task

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
        raise NotImplementedError

    def register(
        self,
        client_id: UUID,
        queue: asyncio.Queue[tuple[UUID, int]] | None = None,
    ) -> None:
        """
        Register an interested client and optionally provide a queue for 
        incoming message notifications.

        :param client_id: The UUID of the interested client
        :type client_id: UUID
        :param queue: An optional notification queue for the client
        :type queue: asyncio.Queue[tuple[UUID, int]] | None
        """
        self.clients[client_id] = queue

    def unregister(self, client_id: UUID) -> None:
        """
        Unregister a client

        :param client_id: The UUID of the client
        :type client_id: UUID
        """
        queue = self.clients.get(client_id, None)

        if queue is not None:
            for _ in range(queue.qsize()):
                pub_id, msg_id = queue.get_nowait()
                if pub_id != self.pub_id:
                    queue.put_nowait((pub_id, msg_id))
                
            del self.clients[client_id]

        self.backpressure.free(client_id)

class RemoteChannel(typing.Generic[T], Channel[T]):

    _pub_protocol: PublisherClientProtocol
    _pub_transport: asyncio.Transport
    _pub_message_queue: asyncio.Queue[T]
    _pub_process_task: asyncio.Task[None]
    
    def close(self):
        super().close()
        self._pub_transport.close()
        self._pub_process_task.cancel()

    async def wait_closed(self) -> None:
        await super().wait_closed()
        with suppress(asyncio.CancelledError):
            await self._pub_process_task

    async def _process_publisher_messages(self) -> None:
        raise NotImplementedError

    def _acknowledge(self, msg_id: int) -> None:
        try:
            ack = Command.RX_ACK.value + uint64_to_bytes(msg_id)
            self._pub_transport.write(ack)
        except (BrokenPipeError, ConnectionResetError):
            logger.info(f"ack fail: channel:{self.id} -> pub:{self.pub_id}")

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
                self._acknowledge(msg_id)


class SHMChannel(RemoteChannel[SHMMessage]):
    """
    Channel specialized for SHM telemetry from publisher.
    Prefers SHM telemetry but will handle TCP if publisher falls back.
    """

    shm: SHMContext

    def __init__(
        self,
        id: UUID,
        pub_id: UUID,
        num_buffers: int,
        graph_address: AddressType | None,
        shm: SHMContext,
        _guard = None,
    ) -> None:
        super().__init__(
            id = id,
            pub_id = pub_id,
            num_buffers = num_buffers,
            graph_address = graph_address,
            _guard = _guard
        )

        self.shm = shm


    async def wait_closed(self) -> None:
        await super().wait_closed()
        if self.shm is not None:
            await self.shm.wait_closed()

    async def _process_publisher_messages(self) -> None:
        try:
            while True:
                msg = await self._pub_message_queue.get()
                buf_idx = msg.msg_id % self.num_buffers
                
                if self.shm.name != msg.shm_name:
                    # SHM resized
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

                    # On resize, server copies all entries from old SHM into new SHM
                    # So just expose old entries in the new SHM.
                    for id in shm_entries:
                        self.cache.put_from_mem(self.shm[id % self.num_buffers])

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


class TCPChannel(RemoteChannel[TCPMessage]):
    """
    Channel specialized for TCP telemetry from publisher.
    Expects only TX_TCP messages after handshake.
    """

    async def _process_publisher_messages(self) -> None:
        try:
            while True:
                msg = await self._pub_message_queue.get()

                assert MessageMarshal.msg_id(msg.tcp_data) == msg.msg_id
                self.cache.put_from_mem(memoryview(msg.tcp_data).toreadonly())

                if not self._notify_clients(msg.msg_id):
                    self.cache.release(msg.msg_id)
                    self._acknowledge(msg.msg_id)

        finally:
            self.cache.clear()
            logger.debug(f"disconnected: channel:{self.id} -> pub:{self.pub_id}")


class LocalChannel(Channel):
    """
    Channel specialized for local publisher communication.
    No network telemetry expected; messages arrive via put.
    NOTE: After creation, the Publisher should inject its backpressure object
    (e.g. chan.local_backpressure = self._backpressure)
    """

    _local_backpressure: Backpressure | None = None

    @property
    def local_backpressure(self) -> Backpressure:
        if self._local_backpressure is None:
            raise RuntimeError("LocalChannel used before publisher has set the local backpressure")
        return self._local_backpressure
    
    @local_backpressure.setter
    def local_backpressure(self, value: Backpressure):
        if self._local_backpressure is not None:
            raise RuntimeError("Local backpressure has already been set and cannot be changed")
        self._local_backpressure = value

    async def _process_publisher_messages(self) -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            pass
        finally:
            self.cache.clear()
            logger.debug(f"disconnected: channel:{self.id} -> pub:{self.pub_id}")

    def put(self, msg_id: int, msg: typing.Any) -> None:
        """
        Put a message DIRECTLY into cache and notify all clients.
        """
        buf_idx = msg_id % self.num_buffers
        if self._notify_clients(msg_id):
            self.cache.put_local(msg, msg_id)
            self.local_backpressure.lease(self.id, buf_idx)
    
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
                self.local_backpressure.free(self.id, buf_idx)

