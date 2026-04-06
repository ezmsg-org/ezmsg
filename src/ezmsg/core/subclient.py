import asyncio
import logging
import typing

from uuid import UUID
from contextlib import asynccontextmanager, suppress
from copy import deepcopy

from .graphserver import GraphService
from .channelmanager import CHANNELS
from .messagechannel import NotificationQueue, LeakyQueue, Channel
from .profiling import PROFILES, PROFILE_TIME

from .netprotocol import (
    AddressType,
    read_str,
    encode_str,
    close_stream_writer,
    Command,
)


logger = logging.getLogger("ezmsg")


class Subscriber:
    """
    A subscriber client for receiving messages from publishers.

    Subscriber manages connections to multiple publishers, handles different
    transport methods (local, shared memory, TCP), and provides both copying
    and zero-copy message access patterns with automatic acknowledgment.
    """

    _SENTINEL = object()

    id: UUID
    topic: str
    leaky: bool

    _graph_address: AddressType | None
    _graph_task: asyncio.Task[None]
    _incoming: NotificationQueue
    _profile: object

    # FIXME: This event allows Subscriber.create to block until
    # incoming initial connections (UPDATE) has completed. The
    # logic is confusing and difficult to follow, but this event
    # serves an important purpose for the time being.
    _initialized: asyncio.Event

    # NOTE: This is an optimization to retain a local handle to channels
    # so that dict lookup and wrapper contextmanager aren't in hotpath
    _channels: dict[UUID, Channel]

    @classmethod
    async def create(
        cls, topic: str, graph_address: AddressType | None, **kwargs
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
        reader, writer = await GraphService(graph_address).open_connection()
        writer.write(Command.SUBSCRIBE.value)
        writer.write(encode_str(topic))
        sub_id_str = await read_str(reader)
        sub_id = UUID(sub_id_str)

        sub = cls(sub_id, topic, graph_address, _guard=cls._SENTINEL, **kwargs)

        sub._graph_task = asyncio.create_task(
            sub._graph_connection(reader, writer),
            name=f"sub-{sub.id}: _graph_connection",
        )

        # FIXME: We need to wait for _graph_task to service an UPDATE
        # then receive a COMPLETE before we return a fully connected
        # subscriber ready for recv.
        await sub._initialized.wait()

        logger.debug(f"created sub {sub.id=} {topic=}")

        return sub

    def __init__(
        self,
        id: UUID,
        topic: str,
        graph_address: AddressType | None,
        _guard=None,
        leaky: bool = False,
        max_queue: int | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize a Subscriber instance.

        DO NOT USE this constructor, use Subscriber.create instead.

        :param id: Unique identifier for this subscriber.
        :type id: UUID
        :param topic: The topic this subscriber listens to.
        :type topic: str
        :param graph_address: Address of the graph server.
        :type graph_address: AddressType | None
        :param leaky: If True, drop oldest messages when queue is full.
        :type leaky: bool
        :param max_queue: Maximum queue size (ignored if leaky=False).
        :type max_queue: int | None
        :param kwargs: Additional keyword arguments (unused).
        """
        if _guard is not self._SENTINEL:
            raise TypeError(
                "Subscriber cannot be instantiated directly."
                "Use 'await Subscriber.create(...)' instead."
            )
        self.id = id
        self.topic = topic
        self.leaky = leaky
        self._graph_address = graph_address

        self._channels = dict()
        self._active_msg_seq: int | None = None
        self._active_trace_sampled = False
        if self.leaky:
            self._incoming = LeakyQueue(
                1 if max_queue is None else max_queue, self._handle_dropped_notification
            )
        else:
            self._incoming = asyncio.Queue()
        self._initialized = asyncio.Event()
        self._profile = PROFILES.register_subscriber(self.id, self.topic)

    def _handle_dropped_notification(
        self, notification: typing.Tuple[UUID, int]
    ) -> None:
        """
        Handle a dropped notification by releasing backpressure.

        Called by LeakyQueue when a notification is dropped to ensure
        backpressure is properly released for messages that will never be read.

        :param notification: Tuple of (publisher_id, message_id) that was dropped.
        :type notification: tuple[UUID, int]
        """
        pub_id, msg_id = notification
        if pub_id in self._channels:
            self._channels[pub_id].release_without_get(msg_id, self.id)

    def close(self) -> None:
        """
        Close the subscriber and cancel all associated tasks.

        Cancels graph connection, all publisher connection tasks,
        and closes all shared memory contexts.
        """
        self._graph_task.cancel()
        PROFILES.unregister_subscriber(self.id)

    async def wait_closed(self) -> None:
        """
        Wait for all subscriber resources to be fully closed.

        Waits for graph connection termination, all publisher connection
        tasks to complete, and all shared memory contexts to close.
        """
        with suppress(asyncio.CancelledError):
            await self._graph_task

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

                if cmd == Command.COMPLETE.value:
                    # FIXME: The only time GraphServer will send us a COMPLETE
                    # is when it is done with Subscriber.create.  Unfortunately
                    # part of creating the subscriber involves receiving an
                    # UPDATE with all of the initial connections so that
                    # messaging resolves as expected immediately after creation
                    # is completed.  The only way we can service this UPDATE
                    # is by passing control of the GraphServer's StreamReader
                    # to this task, which can handle the UPDATE -- mid-creation
                    # then we receive the COMPLETE in here, set the _initialized
                    # event, which we wait on in Subscriber.create, releasing the
                    # block and returning a fully connected/ready subscriber.
                    # While this currently works, its non-obvious what this
                    # accomplishes and why its implemented this way.  I just
                    # wasted 2 hours removing this seemingly un-necessary event
                    # to introduce a bug that is only resolved by reintroducing
                    # the event.  Some thought should be put into replacing the
                    # bespoke communication protocol with something a bit more
                    # standard (JSON RPC?) with a more common/logical pattern
                    # for creating/handling comms.  We will probably want to
                    # keep the bespoke comms for the publisher->channel link
                    # as that is in the hot path.
                    self._initialized.set()

                elif cmd == Command.UPDATE.value:
                    update = await read_str(reader)
                    pub_ids = (
                        set([UUID(id) for id in update.split(",")]) if update else set()
                    )
                    cur_pubs = set(self._channels.keys())

                    # Register new channels
                    for pub_id in set(pub_ids - cur_pubs):
                        channel = await CHANNELS.register(
                            pub_id, self.id, self._incoming, self._graph_address
                        )

                        if self.leaky and self._incoming.maxsize >= channel.num_buffers:
                            logger.warning(
                                f"Leaky Subscriber {self.topic} may cause "
                                f"backpressure in Publisher {channel.topic}. "
                                f"Subscriber's max queue size ({self._incoming.maxsize}) >= "
                                f"Publisher's num_buffers ({channel.num_buffers})."
                            )

                        self._channels[pub_id] = channel

                    # Unregister expired channels
                    for pub_id in set(cur_pubs - pub_ids):
                        await CHANNELS.unregister(pub_id, self.id, self._graph_address)
                        del self._channels[pub_id]

                    writer.write(Command.COMPLETE.value)
                    await writer.drain()

                else:
                    logger.warning(
                        f"Subscriber {self.topic}({self.id}) rx unknown command from GraphServer: {cmd}"
                    )

        except (ConnectionResetError, BrokenPipeError):
            logger.debug(
                f"Subscriber {self.topic}({self.id}) lost connection to graph server"
            )

        finally:
            for pub_id in self._channels:
                await CHANNELS.unregister(pub_id, self.id, self._graph_address)
            await close_stream_writer(writer)

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
    async def recv_zero_copy(self) -> typing.AsyncGenerator[typing.Any, None]:
        """
        Receive the next message with zero-copy access.

        This context manager provides direct access to the cached message
        without copying. The message should not be modified or stored beyond
        the context manager's scope.

        :return: Context manager yielding the received message.
        :rtype: collections.abc.AsyncGenerator[typing.Any, None]
        """
        while True:
            pub_id, msg_id = await self._incoming.get()
            if pub_id in self._channels:
                break
            # Stale notification from an unregistered publisher — skip.

        channel = self._channels[pub_id]
        channel_kind = channel.channel_kind
        self._active_msg_seq = msg_id
        self._active_trace_sampled = self._profile.begin_message(channel_kind)
        try:
            trace_lease = self._profile._trace_lease_time_enabled
            start_ns = PROFILE_TIME() if trace_lease else None
            with channel.get(msg_id, self.id) as msg:
                yield msg
            lease_ns = None
            if trace_lease and start_ns is not None:
                lease_ns = PROFILE_TIME() - start_ns
            self._profile.record_lease_time(
                channel_kind,
                lease_ns,
                msg_seq=msg_id,
                sampled=self._active_trace_sampled,
            )
        finally:
            self._active_msg_seq = None
            self._active_trace_sampled = False

    def begin_profile(self) -> int:
        if not self._profile._trace_user_span_enabled or not self._active_trace_sampled:
            return 0
        return PROFILE_TIME()

    def end_profile(self, start_ns: int, label: str | None = None) -> None:
        if start_ns <= 0:
            return
        end_ns = PROFILE_TIME()
        self._profile.record_user_span(
            end_ns - start_ns,
            label,
            msg_seq=self._active_msg_seq,
            sampled=self._active_trace_sampled,
        )
