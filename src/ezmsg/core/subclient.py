import asyncio
import logging
import typing

from uuid import UUID
from contextlib import asynccontextmanager, suppress
from copy import deepcopy

from .graphserver import GraphService
from .channelmanager import CHANNELS
from .messagechannel import NotificationQueue, Channel

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

    _graph_address: AddressType | None
    _graph_task: asyncio.Task[None]
    _cur_pubs: set[UUID]
    _incoming: NotificationQueue

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
        _guard = None, 
        **kwargs
    ) -> None:
        """
        Initialize a Subscriber instance.

        DO NOT USE this constructor, use Subscriber.create instead.

        :param id: Unique identifier for this subscriber.
        :type id: UUID
        :param topic: The topic this subscriber listens to.
        :type topic: str
        :param graph_service: Service for graph operations.
        :type graph_service: GraphService
        :param kwargs: Additional keyword arguments (unused).
        """
        if _guard is not self._SENTINEL:
            raise TypeError(
                "Subscriber cannot be instantiated directly."
                "Use 'await Subscriber.create(...)' instead."
            )
        self.id = id
        self.topic = topic
        self._graph_address = graph_address

        self._cur_pubs = set()
        self._incoming = asyncio.Queue()
        self._channels = dict()
        self._initialized = asyncio.Event()

    def close(self) -> None:
        """
        Close the subscriber and cancel all associated tasks.

        Cancels graph connection, all publisher connection tasks,
        and closes all shared memory contexts.
        """
        self._graph_task.cancel()

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

                    for pub_id in set(pub_ids - self._cur_pubs):
                        channel = await CHANNELS.register(
                            pub_id, self.id, self._incoming, self._graph_address
                        )
                        self._channels[pub_id] = channel

                    for pub_id in set(self._cur_pubs - pub_ids):
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
        pub_id, msg_id = await self._incoming.get()

        with self._channels[pub_id].get(msg_id, self.id) as msg:
            yield msg
