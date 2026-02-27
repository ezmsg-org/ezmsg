import asyncio
import logging
import pickle
import typing

from uuid import UUID
from types import TracebackType

from .dag import CyclicException
from .netprotocol import (
    AddressType,
    Command,
    close_stream_writer,
    encode_str,
    read_int,
    read_str,
    uint64_to_bytes,
)
from .graphserver import GraphServer, GraphService
from .pubclient import Publisher
from .subclient import Subscriber

logger = logging.getLogger("ezmsg")


class GraphContext:
    """
    GraphContext maintains a list of created publishers, subscribers, and connections in the graph.

    The GraphContext provides a managed environment for creating and tracking publishers,
    subscribers, and graph connections. When the context is no longer needed, it can
    revert changes in the graph which disconnects publishers and removes modifications
    that this context made.

    It also maintains a context manager that ensures the GraphServer is running.

    :param graph_service: Optional graph service instance to use
    :type graph_service: GraphService | None
    :param auto_start: Whether to auto-start a GraphServer if connection fails.
        If None, defaults to auto-start only when graph_address is not provided
        and no environment override is set.
    :type auto_start: bool | None

    .. note::
    The GraphContext is typically managed automatically by the ezmsg runtime
    and doesn't need to be instantiated directly by user code.
    """

    _clients: set[Publisher | Subscriber]
    _edges: set[tuple[str, str]]

    _graph_address: AddressType | None
    _graph_server: GraphServer | None
    _session_id: UUID | None
    _session_reader: asyncio.StreamReader | None
    _session_writer: asyncio.StreamWriter | None
    _session_lock: asyncio.Lock

    def __init__(
        self,
        graph_address: AddressType | None = None,
        auto_start: bool | None = None,
    ) -> None:
        self._clients = set()
        self._edges = set()
        self._graph_address = graph_address
        self._graph_server = None
        self._auto_start = auto_start
        self._session_id = None
        self._session_reader = None
        self._session_writer = None
        self._session_lock = asyncio.Lock()

    @property
    def graph_address(self) -> AddressType | None:
        if self._graph_server is not None:
            return self._graph_server.address
        else:
            return self._graph_address

    async def publisher(self, topic: str, **kwargs) -> Publisher:
        """
        Create a publisher for the specified topic.

        :param topic: The topic name to publish to
        :type topic: str
        :param kwargs: Additional keyword arguments for publisher configuration
        :return: A Publisher instance for the topic
        :rtype: Publisher
        """
        pub = await Publisher.create(topic, self.graph_address, **kwargs)

        self._clients.add(pub)
        return pub

    async def subscriber(self, topic: str, **kwargs) -> Subscriber:
        """
        Create a subscriber for the specified topic.

        :param topic: The topic name to subscribe to
        :type topic: str
        :param kwargs: Additional keyword arguments for subscriber configuration
        :return: A Subscriber instance for the topic
        :rtype: Subscriber
        """
        sub = await Subscriber.create(topic, self.graph_address, **kwargs)

        self._clients.add(sub)
        return sub

    async def connect(self, from_topic: str, to_topic: str) -> None:
        """
        Connect two topics in the message graph.

        :param from_topic: The source topic name
        :type from_topic: str
        :param to_topic: The destination topic name
        :type to_topic: str
        """
        if self._session_writer is not None:
            response = await self._session_command(
                Command.SESSION_CONNECT,
                from_topic,
                to_topic,
            )
            if response == Command.CYCLIC.value:
                raise CyclicException
            if response != Command.COMPLETE.value:
                raise RuntimeError("Unexpected response to session connect")
        else:
            await GraphService(self.graph_address).connect(from_topic, to_topic)
        self._edges.add((from_topic, to_topic))

    async def disconnect(self, from_topic: str, to_topic: str) -> None:
        """
        Disconnect two topics in the message graph.

        :param from_topic: The source topic name
        :type from_topic: str
        :param to_topic: The destination topic name
        :type to_topic: str
        """
        if self._session_writer is not None:
            response = await self._session_command(
                Command.SESSION_DISCONNECT,
                from_topic,
                to_topic,
            )
            if response != Command.COMPLETE.value:
                raise RuntimeError("Unexpected response to session disconnect")
        else:
            await GraphService(self.graph_address).disconnect(from_topic, to_topic)
        self._edges.discard((from_topic, to_topic))

    async def sync(self, timeout: float | None = None) -> None:
        """
        Synchronize with the graph server.

        :param timeout: Optional timeout for the sync operation
        :type timeout: float | None
        """
        await GraphService(self.graph_address).sync(timeout)

    async def pause(self) -> None:
        """
        Pause message processing in the graph.
        """
        await GraphService(self.graph_address).pause()

    async def resume(self) -> None:
        """
        Resume message processing in the graph.
        """
        await GraphService(self.graph_address).resume()

    async def _ensure_servers(self) -> None:
        self._graph_server = await GraphService(self.graph_address).ensure(
            auto_start=self._auto_start
        )

    async def _open_session(self) -> None:
        if self._session_writer is not None:
            return

        reader, writer = await GraphService(self.graph_address).open_connection()
        writer.write(Command.SESSION.value)
        await writer.drain()

        session_id = UUID(await read_str(reader))
        response = await reader.read(1)
        if response != Command.COMPLETE.value:
            await close_stream_writer(writer)
            raise RuntimeError("Failed to create GraphContext session")

        self._session_id = session_id
        self._session_reader = reader
        self._session_writer = writer

    async def _close_session(self) -> None:
        writer = self._session_writer
        if writer is None:
            return

        try:
            await self._session_command(Command.SESSION_CLEAR)
        except (
            ConnectionRefusedError,
            ConnectionResetError,
            BrokenPipeError,
            asyncio.IncompleteReadError,
        ):
            pass

        await close_stream_writer(writer)
        self._session_id = None
        self._session_reader = None
        self._session_writer = None
        self._edges.clear()

    async def _session_command(
        self,
        command: Command,
        *args: str,
        payload: bytes | None = None,
        expect_snapshot: bool = False,
    ) -> typing.Any:
        reader = self._session_reader
        writer = self._session_writer
        if reader is None or writer is None:
            raise RuntimeError("GraphContext session is not active")

        async with self._session_lock:
            writer.write(command.value)
            for arg in args:
                writer.write(encode_str(arg))
            if payload is not None:
                writer.write(uint64_to_bytes(len(payload)))
                writer.write(payload)
            await writer.drain()

            if expect_snapshot:
                num_bytes = await read_int(reader)
                snapshot_bytes = await reader.readexactly(num_bytes)
                response = await reader.read(1)
                if response != Command.COMPLETE.value:
                    raise RuntimeError("Unexpected response to session snapshot")
                return pickle.loads(snapshot_bytes)

            return await reader.read(1)

    async def register_metadata(self, metadata: dict[str, typing.Any]) -> None:
        if self._session_writer is None:
            logger.warning("No active GraphContext session; metadata registration skipped")
            return

        response = await self._session_command(
            Command.SESSION_REGISTER, payload=pickle.dumps(metadata)
        )
        if response != Command.COMPLETE.value:
            raise RuntimeError("Unexpected response to session metadata registration")

    async def snapshot(self) -> dict[str, typing.Any]:
        if self._session_writer is None:
            dag = await GraphService(self.graph_address).dag()
            return {
                "graph": {node: sorted(conns) for node, conns in dag.graph.items()},
                "edge_owners": [],
                "sessions": {},
            }

        snapshot = await self._session_command(
            Command.SESSION_SNAPSHOT, expect_snapshot=True
        )
        if not isinstance(snapshot, dict):
            raise RuntimeError("Session snapshot payload was not a dictionary")
        return snapshot

    async def _shutdown_servers(self) -> None:
        if self._graph_server is not None:
            self._graph_server.stop()
        self._graph_server = None

    async def __aenter__(self) -> "GraphContext":
        await self._ensure_servers()
        await self._open_session()
        return self

    async def __aexit__(
        self,
        exc_t: type[Exception] | None,
        exc_v: typing.Any | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        await self.revert()
        await self._close_session()
        await self._shutdown_servers()
        return False

    async def revert(self) -> None:
        """
        Revert all changes made by this context.

        This method closes all publishers and subscribers created by this
        context and removes all edges that were added to the graph. It is
        automatically called when exiting the context manager.
        """
        for client in self._clients:
            client.close()

        wait = [c.wait_closed() for c in self._clients]
        for future in asyncio.as_completed(wait):
            await future

        self._clients.clear()

        if self._session_writer is not None:
            try:
                response = await self._session_command(Command.SESSION_CLEAR)
                if response != Command.COMPLETE.value:
                    logger.warning("GraphServer returned unexpected response to SESSION_CLEAR")
            except (
                ConnectionRefusedError,
                BrokenPipeError,
                ConnectionResetError,
                asyncio.IncompleteReadError,
            ) as e:
                logger.warning(f"Could not clear GraphContext session state: {e}")
        else:
            for edge in self._edges:
                try:
                    await GraphService(self.graph_address).disconnect(*edge)
                except (
                    ConnectionRefusedError,
                    BrokenPipeError,
                    ConnectionResetError,
                ) as e:
                    logger.warning(f"Could not remove edge {edge} from GraphServer: {e}")

        self._edges.clear()
