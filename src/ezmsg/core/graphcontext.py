import asyncio
import logging
import typing
import enum
import pickle

from uuid import UUID
from types import TracebackType
from dataclasses import dataclass
from contextlib import suppress

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
from .graphmeta import GraphMetadata, GraphSnapshot

logger = logging.getLogger("ezmsg")


class _SessionResponseKind(enum.Enum):
    BYTE = enum.auto()
    SNAPSHOT = enum.auto()


@dataclass
class _SessionCommand:
    command: Command
    args: tuple[str, ...]
    payload: bytes | None
    response_kind: _SessionResponseKind
    response_fut: "asyncio.Future[typing.Any]"


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
    _session_task: asyncio.Task[None] | None
    _session_commands: asyncio.Queue[_SessionCommand | None] | None

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
        self._session_task = None
        self._session_commands = None

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
        response = await self._session_command(
            Command.SESSION_CONNECT,
            from_topic,
            to_topic,
            response_kind=_SessionResponseKind.BYTE,
        )
        if response == Command.CYCLIC.value:
            raise CyclicException
        if response != Command.COMPLETE.value:
            raise RuntimeError("Unexpected response to session connect")
        self._edges.add((from_topic, to_topic))

    async def disconnect(self, from_topic: str, to_topic: str) -> None:
        """
        Disconnect two topics in the message graph.

        :param from_topic: The source topic name
        :type from_topic: str
        :param to_topic: The destination topic name
        :type to_topic: str
        """
        response = await self._session_command(
            Command.SESSION_DISCONNECT,
            from_topic,
            to_topic,
            response_kind=_SessionResponseKind.BYTE,
        )
        if response != Command.COMPLETE.value:
            raise RuntimeError("Unexpected response to session disconnect")
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
        self._session_commands = asyncio.Queue()
        self._session_task = asyncio.create_task(
            self._session_io_loop(),
            name=f"graphctx-session-{session_id}",
        )

    def _require_session(self) -> tuple[asyncio.Queue[_SessionCommand | None], asyncio.Task[None]]:
        if self._session_commands is None or self._session_task is None:
            raise RuntimeError(
                "GraphContext session is not active. Use GraphContext as an async context manager."
            )
        return self._session_commands, self._session_task

    async def _session_io_loop(self) -> None:
        reader = self._session_reader
        writer = self._session_writer
        commands = self._session_commands
        if reader is None or writer is None or commands is None:
            return

        try:
            while True:
                cmd = await commands.get()
                if cmd is None:
                    break

                writer.write(cmd.command.value)
                for arg in cmd.args:
                    writer.write(encode_str(arg))
                if cmd.payload is not None:
                    writer.write(uint64_to_bytes(len(cmd.payload)))
                    writer.write(cmd.payload)
                await writer.drain()

                if cmd.response_kind == _SessionResponseKind.BYTE:
                    response = await reader.read(1)

                elif cmd.response_kind == _SessionResponseKind.SNAPSHOT:
                    num_bytes = await read_int(reader)
                    snapshot_bytes = await reader.readexactly(num_bytes)
                    complete = await reader.read(1)
                    if complete != Command.COMPLETE.value:
                        raise RuntimeError("Unexpected response to session snapshot")
                    response = pickle.loads(snapshot_bytes)

                else:
                    raise RuntimeError(f"Unsupported response kind: {cmd.response_kind}")

                if not cmd.response_fut.done():
                    cmd.response_fut.set_result(response)

        except Exception as exc:
            while True:
                try:
                    pending = commands.get_nowait()
                except asyncio.QueueEmpty:
                    break

                if pending is not None and not pending.response_fut.done():
                    pending.response_fut.set_exception(exc)
        finally:
            while True:
                try:
                    pending = commands.get_nowait()
                except asyncio.QueueEmpty:
                    break

                if pending is not None and not pending.response_fut.done():
                    pending.response_fut.set_exception(
                        RuntimeError("GraphContext session closed")
                    )

    async def _close_session(self) -> None:
        commands = self._session_commands
        task = self._session_task
        writer = self._session_writer
        if writer is None:
            return

        if commands is not None:
            await commands.put(None)
        if task is not None:
            with suppress(asyncio.CancelledError):
                await task

        await close_stream_writer(writer)
        self._session_id = None
        self._session_reader = None
        self._session_writer = None
        self._session_task = None
        self._session_commands = None
        self._edges.clear()

    async def _session_command(
        self,
        command: Command,
        *args: str,
        payload: bytes | None = None,
        response_kind: _SessionResponseKind = _SessionResponseKind.BYTE,
    ) -> typing.Any:
        commands, task = self._require_session()
        if task.done():
            raise RuntimeError("GraphContext session task is not running")

        response_fut: asyncio.Future[typing.Any] = asyncio.get_running_loop().create_future()
        await commands.put(
            _SessionCommand(
                command=command,
                args=tuple(args),
                payload=payload,
                response_kind=response_kind,
                response_fut=response_fut,
            )
        )
        return await response_fut

    async def register_metadata(self, metadata: GraphMetadata) -> None:
        payload = pickle.dumps(metadata)
        response = await self._session_command(
            Command.SESSION_REGISTER,
            payload=payload,
            response_kind=_SessionResponseKind.BYTE,
        )
        if response != Command.COMPLETE.value:
            raise RuntimeError("Unexpected response to session metadata registration")

    async def snapshot(self) -> GraphSnapshot:
        snapshot = await self._session_command(
            Command.SESSION_SNAPSHOT,
            response_kind=_SessionResponseKind.SNAPSHOT,
        )
        if not isinstance(snapshot, GraphSnapshot):
            raise RuntimeError("Session snapshot payload was not a GraphSnapshot")
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
                response = await self._session_command(
                    Command.SESSION_CLEAR,
                    response_kind=_SessionResponseKind.BYTE,
                )
                if response != Command.COMPLETE.value:
                    logger.warning(
                        "GraphServer returned unexpected response to SESSION_CLEAR"
                    )
            except (
                ConnectionRefusedError,
                BrokenPipeError,
                ConnectionResetError,
                asyncio.IncompleteReadError,
                RuntimeError,
            ) as e:
                logger.warning(f"Could not clear GraphContext session state: {e}")

        self._edges.clear()
