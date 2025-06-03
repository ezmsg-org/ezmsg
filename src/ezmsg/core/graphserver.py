import asyncio
import logging
import pickle
from contextlib import suppress
from typing import Dict, List, Optional, Tuple
from uuid import UUID, getnode, uuid1

from . import __version__
from .dag import DAG, CyclicException
from .graph_util import get_compactified_graph, graph_string, prune_graph_connections
from .netprotocol import (
    Address,
    Command,
    ClientInfo,
    SubscriberInfo,
    PublisherInfo,
    AddressType,
    close_stream_writer,
    encode_str,
    read_int,
    read_str,
    uint64_to_bytes,
    GRAPHSERVER_ADDR_ENV,
    GRAPHSERVER_PORT_DEFAULT,
)
from .server import ServiceManager, ThreadedAsyncServer


logger = logging.getLogger("ezmsg")


class GraphServer(ThreadedAsyncServer):
    """
    Pub-Sub Directed Acyclic Graph
    Running as a process: start() and stop()
    Running as a thread: start_server(), stop_server(), join_server()
    """

    graph: DAG
    clients: Dict[UUID, ClientInfo]
    _client_tasks: Dict[UUID, "asyncio.Task[None]"]
    _command_lock: asyncio.Lock

    def __init__(self) -> None:
        super().__init__()
        self.graph = DAG()
        self.clients = dict()
        self._client_tasks = dict()

    async def setup(self) -> None:
        self._command_lock = asyncio.Lock()

    async def shutdown(self) -> None:
        for task in self._client_tasks.values():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self._client_tasks.clear()

    async def api(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            node = await read_int(reader)
            writer.write(encode_str(__version__))
            await writer.drain()

            req = await reader.read(1)

            # Empty bytes object means EOF; Client disconnected
            # This happens frequently when future clients are just pinging
            # GraphServer to check if server is up
            if not req:
                await close_stream_writer(writer)
                return

            if req == Command.SHUTDOWN.value:
                self._shutdown.set()
                await close_stream_writer(writer)
                return

            # We only want to handle one command at a time
            async with self._command_lock:
                if req in [Command.SUBSCRIBE.value, Command.PUBLISH.value]:
                    id = uuid1(node=node)
                    writer.write(encode_str(str(id)))

                    pid = await read_int(reader)
                    topic = await read_str(reader)

                    if req == Command.SUBSCRIBE.value:
                        info = SubscriberInfo(id, writer, pid, topic)
                        self.clients[id] = info
                        self._client_tasks[id] = asyncio.create_task(
                            self._handle_client(id, reader, writer)
                        )
                        iface = writer.transport.get_extra_info("sockname")[0]
                        await self._notify_subscriber(info, iface)

                    elif req == Command.PUBLISH.value:
                        address = await Address.from_stream(reader)
                        info = PublisherInfo(id, writer, pid, topic, address)
                        self.clients[id] = info
                        self._client_tasks[id] = asyncio.create_task(
                            self._handle_client(id, reader, writer)
                        )
                        iface = writer.transport.get_extra_info("peername")[0]
                        for sub in self._downstream_subs(info.topic):
                            await self._notify_subscriber(sub, iface)

                    writer.write(Command.COMPLETE.value)
                    await writer.drain()
                    return

                elif req in [Command.CONNECT.value, Command.DISCONNECT.value]:
                    from_topic = await read_str(reader)
                    to_topic = await read_str(reader)

                    cmd = self.graph.add_edge
                    if req == Command.DISCONNECT.value:
                        cmd = self.graph.remove_edge

                    try:
                        cmd(from_topic, to_topic)
                        for sub in self._downstream_subs(to_topic):
                            await self._notify_subscriber(sub)
                        writer.write(Command.COMPLETE.value)
                    except CyclicException:
                        writer.write(Command.CYCLIC.value)

                    await writer.drain()

                    if req == Command.DISCONNECT.value:
                        await close_stream_writer(writer)

                elif req == Command.SYNC.value:
                    for pub in self._publishers():
                        try:
                            async with pub.sync_writer() as pub_writer:
                                pub_writer.write(Command.SYNC.value)
                        except (ConnectionResetError, BrokenPipeError):
                            continue

                    writer.write(Command.COMPLETE.value)
                    await writer.drain()

                elif req == Command.PAUSE.value:
                    for pub in self._publishers():
                        pub.writer.write(Command.PAUSE.value)

                elif req == Command.RESUME.value:
                    for pub in self._publishers():
                        pub.writer.write(Command.RESUME.value)

                elif req == Command.DAG.value:
                    writer.write(Command.DAG.value)
                    dag_bytes = pickle.dumps(self.graph)
                    writer.write(uint64_to_bytes(len(dag_bytes)) + dag_bytes)
                    writer.write(Command.COMPLETE.value)

                else:
                    logger.warn(f"GraphServer received unknown command {req}")

        except (ConnectionResetError, BrokenPipeError):
            logger.debug("GraphServer connection fail mid-command")

        await close_stream_writer(writer)

    async def _handle_client(
        self, id: UUID, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        logger.debug(f"Graph Server: Client connected: {id}")

        try:
            while True:
                req = await reader.read(1)

                if not req:
                    break

                if req == Command.COMPLETE.value:
                    self.clients[id].set_sync()

        except (ConnectionResetError, BrokenPipeError) as e:
            logger.debug(f"Client {id} disconnected from GraphServer: {e}")

        finally:
            self.clients[id].set_sync()
            del self.clients[id]
            await close_stream_writer(writer)

    async def _notify_subscriber(
        self, sub: SubscriberInfo, iface: Optional[str] = None
    ) -> None:
        try:
            notification = []
            for pub in self._upstream_pubs(sub.topic):
                address = pub.address
                if address is not None:
                    if iface is not None:
                        notification.append(f"{str(pub.id)}@{iface}:{address.port}")
                    else:
                        notification.append(
                            f"{str(pub.id)}@{address.host}:{address.port}"
                        )

            async with sub.sync_writer() as writer:
                notify_str = ",".join(notification)
                writer.write(Command.UPDATE.value)
                writer.write(encode_str(notify_str))

        except (ConnectionResetError, BrokenPipeError) as e:
            logger.debug(f"Failed to update Subscriber {sub.id}: {e}")

    def _publishers(self) -> List[PublisherInfo]:
        return [
            info for info in self.clients.values() if isinstance(info, PublisherInfo)
        ]

    def _subscribers(self) -> List[SubscriberInfo]:
        return [
            info for info in self.clients.values() if isinstance(info, SubscriberInfo)
        ]

    def _upstream_pubs(self, topic: str) -> List[PublisherInfo]:
        """Given a topic, return a set of all publisher IDs upstream of that topic"""
        upstream_topics = self.graph.upstream(topic)
        return [pub for pub in self._publishers() if pub.topic in upstream_topics]

    def _downstream_subs(self, topic: str) -> List[SubscriberInfo]:
        """Given a topic, return a set of all subscriber IDs upstream of that topic"""
        downstream_topics = self.graph.downstream(topic)
        return [sub for sub in self._subscribers() if sub.topic in downstream_topics]


class GraphService(ServiceManager[GraphServer]):
    ADDR_ENV = GRAPHSERVER_ADDR_ENV
    PORT_DEFAULT = GRAPHSERVER_PORT_DEFAULT

    def __init__(self, address: Optional[AddressType] = None) -> None:
        super().__init__(GraphServer, address)

    async def open_connection(
        self,
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await super().open_connection()
        writer.write(uint64_to_bytes(getnode()))
        await writer.drain()
        server_version = await read_str(reader)
        if server_version != __version__:
            logger.warning(
                f"Version Mismatch: GraphServer@{server_version} != Client@{__version__}"
            )
        return reader, writer

    async def connect(self, from_topic: str, to_topic: str) -> None:
        reader, writer = await self.open_connection()
        writer.write(Command.CONNECT.value)
        writer.write(encode_str(from_topic))
        writer.write(encode_str(to_topic))
        await writer.drain()
        response = await reader.read(1)
        if response == Command.CYCLIC.value:
            raise CyclicException
        await close_stream_writer(writer)

    async def disconnect(self, from_topic: str, to_topic: str) -> None:
        reader, writer = await self.open_connection()
        writer.write(Command.DISCONNECT.value)
        writer.write(encode_str(from_topic))
        writer.write(encode_str(to_topic))
        await writer.drain()
        await reader.read(1)  # Complete
        await close_stream_writer(writer)

    async def sync(self, timeout: Optional[float] = None) -> None:
        reader, writer = await self.open_connection()
        writer.write(Command.SYNC.value)
        await writer.drain()
        await asyncio.wait_for(reader.read(1), timeout=timeout)  # Complete
        await close_stream_writer(writer)

    async def pause(self) -> None:
        reader, writer = await self.open_connection()
        writer.write(Command.PAUSE.value)
        await close_stream_writer(writer)

    async def resume(self) -> None:
        reader, writer = await self.open_connection()
        writer.write(Command.RESUME.value)
        await close_stream_writer(writer)

    async def shutdown(self) -> None:
        reader, writer = await self.open_connection()
        writer.write(Command.SHUTDOWN.value)
        await writer.drain()
        await close_stream_writer(writer)

    async def dag(self, timeout: Optional[float] = None) -> DAG:
        reader, writer = await self.open_connection()
        writer.write(Command.DAG.value)
        await writer.drain()
        await asyncio.sleep(1.0)
        await reader.readexactly(1)
        dag_num_bytes = await read_int(reader)
        dag_bytes = await reader.readexactly(dag_num_bytes)
        dag: DAG = pickle.loads(dag_bytes)

        await asyncio.wait_for(reader.read(1), timeout=timeout)  # Complete
        await close_stream_writer(writer)
        return dag

    async def get_formatted_graph(
        self,
        fmt: str,
        direction: str = "LR",
        compact_level: Optional[int] = None,
    ) -> str:
        if fmt not in ["mermaid", "graphviz"]:
            raise ValueError(
                f"Invalid format '{fmt}'. Options are 'mermaid' or 'graphviz'"
            )
        try:
            dag: DAG = await self.dag()
        except (ConnectionRefusedError, ConnectionResetError) as e:
            err_msg = f"GraphServer not running at address '{self.address}', or host is refusing connections"
            logger.error(err_msg)
            raise type(e)(err_msg) from e
        graph_connections = dag.graph.copy()
        if graph_connections is None or not graph_connections:
            return ""

        if compact_level:
            graph_connections, pruned_topics = prune_graph_connections(
                graph_connections
            )
            if pruned_topics is not None and len(pruned_topics) > 0:
                logger.info(f"Pruned the following proxy topics: {pruned_topics}.")
            graph_connections = get_compactified_graph(
                graph_connections,
                compact_level,
                "/",
            )

        formatted_graph = graph_string(graph_connections, fmt=fmt, direction=direction)

        return formatted_graph
