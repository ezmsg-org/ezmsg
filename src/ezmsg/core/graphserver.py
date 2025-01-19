import asyncio
from collections import defaultdict
import logging
import pickle
from textwrap import indent
import typing
from uuid import uuid4

from contextlib import suppress

from uuid import UUID, uuid1, getnode

from . import __version__
from .server import ThreadedAsyncServer, ServiceManager
from .dag import DAG, CyclicException
from .netprotocol import (
    Address,
    close_stream_writer,
    encode_str,
    read_int,
    read_str,
    uint64_to_bytes,
    Command,
    ClientInfo,
    SubscriberInfo,
    PublisherInfo,
    GRAPHSERVER_ADDR_ENV,
    GRAPHSERVER_PORT_DEFAULT,
    AddressType,
)

logger = logging.getLogger("ezmsg")

IND = "  "


class GraphServer(ThreadedAsyncServer):
    """
    Pub-Sub Directed Acyclic Graph
    Running as a process: start() and stop()
    Running as a thread: start_server(), stop_server(), join_server()
    """

    graph: DAG
    clients: typing.Dict[UUID, ClientInfo]
    _client_tasks: typing.Dict[UUID, "asyncio.Task[None]"]
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
        self, sub: SubscriberInfo, iface: typing.Optional[str] = None
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

    def _publishers(self) -> typing.List[PublisherInfo]:
        return [
            info for info in self.clients.values() if isinstance(info, PublisherInfo)
        ]

    def _subscribers(self) -> typing.List[SubscriberInfo]:
        return [
            info for info in self.clients.values() if isinstance(info, SubscriberInfo)
        ]

    def _upstream_pubs(self, topic: str) -> typing.List[PublisherInfo]:
        """Given a topic, return a set of all publisher IDs upstream of that topic"""
        upstream_topics = self.graph.upstream(topic)
        return [pub for pub in self._publishers() if pub.topic in upstream_topics]

    def _downstream_subs(self, topic: str) -> typing.List[SubscriberInfo]:
        """Given a topic, return a set of all subscriber IDs upstream of that topic"""
        downstream_topics = self.graph.downstream(topic)
        return [sub for sub in self._subscribers() if sub.topic in downstream_topics]


class GraphService(ServiceManager[GraphServer]):
    ADDR_ENV = GRAPHSERVER_ADDR_ENV
    PORT_DEFAULT = GRAPHSERVER_PORT_DEFAULT

    def __init__(self, address: typing.Optional[AddressType] = None) -> None:
        super().__init__(GraphServer, address)

    async def open_connection(
        self,
    ) -> typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
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

    async def sync(self, timeout: typing.Optional[float] = None) -> None:
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

    async def dag(self, timeout: typing.Optional[float] = None) -> DAG:
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

    async def get_pruned_graph(self) -> typing.Optional[defaultdict[str, set[str]]]:
        try:
            dag: DAG = await self.dag()
        except (ConnectionRefusedError, ConnectionResetError):
            logger.info(
                f"GraphServer not running @{self.address}, or host is refusing connections"
            )
            return None

        graph_connections = dag.graph.copy()
        # Let's eliminate proxy topics, i.e. connections with inputs and outputs.
        source_nodes = []
        for node, conns in graph_connections.items():
            if len(conns) > 0:
                source_nodes += [node]
        proxy_topics = []
        for conns in graph_connections.values():
            for conn in conns:
                if conn in source_nodes and conn not in proxy_topics:
                    proxy_topics += [conn]
        # Replace Proxy Topics with actual source and downstream
        for proxy_topic in proxy_topics:
            downstreams = graph_connections.pop(proxy_topic)
            logger.info(f"{proxy_topic} downstream connections: {downstreams}")
            for node, conns in graph_connections.items():
                for conn in conns:
                    if conn == proxy_topic:
                        new_conns = conns.copy()
                        new_conns.remove(proxy_topic)
                        new_conns.union(downstreams)
                        logger.info(
                            f"Updating connections for {node} from {conns} to {new_conns}"
                        )
                        graph_connections[node] = new_conns

        return dag.graph.copy()

    async def get_formatted_graph(self, fmt: str, direction: str = "LR") -> str:
        if fmt not in ["mermaid", "graphviz"]:
            raise ValueError(
                f"Invalid format '{fmt}'. Options are 'mermaid' or 'graphviz'"
            )
        graph_connections = await self.get_pruned_graph()

        if graph_connections is None or not graph_connections:
            return ""

        # Let's come up with UUID node names
        nodes = set(graph_connections.keys())
        if fmt == "mermaid":
            node_map = {name: f"{str(uuid4())}" for name in nodes}
        else:
            node_map = {name: f'"{str(uuid4())}"' for name in nodes}  # graphviz

        # Construct the graph
        def tree():
            return defaultdict(tree)

        graph: defaultdict = tree()

        connections = ""
        for node, conns in graph_connections.items():
            subgraph = graph
            path = node.split("/")
            route = path[:-1]
            stream = path[-1]
            for seg in route:
                subgraph = subgraph[seg]
            subgraph[stream] = node

            for sub in conns:
                if fmt == "mermaid":
                    connections += f"{node_map[node]} --> {node_map[sub]}" + "\n"
                else:
                    connections += f"{node_map[node]} -> {node_map[sub]};" + "\n"

        if fmt == "graphviz":
            header = [
                "digraph EZ {",
                indent(f'rankdir="{direction}"', IND),
            ]
            footer = ["}"]
            def per_leaf(g, leaf):
                out = None
                if isinstance(g[leaf], defaultdict):
                    out = [
                        f"subgraph {leaf.lower()} {{",
                        indent("cluster = true;", IND),
                        indent(f'label = "{leaf}";', IND),
                        f"{recurse_graph(g[leaf])}}};",
                    ]
                elif isinstance(g[leaf], str):
                    out = [f"{node_map[g[leaf]]} [label={leaf}];"]
                return out

        else:  # fmt == mermaid
            header = [f"flowchart {direction}"]
            footer = []
            def per_leaf(g, leaf):
                out = None
                if isinstance(g[leaf], defaultdict):
                    out = [
                        f"subgraph {leaf.lower()} [{leaf}]",
                        # "direction LR",
                        f"{recurse_graph(g[leaf])}",
                        "end",
                    ]
                elif isinstance(g[leaf], str):
                    out = [f"{node_map[g[leaf]]}[{leaf}]"]
                return out

        def recurse_graph(g: defaultdict):
            out = ""
            for leaf in g:
                leaf_list = per_leaf(g, leaf)
                if leaf_list is not None:
                    leaf_list += [""]  # Append a newline
                    out += indent("\n".join(leaf_list), IND)
            return out[:-1]

        graph_out = "\n".join(header + [recurse_graph(graph), indent(connections, IND)] + footer)

        return graph_out
