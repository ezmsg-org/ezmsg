import asyncio
import logging
import pickle
from contextlib import suppress
from typing import Dict, List, Optional, Tuple
from uuid import UUID, uuid1

from . import __version__
from .dag import DAG, CyclicException
from .graph_util import get_compactified_graph, graph_string, prune_graph_connections
from .netprotocol import (
    Address,
    Command,
    ClientInfo,
    SubscriberInfo,
    PublisherInfo,
    ChannelInfo,
    AddressType,
    close_stream_writer,
    encode_str,
    read_int,
    read_str,
    uint64_to_bytes,
    GRAPHSERVER_ADDR_ENV,
    GRAPHSERVER_PORT_DEFAULT,
    DEFAULT_SHM_SIZE,
)
from .server import ServiceManager, ThreadedAsyncServer
from .shm import SHMContext, SHMInfo


logger = logging.getLogger("ezmsg")


class GraphServer(ThreadedAsyncServer):
    """
    Pub-Sub Directed Acyclic Graph
    """

    graph: DAG
    clients: Dict[UUID, ClientInfo]
    shms: Dict[str, SHMInfo]

    _client_tasks: Dict[UUID, "asyncio.Task[None]"]
    _command_lock: asyncio.Lock

    def __init__(self) -> None:
        super().__init__()
        self.graph = DAG()
        self.clients = dict()
        self._client_tasks = dict()

        self.shms = dict()


    async def setup(self) -> None:
        self._command_lock = asyncio.Lock()

    async def shutdown(self) -> None:
        for task in self._client_tasks.values():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self._client_tasks.clear()

        for info in self.shms.values():
            for lease_task in list(info.leases):
                lease_task.cancel()
                with suppress(asyncio.CancelledError):
                    await lease_task
            info.leases.clear()

    async def api(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            # Always start communications by telling the client our ezmsg version
            # This helps us get ahead of surprisingly common situations where
            # the graph server and the graph clients are running different versions
            # of ezmsg which could result in borked comms.
            writer.write(encode_str(__version__))
            await writer.drain()

            req = await reader.read(1)
            
            if not req:
                # Empty bytes object means EOF; Client disconnected
                # This happens frequently when future clients are just pinging
                # GraphServer to check if server is up
                await close_stream_writer(writer)
                return

            if req == Command.SHUTDOWN.value:
                self._shutdown.set()
                await close_stream_writer(writer)
                return
            
            elif req in [Command.SHM_CREATE.value, Command.SHM_ATTACH.value]:
                shm_info: Optional[SHMInfo] = None

                if req == Command.SHM_CREATE.value:
                    # TODO: UUID
                    num_buffers = await read_int(reader)
                    buf_size = await read_int(reader)

                    # Create segment 
                    shm_info = SHMInfo.create(num_buffers, buf_size)
                    self.shms[shm_info.shm.name] = shm_info
                    logger.debug(f"created {shm_info.shm.name}")

                elif req == Command.SHM_ATTACH.value:
                    shm_name = await read_str(reader)
                    shm_info = self.shms.get(shm_name, None)

                if shm_info is None:
                    await close_stream_writer(writer)
                    return

                writer.write(Command.COMPLETE.value)
                writer.write(encode_str(shm_info.shm.name))

                with suppress(asyncio.CancelledError):
                    await shm_info.lease(reader, writer)

            elif req == Command.CHANNEL.value:
                channel_id = uuid1()
                pub_id_str = await read_str(reader)
                pub_id = UUID(pub_id_str)

                pub_addr = None
                try:
                    pub_info = self.clients[pub_id]
                    if isinstance(pub_info, PublisherInfo):
                        # assemble an address the channel should be able to resolve
                        port = pub_info.address.port
                        iface = pub_info.writer.transport.get_extra_info("peername")[0]
                        pub_addr = Address(iface, port) 
                    else:
                        logger.warning(f"Connecting channel requested {type(pub_info)}")

                except KeyError:
                    logger.warning(f"Connecting channel requested non-existent publisher {pub_id=}")

                if pub_addr is not None:
                    writer.write(Command.COMPLETE.value)
                    writer.write(encode_str(str(channel_id)))
                    pub_addr.to_stream(writer)

                    info = ChannelInfo(channel_id, writer, pub_id)
                    self.clients[channel_id] = info
                    self._client_tasks[channel_id] = asyncio.create_task(
                        self._handle_client(channel_id, reader, writer)
                    )
                else:
                    # Error, drop connection
                    await close_stream_writer(writer)

                # Created a client; we must return early to avoid closing writer
                return

            else:
                # We only want to handle one command at a time
                async with self._command_lock:
                    if req in [
                        Command.SUBSCRIBE.value, 
                        Command.PUBLISH.value, 
                    ]:
                        client_id = uuid1()
                        topic = await read_str(reader)

                        if req == Command.SUBSCRIBE.value:
                            info = SubscriberInfo(client_id, writer, topic)
                            self.clients[client_id] = info
                            self._client_tasks[client_id] = asyncio.create_task(
                                self._handle_client(client_id, reader, writer)
                            )

                            writer.write(Command.COMPLETE.value)
                            writer.write(encode_str(str(client_id)))

                            await self._notify_subscriber(info)

                        elif req == Command.PUBLISH.value:
                            address = await Address.from_stream(reader)
                            info = PublisherInfo(client_id, writer, topic, address)
                            self.clients[client_id] = info
                            self._client_tasks[client_id] = asyncio.create_task(
                                self._handle_client(client_id, reader, writer)
                            )
                            
                            writer.write(Command.COMPLETE.value)
                            writer.write(encode_str(str(client_id)))
                            
                            for sub in self._downstream_subs(info.topic):
                                await self._notify_subscriber(sub)

                        # Created a client, must return early to avoid closing writer
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
                        logger.warning(f"GraphServer received unknown command {req}")

        except (ConnectionResetError, BrokenPipeError):
            logger.debug("GraphServer connection fail mid-command")

        await close_stream_writer(writer)

    async def _handle_client(
        self, client_id: UUID, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """ 
        The lifecycle of a graph client is tied to the lifecycle of this TCP connection.
        We always attempt to read from the reader, and if the connection is ever closed
        from the other side, reader.read(1) returns empty, we break the loop, and delete
        the client from our list of connected clients.  Because we're always reading 
        from the client within this task, we have to handle all client responses within
        this task.  
        
        NOTE: _notify_subscriber requires a COMPLETE once the subscriber has reconfigured
        its connections.  ClientInfo.sync_writer gives us the mechanism by which to block
        _notify_subscriber until the COMPLETE is received in this context.  
        """
        logger.debug(f"Graph Server: Client connected: {client_id}")

        try:
            while True:
                req = await reader.read(1)

                if not req:
                    break

                if req == Command.COMPLETE.value:
                    self.clients[client_id].set_sync()

        except (ConnectionResetError, BrokenPipeError) as e:
            logger.debug(f"Client {client_id} disconnected from GraphServer: {e}")

        finally:
            self.clients[client_id].set_sync()
            del self.clients[client_id]
            await close_stream_writer(writer)

    async def _notify_subscriber(self, sub: SubscriberInfo) -> None:
        try:
            pub_ids = [str(pub.id) for pub in self._upstream_pubs(sub.topic)]

            # Update requires us to read a 'COMPLETE'
            # This cannot be done from this context
            async with sub.sync_writer() as writer:
                notify_str = ",".join(pub_ids)
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
    
    def _channels(self) -> List[ChannelInfo]:
        return [
            info for info in self.clients.values() if isinstance(info, ChannelInfo)
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
        await reader.read(1)
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

    async def create_shm(
        self,
        # TODO: add UUID parameter 
        num_buffers: int, 
        buf_size: int = DEFAULT_SHM_SIZE
    ) -> SHMContext:
        reader, writer = await self.open_connection()
        writer.write(Command.SHM_CREATE.value)
        # TODO: serialize UUID
        writer.write(uint64_to_bytes(num_buffers))
        writer.write(uint64_to_bytes(buf_size))
        await writer.drain()

        response = await reader.read(1)
        if response != Command.COMPLETE.value:
            raise ValueError("Error creating SHM segment")

        shm_name = await read_str(reader)
        return SHMContext.attach(shm_name, reader, writer)

    async def attach_shm(self, name: str) -> SHMContext:
        reader, writer = await self.open_connection()
        writer.write(Command.SHM_ATTACH.value)
        writer.write(encode_str(name))
        await writer.drain()

        response = await reader.read(1)
        if response != Command.COMPLETE.value:
            raise ValueError("Invalid SHM Name")

        shm_name = await read_str(reader)
        return SHMContext.attach(shm_name, reader, writer)