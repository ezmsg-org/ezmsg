import asyncio
import logging
import pickle
import os
import socket
import threading
from contextlib import suppress
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
    create_socket,
    SERVER_PORT_START_ENV,
    SERVER_PORT_START_DEFAULT,
    DEFAULT_HOST,
)
from .shm import SHMContext, SHMInfo

logger = logging.getLogger("ezmsg")


class GraphServer(threading.Thread):
    """
    Pub-sub directed acyclic graph (DAG) server.

    The GraphServer manages the message routing graph for ezmsg applications,
    handling publisher-subscriber relationships and maintaining the DAG structure.

    Can be run either as a process (``start()`` and ``stop()``) or as a thread
    (``start_server()``, ``stop_server()``, ``join_server()``).

    .. note::
       The GraphServer is typically managed automatically by the ezmsg runtime
       and doesn't need to be instantiated directly by user code.
    """

    _server_up: threading.Event
    _shutdown: threading.Event

    _sock: socket.socket
    _loop: asyncio.AbstractEventLoop

    graph: DAG
    clients: dict[UUID, ClientInfo]
    shms: dict[str, SHMInfo]

    _client_tasks: dict[UUID, "asyncio.Task[None]"]
    _command_lock: asyncio.Lock

    def __init__(self, **kwargs) -> None:
        super().__init__(
            **{**kwargs, **dict(daemon=True, name=kwargs.get("name", "GraphServer"))}
        )
        # threading events for lifecycle
        self._server_up = threading.Event()
        self._shutdown = threading.Event()

        # graph/server data
        self.graph = DAG()
        self.clients = {}
        self._client_tasks = {}
        self.shms = {}

    @property
    def address(self) -> Address:
        return Address(*self._sock.getsockname())

    def start(self, address: AddressType | None = None) -> None:  # type: ignore[override]
        if address is not None:
            self._sock = create_socket(*address)
        else:
            start_port = int(
                os.environ.get(SERVER_PORT_START_ENV, SERVER_PORT_START_DEFAULT)
            )
            self._sock = create_socket(start_port=start_port)

        self._loop = asyncio.new_event_loop()
        super().start()
        self._server_up.wait()

    def stop(self) -> None:
        self._shutdown.set()
        self.join()

    def run(self) -> None:
        try:
            asyncio.set_event_loop(self._loop)
            with suppress(asyncio.CancelledError):
                self._loop.run_until_complete(self._amain())
        finally:
            self._loop.stop()
            self._loop.close()

    async def _setup(self) -> None:
        self._command_lock = asyncio.Lock()

    async def _shutdown_async(self) -> None:
        # Cancel client handler tasks and wait for them to end.
        for task in list(self._client_tasks.values()):
            task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*self._client_tasks.values(), return_exceptions=True)
        self._client_tasks.clear()

        # Cancel SHM leases
        for info in self.shms.values():
            for lease_task in list(info.leases):
                lease_task.cancel()
                with suppress(asyncio.CancelledError):
                    await lease_task
            info.leases.clear()

    async def _amain(self) -> None:
        """
        Start the asyncio server and serve forever until shutdown is requested.
        """
        await self._setup()

        server = await asyncio.start_server(self.api, sock=self._sock)

        async def monitor_shutdown() -> None:
            # Thread event -> wake in event loop
            await self._loop.run_in_executor(None, self._shutdown.wait)
            server.close()
            await self._shutdown_async()
            await server.wait_closed()

        monitor_task = self._loop.create_task(monitor_shutdown())
        self._server_up.set()

        try:
            await server.serve_forever()
        finally:
            self._shutdown.set()
            await monitor_task

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
                shm_info: SHMInfo | None = None

                if req == Command.SHM_CREATE.value:
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

                # NOTE: SHMContexts are like GraphClients in that their
                # lifetime is bound to the lifetime of this connection
                # but rather than the early return that we have with
                # GraphClients, we just await the lease task.
                # This may be a more readable pattern?
                # NOTE: With the current shutdown pattern, we cancel
                # the lease task then await it.  When we cancel the lease
                # task this resolves then it gets awaited in shutdown.
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
                        # Advertise an address the channel should be able to resolve
                        port = pub_info.address.port
                        iface = pub_info.writer.transport.get_extra_info("peername")[0]
                        pub_addr = Address(iface, port)
                    else:
                        logger.warning(f"Connecting channel requested {type(pub_info)}")

                except KeyError:
                    logger.warning(
                        f"Connecting channel requested non-existent publisher {pub_id=}"
                    )

                if pub_addr is None:
                    # FIXME: Channel should not be created; but it feels like we should
                    # have a better communication protocol to tell the channel what the
                    # error was and deliver a better experience from the client side.
                    # for now, drop connection
                    await close_stream_writer(writer)
                    return

                writer.write(Command.COMPLETE.value)
                writer.write(encode_str(str(channel_id)))
                pub_addr.to_stream(writer)

                info = ChannelInfo(channel_id, writer, pub_id)
                self.clients[channel_id] = info
                self._client_tasks[channel_id] = asyncio.create_task(
                    self._handle_client(channel_id, reader, writer)
                )

                # NOTE: Created a client, must return early
                # to avoid closing writer
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

                        writer.write(encode_str(str(client_id)))

                        if req == Command.SUBSCRIBE.value:
                            info = SubscriberInfo(client_id, writer, topic)
                            self.clients[client_id] = info
                            self._client_tasks[client_id] = asyncio.create_task(
                                self._handle_client(client_id, reader, writer)
                            )

                            await self._notify_subscriber(info)

                        elif req == Command.PUBLISH.value:
                            address = await Address.from_stream(reader)
                            info = PublisherInfo(client_id, writer, topic, address)
                            self.clients[client_id] = info
                            self._client_tasks[client_id] = asyncio.create_task(
                                self._handle_client(client_id, reader, writer)
                            )

                            for sub in self._downstream_subs(info.topic):
                                await self._notify_subscriber(sub)

                        writer.write(Command.COMPLETE.value)

                        # NOTE: Created a client, must return early
                        # to avoid closing writer
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

        # NOTE: This prevents code repetition for many graph server commands, but
        # when we create GraphClients, their lifecycle is bound to the lifecycle of
        # this connection.  We do NOT want to close the stream writer if we have
        # created a GraphClient, which requires an early return.  Perhaps a different
        # communication protocol could resolve this
        await close_stream_writer(writer)

    async def _handle_client(
        self,
        client_id: UUID,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
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
            # Ensure any waiter on this client unblocks
            # with suppress(Exception):
            self.clients[client_id].set_sync()
            self.clients.pop(client_id, None)
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

    def _publishers(self) -> list[PublisherInfo]:
        return [
            info for info in self.clients.values() if isinstance(info, PublisherInfo)
        ]

    def _subscribers(self) -> list[SubscriberInfo]:
        return [
            info for info in self.clients.values() if isinstance(info, SubscriberInfo)
        ]

    def _channels(self) -> list[ChannelInfo]:
        return [info for info in self.clients.values() if isinstance(info, ChannelInfo)]

    def _upstream_pubs(self, topic: str) -> list[PublisherInfo]:
        """Given a topic, return a set of all publisher IDs upstream of that topic"""
        upstream_topics = self.graph.upstream(topic)
        return [pub for pub in self._publishers() if pub.topic in upstream_topics]

    def _downstream_subs(self, topic: str) -> list[SubscriberInfo]:
        """Given a topic, return a set of all subscriber IDs upstream of that topic"""
        downstream_topics = self.graph.downstream(topic)
        return [sub for sub in self._subscribers() if sub.topic in downstream_topics]


class GraphService:
    ADDR_ENV = GRAPHSERVER_ADDR_ENV
    PORT_DEFAULT = GRAPHSERVER_PORT_DEFAULT

    _address: Address | None

    def __init__(self, address: AddressType | None = None) -> None:
        self._address = Address(*address) if address is not None else None

    @classmethod
    def default_address(cls) -> Address:
        address_str = os.environ.get(cls.ADDR_ENV, f"{DEFAULT_HOST}:{cls.PORT_DEFAULT}")
        return Address.from_string(address_str)

    @property
    def address(self) -> Address:
        return self._address if self._address is not None else self.default_address()

    def create_server(self) -> GraphServer:
        server = GraphServer(name="GraphServer")
        server.start(self._address)
        self._address = server.address
        return server

    async def ensure(self) -> GraphServer | None:
        """
        Try connecting to an existing server. If none is listening and no explicit
        address/environment is set, start one and return it. If an existing one is
        found, return None.
        """
        server = None
        ensure_server = False
        if self._address is None:
            # Only auto-start if env var not forcing a location
            ensure_server = self.ADDR_ENV not in os.environ

        try:
            reader, writer = await self.open_connection()
            await close_stream_writer(writer)
        except OSError as ref_e:
            if not ensure_server:
                raise ref_e
            server = self.create_server()

        return server

    async def open_connection(
        self,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection(*(self.address))
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
            await close_stream_writer(writer)
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

    async def sync(self, timeout: float | None = None) -> None:
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

    async def dag(self, timeout: float | None = None) -> DAG:
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
        compact_level: int | None = None,
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
        num_buffers: int,
        buf_size: int = DEFAULT_SHM_SIZE,
    ) -> SHMContext:
        reader, writer = await self.open_connection()
        writer.write(Command.SHM_CREATE.value)
        writer.write(uint64_to_bytes(num_buffers))
        writer.write(uint64_to_bytes(buf_size))
        await writer.drain()

        response = await reader.read(1)
        if response != Command.COMPLETE.value:
            await close_stream_writer(writer)
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
            await close_stream_writer(writer)
            raise ValueError("Invalid SHM Name")

        shm_name = await read_str(reader)
        return SHMContext.attach(shm_name, reader, writer)
