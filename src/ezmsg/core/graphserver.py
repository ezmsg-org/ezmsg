import asyncio
import logging
import pickle
import os
import socket
import threading
import time
from contextlib import suppress
from uuid import UUID, uuid1


from . import __version__
from .dag import DAG, CyclicException
from .graph_util import get_compactified_graph, graph_string, prune_graph_connections
from .graphmeta import (
    Edge,
    ProcessControlErrorCode,
    GraphMetadata,
    GraphSnapshot,
    ProcessControlRequest,
    ProcessControlResponse,
    ProcessRegistration,
    ProcessOwnershipUpdate,
    ProcessSettingsUpdate,
    SettingsChangedEvent,
    SettingsEventType,
    SettingsSnapshotValue,
    SnapshotProcess,
    SnapshotSession,
)
from .netprotocol import (
    Address,
    Command,
    ClientInfo,
    ProcessInfo,
    SessionInfo,
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
PERSISTENT_EDGE_OWNER = None


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
    _address: Address | None
    _loop: asyncio.AbstractEventLoop

    graph: DAG
    clients: dict[UUID, ClientInfo]
    shms: dict[str, SHMInfo]

    _client_tasks: dict[UUID, "asyncio.Task[None]"]
    _command_lock: asyncio.Lock
    _settings_current: dict[str, SettingsSnapshotValue]
    _settings_source_session: dict[str, UUID | None]
    _settings_events: list[SettingsChangedEvent]
    _settings_event_seq: int
    _settings_owned_by_session: dict[UUID, set[str]]
    _settings_subscribers: dict[UUID, asyncio.Queue[SettingsChangedEvent]]
    _pending_process_requests: dict[
        str, tuple[UUID, "asyncio.Future[ProcessControlResponse]"]
    ]

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
        self._address = None
        self._settings_current = {}
        self._settings_source_session = {}
        self._settings_events = []
        self._settings_event_seq = 0
        self._settings_owned_by_session = {}
        self._settings_subscribers = {}
        self._pending_process_requests = {}

    @property
    def address(self) -> Address:
        assert self._address is not None, "GraphServer not up yet"
        return self._address
        
    def start(self, address: AddressType | None = None) -> None:  # type: ignore[override]
        if address is not None:
            self._sock = create_socket(*address)
        else:
            start_port = int(
                os.environ.get(SERVER_PORT_START_ENV, SERVER_PORT_START_DEFAULT)
            )
            self._sock = create_socket(start_port=start_port)

        # Cache address immediately to avoid touching a possibly-closed socket later.
        self._address = Address(*self._sock.getsockname())

        self._loop = asyncio.new_event_loop()
        super().start()
        self._server_up.wait()
        logger.info(f'Started GraphServer at {self.address}')

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

            elif req == Command.SESSION.value:
                session_id = uuid1()
                self.clients[session_id] = SessionInfo(session_id, writer)
                writer.write(encode_str(str(session_id)))
                writer.write(Command.COMPLETE.value)
                await writer.drain()
                self._client_tasks[session_id] = asyncio.create_task(
                    self._handle_session(session_id, reader, writer)
                )

                # NOTE: Created a session client, must return early
                # to avoid closing writer
                return

            elif req == Command.SESSION_SETTINGS_SUBSCRIBE.value:
                subscriber_id = uuid1()
                after_seq = int(await read_str(reader))
                writer.write(encode_str(str(subscriber_id)))
                writer.write(Command.COMPLETE.value)
                await writer.drain()
                self._client_tasks[subscriber_id] = asyncio.create_task(
                    self._handle_settings_subscriber(
                        subscriber_id, after_seq, reader, writer
                    )
                )

                # NOTE: Created a stream client, must return early
                # to avoid closing writer
                return

            elif req == Command.PROCESS.value:
                process_client_id = uuid1()
                self.clients[process_client_id] = ProcessInfo(process_client_id, writer)
                writer.write(encode_str(str(process_client_id)))
                writer.write(Command.COMPLETE.value)
                await writer.drain()
                self._client_tasks[process_client_id] = asyncio.create_task(
                    self._handle_process(process_client_id, reader, writer)
                )

                # NOTE: Created a process control client, must return early
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
                        topology_changed = False

                        try:
                            if req == Command.CONNECT.value:
                                topology_changed = self._connect_owner(
                                    from_topic, to_topic, PERSISTENT_EDGE_OWNER
                                )
                            else:
                                topology_changed = self._disconnect_owner(
                                    from_topic, to_topic, PERSISTENT_EDGE_OWNER
                                )
                            writer.write(Command.COMPLETE.value)
                        except CyclicException:
                            writer.write(Command.CYCLIC.value)

                        if topology_changed:
                            await self._notify_downstream_for_topic(to_topic)

                        await writer.drain()

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
            self.clients[client_id].set_sync()
            self.clients.pop(client_id, None)
            await close_stream_writer(writer)

    async def _handle_session(
        self,
        session_id: UUID,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        logger.debug(f"Graph Server: Session connected: {session_id}")

        try:
            while True:
                req = await reader.read(1)

                if not req:
                    break

                if req in [
                    Command.SESSION_CONNECT.value,
                    Command.SESSION_DISCONNECT.value,
                ]:
                    response = await self._handle_session_edge_request(
                        session_id, req, reader
                    )
                    writer.write(response)
                    await writer.drain()

                elif req == Command.SESSION_CLEAR.value:
                    response = await self._handle_session_clear_request(session_id)
                    writer.write(response)
                    await writer.drain()

                elif req == Command.SESSION_REGISTER.value:
                    response = await self._handle_session_register_request(
                        session_id, reader
                    )
                    writer.write(response)
                    await writer.drain()

                elif req == Command.SESSION_SNAPSHOT.value:
                    await self._handle_session_snapshot_request(writer)
                    await writer.drain()

                elif req == Command.SESSION_SETTINGS_SNAPSHOT.value:
                    await self._handle_session_settings_snapshot_request(writer)
                    await writer.drain()

                elif req == Command.SESSION_SETTINGS_EVENTS.value:
                    after_seq = int(await read_str(reader))
                    await self._handle_session_settings_events_request(
                        writer, after_seq
                    )
                    await writer.drain()

                elif req == Command.SESSION_PROCESS_REQUEST.value:
                    await self._handle_session_process_request(writer, reader)
                    await writer.drain()

                else:
                    logger.warning(
                        f"Session {session_id} rx unknown command from GraphServer: {req}"
                    )

        except (ConnectionResetError, BrokenPipeError) as e:
            logger.debug(f"Session {session_id} disconnected from GraphServer: {e}")

        finally:
            async with self._command_lock:
                notify_topics = self._drop_session(session_id)

            for topic in notify_topics:
                await self._notify_downstream_for_topic(topic)

            self._client_tasks.pop(session_id, None)
            await close_stream_writer(writer)

    def _process_info(self, process_client_id: UUID) -> ProcessInfo | None:
        info = self.clients.get(process_client_id)
        if isinstance(info, ProcessInfo):
            return info
        return None

    async def _handle_process(
        self,
        process_client_id: UUID,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        logger.debug(f"Graph Server: Process control connected: {process_client_id}")

        try:
            while True:
                req = await reader.read(1)

                if not req:
                    break

                if req == Command.PROCESS_REGISTER.value:
                    response = await self._handle_process_register_request(
                        process_client_id, reader
                    )
                    await self._write_process_response(
                        process_client_id, writer, response
                    )

                elif req == Command.PROCESS_UPDATE_OWNERSHIP.value:
                    response = await self._handle_process_update_ownership_request(
                        process_client_id, reader
                    )
                    await self._write_process_response(
                        process_client_id, writer, response
                    )

                elif req == Command.PROCESS_SETTINGS_UPDATE.value:
                    response = await self._handle_process_settings_update_request(
                        process_client_id, reader
                    )
                    await self._write_process_response(
                        process_client_id, writer, response
                    )

                elif req == Command.PROCESS_ROUTE_RESPONSE.value:
                    await self._handle_process_route_response_request(
                        process_client_id, reader
                    )

                else:
                    logger.warning(
                        f"Process control {process_client_id} rx unknown command: {req}"
                    )

        except (ConnectionResetError, BrokenPipeError) as e:
            logger.debug(
                f"Process control {process_client_id} disconnected from GraphServer: {e}"
            )

        finally:
            process_info = self._process_info(process_client_id)

            async with self._command_lock:
                request_ids = [
                    request_id
                    for request_id, (owner_process_id, _) in self._pending_process_requests.items()
                    if owner_process_id == process_client_id
                ]
                for request_id in request_ids:
                    pending = self._pending_process_requests.pop(request_id, None)
                    if pending is None:
                        continue
                    _, response_fut = pending
                    if not response_fut.done():
                        response_fut.set_result(
                            ProcessControlResponse(
                                request_id=request_id,
                                ok=False,
                                error="Owning process disconnected before response",
                                error_code=ProcessControlErrorCode.PROCESS_DISCONNECTED,
                                process_id=(
                                    process_info.process_id if process_info is not None else None
                                ),
                            )
                        )

            self.clients.pop(process_client_id, None)
            self._client_tasks.pop(process_client_id, None)
            await close_stream_writer(writer)

    async def _write_process_response(
        self,
        process_client_id: UUID,
        fallback_writer: asyncio.StreamWriter,
        response: bytes,
    ) -> None:
        process_info = self._process_info(process_client_id)
        if process_info is None:
            fallback_writer.write(response)
            await fallback_writer.drain()
            return

        async with process_info.write_lock:
            writer = process_info.writer
            writer.write(response)
            await writer.drain()

    def _queue_settings_event(
        self, queue: asyncio.Queue[SettingsChangedEvent], event: SettingsChangedEvent
    ) -> None:
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            # Keep most recent samples under backpressure.
            with suppress(asyncio.QueueEmpty):
                queue.get_nowait()
            with suppress(asyncio.QueueFull):
                queue.put_nowait(event)

    async def _settings_sender(
        self,
        subscriber_id: UUID,
        queue: asyncio.Queue[SettingsChangedEvent],
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while True:
                event = await queue.get()
                payload = pickle.dumps(event)
                writer.write(uint64_to_bytes(len(payload)))
                writer.write(payload)
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Settings subscriber {subscriber_id} disconnected on send")
        except asyncio.CancelledError:
            raise

    async def _handle_settings_subscriber(
        self,
        subscriber_id: UUID,
        after_seq: int,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        queue: asyncio.Queue[SettingsChangedEvent] = asyncio.Queue(maxsize=1024)

        async with self._command_lock:
            self._settings_subscribers[subscriber_id] = queue
            for event in self._settings_events:
                if event.seq > after_seq:
                    self._queue_settings_event(queue, event)

        sender_task = asyncio.create_task(
            self._settings_sender(subscriber_id, queue, writer),
            name=f"settings-sender-{subscriber_id}",
        )

        try:
            while True:
                req = await reader.read(1)
                if not req:
                    break
        except (ConnectionResetError, BrokenPipeError) as e:
            logger.debug(f"Settings subscriber {subscriber_id} disconnected: {e}")
        finally:
            async with self._command_lock:
                self._settings_subscribers.pop(subscriber_id, None)
            self._client_tasks.pop(subscriber_id, None)
            sender_task.cancel()
            with suppress(asyncio.CancelledError):
                await sender_task
            await close_stream_writer(writer)

    async def _handle_process_register_request(
        self, process_client_id: UUID, reader: asyncio.StreamReader
    ) -> bytes:
        num_bytes = await read_int(reader)
        payload = await reader.readexactly(num_bytes)
        registration: ProcessRegistration | None = None
        try:
            payload_obj = pickle.loads(payload)
            if isinstance(payload_obj, ProcessRegistration):
                registration = payload_obj
            else:
                raise RuntimeError(
                    "process registration payload was not ProcessRegistration"
                )
        except Exception as exc:
            logger.warning(
                "Process control %s registration parse failed; ignoring payload: %s",
                process_client_id,
                exc,
            )

        if registration is None:
            return Command.COMPLETE.value

        async with self._command_lock:
            process_info = self._process_info(process_client_id)
            if process_info is None:
                return Command.COMPLETE.value

            process_info.process_id = registration.process_id
            process_info.pid = registration.pid
            process_info.host = registration.host
            process_info.units = set(registration.units)

        return Command.COMPLETE.value

    async def _handle_process_update_ownership_request(
        self, process_client_id: UUID, reader: asyncio.StreamReader
    ) -> bytes:
        num_bytes = await read_int(reader)
        payload = await reader.readexactly(num_bytes)
        update: ProcessOwnershipUpdate | None = None
        try:
            update_obj = pickle.loads(payload)
            if isinstance(update_obj, ProcessOwnershipUpdate):
                update = update_obj
            else:
                raise RuntimeError(
                    "process ownership payload was not ProcessOwnershipUpdate"
                )
        except Exception as exc:
            logger.warning(
                "Process control %s ownership update parse failed; ignoring payload: %s",
                process_client_id,
                exc,
            )

        if update is None:
            return Command.COMPLETE.value

        async with self._command_lock:
            process_info = self._process_info(process_client_id)
            if process_info is None:
                return Command.COMPLETE.value

            if (
                process_info.process_id is not None
                and process_info.process_id != update.process_id
            ):
                logger.warning(
                    "Process control %s process_id mismatch: %s != %s",
                    process_client_id,
                    process_info.process_id,
                    update.process_id,
                )
            elif process_info.process_id is None:
                process_info.process_id = update.process_id

            process_info.units.update(update.added_units)
            process_info.units.difference_update(update.removed_units)

        return Command.COMPLETE.value

    async def _handle_process_settings_update_request(
        self, process_client_id: UUID, reader: asyncio.StreamReader
    ) -> bytes:
        num_bytes = await read_int(reader)
        payload = await reader.readexactly(num_bytes)
        update: ProcessSettingsUpdate | None = None
        try:
            update_obj = pickle.loads(payload)
            if isinstance(update_obj, ProcessSettingsUpdate):
                update = update_obj
            else:
                raise RuntimeError(
                    "process settings payload was not ProcessSettingsUpdate"
                )
        except Exception as exc:
            logger.warning(
                "Process control %s settings update parse failed; ignoring payload: %s",
                process_client_id,
                exc,
            )

        if update is None:
            return Command.COMPLETE.value

        async with self._command_lock:
            process_info = self._process_info(process_client_id)
            if process_info is None:
                return Command.COMPLETE.value

            if process_info.process_id is None:
                process_info.process_id = update.process_id

            self._settings_current[update.component_address] = update.value
            self._settings_source_session[update.component_address] = None
            self._append_settings_event_locked(
                event_type=SettingsEventType.SETTINGS_UPDATED,
                component_address=update.component_address,
                value=update.value,
                source_session_id=None,
                source_process_id=update.process_id,
                timestamp=update.timestamp,
            )

        return Command.COMPLETE.value

    async def _handle_process_route_response_request(
        self, process_client_id: UUID, reader: asyncio.StreamReader
    ) -> None:
        num_bytes = await read_int(reader)
        payload = await reader.readexactly(num_bytes)
        response: ProcessControlResponse | None = None
        try:
            response_obj = pickle.loads(payload)
            if isinstance(response_obj, ProcessControlResponse):
                response = response_obj
            else:
                raise RuntimeError(
                    "process route response payload was not ProcessControlResponse"
                )
        except Exception as exc:
            logger.warning(
                "Process control %s route response parse failed; ignoring payload: %s",
                process_client_id,
                exc,
            )

        if response is None:
            return

        async with self._command_lock:
            pending = self._pending_process_requests.pop(response.request_id, None)

        if pending is None:
            logger.warning(
                "Process control %s returned unknown request_id: %s",
                process_client_id,
                response.request_id,
            )
            return

        owner_process_id, response_fut = pending
        if owner_process_id != process_client_id:
            if not response_fut.done():
                response_fut.set_result(
                    ProcessControlResponse(
                        request_id=response.request_id,
                        ok=False,
                        error=(
                            "Received response from unexpected process "
                            f"{process_client_id}; expected {owner_process_id}"
                        ),
                        error_code=ProcessControlErrorCode.INVALID_RESPONSE,
                        process_id=response.process_id,
                    )
                )
            return

        if not response_fut.done():
            response_fut.set_result(response)

    def _process_for_unit(self, unit_address: str) -> ProcessInfo | None:
        for info in self.clients.values():
            if isinstance(info, ProcessInfo) and unit_address in info.units:
                return info
        return None

    async def _route_process_request(
        self,
        unit_address: str,
        operation: str,
        payload: bytes | None,
        timeout: float,
    ) -> ProcessControlResponse:
        request_id = str(uuid1())
        response_fut: asyncio.Future[ProcessControlResponse] = (
            asyncio.get_running_loop().create_future()
        )
        request = ProcessControlRequest(
            request_id=request_id,
            unit_address=unit_address,
            operation=operation,
            payload=payload,
        )

        async with self._command_lock:
            process_info = self._process_for_unit(unit_address)
            if process_info is None:
                return ProcessControlResponse(
                    request_id=request_id,
                    ok=False,
                    error=f"No process owns unit '{unit_address}'",
                    error_code=ProcessControlErrorCode.UNROUTABLE_UNIT,
                )

            self._pending_process_requests[request_id] = (process_info.id, response_fut)

            try:
                async with process_info.write_lock:
                    process_writer = process_info.writer
                    request_bytes = pickle.dumps(request)
                    process_writer.write(Command.PROCESS_ROUTE_REQUEST.value)
                    process_writer.write(uint64_to_bytes(len(request_bytes)))
                    process_writer.write(request_bytes)
                    await process_writer.drain()
            except Exception as exc:
                self._pending_process_requests.pop(request_id, None)
                return ProcessControlResponse(
                    request_id=request_id,
                    ok=False,
                    error=f"Failed to route request to owning process: {exc}",
                    error_code=ProcessControlErrorCode.ROUTE_WRITE_FAILED,
                    process_id=process_info.process_id,
                )

        try:
            return await asyncio.wait_for(response_fut, timeout=timeout)
        except asyncio.TimeoutError:
            async with self._command_lock:
                self._pending_process_requests.pop(request_id, None)
            return ProcessControlResponse(
                request_id=request_id,
                ok=False,
                error=(
                    f"Timed out waiting for process response "
                    f"(unit={unit_address}, operation={operation}, timeout={timeout}s)"
                ),
                error_code=ProcessControlErrorCode.TIMEOUT,
                process_id=process_info.process_id,
            )

    async def _handle_session_process_request(
        self,
        writer: asyncio.StreamWriter,
        reader: asyncio.StreamReader,
    ) -> None:
        unit_address = await read_str(reader)
        operation = await read_str(reader)
        timeout = float(await read_str(reader))
        payload_size = await read_int(reader)
        payload: bytes | None = None
        if payload_size > 0:
            payload = await reader.readexactly(payload_size)

        response = await self._route_process_request(
            unit_address=unit_address,
            operation=operation,
            payload=payload,
            timeout=timeout,
        )
        response_bytes = pickle.dumps(response)
        writer.write(uint64_to_bytes(len(response_bytes)))
        writer.write(response_bytes)
        writer.write(Command.COMPLETE.value)

    def _connect_owner(
        self, from_topic: str, to_topic: str, owner: UUID | str | None
    ) -> bool:
        topology_changed = self.graph.add_edge(from_topic, to_topic, owner=owner)
        if isinstance(owner, UUID):
            session = self._session_info(owner)
            if session is not None:
                session.edges.add((from_topic, to_topic))
        return topology_changed

    def _disconnect_owner(
        self, from_topic: str, to_topic: str, owner: UUID | str | None
    ) -> bool:
        topology_changed = self.graph.remove_edge(from_topic, to_topic, owner=owner)
        if isinstance(owner, UUID):
            session = self._session_info(owner)
            if session is not None:
                session.edges.discard((from_topic, to_topic))
        return topology_changed

    def _session_info(self, session_id: UUID) -> SessionInfo | None:
        info = self.clients.get(session_id)
        if isinstance(info, SessionInfo):
            return info
        return None

    def _append_settings_event_locked(
        self,
        event_type: SettingsEventType,
        component_address: str,
        value: SettingsSnapshotValue,
        source_session_id: str | None,
        source_process_id: str | None,
        timestamp: float | None = None,
    ) -> None:
        self._settings_event_seq += 1
        event = SettingsChangedEvent(
            seq=self._settings_event_seq,
            event_type=event_type,
            component_address=component_address,
            timestamp=timestamp if timestamp is not None else time.time(),
            source_session_id=source_session_id,
            source_process_id=source_process_id,
            value=value,
        )
        self._settings_events.append(event)

        for queue in self._settings_subscribers.values():
            self._queue_settings_event(queue, event)

        # Bound memory growth for long-lived servers.
        max_events = 10_000
        if len(self._settings_events) > max_events:
            del self._settings_events[0 : len(self._settings_events) - max_events]

    def _remove_settings_for_session_locked(self, session_id: UUID) -> None:
        component_addresses = self._settings_owned_by_session.pop(session_id, set())
        for component_address in component_addresses:
            if self._settings_source_session.get(component_address) == session_id:
                self._settings_current.pop(component_address, None)
                self._settings_source_session.pop(component_address, None)

    def _apply_session_metadata_settings_locked(
        self, session_id: UUID, metadata: GraphMetadata
    ) -> None:
        session_components: set[str] = set()
        for component in metadata.components.values():
            value = SettingsSnapshotValue(
                serialized=component.initial_settings[0],
                repr_value=component.initial_settings[1],
            )
            self._settings_current[component.address] = value
            self._settings_source_session[component.address] = session_id
            session_components.add(component.address)
            self._append_settings_event_locked(
                event_type=SettingsEventType.INITIAL_SETTINGS,
                component_address=component.address,
                value=value,
                source_session_id=str(session_id),
                source_process_id=None,
            )

        self._settings_owned_by_session[session_id] = session_components

    async def _handle_session_edge_request(
        self,
        session_id: UUID,
        req: bytes,
        reader: asyncio.StreamReader,
    ) -> bytes:
        from_topic = await read_str(reader)
        to_topic = await read_str(reader)

        async with self._command_lock:
            try:
                if req == Command.SESSION_CONNECT.value:
                    topology_changed = self._connect_owner(
                        from_topic, to_topic, session_id
                    )
                else:
                    topology_changed = self._disconnect_owner(
                        from_topic, to_topic, session_id
                    )
            except CyclicException:
                return Command.CYCLIC.value

            if topology_changed:
                await self._notify_downstream_for_topic(to_topic)

        return Command.COMPLETE.value

    async def _handle_session_clear_request(self, session_id: UUID) -> bytes:
        async with self._command_lock:
            notify_topics = self._clear_session_state(session_id)
            for topic in notify_topics:
                await self._notify_downstream_for_topic(topic)
        return Command.COMPLETE.value

    async def _handle_session_register_request(
        self, session_id: UUID, reader: asyncio.StreamReader
    ) -> bytes:
        num_bytes = await read_int(reader)
        payload = await reader.readexactly(num_bytes)
        metadata: GraphMetadata | None = None
        try:
            metadata_obj = pickle.loads(payload)
            if isinstance(metadata_obj, GraphMetadata):
                metadata = metadata_obj
            else:
                raise RuntimeError("metadata payload was not GraphMetadata")
        except Exception as exc:
            logger.warning(
                f"Session {session_id} metadata parse failed; ignoring payload: {exc}"
            )

        async with self._command_lock:
            session = self._session_info(session_id)
            if session is not None and metadata is not None:
                self._remove_settings_for_session_locked(session_id)
                session.metadata = metadata
                self._apply_session_metadata_settings_locked(session_id, metadata)

        return Command.COMPLETE.value

    async def _handle_session_snapshot_request(
        self, writer: asyncio.StreamWriter
    ) -> None:
        async with self._command_lock:
            snapshot = self._snapshot()
            snapshot_bytes = pickle.dumps(snapshot)
            writer.write(uint64_to_bytes(len(snapshot_bytes)))
            writer.write(snapshot_bytes)
            writer.write(Command.COMPLETE.value)

    async def _handle_session_settings_snapshot_request(
        self, writer: asyncio.StreamWriter
    ) -> None:
        async with self._command_lock:
            snapshot = {
                component_address: self._settings_current[component_address]
                for component_address in sorted(self._settings_current)
            }
            snapshot_bytes = pickle.dumps(snapshot)
            writer.write(uint64_to_bytes(len(snapshot_bytes)))
            writer.write(snapshot_bytes)
            writer.write(Command.COMPLETE.value)

    async def _handle_session_settings_events_request(
        self, writer: asyncio.StreamWriter, after_seq: int
    ) -> None:
        async with self._command_lock:
            events = [event for event in self._settings_events if event.seq > after_seq]
            event_bytes = pickle.dumps(events)
            writer.write(uint64_to_bytes(len(event_bytes)))
            writer.write(event_bytes)
            writer.write(Command.COMPLETE.value)

    def _clear_session_state(self, session_id: UUID) -> set[str]:
        notify_topics: set[str] = set()
        session = self._session_info(session_id)
        if session is None:
            return notify_topics

        for from_topic, to_topic in list(session.edges):
            if self._disconnect_owner(from_topic, to_topic, session_id):
                notify_topics.add(to_topic)

        self._remove_settings_for_session_locked(session_id)
        session.metadata = None
        return notify_topics

    def _drop_session(self, session_id: UUID) -> set[str]:
        notify_topics: set[str] = set()
        session = self._session_info(session_id)
        if session is None:
            return notify_topics

        for from_topic, to_topic in list(session.edges):
            if self._disconnect_owner(from_topic, to_topic, session_id):
                notify_topics.add(to_topic)

        self._remove_settings_for_session_locked(session_id)
        session.metadata = None
        self.clients.pop(session_id, None)
        return notify_topics

    def _snapshot(self) -> GraphSnapshot:
        graph = {node: sorted(conns) for node, conns in self.graph.graph.items()}
        edge_owners = {
            Edge(from_topic=from_topic, to_topic=to_topic): [
                "persistent" if owner is None else str(owner)
                for owner in sorted(
                    owners, key=lambda owner: "" if owner is None else str(owner)
                )
            ]
            for (from_topic, to_topic), owners in sorted(self.graph.edge_owners.items())
        }
        sessions = {
            str(session_id): SnapshotSession(
                edges=sorted(
                    [
                        Edge(from_topic=from_topic, to_topic=to_topic)
                        for from_topic, to_topic in session.edges
                    ],
                    key=lambda edge: (edge.from_topic, edge.to_topic),
                ),
                metadata=session.metadata,
            )
            for session_id, session in sorted(
                [
                    (client_id, info)
                    for client_id, info in self.clients.items()
                    if isinstance(info, SessionInfo)
                ],
                key=lambda item: str(item[0]),
            )
        }
        processes = {
            str(client_id): SnapshotProcess(
                process_id=(
                    process.process_id
                    if process.process_id is not None
                    else str(client_id)
                ),
                pid=process.pid,
                host=process.host,
                units=sorted(process.units),
            )
            for client_id, process in sorted(
                [
                    (client_id, info)
                    for client_id, info in self.clients.items()
                    if isinstance(info, ProcessInfo)
                ],
                key=lambda item: str(item[0]),
            )
        }
        return GraphSnapshot(
            graph=graph,
            edge_owners=edge_owners,
            sessions=sessions,
            processes=processes,
        )

    async def _notify_downstream_for_topic(self, topic: str) -> None:
        for sub in self._downstream_subs(topic):
            await self._notify_subscriber(sub)

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

    async def ensure(self, auto_start: bool | None = None) -> GraphServer | None:
        """
        Try connecting to an existing server. If none is listening and no explicit
        address/environment is set, start one and return it. If an existing one is
        found, return None. If auto_start is provided, it overrides the default
        behavior.
        """
        server = None
        ensure_server = False
        if auto_start is not None:
            ensure_server = auto_start
        elif self._address is None:
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
