import asyncio
from collections.abc import Callable, Mapping, Iterable
from collections.abc import Collection as AbstractCollection
import enum
import inspect
import logging
import os
import pickle
import signal
from dataclasses import dataclass
from threading import BrokenBarrierError
from multiprocessing import Event, Barrier
from multiprocessing.synchronize import Event as EventType
from multiprocessing.synchronize import Barrier as BarrierType
from multiprocessing.connection import wait, Connection
from socket import socket

from .netprotocol import DEFAULT_SHM_SIZE, AddressType

from .collection import Collection, NetworkDefinition
from .component import Component
from .stream import (
    Stream,
    InputStream,
    OutputStream,
    Topic,
    InputTopic,
    OutputTopic,
    Relay,
    InputRelay,
    OutputRelay,
)
from .unit import Unit, PROCESS_ATTR, SUBSCRIBES_ATTR, PUBLISHES_ATTR
from .settings import Settings
from .graphmeta import (
    CollectionMetadata,
    ComponentMetadata,
    ComponentMetadataType,
    DynamicSettingsMetadata,
    InputRelayMetadata,
    InputStreamMetadata,
    InputTopicMetadata,
    OutputRelayMetadata,
    OutputStreamMetadata,
    OutputTopicMetadata,
    RelayMetadata,
    RelayMetadataType,
    StreamMetadataType,
    StreamMetadata,
    TopicMetadata,
    TopicMetadataType,
    TaskMetadata,
    GraphMetadata,
    UnitMetadata,
)
from .relay import _RelayRuntime, _relay_runtime, _relay_runtime_info
from .settingsmeta import (
    settings_repr_value,
    settings_schema_from_type,
    settings_schema_from_value,
)

from .graphserver import GraphService
from .graphcontext import GraphContext
from .backendprocess import (
    BackendProcess,
    DefaultBackendProcess,
    ShutdownSummary,
    new_threaded_event_loop,
)

from .util import either_dict_or_kwargs, elevated_fd_limit

logger = logging.getLogger("ezmsg")


def crawl_components(
    component: Component,
    callback: Callable[[Component], None] | None = None,
) -> list[Component]:
    search: list[Component] = [component]
    out: list[Component] = []
    while len(search):
        comp = search.pop()
        out.append(comp)
        search += list(comp.components.values())
        if callback is not None:
            callback(comp)
    return out


@dataclass
class _ProcessSpec:
    units: list[Unit]
    relays: list[_RelayRuntime]


class ExecutionContext:
    _process_specs: list[_ProcessSpec]
    _processes: list[BackendProcess] | None

    term_ev: EventType
    start_barrier: BarrierType
    connections: list[tuple[str, str]]

    def __init__(
        self,
        process_specs: list[_ProcessSpec],
        connections: list[tuple[str, str]] = [],
        start_participant: bool = False,
    ) -> None:
        self.connections = connections
        self._process_specs = process_specs
        self._processes = None

        self.term_ev = Event()
        self.start_barrier = Barrier(
            len(process_specs) + (1 if start_participant else 0)
        )
        self.stop_barrier = Barrier(len(process_specs))

    def create_processes(
        self,
        graph_address: AddressType | None,
        backend_process: type[BackendProcess] = DefaultBackendProcess,
    ) -> None:
        self._processes = [
            backend_process(
                process_spec.units,
                process_spec.relays,
                self.term_ev,
                self.start_barrier,
                self.stop_barrier,
                graph_address,
            )
            for process_spec in self._process_specs
        ]

    @property
    def processes(self) -> list[BackendProcess]:
        if self._processes is None:
            raise ValueError("ExecutionContext has not initialized processes")
        else:
            return self._processes

    @property
    def process_count(self) -> int:
        return len(self._process_specs)

    @classmethod
    def setup(
        cls,
        components: Mapping[str, Component],
        root_name: str | None = None,
        connections: NetworkDefinition | None = None,
        process_components: AbstractCollection[Component] | None = None,
        force_single_process: bool = False,
        start_participant: bool = False,
    ) -> "ExecutionContext | None":
        graph_connections: list[tuple[str, str]] = []
        relay_endpoints_by_collection: dict[
            str, list[Relay | InputRelay | OutputRelay]
        ] = {}

        for name, component in components.items():
            component._set_name(name)
            component._set_location([root_name] if root_name is not None else [])

        def normalize_topic(endpoint: Stream | str | enum.Enum, where: str) -> str:
            if isinstance(endpoint, Stream):
                return endpoint.address
            if isinstance(endpoint, enum.Enum):
                return endpoint.name
            if isinstance(endpoint, str):
                return endpoint
            raise TypeError(
                f"Invalid endpoint type in {where}: {type(endpoint)}. "
                "Expected Stream, str, or Enum."
            )

        if connections is not None:
            for from_topic, to_topic in connections:
                graph_connections.append(
                    (
                        normalize_topic(from_topic, "connections"),
                        normalize_topic(to_topic, "connections"),
                    )
                )

        def gather_relays(comp: Component) -> None:
            if not isinstance(comp, Collection):
                return

            relays = [
                endpoint
                for endpoint in comp.streams.values()
                if isinstance(endpoint, (Relay, InputRelay, OutputRelay))
            ]
            if relays:
                relay_endpoints_by_collection[comp.address] = relays

        for component in components.values():
            if isinstance(component, Collection):
                crawl_components(component, gather_relays)

        def gather_edges(comp: Component):
            if isinstance(comp, Collection):
                for from_stream, to_stream in comp.network():
                    graph_connections.append(
                        (
                            normalize_topic(from_stream, f"{comp.address}.network"),
                            normalize_topic(to_stream, f"{comp.address}.network"),
                        )
                    )

        for component in components.values():
            if isinstance(component, Collection):
                crawl_components(component, gather_edges)

        for component in components.values():
            if isinstance(component, Collection):

                def configure_collections(comp: Component):
                    if isinstance(comp, Collection):
                        comp.configure()

                crawl_components(component, configure_collections)

        relay_specs_by_collection = {
            collection_address: [_relay_runtime(endpoint) for endpoint in endpoints]
            for collection_address, endpoints in relay_endpoints_by_collection.items()
        }
        relay_bindings = {
            relay.endpoint_topic: relay
            for relays in relay_specs_by_collection.values()
            for relay in relays
        }

        if relay_bindings:
            rewritten_connections: list[tuple[str, str]] = []
            for from_topic, to_topic in graph_connections:
                to_binding = relay_bindings.get(to_topic, None)
                if to_binding is not None and to_binding.kind == "output":
                    to_topic = to_binding.relay_input_topic

                from_binding = relay_bindings.get(from_topic, None)
                if from_binding is not None and from_binding.kind == "input":
                    from_topic = from_binding.relay_output_topic

                rewritten_connections.append((from_topic, to_topic))

            for binding in relay_bindings.values():
                if binding.kind == "input":
                    rewritten_connections.append(
                        (binding.endpoint_topic, binding.relay_input_topic)
                    )
                elif binding.kind == "output":
                    rewritten_connections.append(
                        (binding.relay_output_topic, binding.endpoint_topic)
                    )
                else:
                    rewritten_connections.append(
                        (binding.endpoint_topic, binding.relay_input_topic)
                    )
                    rewritten_connections.append(
                        (binding.relay_output_topic, binding.endpoint_topic)
                    )

            graph_connections = rewritten_connections

        processes = collect_processes(
            components.values(),
            process_components,
            relay_specs_by_collection,
        )

        if force_single_process:
            processes = [
                _ProcessSpec(
                    units=[unit for process in processes for unit in process.units],
                    relays=[relay for process in processes for relay in process.relays],
                )
            ]

        if not processes:
            return None

        return cls(
            processes,
            graph_connections,
            start_participant,
        )


class GraphRunnerStartError(RuntimeError):
    pass


class GraphRunner:
    _components: Mapping[str, Component]
    _execution_context: ExecutionContext | None
    _graph_context: GraphContext | None
    _loop: asyncio.AbstractEventLoop | None
    _loop_cm: object | None
    _loop_shutdown_summary: ShutdownSummary | None
    _main_process: BackendProcess | None
    _spawned_processes: list[BackendProcess]
    _start_participant: bool
    _cleanup_done: bool
    _graph_server_spawned: bool
    _started: bool
    _stopped: bool

    def __init__(
        self,
        components: Mapping[str, Component] | None = None,
        root_name: str | None = None,
        connections: NetworkDefinition | None = None,
        process_components: AbstractCollection[Component] | None = None,
        backend_process: type[BackendProcess] = DefaultBackendProcess,
        graph_address: AddressType | None = None,
        force_single_process: bool = False,
        profiler_log_name: str | None = None,
        **components_kwargs: Component,
    ) -> None:
            
        components = either_dict_or_kwargs(components, components_kwargs, "GraphRunner")
        if components is None:
            raise ValueError("Must supply at least one component to run")

        self._components = components
        self._root_name = root_name
        self._connections = connections
        self._process_components = process_components
        self._backend_process = backend_process
        self._graph_address = graph_address
        self._force_single_process = force_single_process
        self._profiler_log_name = profiler_log_name

        self._execution_context = None
        self._graph_context = None
        self._loop = None
        self._loop_cm = None
        self._loop_shutdown_summary = None
        self._main_process = None
        self._spawned_processes = []
        self._start_participant = False
        self._cleanup_done = False
        self._graph_server_spawned = False
        self._started = False
        self._stopped = False

    @property
    def graph_address(self) -> AddressType | None:
        if self._graph_context is not None:
            return self._graph_context.graph_address
        return self._graph_address

    @property
    def strict_shutdown(self) -> bool:
        value = os.environ.get("EZMSG_STRICT_SHUTDOWN", "")
        return value.lower() in ("1", "true", "yes", "on")

    @strict_shutdown.setter
    def strict_shutdown(self, value: bool) -> None:
        os.environ["EZMSG_STRICT_SHUTDOWN"] = "1" if value else "0"

    @property
    def graph_server_spawned(self) -> bool:
        return self._graph_server_spawned

    @property
    def connections(self) -> list[tuple[str, str]]:
        if self._execution_context is None:
            return []
        return list(self._execution_context.connections)

    @property
    def processes(self) -> list[BackendProcess]:
        if self._execution_context is None:
            raise ValueError("GraphRunner has not initialized processes")
        return self._execution_context.processes

    @property
    def running(self) -> bool:
        return self._started

    def _type_name(self, tp: type) -> str:
        return f"{tp.__module__}.{tp.__qualname__}"

    def _stream_type_name(self, stream_type: object) -> str:
        if inspect.isclass(stream_type):
            return self._type_name(stream_type)
        return repr(stream_type)

    def _settings_repr(self, value: object) -> dict[str, object] | str:
        return settings_repr_value(value)

    def _settings_snapshot(self, value: object) -> tuple[bytes | None, dict[str, object] | str]:
        try:
            pickled = pickle.dumps(value)
        except Exception as exc:
            logger.warning(f"Could not pickle settings for metadata: {exc}")
            pickled = None
        return pickled, self._settings_repr(value)

    def _component_metadata(self) -> GraphMetadata:
        components: dict[str, ComponentMetadataType] = {}

        for root in self._components.values():
            for comp in crawl_components(root):
                is_collection = isinstance(comp, Collection)
                input_settings = comp.streams.get("INPUT_SETTINGS")
                dynamic_settings = DynamicSettingsMetadata(
                    enabled=isinstance(input_settings, InputStream),
                    input_topic=(
                        input_settings.address
                        if isinstance(input_settings, InputStream)
                        else None
                    ),
                    settings_type=(
                        self._stream_type_name(input_settings.msg_type)
                        if isinstance(input_settings, InputStream)
                        else None
                    ),
                )

                stream_entries: dict[str, StreamMetadataType] = {}
                topic_entries: dict[str, TopicMetadataType] = {}
                relay_entries: dict[str, RelayMetadataType] = {}
                for stream_name, stream in comp.streams.items():
                    msg_type = self._stream_type_name(stream.msg_type)
                    if isinstance(stream, InputRelay):
                        runtime = _relay_runtime_info(stream)
                        relay_entries[stream_name] = InputRelayMetadata(
                            name=stream_name,
                            address=stream.address,
                            msg_type=msg_type,
                            leaky=stream.leaky,
                            max_queue=stream.max_queue,
                            copy_on_forward=stream.copy_on_forward,
                            relay_group=runtime.group,
                            relay_input_topic=runtime.input_topic,
                            relay_output_topic=runtime.output_topic,
                        )
                    elif isinstance(stream, OutputRelay):
                        runtime = _relay_runtime_info(stream)
                        relay_entries[stream_name] = OutputRelayMetadata(
                            name=stream_name,
                            address=stream.address,
                            msg_type=msg_type,
                            host=stream.host,
                            port=stream.port,
                            num_buffers=stream.num_buffers,
                            buf_size=stream.buf_size,
                            force_tcp=stream.force_tcp,
                            copy_on_forward=stream.copy_on_forward,
                            relay_group=runtime.group,
                            relay_input_topic=runtime.input_topic,
                            relay_output_topic=runtime.output_topic,
                        )
                    elif isinstance(stream, Relay):
                        runtime = _relay_runtime_info(stream)
                        relay_entries[stream_name] = RelayMetadata(
                            name=stream_name,
                            address=stream.address,
                            msg_type=msg_type,
                            leaky=stream.leaky,
                            max_queue=stream.max_queue,
                            host=stream.host,
                            port=stream.port,
                            num_buffers=stream.num_buffers,
                            buf_size=stream.buf_size,
                            force_tcp=stream.force_tcp,
                            copy_on_forward=stream.copy_on_forward,
                            relay_group=runtime.group,
                            relay_input_topic=runtime.input_topic,
                            relay_output_topic=runtime.output_topic,
                        )
                    elif isinstance(stream, InputTopic):
                        topic_entries[stream_name] = InputTopicMetadata(
                            name=stream_name,
                            address=stream.address,
                            msg_type=msg_type,
                        )
                    elif isinstance(stream, OutputTopic):
                        topic_entries[stream_name] = OutputTopicMetadata(
                            name=stream_name,
                            address=stream.address,
                            msg_type=msg_type,
                        )
                    elif isinstance(stream, Topic):
                        topic_entries[stream_name] = TopicMetadata(
                            name=stream_name,
                            address=stream.address,
                            msg_type=msg_type,
                        )
                    elif isinstance(stream, InputStream):
                        if is_collection:
                            topic_entries[stream_name] = InputTopicMetadata(
                                name=stream_name,
                                address=stream.address,
                                msg_type=msg_type,
                            )
                        else:
                            stream_entries[stream_name] = InputStreamMetadata(
                                name=stream_name,
                                address=stream.address,
                                msg_type=msg_type,
                                leaky=stream.leaky,
                                max_queue=stream.max_queue,
                            )
                    elif isinstance(stream, OutputStream):
                        if is_collection:
                            topic_entries[stream_name] = OutputTopicMetadata(
                                name=stream_name,
                                address=stream.address,
                                msg_type=msg_type,
                            )
                        else:
                            stream_entries[stream_name] = OutputStreamMetadata(
                                name=stream_name,
                                address=stream.address,
                                msg_type=msg_type,
                                host=stream.host,
                                port=stream.port,
                                num_buffers=stream.num_buffers,
                                buf_size=stream.buf_size,
                                force_tcp=stream.force_tcp,
                            )
                    else:
                        if is_collection:
                            topic_entries[stream_name] = TopicMetadata(
                                name=stream_name,
                                address=stream.address,
                                msg_type=msg_type,
                            )
                        else:
                            stream_entries[stream_name] = StreamMetadata(
                                name=stream_name,
                                address=stream.address,
                                msg_type=msg_type,
                            )

                task_entries: list[TaskMetadata] = []
                for task_name, task in comp.tasks.items():
                    task_entry = TaskMetadata(name=task_name)

                    if hasattr(task, SUBSCRIBES_ATTR):
                        sub_stream = getattr(task, SUBSCRIBES_ATTR)
                        if hasattr(sub_stream, "name") and sub_stream.name in comp.streams:
                            task_entry.subscribes = comp.streams[sub_stream.name].address

                    if hasattr(task, PUBLISHES_ATTR):
                        pub_streams = getattr(task, PUBLISHES_ATTR)
                        task_entry.publishes = [
                            comp.streams[stream.name].address
                            for stream in pub_streams
                            if hasattr(stream, "name") and stream.name in comp.streams
                        ]

                    task_entries.append(task_entry)

                settings_type = getattr(comp.__class__, "__settings_type__", Settings)
                settings_type_name = (
                    self._type_name(settings_type)
                    if inspect.isclass(settings_type)
                    else repr(settings_type)
                )
                settings_schema = (
                    settings_schema_from_value(comp.SETTINGS)
                    if comp.SETTINGS is not None
                    else settings_schema_from_type(settings_type)
                )

                component_common = dict(
                    address=comp.address,
                    name=comp.name,
                    component_type=self._type_name(comp.__class__),
                    settings_type=settings_type_name,
                    initial_settings=self._settings_snapshot(comp.SETTINGS),
                    dynamic_settings=dynamic_settings,
                    settings_schema=settings_schema,
                )

                metadata_entry: ComponentMetadataType
                if isinstance(comp, Collection):
                    metadata_entry = CollectionMetadata(
                        **component_common,
                        topics=topic_entries,
                        relays=relay_entries,
                        children=sorted(
                            child.address for child in comp.components.values()
                        ),
                    )
                elif isinstance(comp, Unit):
                    metadata_entry = UnitMetadata(
                        **component_common,
                        streams=stream_entries,
                        tasks=sorted(task_entries, key=lambda task: task.name),
                        main=comp.main.__name__ if comp.main is not None else None,
                        threads=sorted(comp.threads.keys()),
                    )
                else:
                    metadata_entry = ComponentMetadata(**component_common)
                components[comp.address] = metadata_entry

        return GraphMetadata(
            schema_version=1,
            root_name=self._root_name,
            components={address: components[address] for address in sorted(components)},
        )

    def start(self) -> None:
        if self._started:
            raise RuntimeError("GraphRunner is already running")
        if self._stopped:
            raise RuntimeError("GraphRunner cannot be restarted")
        if self._force_single_process:
            raise ValueError("force_single_process is only supported with run_blocking")
        with elevated_fd_limit():
            if not self._initialize(force_single_process=False, wait_for_ready=True):
                return

            self._start_processes(self.processes)

            if self._start_participant and self._execution_context is not None:
                try:
                    self._execution_context.start_barrier.wait()
                except BrokenBarrierError as err:
                    self._execution_context.term_ev.set()
                    self._join_spawned_processes()
                    self._cleanup()
                    self._stopped = True
                    raise GraphRunnerStartError(
                        "GraphRunner failed to start. One or more processes exited before "
                        "reaching the start barrier; check logs for earlier exceptions."
                    ) from err
        self._started = True
        if self._stopped:
            self._started = False

    def stop(self) -> None:
        if not self._started:
            raise RuntimeError("GraphRunner is not running")
        if self._execution_context is None:
            raise RuntimeError("GraphRunner execution context is invalid!")
        self._execution_context.term_ev.set()
        self._join_spawned_processes()
        self._cleanup()
        self._started = False
        self._stopped = True

    def run_blocking(self) -> None:
        if self._started:
            raise RuntimeError("GraphRunner is already running")
        if self._stopped:
            raise RuntimeError("GraphRunner cannot be restarted")
        with elevated_fd_limit():
            if not self._initialize(
                force_single_process=self._force_single_process, wait_for_ready=False
            ):
                return
            self._run_main_process()

    def _initialize(self, force_single_process: bool, wait_for_ready: bool) -> bool:
        os.environ["EZMSG_PROFILER"] = self._profiler_log_name or "ezprofiler.log"
        self._cleanup_done = False
        self._spawned_processes = []
        self._start_participant = wait_for_ready

        self._execution_context = ExecutionContext.setup(
            self._components,
            self._root_name,
            self._connections,
            self._process_components,
            force_single_process,
            wait_for_ready,
        )

        if self._execution_context is None:
            return False

        self._loop_shutdown_summary = ShutdownSummary()
        self._loop_cm = new_threaded_event_loop(
            shutdown_summary=self._loop_shutdown_summary
        )
        self._loop = self._loop_cm.__enter__()

        try:

            async def create_graph_context() -> GraphContext:
                return await GraphContext(self._graph_address).__aenter__()

            graph_context = asyncio.run_coroutine_threadsafe(
                create_graph_context(), self._loop
            ).result()
            self._graph_context = graph_context
            self._graph_server_spawned = graph_context._graph_server is not None

            address = graph_context.graph_address
            if address is None:
                address = GraphService.default_address()

            if graph_context._graph_server is None:
                logger.info(f"Connected to GraphServer @ {address}")
            else:
                logger.info(f"Spawned GraphServer @ {address}")

            self._execution_context.create_processes(
                graph_address=address,
                backend_process=self._backend_process,
            )

            async def setup_graph() -> None:
                for edge in self._execution_context.connections:
                    await graph_context.connect(*edge)

            asyncio.run_coroutine_threadsafe(setup_graph(), self._loop).result()

            metadata = self._component_metadata()

            async def register_graph_metadata() -> None:
                await graph_context.register_metadata(metadata)

            asyncio.run_coroutine_threadsafe(
                register_graph_metadata(), self._loop
            ).result()

            if len(self._execution_context.processes) > 1:
                logger.info(
                    f"Running in {len(self._execution_context.processes)} processes."
                )

        except Exception:
            self._cleanup()
            raise

        return True

    def _start_processes(self, processes: list[BackendProcess]) -> None:
        for proc in processes:
            proc.start()
            self._spawned_processes.append(proc)

    def _join_spawned_processes(self) -> None:
        sentinels: dict[Connection | socket | int, BackendProcess] = {
            proc.sentinel: proc for proc in self._spawned_processes
        }

        # Poll sentinels so KeyboardInterrupt remains responsive (notably on Windows)
        while len(sentinels):
            done = wait(list(sentinels.keys()), timeout=0.1)

            for sentinel in done:
                proc = sentinels.pop(sentinel, None)
                if proc is not None:
                    proc.join()

    def _run_main_process(self) -> None:
        if self._execution_context is None or self._loop is None:
            return
        self._main_process = self._execution_context.processes[0]

        interrupts = 0
        forced_sigint = False
        try:
            self._start_processes(self._execution_context.processes[1:])
            self._started = True
            self._main_process.process(self._loop)
            self._join_spawned_processes()
            logger.info("All processes exited normally")

        except KeyboardInterrupt:
            interrupts += 1
            logger.info(
                "Attempting graceful shutdown, interrupt again to force quit..."
            )
            self._execution_context.term_ev.set()

            try:
                self._join_spawned_processes()

            except KeyboardInterrupt:
                interrupts += 1
                forced_sigint = True
                logger.warning("Interrupt intercepted, force quitting")
                self._execution_context.start_barrier.abort()
                self._execution_context.stop_barrier.abort()
                for proc in self._spawned_processes:
                    proc.terminate()

        finally:
            while True:
                try:
                    self._join_spawned_processes()
                    self._cleanup()
                    break
                except KeyboardInterrupt:
                    interrupts += 1
                    if interrupts >= 2:
                        forced_sigint = True
                        logger.warning("Interrupt intercepted, force quitting")
                        if self._execution_context is not None:
                            self._execution_context.start_barrier.abort()
                            self._execution_context.stop_barrier.abort()
                        for proc in self._spawned_processes:
                            proc.terminate()
                        self._cleanup()
                        break
                    logger.info(
                        "Interrupt received during cleanup; attempting graceful shutdown..."
                    )
                    if self._execution_context is not None:
                        self._execution_context.term_ev.set()
            self._started = False
            self._stopped = True
            if interrupts and not forced_sigint and self._shutdown_was_unclean():
                forced_sigint = True
            if forced_sigint:
                self._exit_with_sigint()

    def _shutdown_was_unclean(self) -> bool:
        main_shutdown_errors = bool(
            self._main_process is not None
            and getattr(self._main_process, "_shutdown_errors", False)
        )
        summary = self._loop_shutdown_summary
        loop_unclean = bool(summary is not None and summary.unclean)
        return main_shutdown_errors or loop_unclean

    def _exit_with_sigint(self) -> None:
        code = 0xC000013A if os.name == "nt" else 130
        if os.name == "nt":
            try:
                import ctypes
            except Exception:
                os._exit(1)
            try:
                ctypes.windll.kernel32.ExitProcess(ctypes.c_uint(code).value)
            except Exception:
                os._exit(ctypes.c_int32(code).value)
            return

        prev_handler = None
        try:
            prev_handler = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.raise_signal(signal.SIGINT)
        except Exception:
            raise SystemExit(code)
        finally:
            if prev_handler is not None:
                try:
                    signal.signal(signal.SIGINT, prev_handler)
                except Exception:
                    pass

        raise SystemExit(code)

    def _cleanup(self) -> None:
        if self._cleanup_done:
            return
        self._cleanup_done = True

        if self._graph_context is not None and self._loop is not None:

            async def cleanup_graph() -> None:
                await self._graph_context.__aexit__(None, None, None)

            asyncio.run_coroutine_threadsafe(cleanup_graph(), self._loop).result()

        if self._loop_cm is not None:
            self._loop_cm.__exit__(None, None, None)

        self._loop_cm = None
        self._loop = None
        self._graph_context = None
        self._spawned_processes = []
        self._start_participant = False


def run_system(
    system: Collection,
    num_buffers: int = 32,
    init_buf_size: int = DEFAULT_SHM_SIZE,
    backend_process: type[BackendProcess] = DefaultBackendProcess,
) -> None:
    """
    Deprecated function for running a system (Collection).

    .. deprecated::
       Use :func:`run` instead to run any component (unit, collection).

    :param system: The collection to run
    :type system: Collection
    :param num_buffers: Number of message buffers (deprecated parameter)
    :type num_buffers: int
    :param init_buf_size: Initial buffer size (deprecated parameter)
    :type init_buf_size: int
    :param backend_process: Backend process class to use
    :type backend_process: type[BackendProcess]
    """
    run(SYSTEM=system, backend_process=backend_process)


def run(
    components: Mapping[str, Component] | None = None,
    root_name: str | None = None,
    connections: NetworkDefinition | None = None,
    process_components: AbstractCollection[Component] | None = None,
    backend_process: type[BackendProcess] = DefaultBackendProcess,
    graph_address: AddressType | None = None,
    force_single_process: bool = False,
    profiler_log_name: str | None = None,
    **components_kwargs: Component,
) -> None:
    """
    Begin execution of a set of Components.

    This is the main entry point for running ezmsg applications. It sets up the
    execution environment, initializes components, and manages the message-passing
    infrastructure.

    On initialization, ezmsg will call ``initialize()`` for each :obj:`Unit` and
    ``configure()`` for each :obj:`Collection`, if defined. On initialization, ezmsg
    will create a directed acyclic graph using the contents of ``connections``.

    :param components: Dictionary mapping component names to Component objects. The components
        are the nodes in the ezmsg (directed acyclic) graph.
    :type components: collections.abc.Mapping[str, Component] | None
    :param root_name: Optional root name for the component hierarchy
    :type root_name: str | None
    :param connections: Network definition specifying stream connections between components. These
        are the edges in the ezmsg graph, connecting OutputStreams to InputStreams.
    :type connections: NetworkDefinition | None
    :param process_components: Collection of components that should run in separate processes
    :type process_components: collections.abc.Collection[Component] | None
    :param backend_process: Backend process class to use for execution. Currently under development.
    :type backend_process: type[BackendProcess]
    :param graph_address: Address (hostname and port) of graph server which ezmsg should connect to.
        If not defined, ezmsg will start a new graph server at 127.0.0.1:25978.
    :type graph_address: AddressType | None
    :param force_single_process: Whether to force all components into a single process
    :type force_single_process: bool
    :param components_kwargs: Additional components specified as keyword arguments
    :type components_kwargs: Component

    .. note::
       Since jupyter notebooks run in a single process, you must set `force_single_process=True`.

    .. note::
       The old method :obj:`run_system` has been deprecated and uses ``run()`` instead.
    """
    if components is not None and isinstance(components, Component):
        components = {"SYSTEM": components}
        logger.warning(
            "Passing a single Component without naming the Component is now Deprecated."
        )
        
    components = either_dict_or_kwargs(components, components_kwargs, "run")
    
    runner = GraphRunner(
        components=components,
        root_name=root_name,
        connections=connections,
        process_components=process_components,
        backend_process=backend_process,
        graph_address=graph_address,
        force_single_process=force_single_process,
        profiler_log_name=profiler_log_name,
    )
    
    runner.run_blocking()


def collect_processes(
    collection: Collection | Iterable[Component],
    process_components: AbstractCollection[Component] | None = None,
    relay_specs_by_collection: Mapping[str, list[_RelayRuntime]] | None = None,
) -> list[_ProcessSpec]:
    process_specs, units, relays = _collect_processes(
        [collection] if isinstance(collection, Collection) else collection,
        process_components if process_components is not None else tuple(),
        relay_specs_by_collection if relay_specs_by_collection is not None else {},
    )

    if units or relays:
        process_specs = [_ProcessSpec(units=units, relays=relays)] + process_specs

    return process_specs


def _collect_processes(
    comps: Iterable[Component],
    process_components: AbstractCollection[Component],
    relay_specs_by_collection: Mapping[str, list[_RelayRuntime]],
) -> tuple[list[_ProcessSpec], list[Unit], list[_RelayRuntime]]:
    process_specs: list[_ProcessSpec] = []
    units: list[Unit] = []
    relays: list[_RelayRuntime] = []

    for comp in comps:
        if isinstance(comp, Collection):
            r_process_specs, r_units, r_relays = _collect_processes(
                comp.components.values(),
                comp.process_components(),
                relay_specs_by_collection,
            )
            collection_relays = list(relay_specs_by_collection.get(comp.address, []))

            process_specs = process_specs + r_process_specs
            if comp in process_components:
                if r_units or r_relays or collection_relays:
                    process_specs = process_specs + [
                        _ProcessSpec(
                            units=r_units,
                            relays=r_relays + collection_relays,
                        )
                    ]
            else:
                if r_units:
                    units = units + r_units
                if r_relays or collection_relays:
                    relays = relays + r_relays + collection_relays

        elif isinstance(comp, Unit):
            if comp in process_components:
                process_specs.append(_ProcessSpec(units=[comp], relays=[]))
            else:
                if hasattr(comp, PROCESS_ATTR):
                    process_specs.append(_ProcessSpec(units=[comp], relays=[]))
                else:
                    units.append(comp)

    return process_specs, units, relays
