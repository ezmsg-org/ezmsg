import asyncio
from collections.abc import Callable, Mapping, Iterable
from collections.abc import Collection as AbstractCollection
from dataclasses import asdict, is_dataclass
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
    RelayMetadataType,
    StreamMetadataType,
    StreamMetadata,
    TopicMetadata,
    TopicMetadataType,
    TaskMetadata,
    GraphMetadata,
    UnitMetadata,
)
from .relay import _CollectionRelayUnit, _RelaySettings

from .graphserver import GraphService
from .graphcontext import GraphContext
from .backendprocess import (
    BackendProcess,
    DefaultBackendProcess,
    ShutdownSummary,
    new_threaded_event_loop,
)

from .util import either_dict_or_kwargs

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
class _RelayBinding:
    kind: str  # "input" or "output"
    endpoint_topic: str
    relay_in_topic: str
    relay_out_topic: str
    endpoint: InputRelay | OutputRelay
    relay_unit: _CollectionRelayUnit


class ExecutionContext:
    _process_units: list[list[Unit]]
    _processes: list[BackendProcess] | None

    term_ev: EventType
    start_barrier: BarrierType
    connections: list[tuple[str, str]]

    def __init__(
        self,
        process_units: list[list[Unit]],
        connections: list[tuple[str, str]] = [],
        start_participant: bool = False,
    ) -> None:
        self.connections = connections
        self._process_units = process_units
        self._processes = None

        self.term_ev = Event()
        self.start_barrier = Barrier(
            len(process_units) + (1 if start_participant else 0)
        )
        self.stop_barrier = Barrier(len(process_units))

    def create_processes(
        self,
        graph_address: AddressType | None,
        backend_process: type[BackendProcess] = DefaultBackendProcess,
    ) -> None:
        self._processes = [
            backend_process(
                process_units,
                self.term_ev,
                self.start_barrier,
                self.stop_barrier,
                graph_address,
            )
            for process_units in self._process_units
        ]

    @property
    def processes(self) -> list[BackendProcess]:
        if self._processes is None:
            raise ValueError("ExecutionContext has not initialized processes")
        else:
            return self._processes

    @property
    def process_count(self) -> int:
        return len(self._process_units)

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
        relay_bindings: dict[str, _RelayBinding] = {}

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

        def input_relay_settings(relay: InputRelay) -> _RelaySettings:
            return _RelaySettings(
                leaky=relay.leaky,
                max_queue=relay.max_queue,
                copy_on_forward=relay.copy_on_forward,
            )

        def output_relay_settings(relay: OutputRelay) -> _RelaySettings:
            return _RelaySettings(
                host=relay.host,
                port=relay.port,
                num_buffers=relay.num_buffers,
                buf_size=relay.buf_size,
                force_tcp=relay.force_tcp,
                copy_on_forward=relay.copy_on_forward,
            )

        def add_collection_relay_units(comp: Component) -> None:
            if not isinstance(comp, Collection):
                return

            for endpoint_name, endpoint in comp.streams.items():
                if isinstance(endpoint, InputRelay):
                    relay_name = f"__relay_in_{endpoint_name}"
                    if relay_name in comp.components:
                        raise ValueError(
                            f"{comp.address} already defines component '{relay_name}'."
                        )

                    relay_unit = _CollectionRelayUnit(input_relay_settings(endpoint))
                    relay_unit._set_name(relay_name)
                    relay_unit._set_location(comp.location + [comp.name])
                    comp.components[relay_name] = relay_unit
                    setattr(comp, relay_name, relay_unit)

                    relay_bindings[endpoint.address] = _RelayBinding(
                        kind="input",
                        endpoint_topic=endpoint.address,
                        relay_in_topic=relay_unit.INPUT.address,
                        relay_out_topic=relay_unit.OUTPUT.address,
                        endpoint=endpoint,
                        relay_unit=relay_unit,
                    )

                elif isinstance(endpoint, OutputRelay):
                    relay_name = f"__relay_out_{endpoint_name}"
                    if relay_name in comp.components:
                        raise ValueError(
                            f"{comp.address} already defines component '{relay_name}'."
                        )

                    relay_unit = _CollectionRelayUnit(output_relay_settings(endpoint))
                    relay_unit._set_name(relay_name)
                    relay_unit._set_location(comp.location + [comp.name])
                    comp.components[relay_name] = relay_unit
                    setattr(comp, relay_name, relay_unit)

                    relay_bindings[endpoint.address] = _RelayBinding(
                        kind="output",
                        endpoint_topic=endpoint.address,
                        relay_in_topic=relay_unit.INPUT.address,
                        relay_out_topic=relay_unit.OUTPUT.address,
                        endpoint=endpoint,
                        relay_unit=relay_unit,
                    )

        for component in components.values():
            if isinstance(component, Collection):
                crawl_components(component, add_collection_relay_units)
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

        if relay_bindings:
            rewritten_connections: list[tuple[str, str]] = []
            for from_topic, to_topic in graph_connections:
                to_binding = relay_bindings.get(to_topic, None)
                if to_binding is not None and to_binding.kind == "output":
                    to_topic = to_binding.relay_in_topic

                from_binding = relay_bindings.get(from_topic, None)
                if from_binding is not None and from_binding.kind == "input":
                    from_topic = from_binding.relay_out_topic

                rewritten_connections.append((from_topic, to_topic))

            for binding in relay_bindings.values():
                if binding.kind == "input":
                    rewritten_connections.append(
                        (binding.endpoint_topic, binding.relay_in_topic)
                    )
                else:
                    rewritten_connections.append(
                        (binding.relay_out_topic, binding.endpoint_topic)
                    )

            graph_connections = rewritten_connections

        processes = collect_processes(components.values(), process_components)

        for component in components.values():
            if isinstance(component, Collection):

                def configure_collections(comp: Component):
                    if isinstance(comp, Collection):
                        comp.configure()

                crawl_components(component, configure_collections)

        for binding in relay_bindings.values():
            if isinstance(binding.endpoint, InputRelay):
                binding.relay_unit.apply_settings(input_relay_settings(binding.endpoint))
            elif isinstance(binding.endpoint, OutputRelay):
                binding.relay_unit.apply_settings(
                    output_relay_settings(binding.endpoint)
                )

        if force_single_process:
            processes = [[u for pu in processes for u in pu]]

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
        if is_dataclass(value):
            try:
                asdict_value = asdict(value)
                if isinstance(asdict_value, dict):
                    return asdict_value
            except Exception:
                pass

        return repr(value)

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
                        relay_entries[stream_name] = InputRelayMetadata(
                            name=stream_name,
                            address=stream.address,
                            msg_type=msg_type,
                            leaky=stream.leaky,
                            max_queue=stream.max_queue,
                            copy_on_forward=stream.copy_on_forward,
                        )
                    elif isinstance(stream, OutputRelay):
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

                component_common = dict(
                    address=comp.address,
                    name=comp.name,
                    component_type=self._type_name(comp.__class__),
                    settings_type=settings_type_name,
                    initial_settings=self._settings_snapshot(comp.SETTINGS),
                    dynamic_settings=dynamic_settings,
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
        if not self._initialize(
            force_single_process=self._force_single_process, wait_for_ready=False
        ):
            return
        self._started = True
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

            if graph_context._graph_server is None:
                address = graph_context.graph_address
                if address is None:
                    address = GraphService.default_address()
                logger.info(f"Connected to GraphServer @ {address}")
            else:
                logger.info(f"Spawned GraphServer @ {graph_context.graph_address}")

            self._execution_context.create_processes(
                graph_address=graph_context.graph_address,
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
        self._start_processes(self._execution_context.processes[1:])

        interrupts = 0
        forced_sigint = False
        try:
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
) -> list[list[Unit]]:
    if isinstance(collection, Collection):
        process_units, units = _collect_processes(
            collection._components.values(), collection.process_components()
        )

    else:
        process_units, units = _collect_processes(
            collection,
            process_components if process_components is not None else tuple(),
        )

    if len(units):
        process_units = [units] + process_units

    return process_units


def _collect_processes(
    comps: Iterable[Component], process_components: AbstractCollection[Component]
) -> tuple[list[list[Unit]], list[Unit]]:
    process_units: list[list[Unit]] = []
    units: list[Unit] = []

    for comp in comps:
        if isinstance(comp, Collection):
            r_process_units, r_units = _collect_processes(
                comp.components.values(), comp.process_components()
            )

            process_units = process_units + r_process_units
            if comp in process_components:
                if len(r_units) > 0:
                    process_units = process_units + [r_units]
            else:
                if len(r_units) > 0:
                    units = units + r_units

        elif isinstance(comp, Unit):
            if comp in process_components:
                process_units.append([comp])
            else:
                if hasattr(comp, PROCESS_ATTR):
                    process_units.append([comp])
                else:
                    units.append(comp)

    return process_units, units
