import asyncio
from collections.abc import Callable, Mapping, Iterable
from collections.abc import Collection as AbstractCollection
import enum
import logging
import os
import signal
from threading import BrokenBarrierError
from multiprocessing import Event, Barrier
from multiprocessing.synchronize import Event as EventType
from multiprocessing.synchronize import Barrier as BarrierType
from multiprocessing.connection import wait, Connection
from socket import socket

from .netprotocol import DEFAULT_SHM_SIZE, AddressType

from .collection import Collection, NetworkDefinition
from .component import Component
from .stream import Stream
from .unit import Unit, PROCESS_ATTR

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

        for name, component in components.items():
            component._set_name(name)
            component._set_location([root_name] if root_name is not None else [])

        if connections is not None:
            for from_topic, to_topic in connections:
                if isinstance(from_topic, Stream):
                    from_topic = from_topic.address
                if isinstance(to_topic, Stream):
                    to_topic = to_topic.address
                if isinstance(to_topic, enum.Enum):
                    to_topic = to_topic.name
                if isinstance(from_topic, enum.Enum):
                    from_topic = from_topic.name
                graph_connections.append((from_topic, to_topic))

        def crawl_components(
            component: Component, callback: Callable[[Component], None]
        ) -> None:
            search: list[Component] = [component]
            while len(search):
                comp = search.pop()
                search += list(comp.components.values())
                callback(comp)

        def gather_edges(comp: Component):
            if isinstance(comp, Collection):
                for from_stream, to_stream in comp.network():
                    if isinstance(from_stream, Stream):
                        from_stream = from_stream.address
                    if isinstance(to_stream, Stream):
                        to_stream = to_stream.address
                    if isinstance(to_stream, enum.Enum):
                        to_stream = to_stream.name
                    if isinstance(from_stream, enum.Enum):
                        from_stream = from_stream.name
                    graph_connections.append((from_stream, to_stream))

        for component in components.values():
            if isinstance(component, Collection):
                crawl_components(component, gather_edges)

        processes = collect_processes(components.values(), process_components)

        for component in components.values():
            if isinstance(component, Collection):

                def configure_collections(comp: Component):
                    if isinstance(comp, Collection):
                        comp.configure()

                crawl_components(component, configure_collections)

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
            os._exit(code)

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
