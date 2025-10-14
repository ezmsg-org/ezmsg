import asyncio
from collections.abc import Callable, Mapping, Iterable
from collections.abc import Collection as AbstractCollection
import enum
import logging
import os

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
    new_threaded_event_loop,
)

from .util import either_dict_or_kwargs

logger = logging.getLogger("ezmsg")


class ExecutionContext:
    processes: list[BackendProcess]
    term_ev: EventType
    start_barrier: BarrierType
    connections: list[tuple[str, str]]

    def __init__(
        self,
        processes: list[list[Unit]],
        graph_service: GraphService,
        connections: list[tuple[str, str]] = [],
        backend_process: type[BackendProcess] = DefaultBackendProcess,
    ) -> None:
        if not processes:
            raise ValueError("Cannot create an execution context for zero processes")

        self.connections = connections

        self.term_ev = Event()
        self.start_barrier = Barrier(len(processes))
        self.stop_barrier = Barrier(len(processes))

        self.processes = [
            backend_process(
                process_units,
                self.term_ev,
                self.start_barrier,
                self.stop_barrier,
                graph_service,
            )
            for process_units in processes
        ]

    @classmethod
    def setup(
        cls,
        components: Mapping[str, Component],
        graph_service: GraphService,
        root_name: str | None = None,
        connections: NetworkDefinition | None = None,
        process_components: AbstractCollection[Component] | None = None,
        backend_process: type[BackendProcess] = DefaultBackendProcess,
        force_single_process: bool = False,
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

        try:
            return cls(
                processes,
                graph_service,
                graph_connections,
                backend_process,
            )
        except ValueError:
            return None


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

    On initialization, ``ezmsg`` will call ``initialize()`` for each :obj:`Unit` and 
    ``configure()`` for each :obj:`Collection`, if defined. On initialization, ``ezmsg``
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
        If not defined, ``ezmsg`` will start a new graph server at 127.0.0.1:25978.
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
    os.environ["EZMSG_PROFILER"] = profiler_log_name or "ezprofiler.log"
    # FIXME: This function is the last major re-implementation needed to make this
    # codebase more maintainable.
    graph_service = GraphService(graph_address)

    if components is not None and isinstance(components, Component):
        components = {"SYSTEM": components}
        logger.warning(
            "Passing a single Component without naming the Component is now Deprecated."
        )
    components = either_dict_or_kwargs(components, components_kwargs, "run")
    if components is None:
        raise ValueError("Must supply at least one component to run")

    with new_threaded_event_loop() as loop:
        execution_context = ExecutionContext.setup(
            components,
            graph_service,
            root_name,
            connections,
            process_components,
            backend_process,
            force_single_process,
        )

        if execution_context is None:
            return

        # FIXME: When done this way, we don't exit the graph_context on exception
        async def create_graph_context() -> GraphContext:
            return await GraphContext(graph_service).__aenter__()

        # FIXME: This sort of stuff should all be done in a separate async function...
        # Done this way, its ugly as hell and opens us up to a lot of issues with
        # entering and exiting context properly on exceptions.
        graph_context = asyncio.run_coroutine_threadsafe(
            create_graph_context(), loop
        ).result()

        async def cleanup_graph() -> None:
            await graph_context.__aexit__(None, None, None)

        async def setup_graph() -> None:
            for edge in execution_context.connections:
                await graph_context.connect(*edge)

        asyncio.run_coroutine_threadsafe(setup_graph(), loop).result()

        if len(execution_context.processes) > 1:
            logger.info(f"Running in {len(execution_context.processes)} processes.")

        main_process = execution_context.processes[0]
        other_processes = execution_context.processes[1:]

        sentinels: set[Connection | socket | int] = set()

        for proc in other_processes:
            proc.start()
            sentinels.add(proc.sentinel)

        def join_all_other_processes():
            while len(sentinels):
                done = wait(sentinels, timeout=0.1)
                for sentinel in done:
                    sentinels.discard(sentinel)

        try:
            main_process.process(loop)
            join_all_other_processes()
            logger.info("All processes exited normally")

        except KeyboardInterrupt:
            logger.info(
                "Attempting graceful shutdown, interrupt again to force quit..."
            )
            execution_context.term_ev.set()

            try:
                join_all_other_processes()

            except KeyboardInterrupt:
                logger.warning("Interrupt intercepted, force quitting")
                execution_context.start_barrier.abort()
                execution_context.stop_barrier.abort()
                for proc in other_processes:
                    proc.terminate()

        finally:
            join_all_other_processes()
            asyncio.run_coroutine_threadsafe(cleanup_graph(), loop).result()


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
