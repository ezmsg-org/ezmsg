import asyncio
import logging

from socket import socket
from multiprocessing import Event, Barrier
from multiprocessing.synchronize import Event as EventType
from multiprocessing.synchronize import Barrier as BarrierType
from multiprocessing.connection import wait, Connection

import ezmsg.core as ez

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

from typing import List, Callable, Tuple, Optional, Type, Set, Union

logger = logging.getLogger("ezmsg")


class ExecutionContext:

    processes: List[BackendProcess]
    term_ev: EventType
    start_barrier: BarrierType
    connections: List[Tuple[str, str]]

    def __init__(
        self,
        processes: List[List[Unit]],
        connections: List[Tuple[str, str]] = [],
        backend_process: Type[BackendProcess] = DefaultBackendProcess,
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
                ez.GRAPH.address,
                ez.SHM.address
            )
            for process_units in processes
        ]

    @classmethod
    def setup(
        cls,
        component: Component,
        name: Optional[str] = None,
        connections: Optional[NetworkDefinition] = None,
        backend_process: Type[BackendProcess] = DefaultBackendProcess,
        force_single_process: bool = False,
    ) -> Optional["ExecutionContext"]:

        component._set_name(name)
        component._set_location()

        graph_connections: List[Tuple[str, str]] = []

        if connections is not None:
            for from_topic, to_topic in connections:
                if isinstance(from_topic, Stream):
                    from_topic = from_topic.address
                if isinstance(to_topic, Stream):
                    to_topic = to_topic.address
                graph_connections.append((from_topic, to_topic))

        def crawl_components(
            component: Component, callback: Callable[[Component], None]
        ) -> None:
            search: List[Component] = [component]
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
                    graph_connections.append((from_stream, to_stream))

        if isinstance(component, Collection):
            crawl_components(component, gather_edges)

        processes = []
        if isinstance(component, Collection):
            processes = collect_processes(component)

            def configure_collections(comp: Component):
                if isinstance(comp, Collection):
                    comp.configure()

            crawl_components(component, configure_collections)
        elif isinstance(component, Unit):
            processes = [[component]]

        if force_single_process:
            processes = [[u for pu in processes for u in pu]]

        try:
            return cls(
                processes, graph_connections, backend_process, 
            )
        except ValueError:
            return None
        

def run_system(
    system: Collection,
    num_buffers: int = 32,
    init_buf_size: int = DEFAULT_SHM_SIZE,
    backend_process: Type[BackendProcess] = DefaultBackendProcess,
) -> None:
    """ Deprecated; just use run any component (unit, collection) """
    run(system, backend_process=backend_process)


def run(
    component: Component,
    name: Optional[str] = None,
    connections: Optional[NetworkDefinition] = None,
    backend_process: Type[BackendProcess] = DefaultBackendProcess,
    graph_address: Optional[AddressType] = None,
    force_single_process: bool = False,
) -> None:
    
    ez.GRAPH = GraphService(graph_address)

    with new_threaded_event_loop() as loop:

        async def create_graph_context() -> GraphContext:
            return await GraphContext().__aenter__()
        
        graph_context = asyncio.run_coroutine_threadsafe(create_graph_context(), loop).result()

        execution_context = ExecutionContext.setup(
            component,
            name,
            connections,
            backend_process,
            force_single_process,
        )

        if execution_context is None:
            return

        async def setup_graph() -> None:
            for edge in execution_context.connections:
                await graph_context.connect(*edge)

        async def cleanup_graph(context: GraphContext) -> None:
            await context.__aexit__(None, None, None)

        asyncio.run_coroutine_threadsafe(setup_graph(), loop).result()

        main_process = execution_context.processes[0]
        other_processes = execution_context.processes[1:]

        sentinels: Set[Union[Connection, socket, int]] = set()
        if other_processes:
            logger.info(f"Spinning up {len(other_processes)} extra processes.")

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
            asyncio.run_coroutine_threadsafe(
                cleanup_graph(graph_context), loop
            ).result()


def collect_processes(collection: Collection) -> List[List[Unit]]:
    process_units, units = _collect_processes(collection)
    if len(units):
        process_units = process_units + [units]
    return process_units


def _collect_processes(collection: Collection) -> Tuple[List[List[Unit]], List[Unit]]:
    process_units: List[List[Unit]] = []
    units: List[Unit] = []
    for comp in collection._components.values():
        if isinstance(comp, Collection):
            r_process_units, r_units = _collect_processes(comp)
            process_units = process_units + r_process_units
            if comp in collection.process_components():
                if len(r_units) > 0:
                    process_units = process_units + [r_units]
            else:
                if len(r_units) > 0:
                    units = units + r_units
        elif isinstance(comp, Unit):
            if comp in collection.process_components():
                process_units.append([comp])
            else:
                if hasattr(comp, PROCESS_ATTR):
                    process_units.append([comp])
                else:
                    units.append(comp)
    return process_units, units
