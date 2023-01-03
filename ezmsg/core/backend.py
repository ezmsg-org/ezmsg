import os
import asyncio
import logging
import platform
import signal

from threading import BrokenBarrierError
from multiprocessing import Event, Barrier
from multiprocessing.synchronize import Event as EventType
from dataclasses import dataclass, field

from .netprotocol import DEFAULT_SHM_SIZE, AddressType, GRAPHSERVER_ADDR

from .collection import Collection, NetworkDefinition
from .component import Component
from .stream import Stream
from .unit import Unit, PROCESS_ATTR

from .graphcontext import GraphContext
from .backendprocess import BackendProcess, DefaultBackendProcess

from types import FrameType
from typing import List, Callable, Tuple, Optional, Type

logger = logging.getLogger( 'ezmsg' )

@dataclass
class RunContext:
    process_units: List[List[Unit]]
    graph_connections: List[Tuple[str, str]]
    term_ev: EventType = field(default_factory=Event)
    go_ev: EventType = field(default_factory=Event)


def run_system(
    system: Collection,
    num_buffers: int = 32,
    init_buf_size: int = DEFAULT_SHM_SIZE,
    backend_process: Type[BackendProcess] = DefaultBackendProcess
) -> None:
    """ Deprecated; just use run any component (unit, collection) """
    run(system, backend_process=backend_process)


def run(
    component: Component,
    name: Optional[str] = None,
    connections: Optional[NetworkDefinition] = None,
    backend_process: Type[BackendProcess] = DefaultBackendProcess,
    graph_address: AddressType = GRAPHSERVER_ADDR
) -> None:

    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(
            asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore
        )

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

    def crawl_components(component: Component, callback: Callable[[Component], None]) -> None:
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

    term_ev = Event()
    start_barrier = Barrier(len(processes) + 1)
    stop_barrier = Barrier(len(processes))

    backend_processes = [
        backend_process(graph_address, process_units, term_ev, start_barrier, stop_barrier)
        for process_units in processes
    ]

    loop = asyncio.new_event_loop()

    async def main_process() -> None:

        async with GraphContext(graph_address) as context:
            try:
                await context.sync(timeout=5.0)
            except asyncio.exceptions.TimeoutError:
                raise Exception('Could not synchronize graph (is there a zombie graph running?)')

            for edge in graph_connections:
                await context.connect(*edge)

            def wait_to_start() -> None:
                start_barrier.wait()
                context.resume()

            if len(backend_processes) == 1:
                logger.info('Running in single-process mode')

                process_callable = backend_processes[0]
                loop.run_in_executor(None, wait_to_start)
                process_callable.run()

            else:
                logger.info(f'Running {len(backend_processes)} processes.')
                for proc in backend_processes:
                    proc.start()

                def graceful_shutdown(signum: int, frame: Optional[FrameType]) -> None:
                    if not term_ev.is_set():
                        logger.info('Attempting graceful shutdown, interrupt again to force quit...')
                        term_ev.set()
                    else:
                        logger.warning('Interrupt intercepted, force quitting')
                        start_barrier.abort()
                        stop_barrier.abort()
                        for proc in backend_processes:
                            proc.terminate()

                signal.signal(signal.SIGINT, graceful_shutdown)

                try:
                    loop.run_in_executor(None, wait_to_start)
                    for proc in backend_processes:
                        await loop.run_in_executor(None, proc.join)

                except BrokenBarrierError:
                    logger.error('Could not initialize system, exiting.')
                    return

                finally:
                    for proc in backend_processes:
                        if proc.is_alive():
                            logger.warning(f'Process {proc.pid} did not complete; terminating.')
                            proc.terminate()

    main_task = loop.create_task(main_process())

    loop.run_until_complete(main_task)


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
