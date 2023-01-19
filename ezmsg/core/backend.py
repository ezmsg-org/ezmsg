import asyncio
import logging
import platform

from socket import socket
from threading import BrokenBarrierError
from multiprocessing import Event, Barrier
from multiprocessing.synchronize import Event as EventType
from multiprocessing.connection import wait, Connection

from dataclasses import dataclass, field

from .netprotocol import DEFAULT_SHM_SIZE, AddressType, GRAPHSERVER_ADDR

from .collection import Collection, NetworkDefinition
from .component import Component
from .stream import Stream
from .unit import Unit, PROCESS_ATTR

from .graphcontext import GraphContext
from .backendprocess import BackendProcess, DefaultBackendProcess

from typing import List, Callable, Tuple, Optional, Type, Set, Union

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

    if len(processes) == 0:
        logger.info('No processes to run.')
        return

    term_ev = Event()
    start_barrier = Barrier(len(processes))
    stop_barrier = Barrier(len(processes))

    backend_processes = [
        backend_process(graph_address, process_units, term_ev, start_barrier, stop_barrier)
        for process_units in processes
    ]

    loop = asyncio.new_event_loop()

    async def main_process() -> None:

        async with GraphContext(graph_address) as context:

            for edge in graph_connections:
                await context.connect(*edge)

            if len(backend_processes) == 1:
                logger.info('Running in single-process mode')

                try:
                    backend_processes[0].run()
                except BrokenBarrierError:
                    logger.error('Could not initialize system, exiting.')
                except KeyboardInterrupt:
                    logger.info('Interrupt detected, shutting down')
                finally:
                    ...

            else:
                logger.info(f'Running {len(backend_processes)} processes.')
                for proc in backend_processes:
                    proc.start()

                sentinels: Set[Union[Connection, socket, int]] = \
                    set([proc.sentinel for proc in backend_processes])

                async def join_all():
                    while len(sentinels):
                        # FIXME?: `wait` should be called from an executor so as to
                        # not block the loop, but strangely this seems to raise a 
                        # KeyboardInterrupt that I cannot catch from this scope
                        # done = await loop.run_in_executor(None, wait, sentinels)
                        done = wait(sentinels)
                        for sentinel in done:
                            sentinels.discard(sentinel)

                try:
                    await join_all()
                    logger.info('All processes exited normally')

                except KeyboardInterrupt:
                    # At this point it is assumed that KeyboardInterrupt was forwarded
                    # on to all subprocesses.  Every subprocess should catch/handle this
                    # KeyboardInterrupt and they will ALL set the term_ev individually
                    # around the same time.  I hope this isn't an issue.
                    logger.info('Attempting graceful shutdown, interrupt again to force quit...')

                    term_ev.set() # FIXME?: This line is only necessary for windows... ?!

                    try:
                        await join_all()

                    except KeyboardInterrupt:
                        logger.warning('Interrupt intercepted, force quitting')
                        start_barrier.abort()
                        stop_barrier.abort()
                        for proc in backend_processes:
                            proc.terminate()

                except BrokenBarrierError:
                    logger.error('Could not initialize system, exiting.')

                finally:
                    await join_all()

    try:
        main_task = loop.create_task(main_process())
        loop.run_until_complete(main_task)
    finally:
        loop.close()


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
