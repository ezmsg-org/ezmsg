import asyncio
import logging
import time
import traceback
import threading
import platform

from collections import defaultdict
from functools import wraps
from copy import deepcopy
from multiprocessing import Process
from multiprocessing.synchronize import Event as EventType
from multiprocessing.synchronize import Barrier as BarrierType
from contextlib import suppress

from .stream import Stream, InputStream, OutputStream
from .unit import Unit, TIMEIT_ATTR, PUBLISHES_ATTR, SUBSCRIBES_ATTR, ZERO_COPY_ATTR

from .graphcontext import GraphContext
from .pubclient import Publisher
from .subclient import Subscriber
from .netprotocol import AddressType

from typing import List, Dict, Callable, Any, Tuple, Optional, Set, Coroutine, DefaultDict

logger = logging.getLogger('ezmsg')


class Complete(Exception):
    pass


class NormalTermination(Exception):
    pass


class BackendProcess(Process):
    units: List[Unit]
    term_ev: EventType
    start_barrier: BarrierType
    stop_barrier: BarrierType
    graph_address: AddressType

    def __init__(self, graph_address: AddressType, units: List[Unit], term_ev: EventType, start_barrier: BarrierType, stop_barrier: BarrierType) -> None:
        super().__init__()
        self.units = units
        self.term_ev = term_ev
        self.start_barrier = start_barrier
        self.stop_barrier = stop_barrier

        self.graph_address = graph_address


class DefaultBackendProcess(BackendProcess):

    pubs: Dict[str, Publisher]

    def run(self) -> None:
        if platform.system() == 'Windows':
            asyncio.set_event_loop_policy(
                asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore
            )

        self.pubs = dict()

        # Set context for all components and initialize them
        main_func: Optional[Tuple[Unit, Callable]] = None
        threads: List[Tuple[Unit, Callable]] = []

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop( loop )

        for unit in self.units:
            unit.setup()

            for thread_fn in unit.threads.values():
                threads.append((unit, thread_fn))

            if unit.main is not None:
                if main_func is not None:
                    raise Exception("Process has more than one main-thread functions")
                else:
                    main_func = (unit, unit.main)

        run_task = loop.create_task(self.run_units(), name=f'run_units_{self.pid}')

        def run_thread() -> None:
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(run_task)

        task_thread = threading.Thread(target=run_thread, name="TaskThread")

        try:
            task_thread.start()

            if main_func is not None:
                unit, fn = main_func
                try:
                    fn(unit)
                except NormalTermination:
                    self.term_ev.set()

            task_thread.join()

        except KeyboardInterrupt:
            self.term_ev.set()

        finally:

            task_thread.join()

            for unit in self.units:
                unit.shutdown()

    async def run_units(self) -> None:

        async with GraphContext(self.graph_address) as context:

            async def handle_subscriber(sub: Subscriber, callables: Set[Callable[..., Coroutine[Any, Any, None]]]):
                while True:
                    if not callables:
                        break
                    async with sub.recv_zero_copy() as msg:
                        try:
                            for callable in list(callables):
                                try:
                                    await callable(msg)
                                except (Complete, NormalTermination):
                                    callables.remove(callable)
                        finally:
                            del msg

            coros: Dict[str, Coroutine[Any, Any, None]] = dict()

            for unit in self.units:

                sub_callables: DefaultDict[str, Set[Callable[..., Coroutine[Any, Any, None]]]] = defaultdict(set)
                for task in unit.tasks.values():
                    task_callable = self.task_wrapper(unit, task)
                    if hasattr(task, SUBSCRIBES_ATTR):
                        sub_stream: Stream = getattr(task, SUBSCRIBES_ATTR)
                        sub_topic = unit.streams[sub_stream.name].address
                        sub_callables[sub_topic].add(task_callable)
                    else:
                        task_name = f'TASK|{unit.address}:{task.__name__}'
                        coros[task_name]=task_callable()

                for stream in unit.streams.values():

                    if isinstance(stream, InputStream):
                        logger.debug(f'Creating Subscriber from {stream}')
                        sub = await context.subscriber(stream.address)
                        task_name = f'SUBSCRIBER|{stream.address}'
                        coros[task_name] = handle_subscriber(sub, sub_callables[stream.address])

                    elif isinstance(stream, OutputStream):
                        logger.debug(f'Creating Publisher from {stream}')
                        self.pubs[stream.address] = await context.publisher(
                            stream.address,
                            host = stream.host,
                            port = stream.port,
                            num_buffers = stream.num_buffers,
                            buf_size = stream.buf_size,
                            start_paused = True, 
                            force_tcp = stream.force_tcp
                        )

            await asyncio.get_running_loop().run_in_executor(None, self.start_barrier.wait)

            for pub in self.pubs.values():
                pub.resume()

            tasks = [asyncio.create_task(coro, name = name) for name, coro in coros.items()]

            loop = asyncio.get_running_loop()
            monitor = loop.run_in_executor(None, self.monitor_termination, tasks, loop)

            try:
                for task in asyncio.as_completed(tasks):
                    with suppress(Complete, NormalTermination, asyncio.CancelledError):
                        await task

            finally:
            
                # This stop barrier prevents publishers/subscribers
                # from getting destroyed before all other processes have 
                # drained communication channels
                logger.debug(f'Waiting at stop barrier')
                await asyncio.get_running_loop().run_in_executor(None, self.stop_barrier.wait)

                logger.debug(f'Terminating monitor')
                self.term_ev.set()
                await monitor # should cancel all tasks

                for task in tasks:
                    with suppress(Complete, NormalTermination, asyncio.CancelledError):
                        await task

        logger.debug(f'Process Completed. All Done: {[task.get_name() for task in tasks]}')

    def monitor_termination(self, tasks: List[asyncio.Task], loop: asyncio.AbstractEventLoop):
        self.term_ev.wait()
        logger.debug(f'Detected term_ev')
        for task in tasks:
            logger.debug(f'Cancelling {task.get_name()}')
            loop.call_soon_threadsafe(task.cancel)

    def task_wrapper(self, unit: Unit, task: Callable) -> Callable[..., Coroutine[Any, Any, None]]:

        task_address = f'{unit.address}:{task.__name__}'

        async def publish(stream: Stream, obj: Any) -> None:
            if stream.address in self.pubs:
                await self.pubs[stream.address].broadcast(obj)

        async def perf_publish(stream: Stream, obj: Any) -> None:
            start = time.perf_counter()
            await publish(stream, obj)
            stop = time.perf_counter()
            logger.info(f"{task_address} send duration = " + f"{(stop-start)*1e3:0.4f}ms")

        pub_fn = perf_publish if hasattr(task, TIMEIT_ATTR) else publish

        @wraps(task)
        async def wrapped_task(msg: Any = None) -> None:

            try:
                # If we don't sub or pub anything, we are a simple task
                if (not hasattr(task, SUBSCRIBES_ATTR) and not hasattr(task, PUBLISHES_ATTR)):
                    await task(unit)

                # No subscriptions; only publications...
                elif not hasattr(task, SUBSCRIBES_ATTR):
                    async for stream, obj in task(unit):
                        await pub_fn(stream, obj)

                # Subscribers need to be called with a message
                else:
                    if not getattr(task, ZERO_COPY_ATTR): 
                        msg = deepcopy(msg)
                    if hasattr(task, PUBLISHES_ATTR):
                        async for stream, obj in task(unit, msg):
                            if getattr(task, ZERO_COPY_ATTR) and obj is msg:
                                obj = deepcopy(obj)
                            await pub_fn(stream, obj)
                    else:
                        await task(unit, msg)

            except Complete:
                logger.info(f'{task_address} Complete')
                raise

            except NormalTermination:
                logger.info(f'Normal Termination raised in {task_address}')
                self.term_ev.set()
                raise

            except Exception as e:
                logger.error(f'Exception in Task: {task_address}')
                logger.error(traceback.format_exc())
                raise

        return wrapped_task