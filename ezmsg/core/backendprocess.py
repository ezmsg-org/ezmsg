import asyncio
import logging
import time
import traceback
import threading
import platform

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

from typing import List, Dict, Callable, Any, Tuple, Optional, Set

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
    sub_queues: Dict[str, Set[asyncio.Queue]]
    sub_tasks: Set["asyncio.Task[None]"]

    def run(self) -> None:
        if platform.system() == 'Windows':
            asyncio.set_event_loop_policy(
                asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore
            )

        self.pubs = dict()
        self.sub_queues = dict()
        self.sub_tasks = set()

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
            logger.debug(f'Keyboard Interrupt')
            self.term_ev.set()

        finally:

            task_thread.join()

            for unit in self.units:
                unit.shutdown()

    async def run_units(self) -> None:

        async with GraphContext(self.graph_address) as context:

            async def handle_subscriber(sub: Subscriber, queues: Set[asyncio.Queue]):
                while True:
                    async with sub.recv_zero_copy() as msg:
                        for queue in queues:
                            queue.put_nowait(msg)
                        await asyncio.gather(*[q.join() for q in queues])

            process_tasks: Dict[str, Tuple[Unit, Callable, Optional[Tuple[str, asyncio.Queue]]]] = dict()

            for unit in self.units:

                for stream in unit.streams.values():
                    if isinstance(stream, InputStream):
                        sub = await context.subscriber(stream.address)
                        self.sub_queues[stream.address] = set()
                        handler = handle_subscriber(sub, self.sub_queues[stream.address])
                        self.sub_tasks.add(asyncio.create_task(handler))
                    elif isinstance(stream, OutputStream):
                        self.pubs[stream.address] = await context.publisher(stream.address)

                for task_name, task in unit.tasks.items():
                    sub_info: Optional[Tuple[str, asyncio.Queue]] = None
                    if hasattr(task, SUBSCRIBES_ATTR):
                        sub_stream: Stream = getattr(task, SUBSCRIBES_ATTR)
                        sub_topic = unit.streams[sub_stream.name].address
                        sub_queue = asyncio.Queue()
                        self.sub_queues[sub_topic].add(sub_queue)
                        sub_info = (sub_topic, sub_queue)
                    process_tasks[f'{unit.address}:{task_name}'] = (unit, task, sub_info)

            await asyncio.get_running_loop().run_in_executor(None, self.start_barrier.wait)

            tasks: List[asyncio.Task[None]] = []
            for task_name, (unit, task, sub_info) in process_tasks.items():
                task_coro = self.task_wrapper(unit, task, sub_info)
                task_obj = asyncio.create_task(task_coro, name = task_name)
                tasks.append(task_obj)

            monitor = asyncio.create_task(
                self.monitor_termination(tasks),
                name=f'pid_{self.pid}'
            )

            logger.debug(f'Starting tasks: {process_tasks.keys()}')

            try:
                for task in asyncio.as_completed(tasks):
                    # Should raise exceptions in user code
                    await task

            finally:
                with suppress(asyncio.CancelledError):
                    monitor.cancel()
                    await monitor

                for task in self.sub_tasks:
                    with suppress(asyncio.CancelledError):
                        task.cancel()
                        await task
        
                # This stop barrier prevents publishers/subscribers
                # from getting destroyed before all other processes have 
                # drained communication channels
                logger.debug(f'Waiting at stop barrier')
                await asyncio.get_running_loop().run_in_executor( None, self.stop_barrier.wait )

        logger.debug(f'Completed. All Done: {[task.get_name() for task in tasks]}')

    async def monitor_termination(self, tasks: List[asyncio.Task]):
        while True:
            # will never wake, even for cancellation!! ( :S -Griff )
            # await asyncio.get_running_loop().run_in_executor( None, self.term_ev.wait )
            await asyncio.sleep(0.5)
            if self.term_ev.is_set():
                logger.debug(f'Detected term_ev')
                for task in tasks:
                    logger.debug(f'Cancelling {task.get_name()}')
                    task.cancel()

    async def task_wrapper(self, unit: Unit, task: Callable, sub_info: Optional[Tuple[str, asyncio.Queue]] = None) -> None:

        task_address = task.__name__
        cur_task = asyncio.current_task()
        if cur_task is not None:
            task_address = cur_task.get_name()

        async def publish(stream: Stream, obj: Any):
            if stream.address in self.pubs:
                await self.pubs[stream.address].broadcast(obj)

        async def perf_publish(stream: Stream, obj: Any):
            start = time.perf_counter()
            await publish(stream, obj)
            stop = time.perf_counter()
            logger.info(f"{task_address} send duration = " + f"{(stop-start)*1e3:0.4f}ms")

        pub_fn = perf_publish if hasattr(task, TIMEIT_ATTR) else publish

        try:
            # If we don't sub or pub anything, we are a simple task
            if (not hasattr(task, SUBSCRIBES_ATTR) and not hasattr(task, PUBLISHES_ATTR)):
                await task(unit)

            # If we sub we need to be wrapped in a message loop
            elif sub_info is not None:

                sub_topic, sub_queue = sub_info

                async def handle_message(msg: Any) -> None:
                    if hasattr(task, PUBLISHES_ATTR):
                        async for stream, obj in task(unit, msg):
                            await pub_fn(stream, obj)
                    else:
                        await task(unit, msg)

                while True:
                    msg = await sub_queue.get()
                    if getattr(task, ZERO_COPY_ATTR) == False:
                        msg = deepcopy(msg)
                    await handle_message(msg)
                    sub_queue.task_done()

            else:  # No subscriptions; only publications...
                async for stream, obj in task(unit):
                    await pub_fn(stream, obj)

        except Complete:
            logger.info(f'{task_address} Complete')

        except NormalTermination:
            logging.getLogger(__name__).info(
                f'Normal Termination raised in {task_address}'
            )
            self.term_ev.set()

        except Exception as e:
            logger.error(f'Exception in Task: {task_address}')
            logger.error(traceback.format_exc())
            raise

        finally:
            if sub_info is not None:
                sub_topic, sub_queue = sub_info
                self.sub_queues[sub_topic].remove(sub_queue)
            logger.debug(f'Task Complete: {task_address}')
