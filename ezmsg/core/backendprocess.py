import asyncio
import logging
import time
import traceback
import threading
import platform

from multiprocessing import Process
from multiprocessing.synchronize import Event as EventType
from multiprocessing.synchronize import Barrier as BarrierType
from contextlib import suppress

from .stream import Stream
from .unit import Unit, TIMEIT_ATTR, PUBLISHES_ATTR, SUBSCRIBES_ATTR, ZERO_COPY_ATTR

from .graphcontext import GraphContext
from .pubclient import Publisher
from .subclient import Subscriber
from .netprotocol import AddressType

from typing import List, Dict, Callable, Any, Tuple, Optional

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

    pubs: Dict[str, Dict[str, Publisher]]

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
            logger.debug(f'Keyboard Interrupt in PID {self.pid}')

        finally:

            task_thread.join()

            for unit in self.units:
                unit.shutdown()

    async def run_units(self) -> None:

        async with GraphContext(self.graph_address) as context:

            process_tasks: Dict[str, Tuple[Unit, Callable, Optional[Subscriber]]] = dict()

            for unit in self.units:

                self.pubs[unit.address] = dict()
                for task_name, task in unit.tasks.items():

                    sub: Optional[Subscriber] = None
                    if hasattr(task, SUBSCRIBES_ATTR):
                        sub_stream: Stream = getattr(task, SUBSCRIBES_ATTR)
                        sub = await context.subscriber(unit.streams[sub_stream.name].address)

                    if hasattr(task, PUBLISHES_ATTR):
                        pub_streams: List[Stream] = getattr(task, PUBLISHES_ATTR, [])
                        for stream in pub_streams:
                            global_topic = unit.streams[stream.name].address
                            pub = self.pubs[unit.address].get(global_topic, None)
                            pub = await context.publisher(global_topic, start_paused=True) if pub is None else pub
                            self.pubs[unit.address][global_topic] = pub

                    process_tasks[f'{unit.address}:{task_name}'] = (unit, task, sub)

            await asyncio.get_running_loop().run_in_executor(None, self.start_barrier.wait)

            tasks: List[asyncio.Task[None]] = [
                asyncio.create_task(
                    self.task_wrapper(*task_args),
                    name=f'{task_name}'
                )
                for task_name, task_args in process_tasks.items()
            ]

            monitor = asyncio.create_task(
                self.monitor_termination(tasks),
                name=f'pid_{self.pid}'
            )

            logger.info(f'Process {self.pid} kicking off tasks: {process_tasks.keys()}')

            try:
                for task in asyncio.as_completed(tasks):
                    # Should raise exceptions in user code
                    await task

            finally:
                monitor.cancel()
                with suppress(asyncio.CancelledError):
                    await monitor
        
            logger.info(f'Process {self.pid} waiting for all other processes')
            await asyncio.get_running_loop().run_in_executor( None, self.stop_barrier.wait )

        logger.info(f'Process {self.pid} Completed. All Done: {[task.get_name() for task in tasks]}')

    async def monitor_termination(self, tasks: List[asyncio.Task]):
        while True:
            # will never wake, even for cancellation!! ( :S -Griff )
            # await asyncio.get_running_loop().run_in_executor( None, self.term_ev.wait )
            await asyncio.sleep(0.5)
            if self.term_ev.is_set():
                logger.info(f'Process {self.pid} detected term_ev;')
                for task in tasks:
                    logger.info(f'Cancelling {task}')
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
                        logger.info(f'Cancelled {task}')

    async def task_wrapper(self, unit: Unit, task: Callable, sub: Optional[Subscriber] = None) -> None:

        task_address = task.__name__
        cur_task = asyncio.current_task()
        if cur_task is not None:
            task_address = cur_task.get_name()

        async def publish(stream: Stream, obj: Any):
            if stream.address in self.pubs[unit.address]:
                await self.pubs[unit.address][stream.address].broadcast(obj)

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
            elif sub is not None:

                async def handle_message(msg: Any) -> None:
                    if hasattr(task, PUBLISHES_ATTR):
                        async for stream, obj in task(unit, msg):
                            await pub_fn(stream, obj)
                    else:
                        await task(unit, msg)

                while True:
                    if getattr(task, ZERO_COPY_ATTR) == False:
                        msg = await sub.recv()
                        await handle_message(msg)
                    else:
                        async with sub.recv_zero_copy() as msg:
                            await handle_message(msg)
                        del msg

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

        finally:
            ...
