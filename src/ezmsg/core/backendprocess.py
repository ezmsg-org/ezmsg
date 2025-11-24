import asyncio
import concurrent.futures
import logging
import inspect
import time
import traceback
import threading

from abc import abstractmethod
from collections import defaultdict
from collections.abc import Callable, Coroutine, Generator, Sequence
from functools import wraps, partial
from copy import deepcopy
from multiprocessing import Process
from multiprocessing.synchronize import Event as EventType
from multiprocessing.synchronize import Barrier as BarrierType
from contextlib import suppress, contextmanager
from concurrent.futures import TimeoutError
from typing import Any

from .stream import Stream, InputStream, OutputStream
from .unit import Unit, TIMEIT_ATTR, SUBSCRIBES_ATTR, ZERO_COPY_ATTR

from .graphcontext import GraphContext
from .pubclient import Publisher
from .subclient import Subscriber
from .netprotocol import AddressType

logger = logging.getLogger("ezmsg")


class Complete(Exception):
    """
    A type of Exception raised by Unit methods, which signals to ezmsg that the
    function can be shut down gracefully.

    If all functions in all Units raise Complete, the entire pipeline will
    terminate execution. This exception is used to signal normal completion
    of processing tasks.

    .. note::
       This exception indicates successful completion, not an error condition.
    """

    pass


class NormalTermination(Exception):
    """
    A type of Exception which signals to ezmsg that the pipeline can be shut down gracefully.

    This exception is used to indicate that the system should terminate normally,
    typically when all processing is complete or when a graceful shutdown is requested.

    .. note::
       This exception indicates normal termination, not an error condition.
    """

    pass


class BackendProcess(Process):
    """
    Abstract base class for backend processes that execute Units.

    BackendProcess manages the execution of Units in a separate process,
    handling initialization, coordination with other processes via barriers,
    and cleanup operations.
    """

    units: list[Unit]
    term_ev: EventType
    start_barrier: BarrierType
    stop_barrier: BarrierType
    graph_address: AddressType | None

    def __init__(
        self,
        units: list[Unit],
        term_ev: EventType,
        start_barrier: BarrierType,
        stop_barrier: BarrierType,
        graph_address: AddressType | None,
    ) -> None:
        """
        Initialize the backend process.

        :param units: List of Units to execute in this process.
        :type units: list[Unit]
        :param term_ev: Event for coordinated termination.
        :type term_ev: EventType
        :param start_barrier: Barrier for synchronized startup.
        :type start_barrier: BarrierType
        :param stop_barrier: Barrier for synchronized shutdown.
        :type stop_barrier: BarrierType
        :param graph_service: Service for graph server communication.
        :type graph_service: GraphService
        :param shm_service: Service for shared memory management.
        :type shm_service: SHMService
        """
        super().__init__()
        self.units = units
        self.term_ev = term_ev
        self.start_barrier = start_barrier
        self.stop_barrier = stop_barrier
        self.graph_address = graph_address
        self.task_finished_ev: threading.Event | None = None

    def run(self) -> None:
        """
        Main entry point for the process execution.

        Sets up the event loop and handles the main processing logic
        with proper exception handling for interrupts.
        """
        self.task_finished_ev = threading.Event()
        with new_threaded_event_loop(self.task_finished_ev) as loop:
            try:
                self.process(loop)
            except KeyboardInterrupt:
                logger.debug("Process Interrupted.")

    @abstractmethod
    def process(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Abstract method for implementing the main processing logic.

        Subclasses must implement this method to define how Units
        are executed within the event loop.

        :param loop: The asyncio event loop for this process.
        :type loop: asyncio.AbstractEventLoop
        :raises NotImplementedError: Must be implemented by subclasses.
        """
        raise NotImplementedError


class DefaultBackendProcess(BackendProcess):
    """
    Default implementation of BackendProcess for executing Units.

    This class provides the standard execution model for ezmsg Units,
    handling publishers, subscribers, and the complete Unit lifecycle
    including initialization, execution, and shutdown.
    """

    pubs: dict[str, Publisher]

    def process(self, loop: asyncio.AbstractEventLoop) -> None:
        main_func = None
        context = GraphContext(self.graph_address)
        coro_callables: dict[str, Callable[[], Coroutine[Any, Any, None]]] = dict()

        try:
            self.pubs = dict()

            async def setup_state():
                for unit in self.units:
                    await unit.setup()

            asyncio.run_coroutine_threadsafe(setup_state(), loop).result()

            main_funcs = [
                (unit, unit.main) for unit in self.units if unit.main is not None
            ]

            if len(main_funcs) > 1:
                details = "".join(
                    [
                        f"\t* {unit.name}:{main_fn.__name__}\n"
                        for unit, main_fn in main_funcs
                    ]
                )
                suggestion = "Use a Collection and define process_components to separate these units."
                raise Exception(
                    "Process has more than one main-thread functions\n"
                    + details
                    + suggestion
                )

            elif len(main_funcs) == 1:
                main_func = main_funcs[0]
            else:
                main_func = None

            for unit in self.units:
                sub_callables: defaultdict[
                    str, set[Callable[..., Coroutine[Any, Any, None]]]
                ] = defaultdict(set)
                for task in unit.tasks.values():
                    task_callable = self.task_wrapper(unit, task)
                    if hasattr(task, SUBSCRIBES_ATTR):
                        sub_stream: Stream = getattr(task, SUBSCRIBES_ATTR)
                        sub_topic = unit.streams[sub_stream.name].address
                        sub_callables[sub_topic].add(task_callable)
                    else:
                        task_name = f"TASK|{unit.address}:{task.__name__}"
                        coro_callables[task_name] = task_callable

                for stream in unit.streams.values():
                    if isinstance(stream, InputStream):
                        logger.debug(f"Creating Subscriber from {stream}")
                        sub = asyncio.run_coroutine_threadsafe(
                            context.subscriber(stream.address), loop
                        ).result()
                        task_name = f"SUBSCRIBER|{stream.address}"
                        coro_callables[task_name] = partial(
                            handle_subscriber, sub, sub_callables[stream.address]
                        )

                    elif isinstance(stream, OutputStream):
                        logger.debug(f"Creating Publisher from {stream}")
                        self.pubs[stream.address] = asyncio.run_coroutine_threadsafe(
                            context.publisher(
                                stream.address,
                                host=stream.host,
                                port=stream.port,
                                num_buffers=stream.num_buffers,
                                buf_size=stream.buf_size,
                                start_paused=True,
                                force_tcp=stream.force_tcp,
                            ),
                            loop=loop,
                        ).result()
        except Exception:
            self.start_barrier.abort()
            logger.error(f"{traceback.format_exc()}")

        try:
            logger.debug("Waiting at start barrier!")
            self.start_barrier.wait()

            for unit in self.units:
                for thread_fn in unit.threads.values():
                    loop.run_in_executor(None, thread_fn, unit)

            for pub in self.pubs.values():
                pub.resume()

            async def coro_wrapper(coro):
                with suppress(Complete, NormalTermination, asyncio.CancelledError):
                    await coro

            complete_tasks = [
                asyncio.run_coroutine_threadsafe(coro_wrapper(coro()), loop)
                for coro in coro_callables.values()
            ]

            loop.run_in_executor(None, self.monitor_termination, complete_tasks, loop)

            if main_func is not None:
                unit, fn = main_func
                try:
                    fn(unit)
                except NormalTermination:
                    self.term_ev.set()
                except Exception:
                    logger.error(f"Exception in Main: {unit.address}")
                    logger.error(traceback.format_exc())

            while True:
                try:
                    _, not_done_set = concurrent.futures.wait(
                        complete_tasks, timeout=1.0
                    )
                    if len(not_done_set) == 0:
                        break
                except TimeoutError:
                    pass

        except threading.BrokenBarrierError:
            logger.info("Process exiting due to error on startup")

        finally:
            # This stop barrier prevents publishers/subscribers
            # from getting destroyed before all other processes have
            # drained communication channels
            logger.debug("Waiting at stop barrier")
            while True:
                try:
                    self.stop_barrier.wait()
                    break
                except KeyboardInterrupt:
                    ...

            self.term_ev.set()

            # concurrent.futures.wait(complete_tasks).result()

            # TODO: Currently, threads have no shutdown mechanism...
            # We should really change the call signature for @ez.thread
            # functions to receive the term_ev so that the user can
            # terminate the thread when shutdown occurs.
            # for thread in threads:
            #     thread.result()

            logger.debug("Shutting down Units")

            async def shutdown_units() -> None:
                for unit in self.units:
                    if inspect.iscoroutinefunction(unit.shutdown):
                        await unit.shutdown()
                    else:
                        unit.shutdown()  # type: ignore

            asyncio.run_coroutine_threadsafe(shutdown_units(), loop=loop).result()

            # for cache in MessageCache.values():
            #     cache.clear()

            asyncio.run_coroutine_threadsafe(context.revert(), loop=loop).result()

            logger.debug(f"Remaining tasks in event loop = {asyncio.all_tasks(loop)}")

            if self.task_finished_ev is not None:
                logger.debug("Setting task finished event")
                self.task_finished_ev.set()

        # logger.debug(
        #     f"Process Completed. All Done: {[task.get_name() for task in tasks]}"
        # )

    def monitor_termination(
        self,
        tasks: Sequence[concurrent.futures.Future[None]],
        loop: asyncio.AbstractEventLoop,
    ):
        self.term_ev.wait()
        logger.debug("Detected term_ev")
        for task in tasks:
            loop.call_soon_threadsafe(task.cancel)

    def task_wrapper(
        self, unit: Unit, task: Callable
    ) -> Callable[..., Coroutine[Any, Any, None]]:
        task_address = f"{unit.address}:{task.__name__}"

        async def publish(stream: Stream, obj: Any) -> None:
            if stream.address in self.pubs:
                await self.pubs[stream.address].broadcast(obj)
            await asyncio.sleep(0)

        async def perf_publish(stream: Stream, obj: Any) -> None:
            start = time.perf_counter()
            await publish(stream, obj)
            stop = time.perf_counter()
            logger.info(
                f"{task_address} send duration = " + f"{(stop - start) * 1e3:0.4f}ms"
            )

        pub_fn = perf_publish if hasattr(task, TIMEIT_ATTR) else publish

        signature = inspect.signature(task)
        if len(signature.parameters) == 1:

            def call_fn(_):  # type: ignore
                return task(unit)
        elif len(signature.parameters) == 2:

            def call_fn(msg):
                return task(unit, msg)
        else:
            logger.error(f"Incompatible call signature on task: {task.__name__}")

        @wraps(task)
        async def wrapped_task(msg: Any = None) -> None:
            try:
                result = call_fn(msg)
                if inspect.isasyncgen(result):
                    async for stream, obj in result:
                        if obj and getattr(task, ZERO_COPY_ATTR, False) and obj is msg:
                            obj = deepcopy(obj)
                        await pub_fn(stream, obj)

                elif asyncio.iscoroutine(result):
                    await result

            except Complete:
                logger.info(f"{task_address} Complete")
                raise

            except NormalTermination:
                logger.info(f"Normal Termination raised in {task_address}")
                self.term_ev.set()
                raise

            except Exception:
                logger.error(f"Exception in Task: {task_address}")
                logger.error(traceback.format_exc())

        return wrapped_task


async def handle_subscriber(
    sub: Subscriber, callables: set[Callable[..., Coroutine[Any, Any, None]]]
):
    """
    Handle incoming messages from a subscriber and distribute to callables.

    Continuously receives messages from the subscriber and calls all registered
    callables with each message. Removes callables that raise Complete or
    NormalTermination exceptions.

    :param sub: Subscriber to receive messages from.
    :type sub: Subscriber
    :param callables: Set of async callables to invoke with messages.
    :type callables: set[Callable[..., Coroutine[Any, Any, None]]]
    """
    while True:
        if not callables:
            sub.close()
            await sub.wait_closed()
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

        if len(callables) > 1:
            await asyncio.sleep(0)


def run_loop(loop: asyncio.AbstractEventLoop):
    """
    Run an asyncio event loop in the current thread.

    Sets the event loop for the current thread and runs it forever
    until interrupted or stopped.

    :param loop: The asyncio event loop to run.
    :type loop: asyncio.AbstractEventLoop
    """
    asyncio.set_event_loop(loop)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Stopping event loop.")


@contextmanager
def new_threaded_event_loop(
    ev: threading.Event | None = None,
) -> Generator[asyncio.AbstractEventLoop, None, None]:
    """
    Create a new asyncio event loop running in a separate thread.

    Provides a context manager that yields an event loop running in its own
    thread, allowing async operations to be run from synchronous code.

    :param ev: Optional event to signal when the loop is ready.
    :type ev: threading.Event | None
    :return: Context manager yielding the event loop.
    :rtype: Generator[asyncio.AbstractEventLoop, None, None]
    """
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=run_loop, name="TaskThread", args=(loop,))
    thread.start()

    try:
        yield loop

    finally:
        if ev is not None:
            logger.debug("Waiting at event...")
            # ev.wait()
        logger.debug("Stopping and closing task thread")
        loop.call_soon_threadsafe(loop.stop)
        thread.join()
        loop.close()
        logger.debug("Task thread stopped and closed")
