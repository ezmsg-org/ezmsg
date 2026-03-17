import asyncio
import concurrent.futures
import logging
import inspect
import os
import pickle
import traceback
import threading
import weakref
from copy import deepcopy

from abc import abstractmethod
from dataclasses import dataclass, fields as dataclass_fields, is_dataclass, replace
from collections import defaultdict
from collections.abc import Awaitable, Callable, Coroutine, Generator, Mapping, Sequence
from functools import wraps, partial
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.thread import _worker
from multiprocessing import Process
from multiprocessing.synchronize import Event as EventType
from multiprocessing.synchronize import Barrier as BarrierType
from contextlib import suppress, contextmanager
from concurrent.futures import TimeoutError
from typing import Any

from .stream import Stream, InputStream, OutputStream
from .unit import Unit, TIMEIT_ATTR, SUBSCRIBES_ATTR

from .graphcontext import GraphContext
from .graphmeta import (
    ProcessControlErrorCode,
    ProcessControlOperation,
    ProcessControlRequest,
    ProcessControlResponse,
    SettingsFieldUpdateRequest,
    SettingsSnapshotValue,
)
from .profiling import PROFILES, PROFILE_TIME
from .processclient import ProcessControlClient
from .pubclient import Publisher
from .subclient import Subscriber
from .netprotocol import AddressType
from .settingsmeta import (
    settings_repr_value,
    settings_schema_from_value,
    settings_structured_value,
)

logger = logging.getLogger("ezmsg")

STRICT_SHUTDOWN_ENV = "EZMSG_STRICT_SHUTDOWN"


def _strict_shutdown_enabled() -> bool:
    value = os.environ.get(STRICT_SHUTDOWN_ENV, "")
    return value.lower() in ("1", "true", "yes", "on")


@dataclass
class ShutdownSummary:
    cancelled_tasks: int = 0
    executor_active: int = 0
    pending_tasks: int = 0
    suppressed_errors: int = 0
    forced_interrupt: bool = False

    @property
    def unclean(self) -> bool:
        return bool(
            self.executor_active
            or self.pending_tasks
            or self.suppressed_errors
            or self.forced_interrupt
        )


class _DaemonThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._active_count = 0
        self._active_lock = threading.Lock()

    def _adjust_thread_count(self) -> None:
        if self._broken:
            return
        num_threads = len(self._threads)
        if num_threads >= self._max_workers:
            return
        thread_name = f"{self._thread_name_prefix or 'ThreadPool'}_{num_threads}"
        thread = threading.Thread(
            name=thread_name,
            target=_worker,
            args=(
                weakref.ref(self),
                self._work_queue,
                self._initializer,
                self._initargs,
            ),
        )
        thread.daemon = True
        thread.start()
        self._threads.add(thread)

    def submit(self, fn, /, *args, **kwargs):
        fut = super().submit(fn, *args, **kwargs)
        with self._active_lock:
            self._active_count += 1

        def _decrement(_):
            with self._active_lock:
                self._active_count -= 1

        fut.add_done_callback(_decrement)
        return fut

    def active_count(self) -> int:
        with self._active_lock:
            return self._active_count


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
    _shutdown_errors: bool

    def _settings_snapshot_value(self, value: object) -> SettingsSnapshotValue:
        try:
            serialized = pickle.dumps(value)
        except Exception:
            serialized = None

        return SettingsSnapshotValue(
            serialized=serialized,
            repr_value=settings_repr_value(value),
            structured_value=settings_structured_value(value),
            settings_schema=settings_schema_from_value(value),
        )

    def _replace_settings_field(
        self, settings_value: object, field_path: str, value: object
    ) -> object:
        if field_path == "":
            raise ValueError("field_path must not be empty")
        path = field_path.split(".")

        def apply(current: object, idx: int) -> object:
            field_name = path[idx]
            if isinstance(current, Mapping):
                if field_name not in current:
                    raise AttributeError(
                        f"Settings field '{field_name}' does not exist in mapping"
                    )
                if idx == len(path) - 1:
                    updated = dict(current)
                    updated[field_name] = value
                    return updated
                patched_child = apply(current[field_name], idx + 1)
                updated = dict(current)
                updated[field_name] = patched_child
                return updated

            if not hasattr(current, field_name):
                raise AttributeError(
                    f"Settings field '{field_name}' does not exist on "
                    f"{type(current).__name__}"
                )

            if idx == len(path) - 1:
                return self._patch_object_field(current, field_name, value)

            child_value = getattr(current, field_name)
            patched_child = apply(child_value, idx + 1)
            return self._patch_object_field(current, field_name, patched_child)

        return apply(settings_value, 0)

    def _patch_object_field(
        self, obj: object, field_name: str, value: object
    ) -> object:
        if is_dataclass(obj):
            valid_fields = {f.name for f in dataclass_fields(obj)}
            if field_name not in valid_fields:
                raise AttributeError(
                    f"Settings field '{field_name}' does not exist on "
                    f"{type(obj).__name__}"
                )
            return replace(obj, **{field_name: value})

        if hasattr(obj, "model_copy") and callable(getattr(obj, "model_copy")):
            return obj.model_copy(update={field_name: value})  # type: ignore[attr-defined]

        if hasattr(obj, "copy") and callable(getattr(obj, "copy")):
            try:
                return obj.copy(update={field_name: value})  # type: ignore[attr-defined]
            except Exception:
                pass

        if hasattr(obj, field_name):
            patched = deepcopy(obj)
            setattr(patched, field_name, value)
            return patched

        raise TypeError(f"Cannot patch settings object of type {type(obj).__name__}")

    def process(self, loop: asyncio.AbstractEventLoop) -> None:
        main_func = None
        context = GraphContext(self.graph_address)
        process_client = ProcessControlClient(self.graph_address)
        process_register_future: concurrent.futures.Future[None] | None = None
        coro_callables: dict[str, Callable[[], Coroutine[Any, Any, None]]] = dict()
        settings_input_topics: dict[str, str] = {}
        current_settings: dict[str, object] = {}
        control_publishers: dict[str, Publisher] = {}
        self._shutdown_errors = False

        async def process_request_handler(
            request: ProcessControlRequest,
        ) -> ProcessControlResponse:
            if request.operation != ProcessControlOperation.UPDATE_SETTING_FIELD.value:
                return ProcessControlResponse(
                    request_id=request.request_id,
                    ok=False,
                    error=f"Unsupported process control operation: {request.operation}",
                    error_code=ProcessControlErrorCode.UNSUPPORTED_OPERATION,
                    process_id=process_client.process_id,
                )

            if request.payload is None:
                return ProcessControlResponse(
                    request_id=request.request_id,
                    ok=False,
                    error="Missing settings field update payload",
                    error_code=ProcessControlErrorCode.INVALID_RESPONSE,
                    process_id=process_client.process_id,
                )

            try:
                update_obj = pickle.loads(request.payload)
                if not isinstance(update_obj, SettingsFieldUpdateRequest):
                    raise RuntimeError(
                        "settings field update payload was not SettingsFieldUpdateRequest"
                    )
            except Exception as exc:
                return ProcessControlResponse(
                    request_id=request.request_id,
                    ok=False,
                    error=f"Invalid settings field update payload: {exc}",
                    error_code=ProcessControlErrorCode.INVALID_RESPONSE,
                    process_id=process_client.process_id,
                )

            unit_address = request.unit_address
            input_topic = settings_input_topics.get(unit_address)
            if input_topic is None:
                return ProcessControlResponse(
                    request_id=request.request_id,
                    ok=False,
                    error=(
                        f"Unit '{unit_address}' does not expose INPUT_SETTINGS; "
                        "settings field update unsupported"
                    ),
                    error_code=ProcessControlErrorCode.UNSUPPORTED_OPERATION,
                    process_id=process_client.process_id,
                )

            if unit_address not in current_settings:
                return ProcessControlResponse(
                    request_id=request.request_id,
                    ok=False,
                    error=(
                        f"No current settings value tracked for unit '{unit_address}'. "
                        "Send a full settings object first via update_settings()."
                    ),
                    error_code=ProcessControlErrorCode.HANDLER_ERROR,
                    process_id=process_client.process_id,
                )

            try:
                patched = self._replace_settings_field(
                    current_settings[unit_address],
                    update_obj.field_path,
                    update_obj.value,
                )
                control_pub = control_publishers.get(input_topic)
                if control_pub is None:
                    control_pub = await context.publisher(input_topic)
                    control_publishers[input_topic] = control_pub
                await control_pub.broadcast(patched)
                current_settings[unit_address] = patched
            except Exception as exc:
                return ProcessControlResponse(
                    request_id=request.request_id,
                    ok=False,
                    error=f"Failed to patch settings field: {exc}",
                    error_code=ProcessControlErrorCode.HANDLER_ERROR,
                    process_id=process_client.process_id,
                )

            result_value = self._settings_snapshot_value(patched)
            return ProcessControlResponse(
                request_id=request.request_id,
                ok=True,
                payload=pickle.dumps(result_value),
                process_id=process_client.process_id,
            )

        process_client.set_request_handler(process_request_handler)

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
                if unit.SETTINGS is not None:
                    current_settings[unit.address] = unit.SETTINGS
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
                            context.subscriber(
                                stream.address,
                                leaky=stream.leaky,
                                max_queue=stream.max_queue,
                            ),
                            loop,
                        ).result()
                        task_name = f"SUBSCRIBER|{stream.address}"
                        report_settings_update: (
                            Callable[[object], Awaitable[None]] | None
                        ) = None
                        if stream.name == "INPUT_SETTINGS":
                            component_address = unit.address
                            settings_input_topics[component_address] = stream.address

                            async def report_settings_update_cb(
                                msg: object,
                                *,
                                _component_address: str = component_address,
                            ) -> None:
                                current_settings[_component_address] = msg
                                value = self._settings_snapshot_value(msg)
                                await process_client.report_settings_update(
                                    component_address=_component_address,
                                    value=value,
                                )

                            report_settings_update = report_settings_update_cb

                        coro_callables[task_name] = partial(
                            handle_subscriber,
                            sub,
                            sub_callables[stream.address],
                            on_message=report_settings_update,
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

        except asyncio.CancelledError:
            pass

        except Exception:
            self.start_barrier.abort()
            # logger.error(f"{traceback.format_exc()}")
            raise

        try:
            logger.debug("Waiting at start barrier!")
            self.start_barrier.wait()

            async def register_process_control() -> None:
                try:
                    await process_client.register([unit.address for unit in self.units])
                except Exception as exc:
                    logger.warning(f"Process control registration failed: {exc}")

            process_register_future = asyncio.run_coroutine_threadsafe(
                register_process_control(),
                loop,
            )

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
                # except Exception:
                #     logger.error(f"Exception in Main: {unit.address}")
                #     logger.error(traceback.format_exc())

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

            shutdown_future = asyncio.run_coroutine_threadsafe(shutdown_units(), loop=loop)
            try:
                shutdown_future.result()
            except KeyboardInterrupt:
                logger.warning("Interrupted during unit shutdown. This may indicate units with slow shutdown methods."
                               "Re-trying... Press ctrl-c again to terminate immediately.")
                shutdown_future.result()

            revert_future = asyncio.run_coroutine_threadsafe(context.revert(), loop=loop)
            try:
                revert_future.result()
            except KeyboardInterrupt:
                logger.warning("Interrupted during context revert."
                               "Re-trying... (will timeout in 10 seconds)")
                try:
                    revert_future.result(timeout=10.0)
                except TimeoutError:
                    logger.warning("Timed out waiting for retry on context revert")

            process_close_future = asyncio.run_coroutine_threadsafe(
                process_client.close(),
                loop=loop,
            )
            with suppress(Exception):
                if process_register_future is not None:
                    process_register_future.result(timeout=0.5)
                process_close_future.result()

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
        strict_shutdown = _strict_shutdown_enabled()

        async def publish(stream: Stream, obj: Any) -> None:
            if stream.address in self.pubs:
                await self.pubs[stream.address].broadcast(obj)
            await asyncio.sleep(0)

        async def perf_publish(stream: Stream, obj: Any) -> None:
            start = PROFILE_TIME()
            await publish(stream, obj)
            stop = PROFILE_TIME()
            logger.info(
                f"{task_address} send duration = "
                f"{((stop - start) / 1_000_000.0):0.4f}ms"
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

            except asyncio.CancelledError:
                # Normal during shutdown; propagate without logging.
                raise

            except Exception:
                logger.error(f"Exception in Task: {task_address}")
                logger.error(traceback.format_exc())
                # Any task exception should mark shutdown as unclean so
                # interrupt-driven teardown can return a non-zero exit code.
                # Gating this on term_ev introduces timing-dependent behavior.
                self._shutdown_errors = True
                if strict_shutdown:
                    raise

        return wrapped_task


async def handle_subscriber(
    sub: Subscriber,
    callables: set[Callable[..., Coroutine[Any, Any, None]]],
    on_message: Callable[[Any], Awaitable[None]] | None = None,
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
    # Leaky subscribers use recv() to copy and release backpressure immediately,
    # allowing publishers to continue without blocking during slow processing.
    # Non-leaky subscribers use recv_zero_copy() to hold backpressure during
    # processing, which provides zero-copy performance but applies backpressure.

    while True:
        if not callables:
            sub.close()
            await sub.wait_closed()
            break

        if sub.leaky:
            msg = await sub.recv()
            try:
                if on_message is not None:
                    try:
                        await on_message(msg)
                    except Exception as exc:
                        logger.warning(
                            f"Failed to report subscriber message metadata: {exc}"
                        )
                for callable in list(callables):
                    try:
                        span_start_ns = sub.begin_profile()
                        try:
                            await callable(msg)
                        finally:
                            sub.end_profile(
                                span_start_ns, getattr(callable, "__name__", None)
                            )
                    except (Complete, NormalTermination):
                        callables.remove(callable)
            finally:
                del msg
        else:
            async with sub.recv_zero_copy() as msg:
                try:
                    if on_message is not None:
                        try:
                            await on_message(msg)
                        except Exception as exc:
                            logger.warning(
                                f"Failed to report subscriber message metadata: {exc}"
                            )
                    for callable in list(callables):
                        try:
                            span_start_ns = sub.begin_profile()
                            try:
                                await callable(msg)
                            finally:
                                sub.end_profile(
                                    span_start_ns, getattr(callable, "__name__", None)
                                )
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
    shutdown_summary: ShutdownSummary | None = None,
) -> Generator[asyncio.AbstractEventLoop, None, None]:
    """
    Create a new asyncio event loop running in a separate thread.

    Provides a context manager that yields an event loop running in its own
    thread, allowing async operations to be run from synchronous code.

    :param ev: Optional event to signal when the loop is ready.
    :type ev: threading.Event | None
    :param shutdown_summary: Optional shutdown summary object to populate.
    :type shutdown_summary: ShutdownSummary | None
    :return: Context manager yielding the event loop.
    :rtype: Generator[asyncio.AbstractEventLoop, None, None]
    """
    loop = asyncio.new_event_loop()
    strict_shutdown = _strict_shutdown_enabled()
    shutdown_suppress = threading.Event()
    suppressed_shutdown_errors = {"count": 0}
    suppressed_lock = threading.Lock()
    executor = None
    if not strict_shutdown:
        executor = _DaemonThreadPoolExecutor(thread_name_prefix="EZMSG")
        loop.set_default_executor(executor)
        def _loop_exception_handler(
            loop_obj: asyncio.AbstractEventLoop, context: dict
        ) -> None:
            if shutdown_suppress.is_set():
                with suppressed_lock:
                    suppressed_shutdown_errors["count"] += 1
                return
            loop_obj.default_exception_handler(context)

        loop.set_exception_handler(_loop_exception_handler)
    thread = threading.Thread(target=run_loop, name="TaskThread", args=(loop,))
    thread.start()

    try:
        yield loop

    finally:
        if ev is not None:
            logger.debug("Waiting at event...")
            # ev.wait()
        logger.debug("Stopping and closing task thread")

        if not strict_shutdown:
            shutdown_suppress.set()
            # Cancel and await remaining tasks before stopping the loop.
            async def _cancel_remaining(timeout: float = 1.0) -> tuple[int, int]:
                tasks = [
                    t
                    for t in asyncio.all_tasks()
                    if t is not asyncio.current_task() and not t.done()
                ]
                for t in tasks:
                    t.cancel()
                if not tasks:
                    return 0, 0
                _, pending = await asyncio.wait(tasks, timeout=timeout)
                return len(tasks), len(pending)

            cancelled_count = 0
            pending_count = 0
            forced_interrupt = False
            fut = asyncio.run_coroutine_threadsafe(_cancel_remaining(), loop)
            try:
                cancelled_count, pending_count = fut.result()
            except KeyboardInterrupt:
                forced_interrupt = True
                fut.cancel()
            except Exception:
                cancelled_count = 0
                pending_count = 0

            suppressed_count = suppressed_shutdown_errors["count"]
            if cancelled_count or suppressed_count or forced_interrupt or pending_count:
                if forced_interrupt and not cancelled_count and not suppressed_count:
                    logger.warning(
                        "Shutdown interrupted; tasks may still be running. "
                        "Re-run with EZMSG_STRICT_SHUTDOWN=1 to debug tasks with poor shutdown behavior."
                    )
                elif pending_count:
                    logger.warning(
                        "Shutdown timed out waiting for %d task(s). "
                        "Re-run with EZMSG_STRICT_SHUTDOWN=1 to debug tasks with poor shutdown behavior.",
                        pending_count,
                    )
                else:
                    logger.warning(
                        "Shutdown suppressed %d error(s) and cancelled %d task(s). "
                        "Shutdown was NOT clean; re-run with EZMSG_STRICT_SHUTDOWN=1 "
                        "to debug tasks with poor shutdown behavior.",
                        suppressed_count,
                        cancelled_count,
                    )

            if shutdown_summary is not None:
                shutdown_summary.cancelled_tasks = cancelled_count
                shutdown_summary.pending_tasks = pending_count
                shutdown_summary.executor_active = (
                    executor.active_count() if executor is not None else 0
                )
                shutdown_summary.suppressed_errors = suppressed_count
                shutdown_summary.forced_interrupt = forced_interrupt

        loop.call_soon_threadsafe(loop.stop)
        thread.join()
        loop.close()
        logger.debug("Task thread stopped and closed")
