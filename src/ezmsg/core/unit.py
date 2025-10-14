import time
import inspect
import functools
from .stream import InputStream, OutputStream
from .component import ComponentMeta, Component
from .settings import Settings

from collections.abc import Callable
from typing import Any

import logging

logger = logging.getLogger("ezmsg")

MAIN_ATTR = "__ez_main__"
THREAD_ATTR = "__ez_thread__"
TASK_ATTR = "__ez_task__"
PUBLISHES_ATTR = "__ez_publishes__"
SUBSCRIBES_ATTR = "__ez_subscribes__"
TIMEIT_ATTR = "__ez_timeit__"
ZERO_COPY_ATTR = "__ez_zerocopy__"
PROCESS_ATTR = "__ez_process__"


class UnitMeta(ComponentMeta):
    def __init__(
        cls, name: str, bases: tuple[type, ...], fields: dict[str, Any], **kwargs: Any
    ) -> None:
        super(UnitMeta, cls).__init__(name, bases, fields)

        cls.__tasks__ = {}
        cls.__main__ = None
        cls.__threads__ = {}

        for base in bases:
            if hasattr(base, "__tasks__"):
                tasks = getattr(base, "__tasks__")
                for task_name, task in tasks.items():
                    cls.__tasks__[task_name] = task
            if hasattr(base, "__main__"):
                main = getattr(base, "__main__")
                if cls.__main__ is not None and main is not None:
                    raise Exception(
                        f"Attempting to replace {name}'s main func {cls.__main__} with {main}!"
                    )
                cls.__main__ = main
            if hasattr(base, "__threads__"):
                threads = getattr(base, "__threads__")
                for thread_name, thread in threads.items():
                    cls.__threads__[thread_name] = thread

        for field_name, field_value in fields.items():
            if callable(field_value):
                if hasattr(field_value, TASK_ATTR):
                    cls.__tasks__[field_name] = field_value
                elif hasattr(field_value, MAIN_ATTR):
                    cls.__main__ = field_value
                elif hasattr(field_value, THREAD_ATTR):
                    cls.__threads__[field_name] = field_value


class Unit(Component, metaclass=UnitMeta):
    """
    Represents a single step in the graph.
    
    Units can subscribe, publish, and have tasks. Units are the fundamental building blocks
    of ezmsg applications that perform actual computation and message processing.
    To create a Unit, inherit from the Unit class.
    
    :param settings: Optional settings object for unit configuration
    :type settings: Settings | None
    """

    def __init__(self, *args, settings: Settings | None = None, **kwargs):
        super(Unit, self).__init__(*args, settings=settings, **kwargs)

        for task_name, task in self.__class__.__tasks__.items():
            self._tasks[task_name] = task
        self._main = self.__class__.__main__
        self._threads = self.__class__.__threads__

    async def setup(self):
        """This is called from within the same process this unit will live"""
        self._instantiate_state()
        if not self._settings_applied:
            raise ValueError(
                f"{self.address} has not had settings applied before initialization"
            )

        if inspect.iscoroutinefunction(self.initialize):
            await self.initialize()
        else:
            self.initialize()  # type: ignore

        self._check_state()

    async def initialize(self) -> None:
        """
        Runs when the Unit is instantiated.
        
        This is called from within the same process this unit will live in.
        This lifecycle hook can be overridden. It can be run as async functions 
        by simply adding the async keyword when overriding.
        
        This method is where you should initialize your unit's state and prepare
        for message processing.
        """
        pass

    async def shutdown(self) -> None:
        """
        Runs when the Unit terminates.
        
        This is called from within the same process this unit will live in.
        This lifecycle hook can be overridden. It can be run as async functions 
        by simply adding the async keyword when overriding.
        
        This method is where you should clean up resources and perform
        any necessary shutdown procedures.
        """
        pass


def publisher(stream: OutputStream):
    """
    A decorator for a method that publishes to a stream in the task/messaging thread.
    
    An async function will yield messages on the designated :obj:`OutputStream`.
    A function can have both ``@subscriber`` and ``@publisher`` decorators.

    :param stream: The output stream to publish messages to
    :type stream: OutputStream
    :return: Decorated function that can publish to the stream
    :rtype: Callable
    :raises ValueError: If stream is not an OutputStream

    .. code-block:: python

      from collections.abc import AsyncGenerator

      OUTPUT = OutputStream(ez.Message)

      @publisher(OUTPUT)
      async def send_message(self) -> AsyncGenerator:
         message = Message()
         yield(OUTPUT, message)
    """

    if not isinstance(stream, OutputStream):
        raise ValueError(f"Cannot publish to object of type {type(stream)}")

    def pub_factory(func):
        published_streams: list[OutputStream] = getattr(func, PUBLISHES_ATTR, [])
        published_streams.append(stream)
        setattr(func, PUBLISHES_ATTR, published_streams)
        return task(func)

    return pub_factory


def subscriber(stream: InputStream, zero_copy: bool = False):
    """
    A decorator for a method that subscribes to a stream in the task/messaging thread.
    
    An async function will run once per message received from the :obj:`InputStream` 
    it subscribes to. A function can have both ``@subscriber`` and ``@publisher`` decorators.

    :param stream: The input stream to subscribe to
    :type stream: InputStream
    :param zero_copy: Whether to use zero-copy message passing (default: False)
    :type zero_copy: bool
    :return: Decorated function that can subscribe to the stream
    :rtype: Callable
    :raises ValueError: If stream is not an InputStream

    .. code-block:: python

      INPUT = ez.InputStream(Message)

      @subscriber(INPUT)
      async def print_message(self, message: Message) -> None:
         print(message)
    """

    if not isinstance(stream, InputStream):
        raise ValueError(f"Cannot subscribe to object of type {type(stream)}")

    def sub_factory(func):
        subscribed_streams: InputStream | None = getattr(func, SUBSCRIBES_ATTR, None)
        if subscribed_streams is not None:
            raise Exception(f"{func} cannot subscribe to more than one stream")
        setattr(func, SUBSCRIBES_ATTR, stream)
        setattr(func, ZERO_COPY_ATTR, zero_copy)
        return task(func)

    return sub_factory


def main(func: Callable):
    """
    A decorator which designates this function to run as the main thread for this Unit.
    
    A Unit may only have one main function. The main function runs independently
    of the message processing and is typically used for initialization, background
    processing, or cleanup tasks.

    :param func: The function to designate as main
    :type func: Callable
    :return: The decorated function
    :rtype: Callable
    """
    setattr(func, MAIN_ATTR, True)
    return func


def timeit(func: Callable):
    """
    A decorator that logs the execution time of the decorated function.
    
    ezmsg will log the amount of time this function takes to execute to
    the ezmsg logger. This is useful for performance monitoring and
    optimization. The execution time is logged in milliseconds.

    .. note:: Use the ``@profile_subpub`` or ``@profile_method`` decorators
    from ezmsg-sigproc for more detailed profiling that is stored in a
    dedicated profiling log file.

    :param func: The function to time
    :type func: Callable
    :return: The decorated function with timing functionality
    :rtype: Callable
    """
    setattr(func, TIMEIT_ATTR, True)

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        start = time.perf_counter()
        res = func(self, *args, **kwargs)
        stop = time.perf_counter()
        _, task_info = self._tasks[func.__name__]
        address = task_info.address
        logger.info(f"{address} task duration = {(stop - start) * 1e3:0.4f}ms")
        return res

    return wrapper


def thread(func: Callable):
    """
    A decorator which designates this function to run as a background thread for this Unit.
    
    Thread functions run concurrently with the main message processing and can be used
    for background tasks, monitoring, or other concurrent operations.

    :param func: The function to run as a background thread
    :type func: Callable
    :return: The decorated function
    :rtype: Callable
    """
    setattr(func, THREAD_ATTR, True)
    return func


def task(func: Callable):
    """
    A decorator which designates this function to run as a task in the task/messaging thread.
    
    Task functions are part of the main message processing pipeline and are executed
    within the unit's primary execution context.

    :param func: The function to run as a task
    :type func: Callable
    :return: The decorated function
    :rtype: Callable
    """
    setattr(func, TASK_ATTR, True)
    return func


def process(func: Callable):
    """
    A decorator which designates this function to run in its own process.
    
    Process functions run in separate processes for isolation and can be used
    for CPU-intensive operations or when process isolation is required.

    :param func: The function to run in its own process
    :type func: Callable
    :return: The decorated function
    :rtype: Callable
    """
    setattr(func, PROCESS_ATTR, True)
    return func
