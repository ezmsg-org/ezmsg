import time
import functools
from .stream import Stream, InputStream, OutputStream
from .component import ComponentMeta, Component
from .settings import Settings

from typing import Any, Dict, List, Tuple, Callable, Optional

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
        cls, name: str, bases: Tuple[type, ...], fields: Dict[str, Any], **kwargs: Any
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
    """Units can subscribe, publish, and have tasks"""

    def __init__(self, settings: Optional[Settings] = None):
        super(Unit, self).__init__(settings)

        for task_name, task in self.__class__.__tasks__.items():
            self._tasks[task_name] = task
        self._main = self.__class__.__main__
        self._threads = self.__class__.__threads__

    def setup(self):
        """This is called from within the same process this unit will live"""
        self._instantiate_state()
        if not self._settings_applied:
            raise ValueError(
                f"{self.address} has not had settings applied before initialization"
            )
        self.initialize()
        self._check_state()

    def initialize(self) -> None:
        """This is called from within the same process this unit will live"""
        pass

    def shutdown(self) -> None:
        """This is called from within the same process this unit will live"""
        pass


def publisher(stream: OutputStream):
    """A decorator for a method that publishes to a stream in the task/messaging thread"""

    if not isinstance(stream, OutputStream):
        raise ValueError(f"Cannot publish to object of type {type(stream)}")

    def pub_factory(func):
        published_streams: List[OutputStream] = getattr(func, PUBLISHES_ATTR, [])
        published_streams.append(stream)
        setattr(func, PUBLISHES_ATTR, published_streams)
        return task(func)

    return pub_factory


def subscriber(stream: InputStream, zero_copy: bool = False):
    """A decorator for a method that subscribes to a stream in the task/messaging thread"""

    if not isinstance(stream, InputStream):
        raise ValueError(f"Cannot subscribe to object of type {type(stream)}")

    def sub_factory(func):
        subscribed_streams: Optional[InputStream] = getattr(func, SUBSCRIBES_ATTR, None)
        if subscribed_streams is not None:
            raise Exception(f"{func} cannot subscribe to more than one stream")
        setattr(func, SUBSCRIBES_ATTR, stream)
        setattr(func, ZERO_COPY_ATTR, zero_copy)
        return task(func)

    return sub_factory


def main(func: Callable):
    """A decorator for a function that runs as the main thread.  A Unit may only have one of these."""
    setattr(func, MAIN_ATTR, True)
    return func


def timeit(func: Callable):
    setattr(func, TIMEIT_ATTR, True)

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        start = time.perf_counter()
        res = func(self, *args, **kwargs)
        stop = time.perf_counter()
        _, task_info = self._tasks[func.__name__]
        address = task_info.address
        logger.info(f"{address} task duration = {(stop-start)*1e3:0.4f}ms")
        return res

    return wrapper


def thread(func: Callable):
    """A decorator for a function that runs in a background thread"""
    setattr(func, THREAD_ATTR, True)
    return func


def task(func: Callable):
    """A decorator for a function that runs as a task in the task/messaging thread"""
    setattr(func, TASK_ATTR, True)
    return func


def process(func: Callable):
    setattr(func, PROCESS_ATTR, True)
    return func
