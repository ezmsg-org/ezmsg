import functools
import logging
import os
from pathlib import Path
import time
import typing

import ezmsg.core as ez


HEADER = "Time,Source,Topic,SampleTime,PerfCounter,Elapsed"


def get_logger_path() -> Path:
    # Retrieve the logfile name from the environment variable
    logfile = os.environ.get("EZMSG_PROFILER", None)

    # Determine the log file path, defaulting to "ezprofiler.log" if not set
    logpath = Path(logfile or "ezprofiler.log")

    # If the log path is not absolute, prepend it with the user's home directory and ".ezmsg/profiler"
    if not logpath.is_absolute():
        logpath = Path.home() / ".ezmsg" / "profiler" / logpath

    return logpath


def _setup_logger(append: bool = False) -> logging.Logger:
    logpath = get_logger_path()
    logpath.parent.mkdir(parents=True, exist_ok=True)

    write_header = True
    if logpath.exists() and logpath.is_file():
        if append:
            with open(logpath) as f:
                first_line = f.readline().rstrip()
            if first_line:
                if first_line == HEADER:
                    write_header = False
                else:
                    # Remove the file if appending, but headers do not match
                    ezmsg_logger = logging.getLogger("ezmsg")
                    ezmsg_logger.warning(
                        "Profiling header mismatch: please make sure to use the same version of ezmsg for all processes."
                    )
                    logpath.unlink()
        else:
            # Remove the file if not appending
            logpath.unlink()

    # Create a logger with the name "ezprofiler"
    _logger = logging.getLogger("ezprofiler")

    # Set the logger's level to EZMSG_LOGLEVEL env var value if it exists, otherwise INFO
    _logger.setLevel(os.environ.get("EZMSG_LOGLEVEL", "INFO").upper())

    # Create a file handler to write log messages to the log file
    fh = logging.FileHandler(logpath)
    fh.setLevel(logging.DEBUG)  # Set the file handler log level to DEBUG

    # Add the file handler to the logger
    _logger.addHandler(fh)

    # Add the header if writing to new file or if header matched header in file.
    if write_header:
        _logger.debug(HEADER)

    # Set the log message format
    formatter = logging.Formatter(
        "%(asctime)s,%(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z"
    )
    fh.setFormatter(formatter)

    return _logger


logger = _setup_logger(append=True)


def _process_obj(obj, trace_oldest: bool = True):
    samp_time = None
    if hasattr(obj, "axes") and ("time" in obj.axes or "win" in obj.axes):
        axis = "win" if "win" in obj.axes else "time"
        ax = obj.get_axis(axis)
        len = obj.data.shape[obj.get_axis_idx(axis)]
        if len > 0:
            idx = 0 if trace_oldest else (len - 1)
            if hasattr(ax, "data"):
                samp_time = ax.data[idx]
            else:
                samp_time = ax.value(idx)
            if ax == "win" and "time" in obj.axes:
                if hasattr(obj.axes["time"], "data"):
                    samp_time += obj.axes["time"].data[idx]
                else:
                    samp_time += obj.axes["time"].value(idx)
    return samp_time


def profile_method(trace_oldest: bool = True):
    """
    Decorator to profile a method by logging its execution time and other details.

    Log messages are of the form:
        module.classname (of caller), caller.address, start_time, stop_time, elapsed_time

    :param trace_oldest: If True, trace the oldest sample time; otherwise, trace the newest.
    :type trace_oldest: bool

    :return: The decorated function with profiling enabled.
    :rtype: Callable
    """

    def profiling_decorator(func: typing.Callable):
        @functools.wraps(func)
        def wrapped_func(caller, *args, **kwargs):
            start = time.perf_counter()
            res = func(caller, *args, **kwargs)
            stop = time.perf_counter()
            source = ".".join((caller.__class__.__module__, caller.__class__.__name__))
            topic = f"{caller.address}"
            samp_time = _process_obj(res, trace_oldest=trace_oldest)
            logger.debug(
                ",".join(
                    [
                        source,
                        topic,
                        f"{samp_time}",
                        f"{stop}",
                        f"{(stop - start) * 1e3:0.4f}",
                    ]
                )
            )
            return res

        return wrapped_func if logger.level == logging.DEBUG else func

    return profiling_decorator


def profile_subpub(trace_oldest: bool = True):
    """
    Decorator to profile a subscriber-publisher method in an ezmsg Unit
    by logging its execution time and other details.
    Decorator to profile a subscriber-publisher method in an ezmsg Unit by logging its execution
    time and other details.

    Log messages are of the form:
        module.classname (of unit), unit.address, start_time, stop_time, elapsed_time

    :param trace_oldest: If True, trace the oldest sample time; otherwise, trace the newest.
    :type trace_oldest: bool

    :return: The decorated async task with profiling enabled.
    :rtype: Callable
    """

    def profiling_decorator(func: typing.Callable):
        @functools.wraps(func)
        async def wrapped_task(unit: ez.Unit, msg: typing.Any = None):
            source = ".".join((unit.__class__.__module__, unit.__class__.__name__))
            topic = f"{unit.address}"
            start = time.perf_counter()
            async for stream, obj in func(unit, msg):
                stop = time.perf_counter()
                samp_time = _process_obj(obj, trace_oldest=trace_oldest)
                logger.debug(
                    ",".join(
                        [
                            source,
                            topic,
                            f"{samp_time}",
                            f"{stop}",
                            f"{(stop - start) * 1e3:0.4f}",
                        ]
                    )
                )
                yield stream, obj
                start = time.perf_counter()

        return wrapped_task if logger.level == logging.DEBUG else func

    return profiling_decorator
