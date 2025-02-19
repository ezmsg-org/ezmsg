import json
import time
import typing

from io import TextIOWrapper
from dataclasses import field
from pathlib import Path

import ezmsg.core as ez

from .messagecodec import MessageEncoder, LogStart
from .rate import Rate


def log_object(obj: typing.Any) -> str:
    return json.dumps({"ts": time.time(), "obj": obj}, cls=MessageEncoder)


class MessageLoggerSettings(ez.Settings):
    """
    Settings for :obj:`MessageLogger` Unit.

    Args:
        output: :py:class:`pathlib.Path` for a file where the messages will be logged.
            If the file path already exists, the existing file will be truncated to 0 length.
        write_period: Period in seconds for performing a write to disk.
            If <=0, write incoming messages to disk immediately, applying backpressure as needed.
            If >0, messages are buffered and periodically written to disk in a separate task
    """

    output: typing.Optional[Path] = None
    write_period: float = 0.0 # sec


class MessageLoggerState(ez.State):
    output_files: typing.Dict[Path, TextIOWrapper] = field(default_factory=dict)
    write_buffer: typing.List[str] = field(default_factory=list)


class MessageLogger(ez.Unit):
    """
    Logs all messages it receives to a file.
    File path can be set in ``SETTINGS`` or set dynamically by passing a
    :py:class:`pathlib.Path` to ``INPUT_START``.
    """

    SETTINGS = MessageLoggerSettings
    STATE = MessageLoggerState

    INPUT_START = ez.InputStream(Path)
    """
    Pass a :py:class:`pathlib.Path` 
    to begin logging messages to that path. If the file path already exists, the existing 
    file will be truncated to 0 length. If the file is already open, nothing will happen.
    """

    INPUT_STOP = ez.InputStream(Path)
    """
    Pass a :py:class:`pathlib.Path` 
    to stop logging messages to that path.
    """

    INPUT_MESSAGE = ez.InputStream(typing.Any)
    """Pass a piece of data to log it to every open file which the ``MessageLogger`` is using."""

    OUTPUT_MESSAGE = ez.OutputStream(typing.Any)
    """Messages which are sent to ``INPUT_MESSAGE`` will pass through and be published on ``OUTPUT_MESSAGE``."""

    OUTPUT_START = ez.OutputStream(Path)
    """If a file passed to ``INPUT_START`` is successfully opened, its path will be published to
    ``OUTPUT_START``, otherwise ``None``."""

    OUTPUT_STOP = ez.OutputStream(Path)
    """If a file passed to ``INPUT_STOP`` is successfully closed, its path will be published to
    ``OUTPUT_STOP``, otherwise ``None``."""

    def open_file(self, filepath: Path) -> typing.Optional[Path]:
        """Returns file path if file successfully opened, otherwise None"""
        if filepath in self.STATE.output_files:
            # If the file is already open, we return None
            return None

        if not filepath.parent.exists():
            filepath.parent.mkdir(parents=True, exist_ok=True)
        output_f = open(filepath, mode="w")
        strmessage = log_object(LogStart())
        output_f.write(f"{strmessage}\n")
        output_f.flush()
        self.STATE.output_files[filepath] = output_f

        return filepath

    def close_file(self, filepath: Path) -> typing.Optional[Path]:
        """Returns file path if file successfully closed, otherwise None"""
        if filepath not in self.STATE.output_files:
            # We haven't opened this file
            return None

        self.STATE.output_files[filepath].close()
        del self.STATE.output_files[filepath]

        return filepath

    async def initialize(self) -> None:
        """Note that files defined at startup are not published to outputs"""
        if self.SETTINGS.output is not None:
            self.open_file(self.SETTINGS.output)

    @ez.subscriber(INPUT_START)
    @ez.publisher(OUTPUT_START)
    async def start_file(self, message: Path) -> typing.AsyncGenerator:
        out = self.open_file(message)
        if out is not None:
            yield (self.OUTPUT_START, out)

    @ez.subscriber(INPUT_STOP)
    @ez.publisher(OUTPUT_STOP)
    async def stop_file(self, message: Path) -> typing.AsyncGenerator:
        out = self.close_file(message)
        if out is not None:
            yield (self.OUTPUT_STOP, out)

    @ez.subscriber(INPUT_MESSAGE)
    @ez.publisher(OUTPUT_MESSAGE)
    async def on_message(self, message: typing.Any) -> typing.AsyncGenerator:
        strmessage = f"{log_object(message)}\n"

        if self.SETTINGS.write_period <= 0:
            for output_f in self.STATE.output_files.values():
                output_f.write(strmessage)
                output_f.flush()
        else:
            self.STATE.write_buffer.append(strmessage)

        yield (self.OUTPUT_MESSAGE, message)

    @ez.task
    async def buffered_write(self) -> None:
        if self.SETTINGS.write_period <= 0:
            return
        
        rate = Rate(1.0 / self.SETTINGS.write_period)
        while True:
            for output_f in self.STATE.output_files.values():
                output_f.writelines(self.STATE.write_buffer)
                output_f.flush()
            self.STATE.write_buffer.clear()
            await rate.sleep()

    async def shutdown(self) -> None:
        """Note that files that are closed at shutdown don't publish messages"""

        if len(self.STATE.write_buffer):
            for output_f in self.STATE.output_files.values():
                output_f.writelines(self.STATE.write_buffer)
                output_f.flush()
            self.STATE.write_buffer.clear()

        for filepath in list(self.STATE.output_files):
            self.close_file(filepath)
