from dataclasses import asdict, dataclass
from collections.abc import AsyncGenerator
from contextlib import contextmanager

import json
import os
from pathlib import Path
import tempfile
import typing

import ezmsg.core as ez


@contextmanager
def get_test_fn(test_name: str | None = None, extension: str = "txt") -> typing.Generator[Path, None, None]:
    """PYTEST compatible temporary test file creator"""

    # Get current test name if we can..
    if test_name is None:
        test_name = os.environ.get("PYTEST_CURRENT_TEST")
        if test_name is not None:
            test_name = test_name.split(":")[-1].split(" ")[0]
        else:
            test_name = __name__

    # Create a unique temporary file name to avoid collisions when running the
    # full test suite in parallel or when other tests use the same test name.
    # Use NamedTemporaryFile with delete=False so callers can open/remove it.
    prefix = f"{test_name}-" if test_name else "test-"
    tmp = tempfile.NamedTemporaryFile(prefix=prefix, suffix=f".{extension}")
    tmp.close()  # Close so others can open it on Windows
    path = Path(tmp.name)
    try:
        yield path
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


# MESSAGE DEFINITIONS
@dataclass
class SimpleMessage:
    number: float


# MESSAGE GENERATOR
class MessageGeneratorSettings(ez.Settings):
    num_msgs: int


class MessageGenerator(ez.Unit):
    SETTINGS = MessageGeneratorSettings

    OUTPUT = ez.OutputStream(SimpleMessage)

    @ez.publisher(OUTPUT)
    async def spawn(self) -> AsyncGenerator:
        for i in range(self.SETTINGS.num_msgs):
            yield self.OUTPUT, SimpleMessage(i)
        raise ez.Complete


# MESSAGE RECEIVER
class MessageReceiverSettings(ez.Settings):
    num_msgs: int
    output_fn: str


class MessageReceiverState(ez.State):
    num_received: int = 0


class MessageReceiver(ez.Unit):
    STATE = MessageReceiverState
    SETTINGS = MessageReceiverSettings

    INPUT = ez.InputStream(SimpleMessage)

    @ez.subscriber(INPUT)
    async def on_message(self, msg: SimpleMessage) -> None:
        ez.logger.info(f"Msg: {msg}")
        self.STATE.num_received += 1
        with open(self.SETTINGS.output_fn, "a") as output_file:
            output_file.write(json.dumps(asdict(msg)) + "\n")
        if self.STATE.num_received == self.SETTINGS.num_msgs:
            raise ez.Complete
