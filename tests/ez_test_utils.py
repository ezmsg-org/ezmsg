from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import tempfile
import typing

import ezmsg.core as ez


def get_test_fn(test_name: typing.Optional[str] = None, extension: str = "txt") -> Path:
    """PYTEST compatible temporary test file creator"""

    # Get current test name if we can..
    if test_name is None:
        test_name = os.environ.get("PYTEST_CURRENT_TEST")
        if test_name is not None:
            test_name = test_name.split(":")[-1].split(" ")[0]
        else:
            test_name = __name__

    file_path = Path(tempfile.gettempdir())
    file_path = file_path / Path(f"{test_name}.{extension}")

    # Create the file
    with open(file_path, "w"):
        pass

    return file_path


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
    async def spawn(self) -> typing.AsyncGenerator:
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
