import json
import os
import pytest
import logging
from dataclasses import asdict, dataclass

import ezmsg.core as ez

from ezmsg.testing import get_test_fn

from typing import AsyncGenerator

logger = logging.getLogger(__name__)

# MESSAGE DEFINITIONS


@dataclass
class SimpleMessage:
    number: float

# MESSAGE GENERATOR


class MessageGeneratorSettings(ez.Settings):
    num_msgs: int


class MessageGenerator(ez.Unit):

    SETTINGS: MessageGeneratorSettings

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
    STATE: MessageReceiverState
    SETTINGS: MessageReceiverSettings

    INPUT = ez.InputStream(SimpleMessage)

    @ez.subscriber(INPUT)
    async def on_message(self, msg: SimpleMessage) -> None:
        logger.info(f"Msg: {msg}")
        self.STATE.num_received += 1
        with open(self.SETTINGS.output_fn, "a") as output_file:
            output_file.write(json.dumps(asdict(msg)) + "\n")
        if self.STATE.num_received == self.SETTINGS.num_msgs:
            raise ez.Complete

# Define and configure a system of modules to launch


class ToySystemSettings(ez.Settings):
    num_msgs: int
    output_fn: str


class ToySystem(ez.System):

    SETTINGS: ToySystemSettings

    # Publishers
    SIMPLE_PUB = MessageGenerator()

    # Subscribers
    SIMPLE_SUB = MessageReceiver()

    def configure(self) -> None:
        self.SIMPLE_PUB.apply_settings(
            MessageGeneratorSettings(
                num_msgs=self.SETTINGS.num_msgs
            ))

        self.SIMPLE_SUB.apply_settings(
            MessageReceiverSettings(
                num_msgs=self.SETTINGS.num_msgs,
                output_fn=self.SETTINGS.output_fn
            ))

    # Define Connections
    def network(self) -> ez.NetworkDefinition:
        return (
            (self.SIMPLE_PUB.OUTPUT, self.SIMPLE_SUB.INPUT),
        )


@pytest.fixture(params=[
    (ToySystem,), (ToySystem.SIMPLE_PUB, ToySystem.SIMPLE_SUB,)
])
def toy_system_fixture(request):
    def func(self):
        return request.param
    ToySystem.process_components = func
    return ToySystem


@pytest.mark.parametrize("num_messages", [1, 5, 10])
def test_local_system(toy_system_fixture, num_messages):
    test_filename = get_test_fn()
    system = toy_system_fixture(
        ToySystemSettings(
            num_msgs=num_messages,
            output_fn=test_filename
        )
    )
    ez.run_system(system)

    results = []
    with open(test_filename, "r") as file:
        lines = file.readlines()
        for line in lines:
            results.append(json.loads(line))
    os.remove(test_filename)
    assert len(results) == num_messages
