import json
import os
from dataclasses import asdict, dataclass

import ezmsg.core as ez

from typing import AsyncGenerator


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
            raise ez.NormalTermination


# Define and configure a system of modules to launch


class ToySystemSettings(ez.Settings):
    num_msgs: int
    output_fn: str


class ToySystem(ez.Collection):
    SETTINGS = ToySystemSettings

    # Publishers
    SIMPLE_PUB = MessageGenerator()

    # Subscribers
    SIMPLE_SUB = MessageReceiver()

    def configure(self) -> None:
        self.SIMPLE_PUB.apply_settings(
            MessageGeneratorSettings(num_msgs=self.SETTINGS.num_msgs)
        )

        self.SIMPLE_SUB.apply_settings(
            MessageReceiverSettings(
                num_msgs=self.SETTINGS.num_msgs, output_fn=self.SETTINGS.output_fn
            )
        )

    # Define Connections
    def network(self) -> ez.NetworkDefinition:
        return ((self.SIMPLE_PUB.OUTPUT, self.SIMPLE_SUB.INPUT),)

    def process_components(self):
        return (
            self.SIMPLE_PUB,
            self.SIMPLE_SUB,
        )


def main():
    test_filename = "./test.txt"
    num_messages = 5
    with open(test_filename, "w") as _:
        ...
    system = ToySystem(
        ToySystemSettings(num_msgs=num_messages, output_fn=test_filename)
    )
    ez.run(SYSTEM=system)

    results = []
    with open(test_filename, "r") as file:
        lines = file.readlines()
        for line in lines:
            results.append(json.loads(line))
    os.remove(test_filename)


if __name__ == "__main__":
    main()
