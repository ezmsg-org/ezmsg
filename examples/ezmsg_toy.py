import asyncio
import time
import math

from dataclasses import dataclass

from typing import AsyncGenerator, Optional

import ezmsg.core as ez


# MESSAGE DEFINITIONS
@dataclass
class CombinedMessage:
    string: str
    number: float


# LFO: Low Frequency Oscillator

class LFOSettings(ez.Settings):
    freq: float = 0.2  # Hz, sinus frequency
    update_rate: float = 2.0  # Hz, update rate


class LFO(ez.Unit):
    SETTINGS: LFOSettings

    OUTPUT = ez.OutputStream(float)

    def initialize(self) -> None:
        self.start_time = time.time()

    @ez.publisher(OUTPUT)
    async def generate(self) -> AsyncGenerator:
        while True:
            t = time.time() - self.start_time
            yield self.OUTPUT, math.sin(2.0 * math.pi * self.SETTINGS.freq * t)
            await asyncio.sleep(1.0 / self.SETTINGS.update_rate)


# MESSAGE GENERATOR
class MessageGeneratorSettings(ez.Settings):
    message: str


class MessageGenerator(ez.Unit):
    SETTINGS: MessageGeneratorSettings

    OUTPUT = ez.OutputStream(str)

    @ez.publisher(OUTPUT)
    async def spawn_message(self) -> AsyncGenerator:
        while True:
            await asyncio.sleep(1.0)
            ez.logger.info(f"Spawning {self.SETTINGS.message}")
            yield self.OUTPUT, self.SETTINGS.message

    @ez.publisher(OUTPUT)
    async def spawn_once(self) -> AsyncGenerator:
        yield self.OUTPUT, "Spawned Once"
        raise ez.Complete


# DEBUG OUTPUT
class DebugOutputSettings(ez.Settings):
    name: Optional[str] = "Default"


class DebugOutput(ez.Unit):
    SETTINGS: DebugOutputSettings

    INPUT = ez.InputStream(str)

    @ez.subscriber(INPUT)
    async def on_message(self, message: str) -> None:
        ez.logger.info(f"Output[{self.SETTINGS.name}]: {message}")


# MESSAGE MODIFIER
class MessageModifierState(ez.State):
    number: float


class MessageModifier(ez.Unit):
    """Store number input, and append it to message"""

    STATE: MessageModifierState

    MESSAGE = ez.InputStream(str)
    NUMBER = ez.InputStream(float)

    JOINED = ez.OutputStream(str)
    REPUB = ez.OutputStream(CombinedMessage)

    def initialize(self):
        self.STATE.number = 0.0

    @ez.subscriber(NUMBER)
    async def on_number(self, number: float) -> None:
        self.STATE.number = number

    @ez.subscriber(MESSAGE)
    @ez.publisher(JOINED)
    @ez.publisher(REPUB)
    async def on_message(self, message: str) -> AsyncGenerator:
        yield self.REPUB, CombinedMessage(string=message, number=self.STATE.number)

        if self.STATE.number is not None:
            message = f"{message}|{self.STATE.number}"

        yield self.JOINED, message

    @ez.main
    def blocking_main(self) -> None:
        for i in range(10):
            ez.logger.info(i)
            time.sleep(1.0)


class ModifierCollection(ez.Collection):
    """This collection will subscribe to messages
    and append the most recent LFO output"""

    INPUT = ez.InputStream(str)
    OUTPUT = ez.OutputStream(str)

    SIN = LFO()
    # SIN2 = LFO()
    MODIFIER = MessageModifier()

    REPUB_OUT = DebugOutput(DebugOutputSettings(name="REPUB"))

    def configure(self) -> None:
        self.SIN.apply_settings(LFOSettings(freq=0.1))

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.SIN.OUTPUT, self.MODIFIER.NUMBER),
            (self.INPUT, self.MODIFIER.MESSAGE),
            (self.MODIFIER.JOINED, self.OUTPUT),
            (self.MODIFIER.REPUB, self.REPUB_OUT.INPUT),
        )


# Define and configure a system of modules to launch
class TestSystemSettings(ez.Settings):
    name: str


class TestSystem(ez.Collection):
    SETTINGS: TestSystemSettings

    # Publishers
    PING = MessageGenerator()
    FOO = MessageGenerator()

    # Transformers
    MODIFIER_COLLECTION = ModifierCollection()

    # Subscribers
    PINGSUB1 = DebugOutput()
    PINGSUB2 = DebugOutput()
    FOOSUB = DebugOutput()

    def configure(self) -> None:
        self.PING.apply_settings(MessageGeneratorSettings(message="PING"))
        self.FOO.apply_settings(MessageGeneratorSettings(message="FOO"))
        self.PINGSUB1.apply_settings(DebugOutputSettings(name=f"{self.SETTINGS.name}1"))
        self.PINGSUB2.apply_settings(DebugOutputSettings(name=f"{self.SETTINGS.name}2"))

    # Define Connections
    def network(self) -> ez.NetworkDefinition:
        return (
            (self.PING.OUTPUT, self.PINGSUB1.INPUT),
            (self.PING.OUTPUT, self.MODIFIER_COLLECTION.INPUT),
            (self.MODIFIER_COLLECTION.OUTPUT, self.PINGSUB2.INPUT),
            (self.FOO.OUTPUT, self.FOOSUB.INPUT),
            (self.PING.OUTPUT, self.FOOSUB.INPUT),
        )

    def process_components(self):
        return (self.PING, self.FOOSUB, self.MODIFIER_COLLECTION, self.PINGSUB1)


if __name__ == "__main__":
    # import multiprocessing as mp
    # mp.set_start_method( 'fork', force = True )

    system = TestSystem(TestSystemSettings(name="A"))

    ez.run(
        SYSTEM = system,
        # connections = [
        #     ( system.PING.OUTPUT, 'PING_OUTPUT' ),
        #     ( 'FOO_SUB', system.FOOSUB.INPUT )
        # ]
    )
