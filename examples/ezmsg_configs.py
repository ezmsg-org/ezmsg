import asyncio

import ezmsg.core as ez
from ezmsg.util.debuglog import DebugLog

from typing import AsyncGenerator, Optional


# Terminator -- Arnold Schwarzenegger


class TerminatorSettings(ez.Settings):
    term_after: float = 1.0  # sec


class Terminator(ez.Unit):
    SETTINGS: TerminatorSettings

    @ez.task
    async def terminate(self) -> None:
        await asyncio.sleep(self.SETTINGS.term_after)
        raise ez.NormalTermination


# Generator -- A Pure Publisher


class GeneratorSettings(ez.Settings):
    pub_rate: float = 4.0  # Hz
    num_msgs: int = 10


class Generator(ez.Unit):
    SETTINGS: GeneratorSettings

    OUTPUT = ez.OutputStream(int)

    @ez.publisher(OUTPUT)
    async def generate(self) -> AsyncGenerator:
        for msg_idx in range(self.SETTINGS.num_msgs):
            await asyncio.sleep(1.0 / self.SETTINGS.pub_rate)
            yield self.OUTPUT, msg_idx
        raise ez.Complete()


# Modulus -- A Pure Modifier


class ModulusSettings(ez.Settings):
    mod: int = 3


class Modulus(ez.Unit):
    SETTINGS: ModulusSettings

    INPUT = ez.InputStream(int)
    OUTPUT = ez.OutputStream(int)

    @ez.subscriber(INPUT)
    @ez.publisher(OUTPUT)
    async def modulus(self, msg: int) -> AsyncGenerator:
        yield self.OUTPUT, msg % self.SETTINGS.mod


# Listener -- A Pure Subscriber


class ListenerState(ez.State):
    value: Optional[int] = None


class Listener(ez.Unit):
    STATE: ListenerState

    INPUT = ez.InputStream(int)

    @ez.subscriber(INPUT)
    async def listen(self, msg: int) -> None:
        self.STATE.value = msg


# Weird passthrough collection


class PassthroughCollection(ez.Collection):
    INPUT = ez.InputStream(int)
    OUTPUT = ez.OutputStream(int)

    def network(self) -> ez.NetworkDefinition:
        return ((self.INPUT, self.OUTPUT),)


# CORNER CASES


class EmptySystem(ez.Collection):
    ...


class EmptyTerminateSystem(ez.Collection):
    TERMINATE = Terminator()


class OnlyPassthroughSystem(ez.Collection):
    TERMINATE = Terminator()
    PASSTHROUGH = PassthroughCollection()

    def configure(self) -> None:
        ez.logger.info("No output expected")


# SYSTEMS WITH HANGING PUBS AND SUBS


class PubNoSubSystem(ez.Collection):
    TERMINATE = Terminator()
    GENERATE = Generator()
    LOG = DebugLog()


class SubNoPubSystem(ez.Collection):
    TERMINATE = Terminator()
    LISTEN = Listener()

    def configure(self) -> None:
        ez.logger.info("No output expected")


class NoPubNoSubSystem(ez.Collection):
    TERMINATE = Terminator()
    MODULUS = Modulus()

    def configure(self) -> None:
        ez.logger.info("No output expected")


# Systems with collections that have hanging pubs and subs


class PubNoSubCollection(ez.Collection):
    OUTPUT = ez.OutputStream(int)
    GENERATE = Generator()
    LOG = DebugLog()

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.GENERATE.OUTPUT, self.OUTPUT),
            (self.GENERATE.OUTPUT, self.LOG.INPUT),
        )


class SubNoPubCollection(ez.Collection):
    INPUT = ez.InputStream(int)
    LISTEN = Listener()

    def network(self) -> ez.NetworkDefinition:
        return ((self.INPUT, self.LISTEN.INPUT),)


class PubNoSubCollectionSystem(ez.Collection):
    TERMINATE = Terminator()
    COLLECTION = PubNoSubCollection()


class SubNoPubCollectionSystem(ez.Collection):
    TERMINATE = Terminator()
    COLLECTION = SubNoPubCollection()

    def configure(self) -> None:
        ez.logger.info("No output expected")


# Passthrough Collection Tests


class PubNoSubPassthroughCollection(ez.Collection):
    COLLECTION = PubNoSubCollection()
    PASSTHROUGH = PassthroughCollection()

    OUTPUT = ez.OutputStream(int)

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.COLLECTION.OUTPUT, self.PASSTHROUGH.INPUT),
            (self.PASSTHROUGH.OUTPUT, self.OUTPUT),
        )


class SubNoPubPassthroughCollection(ez.Collection):
    COLLECTION = SubNoPubCollection()
    PASSTHROUGH = PassthroughCollection()

    INPUT = ez.InputStream(int)

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.INPUT, self.PASSTHROUGH.INPUT),
            (self.PASSTHROUGH.OUTPUT, self.COLLECTION.INPUT),
        )


class PubNoSubPassthroughCollectionSystem(ez.Collection):
    TERMINATE = Terminator()
    COLLECTION = PubNoSubPassthroughCollection()


class SubNoPubPassthroughCollectionSystem(ez.Collection):
    TERMINATE = Terminator()
    COLLECTION = SubNoPubPassthroughCollection()

    def configure(self) -> None:
        ez.logger.info("No output expected")


class PassthroughSystem(ez.Collection):
    TERMINATE = Terminator()
    GENERATE = Generator()
    PASSTHROUGH = PassthroughCollection()
    LOG = DebugLog()

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.GENERATE.OUTPUT, self.PASSTHROUGH.INPUT),
            (self.PASSTHROUGH.OUTPUT, self.LOG.INPUT),
        )


if __name__ == "__main__":
    test_systems = [
        EmptySystem,
        EmptyTerminateSystem,
        OnlyPassthroughSystem,
        PubNoSubSystem,
        SubNoPubSystem,
        NoPubNoSubSystem,
        PubNoSubCollectionSystem,
        SubNoPubCollectionSystem,
        PubNoSubPassthroughCollectionSystem,
        SubNoPubPassthroughCollectionSystem,
        PassthroughSystem,
    ]

    for system in test_systems:
        ez.logger.info(f"Testing { system.__name__ }")
        ez.run(system())
