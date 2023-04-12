from typing import AsyncGenerator

import ezmsg.core as ez

from ezmsg.util.debuglog import DebugLog
from ezmsg.util.terminate import TerminateOnTimeout as TerminateTest


class Source(ez.Unit):
    OUTPUT = ez.OutputStream(str)

    @ez.publisher(OUTPUT)
    async def pub(self) -> AsyncGenerator:
        yield self.OUTPUT, "HELLO!"


class SplitCollection(ez.Collection):
    INPUT1 = ez.InputStream(int)
    INPUT2 = ez.InputStream(int)

    LOG = DebugLog()

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.INPUT1, self.LOG.INPUT),
            (self.INPUT2, self.LOG.INPUT),
        )


class SplitSystem(ez.Collection):
    SOURCE = Source()
    SPLIT = SplitCollection()
    TERM = TerminateTest()

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.SOURCE.OUTPUT, self.TERM.INPUT),
            (self.SOURCE.OUTPUT, self.SPLIT.INPUT1),
            (self.SOURCE.OUTPUT, self.SPLIT.INPUT2),
        )


if __name__ == "__main__":
    ez.run(SYSTEM = SplitSystem())
