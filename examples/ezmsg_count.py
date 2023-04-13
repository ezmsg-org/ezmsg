import asyncio
import array
from dataclasses import dataclass
import ezmsg.core as ez

from ezmsg.util.debuglog import DebugLog, DebugLogSettings
from ezmsg.util.terminate import TerminateOnTimeout as TerminateTest

from typing import AsyncGenerator


@dataclass
class CountMessage:
    value: int
    arr: array.array


class CountSettings(ez.Settings):
    num_msgs: int = 20


class Count(ez.Unit):
    SETTINGS: CountSettings

    OUTPUT = ez.OutputStream(int)

    @ez.publisher(OUTPUT)
    async def count(self) -> AsyncGenerator:
        count = 0
        while True:
            await asyncio.sleep(0.1)
            yield self.OUTPUT, CountMessage(
                value=count, arr=array.array("b", [0x00] * (2**count))
            )
            count = count + 1

            if count >= self.SETTINGS.num_msgs:
                raise ez.Complete


class CountSystem(ez.Collection):
    COUNT = Count()
    TERM = TerminateTest()

    SUB1 = DebugLog(DebugLogSettings(name="SUB1"))
    SUB2 = DebugLog(DebugLogSettings(name="SUB2"))

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.COUNT.OUTPUT, self.SUB1.INPUT),
            (self.COUNT.OUTPUT, self.SUB2.INPUT),
            (self.COUNT.OUTPUT, self.TERM.INPUT),
        )

    def process_components(self):
        return (self.COUNT, self.SUB1, self.SUB2)


if __name__ == "__main__":
    # import multiprocessing
    # multiprocessing.set_start_method('spawn', force=True)

    system = CountSystem()
    ez.run(SYSTEM = system)
