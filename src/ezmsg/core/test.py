# tutorial_pipeline.py
import ezmsg.core as ez
from dataclasses import dataclass
from collections.abc import AsyncGenerator


class CountSettings(ez.Settings):
    iterations: int


@dataclass
class CountMessage:
    value: int


class Count(ez.Unit):
    SETTINGS = CountSettings

    OUTPUT_COUNT = ez.OutputStream(CountMessage)

    @ez.publisher(OUTPUT_COUNT)
    async def count(self) -> AsyncGenerator:
        count = 0
        while count < self.SETTINGS.iterations:
            yield self.OUTPUT_COUNT, CountMessage(value=count)
            count = count + 1

        raise ez.NormalTermination


class AddOne(ez.Unit):
    INPUT_COUNT = ez.InputStream(CountMessage)
    OUTPUT_PLUS_ONE = ez.OutputStream(CountMessage)

    @ez.subscriber(INPUT_COUNT)
    @ez.publisher(OUTPUT_PLUS_ONE)
    async def on_message(self, message) -> AsyncGenerator:
        yield self.OUTPUT_PLUS_ONE, CountMessage(value=message.value + 1)


class PrintValue(ez.Unit):
    INPUT = ez.InputStream(CountMessage)

    @ez.subscriber(INPUT)
    async def on_message(self, message) -> None:
        print(message.value)


components = {
    "COUNT": Count(settings=CountSettings(iterations=10)),
    "ADD_ONE": AddOne(),
    "PRINT": PrintValue(),
}
connections = (
    (components["COUNT"].OUTPUT_COUNT, components["ADD_ONE"].INPUT_COUNT),
    (components["ADD_ONE"].OUTPUT_PLUS_ONE, components["PRINT"].INPUT),
)
ez.run(components=components, connections=connections)
