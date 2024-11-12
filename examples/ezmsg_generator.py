"""
This ezmsg example showcases a design pattern where core computational
    logic is encapsulated within a Python generator. This approach is
    beneficial for several reasons:

1. Reusability: By separating the processing logic into a generator, the same code can
    be used for both offline processing and within an ezmsg real-time system. This
    allows for greater code reuse and consistency across different execution contexts.

2. Testability: Generators can be easily unit tested with various inputs, making it
    easier to verify the processing logic independently of the ezmsg framework.

3. Flexibility: Generators yield control back to the caller after each iteration,
    which fits well with ezmsg's asynchronous message handling.

4. Simplicity: This pattern simplifies the unit's logic as the state is managed
    implicitly by the generator, leading to code that is easier to read and maintain.

Overall, this pattern enables developers to write units in ezmsg that are portable,
    testable, and maintainable, supporting both real-time message processing and offline
    data transformation with the same underlying logic.
"""

import asyncio
import ezmsg.core as ez
import numpy as np
from typing import Any, Generator
from ezmsg.util.messages.axisarray import AxisArray, replace
from ezmsg.util.debuglog import DebugLog
from ezmsg.util.gen_to_unit import gen_to_unit
from ezmsg.util.generator import consumer, compose, Gen


@consumer
def pow(n: float) -> Generator[AxisArray, AxisArray, None]:
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out = AxisArray(np.array([]), dims=[""])
    while True:
        axis_arr_in = yield axis_arr_out
        axis_arr_out = replace(axis_arr_in, data=axis_arr_in.data**n)


class PowSettings(ez.Settings):
    n: float


class Pow(Gen):
    SETTINGS = PowSettings

    def construct_generator(self):
        self.STATE.gen = pow(self.SETTINGS.n)


@consumer
def add(n: float) -> Generator[AxisArray, AxisArray, None]:
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out = AxisArray(np.array([]), dims=[""])
    while True:
        axis_arr_in = yield axis_arr_out
        axis_arr_out = replace(axis_arr_in, data=axis_arr_in.data + n)


class AddSettings(ez.Settings):
    n: float


class Add(Gen):
    SETTINGS = AddSettings

    def construct_generator(self):
        self.STATE.gen = add(self.SETTINGS.n)


if __name__ == "__main__":
    # This example will show the offline processing use-case of the generator pattern,
    # illustrating how the same generator logic used in ezmsg systems can be applied
    # to process data in a standalone, batch-processing environment.
    print("Running offline example")
    pipeline = compose(
        pow(3),
        add(12),
        pow(0.5),
    )

    _in = AxisArray(np.arange(100), dims=["data"])
    _out = pipeline(_in)
    print(f"Input: {_in.data[::10]}\nOutput: {_out.data[::10]}")

    # This example will show the inheritance-based ezmsg implementation with generators,
    # highlighting how to integrate generator-based processing logic within ezmsg Units
    # through object-oriented subclassing to manage stateful computations.
    print("Running online example")

    class MessageSender(ez.Unit):
        OUTPUT = ez.OutputStream(Any)

        @ez.publisher(OUTPUT)
        async def send_data(self):
            yield self.OUTPUT, _in
            await asyncio.sleep(1)
            raise ez.NormalTermination

    class GenOneExample(ez.Collection):
        SEND = MessageSender()
        POW_3 = Pow()
        ADD_12 = Add()
        POW_0_5 = Pow()
        LOG_IN = DebugLog()
        LOG_OUT = DebugLog()

        def configure(self) -> None:
            self.POW_3.apply_settings(PowSettings(3))
            self.ADD_12.apply_settings(AddSettings(12))
            self.POW_0_5.apply_settings(PowSettings(0.5))

        def network(self):
            return (
                (self.SEND.OUTPUT, self.POW_3.INPUT),
                (self.SEND.OUTPUT, self.LOG_IN.INPUT),
                (self.POW_3.OUTPUT, self.ADD_12.INPUT),
                (self.ADD_12.OUTPUT, self.POW_0_5.OUTPUT),
                (self.POW_0_5.OUTPUT, self.LOG_OUT.INPUT),
            )

    ez.run(SYSTEM=GenOneExample())

    # This example will show the type-hint based introspection to construct ezmsg Units
    # at runtime, which simplifies the integration of generators into Units, reducing
    # setup boilerplate, though it omits compile-time type checking in development.
    print("Running ezmsg system with gen-to-unit")

    AutoAddSettings, AutoAdd = gen_to_unit(add)
    AutoPowSettings, AutoPow = gen_to_unit(pow)

    class GenTwoExample(ez.Collection):
        SEND = MessageSender()
        POW_3 = AutoPow()
        ADD_12 = AutoAdd()
        POW_0_5 = AutoPow()
        LOG_IN = DebugLog()
        LOG_OUT = DebugLog()

        def configure(self) -> None:
            self.POW_3.apply_settings(AutoPowSettings(3))
            self.ADD_12.apply_settings(AutoAddSettings(12))
            self.POW_0_5.apply_settings(AutoPowSettings(0.5))

        def network(self):
            return (
                (self.SEND.OUTPUT, self.POW_3.INPUT),
                (self.SEND.OUTPUT, self.LOG_IN.INPUT),
                (self.POW_3.OUTPUT, self.ADD_12.INPUT),
                (self.ADD_12.OUTPUT, self.POW_0_5.OUTPUT),
                (self.POW_0_5.OUTPUT, self.LOG_OUT.INPUT),
            )

    ez.run(SYSTEM=GenTwoExample())
