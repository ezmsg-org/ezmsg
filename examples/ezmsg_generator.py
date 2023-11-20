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
import traceback
from dataclasses import replace
from typing import Any, AsyncGenerator, Generator, Callable, TypeVar
from typing_extensions import ParamSpec
from functools import wraps, reduce
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.debuglog import DebugLog

# Define type variables for the decorators
P = ParamSpec("P")
R = TypeVar("R")


def consumer(
    func: Callable[P, Generator[R, R, None]]
) -> Callable[P, Generator[R, R, None]]:
    """
    A decorator that primes a generator by advancing it to the first yield statement.

    This is necessary because a generator cannot receive any input through 'send' until it has
    been primed to the point of the first 'yield'. Applying this decorator to a generator function
    ensures that it is immediately ready to accept input.

    Args:
        func: The generator function to be decorated.

    Returns:
        The primed generator ready to accept input.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Generator[R, R, None]:
        gen = func(*args, **kwargs)
        next(gen)  # Prime the generator.
        return gen

    return wrapper


def compose(*funcs):
    """
    Compose a chain of generator functions into a single callable.

    Each generator in the sequence is passed the value from the previous one, starting with the initial value `x`.
    This allows for creating a pipeline of data transformations that are applied in sequence when the returned
    callable is invoked.

    Args:
        *funcs: A variable number of generator functions to be composed.

    Returns:
        A callable that, when called with an initial value, will pass that value through the chain of generators.
    """
    return lambda x: reduce(lambda f, g: g.send(f), list(funcs), x)


# State class to hold the generator instance
class GenState(ez.State):
    gen: Generator


# Abstract Unit class that uses a generator for processing messages
class Gen(ez.Unit):
    STATE: GenState

    INPUT = ez.InputStream(AxisArray)
    OUTPUT = ez.OutputStream(AxisArray)

    def initialize(self) -> None:
        self.construct_generator()

    # Method to be implemented by subclasses to construct the specific generator
    def construct_generator(self):
        raise NotImplementedError

    # Subscriber method that sends incoming messages to the generator and publishes the result
    @ez.subscriber(INPUT)
    @ez.publisher(OUTPUT)
    async def on_message(self, message: AxisArray) -> AsyncGenerator:
        try:
            ret = self.STATE.gen.send(message)
            if ret is not None:
                yield self.OUTPUT, ret
        except (StopIteration, GeneratorExit):
            ez.logger.debug(f"Generator closed in {self.address}")
        except Exception:
            ez.logger.info(traceback.format_exc())


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
    SETTINGS: PowSettings

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
    SETTINGS: AddSettings

    def construct_generator(self):
        self.STATE.gen = add(self.SETTINGS.n)


if __name__ == "__main__":
    print("Running offline example")
    pipeline = compose(
        pow(3),
        add(12),
        pow(0.5),
    )

    _in = AxisArray(np.arange(100), dims=["data"])
    _out = pipeline(_in)
    print(f"Input: {_in.data[::10]}\nOutput: {_out.data[::10]}")

    print("Running online example")

    class MessageSender(ez.Unit):
        OUTPUT = ez.OutputStream(Any)

        @ez.publisher(OUTPUT)
        async def send_data(self):
            yield self.OUTPUT, _in
            await asyncio.sleep(1)
            raise ez.NormalTermination

    send = MessageSender()
    pow_3 = Pow(PowSettings(3))
    add_12 = Add(AddSettings(12))
    pow_0_5 = Pow(PowSettings(0.5))
    log_in = DebugLog()
    log_out = DebugLog()

    ez.run(
        SEND=send,
        POW_3=pow_3,
        ADD_12=add_12,
        POW_0_5=pow_0_5,
        LOG_IN=log_in,
        LOG_OUT=log_out,
        connections=(
            (send.OUTPUT, pow_3.INPUT),
            (send.OUTPUT, log_in.INPUT),
            (pow_3.OUTPUT, add_12.INPUT),
            (add_12.OUTPUT, pow_0_5.INPUT),
            (pow_0_5.OUTPUT, log_out.INPUT),
        ),
    )
