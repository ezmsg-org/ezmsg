import traceback
import typing

import numpy as np

import ezmsg.core as ez
from .axisarray import AxisArray, replace
from ..generator import consumer, GenState


@consumer
def set_key(key: str = "") -> typing.Generator[AxisArray, AxisArray, None]:
    """
    Set the key of an AxisArray.

    Args:
        key: The string to set as the key.

    Returns:
        A primed generator object ready to yield an AxisArray with the key set for each .send(axis_array)
    """
    # State variables
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out = AxisArray(np.array([]), dims=[""])

    while True:
        axis_arr_in = yield axis_arr_out

        axis_arr_out = replace(axis_arr_in, key=key)


class KeySettings(ez.Settings):
    key: str = ""


class SetKey(ez.Unit):
    STATE = GenState
    SETTINGS = KeySettings

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)
    INPUT_SETTINGS = ez.InputStream(ez.Settings)

    async def initialize(self) -> None:
        self.construct_generator()

    def construct_generator(self):
        self.STATE.gen = set_key(key=self.SETTINGS.key)

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: ez.Settings) -> None:
        self.apply_settings(msg)
        self.construct_generator()

    @ez.subscriber(INPUT_SIGNAL, zero_copy=True)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_message(self, message: AxisArray) -> typing.AsyncGenerator:
        try:
            ret = self.STATE.gen.send(message)
            if ret is not None:
                yield self.OUTPUT_SIGNAL, ret
        except (StopIteration, GeneratorExit):
            ez.logger.debug(f"SetKey closed in {self.address}")
        except Exception:
            ez.logger.info(traceback.format_exc())


class FilterOnKey(ez.Unit):
    """
    Filter an AxisArray based on its key.
    Note: There is no associated generator method for this Unit because messages that fail the filter
     would still be yielded (as None), which complicates downstream processing. For contexts where filtering
     on key is desired but the ezmsg framework is not used, use normal Python functional programming.
     See https://github.com/ezmsg-org/ezmsg/issues/142#issuecomment-2323110318
    """

    SETTINGS = KeySettings

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    @ez.subscriber(INPUT_SIGNAL, zero_copy=True)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_message(self, message: AxisArray) -> typing.AsyncGenerator:
        if message.key == self.SETTINGS.key:
            # Minimal 'touch' to prevent deepcopy by framework
            out = replace(message, key=message.key)
            yield self.OUTPUT_SIGNAL, out
