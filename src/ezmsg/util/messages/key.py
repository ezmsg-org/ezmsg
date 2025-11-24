from collections.abc import AsyncGenerator, Generator
import traceback

import numpy as np

import ezmsg.core as ez
from .axisarray import AxisArray, replace
from ..generator import consumer, GenState


@consumer
def set_key(key: str = "") -> Generator[AxisArray, AxisArray, None]:
    """
    Set the key of an AxisArray.

    :param key: The string to set as the key.
    :type key: str
    :return: A primed generator object ready to yield an AxisArray with the key set for each .send(axis_array).
    :rtype: collections.abc.Generator[AxisArray, AxisArray, None]
    """
    # State variables
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out = AxisArray(np.array([]), dims=[""])

    while True:
        axis_arr_in = yield axis_arr_out

        axis_arr_out = replace(axis_arr_in, key=key)


class KeySettings(ez.Settings):
    """
    Settings for key manipulation units.

    Configuration for setting or filtering AxisArray keys.

    :param key: The string to set as the key.
    :type key: str
    """

    key: str = ""


class SetKey(ez.Unit):
    """
    Unit for setting the key of incoming AxisArray messages.

    Modifies the key field of AxisArray messages while preserving all other data.
    Uses zero-copy operations for efficient processing.
    """

    STATE = GenState
    SETTINGS = KeySettings

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)
    INPUT_SETTINGS = ez.InputStream(ez.Settings)

    async def initialize(self) -> None:
        """
        Initialize the SetKey unit.

        Sets up the generator for key modification operations.
        """
        self.construct_generator()

    def construct_generator(self):
        """
        Construct the key-setting generator with current settings.

        Creates a new set_key generator instance using the unit's key setting.
        """
        self.STATE.gen = set_key(key=self.SETTINGS.key)

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: ez.Settings) -> None:
        """
        Handle incoming settings updates.

        :param msg: New settings to apply.
        :type msg: ez.Settings
        """
        self.apply_settings(msg)
        self.construct_generator()

    @ez.subscriber(INPUT_SIGNAL, zero_copy=True)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_message(self, message: AxisArray) -> AsyncGenerator:
        """
        Process incoming AxisArray messages and set their keys.

        Uses zero-copy operations to efficiently modify the key field while
        preserving all other data.

        :param message: Input AxisArray to modify.
        :type message: AxisArray
        :return: Async generator yielding AxisArray with modified key.
        :rtype: collections.abc.AsyncGenerator
        """
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

    Only passes through AxisArray messages whose key matches the configured key setting.
    Uses zero-copy operations for efficient filtering.

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
    async def on_message(self, message: AxisArray) -> AsyncGenerator:
        """
        Filter incoming AxisArray messages based on their key.

        Only yields messages whose key matches the configured filter key.
        Uses minimal 'touch' to prevent unnecessary deep copying by the framework.

        :param message: Input AxisArray to filter.
        :type message: AxisArray
        :return: Async generator yielding filtered AxisArray messages.
        :rtype: collections.abc.AsyncGenerator
        """
        if message.key == self.SETTINGS.key:
            # Minimal 'touch' to prevent deepcopy by framework
            out = replace(message, key=message.key)
            yield self.OUTPUT_SIGNAL, out
