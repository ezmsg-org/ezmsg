from dataclasses import replace
import traceback
import typing

import numpy as np

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.generator import consumer, GenState


@consumer
def modify_axis(
    name_map: typing.Optional[typing.Dict[str, str]] = None
) -> typing.Generator[AxisArray, AxisArray, None]:
    # State variables
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out = AxisArray(np.array([]), dims=[""])

    while True:
        axis_arr_in = yield axis_arr_out

        if name_map is not None:
            new_dims = [name_map.get(old_k, old_k) for old_k in axis_arr_in.dims]
            new_axes = {name_map.get(old_k, old_k): v for old_k, v in axis_arr_in.axes.items()}
            axis_arr_out = replace(axis_arr_in, dims=new_dims, axes=new_axes)
        else:
            axis_arr_out = axis_arr_in


class ModifyAxisSettings(ez.Settings):
    name_map: typing.Optional[typing.Dict[str, str]] = None


class ModifyAxis(ez.Unit):
    STATE: GenState
    SETTINGS: ModifyAxisSettings

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)
    INPUT_SETTINGS = ez.InputStream(ez.Settings)

    def initialize(self) -> None:
        self.construct_generator()

    def construct_generator(self):
        self.STATE.gen = modify_axis(name_map=self.SETTINGS.name_map)

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
            ez.logger.debug(f"Generator closed in {self.address}")
        except Exception:
            ez.logger.info(traceback.format_exc())
