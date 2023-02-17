from dataclasses import dataclass, replace

from ezmsg.util.messages import AxisArray, DimensionalAxis

import ezmsg.core as ez
import numpy as np
import numpy.typing as npt

from typing import (
    AsyncGenerator,
    Optional,
)

@dataclass( frozen = True )
class DownsampleSettingsMessage:
    axis: Optional[str] = None
    factor: int = 1


class DownsampleSettings(DownsampleSettingsMessage, ez.Settings):
    ...


class DownsampleState(ez.State):
    cur_settings: DownsampleSettingsMessage
    s_idx: int = 0


class Downsample(ez.Unit):

    SETTINGS: DownsampleSettings
    STATE: DownsampleState

    INPUT_SETTINGS = ez.InputStream(DownsampleSettingsMessage)
    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    def initialize(self) -> None:
        self.STATE.cur_settings = self.SETTINGS

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: DownsampleSettingsMessage) -> None:
        self.STATE.cur_settings = msg

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_signal(self, msg: AxisArray) -> AsyncGenerator:

        if self.STATE.cur_settings.factor < 1:
            raise ValueError("Downsample factor must be at least 1 (no downsampling)")

        axis_name = self.STATE.cur_settings.axis
        if axis_name is None:
            axis_name = msg.dims[0]
        axis = msg.axes.get(axis_name, None)
        axis_idx = msg.get_axis_num(axis_name)

        samples = np.arange(msg.data.shape[axis_idx]) + self.STATE.s_idx
        samples = samples % self.STATE.cur_settings.factor
        self.STATE.s_idx = samples[-1] + 1

        pub_samples = np.where(samples == 0)[0]

        if len(pub_samples) != 0:

            out_axes = msg.axes
            if isinstance(axis, DimensionalAxis):
                axis.gain *= self.STATE.cur_settings.factor
                out_axes[axis_name] = axis
            
            out_coords = msg.coords
            for coord in out_coords.values():
                if axis_name in coord.axes:
                    caxis_idx = coord.get_axis_num(axis_name)
                    coord.data = grab(coord.data, pub_samples, caxis_idx)

            down_data = grab(msg.data, pub_samples, axis_idx)

            out_msg = replace(msg, data = down_data, axes = out_axes, coords = out_coords)
            out_msg.axes = out_axes

            yield ( self.OUTPUT_SIGNAL, out_msg )

def grab( data: npt.NDArray, index: npt.ArrayLike, axis_idx: int ):
    sl = [index if d == axis_idx else slice(None) for d in range(data.ndim)]
    return data[tuple(sl)]