from dataclasses import replace

from ezmsg.util.messages.axisarray import AxisArray

import ezmsg.core as ez
import numpy as np

from typing import (
    AsyncGenerator,
    Optional,
)


class DownsampleSettings(ez.Settings):
    axis: Optional[str] = None
    factor: int = 1


class DownsampleState(ez.State):
    cur_settings: DownsampleSettings
    s_idx: int = 0


class Downsample(ez.Unit):
    SETTINGS: DownsampleSettings
    STATE: DownsampleState

    INPUT_SETTINGS = ez.InputStream(DownsampleSettings)
    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    def initialize(self) -> None:
        self.STATE.cur_settings = self.SETTINGS

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: DownsampleSettings) -> None:
        self.STATE.cur_settings = msg

    @ez.subscriber(INPUT_SIGNAL, zero_copy=True)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_signal(self, msg: AxisArray) -> AsyncGenerator:
        if self.STATE.cur_settings.factor < 1:
            raise ValueError("Downsample factor must be at least 1 (no downsampling)")

        axis_name = self.STATE.cur_settings.axis
        if axis_name is None:
            axis_name = msg.dims[0]
        axis = msg.get_axis(axis_name)
        axis_idx = msg.get_axis_idx(axis_name)

        samples = np.arange(msg.data.shape[axis_idx]) + self.STATE.s_idx
        samples = samples % self.STATE.cur_settings.factor
        self.STATE.s_idx = samples[-1] + 1

        pub_samples = np.where(samples == 0)[0]
        if len(pub_samples) != 0:
            new_axes = {ax_name: msg.get_axis(ax_name) for ax_name in msg.dims}
            new_offset = axis.offset + (axis.gain * pub_samples[0].item())
            new_gain = axis.gain * self.STATE.cur_settings.factor
            new_axes[axis_name] = replace(axis, gain=new_gain, offset=new_offset)
            down_data = np.take(msg.data, pub_samples, axis_idx)
            out_msg = replace(msg, data=down_data, dims=msg.dims, axes=new_axes)
            yield self.OUTPUT_SIGNAL, out_msg
