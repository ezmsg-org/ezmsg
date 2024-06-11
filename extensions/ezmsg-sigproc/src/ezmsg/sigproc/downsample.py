import copy
from dataclasses import replace
import traceback
import typing

import numpy as np

from ezmsg.util.messages.axisarray import AxisArray, slice_along_axis
from ezmsg.util.generator import consumer
import ezmsg.core as ez


@consumer
def downsample(
        axis: typing.Optional[str] = None, factor: int = 1
) -> typing.Generator[AxisArray, AxisArray, None]:
    """
    Construct a generator that yields a downsampled version of the data .send() to it.
    Downsampled data simply comprise every `factor`th sample.
    This should only be used following appropriate lowpass filtering.
    If your pipeline does not already have lowpass filtering then consider
    using the :obj:`Decimate` collection instead.

    Args:
        axis: The name of the axis along which to downsample.
        factor: Downsampling factor.

    Returns:
        A primed generator object ready to receive a `.send(axis_array)`
        and yields the downsampled data.
        Note that if a send chunk does not have sufficient samples to reach the
        next downsample interval then `None` is yielded.

    """
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out = AxisArray(np.array([]), dims=[""])

    # state variables
    s_idx: int = 0  # Index of the next msg's first sample into the virtual rotating ds_factor counter.
    template: typing.Optional[AxisArray] = None

    while True:
        axis_arr_in = yield axis_arr_out

        if axis is None:
            axis = axis_arr_in.dims[0]
        axis_info = axis_arr_in.get_axis(axis)
        axis_idx = axis_arr_in.get_axis_idx(axis)

        if template is None:
            # Reset state variables
            s_idx = 0
            # Template used as a convenient struct for holding metadata and size-zero data.
            template = copy.deepcopy(axis_arr_in)
            template.axes[axis].gain *= factor
            template.data = slice_along_axis(template.data, slice(None, 0, None), axis=axis_idx)

        n_samples = axis_arr_in.data.shape[axis_idx]
        samples = np.arange(s_idx, s_idx + n_samples) % factor
        if n_samples > 0:
            # Update state for next iteration.
            s_idx = samples[-1] + 1

        pub_samples = np.where(samples == 0)[0]
        if len(pub_samples) > 0:
            # Update the template directly, because we want
            #  future size-0 msgs to have approx. correct offset.
            update_ax = template.axes[axis]
            update_ax.offset = axis_info.offset + axis_info.gain * pub_samples[0].item()
            axis_arr_out = replace(
                template,
                data=slice_along_axis(axis_arr_in.data, pub_samples, axis=axis_idx),
                axes={**template.axes, axis: replace(update_ax, offset=update_ax.offset)}
            )
            template.axes[axis].offset = axis_info.offset + axis_info.gain * (n_samples + 1)
        else:
            # This iteration did not yield any samples. Return a size-0 array
            #  with time offset expected for _next_ sample.
            axis_arr_out = template


class DownsampleSettings(ez.Settings):
    """
    Settings for :obj:`Downsample` node.
    See :obj:`downsample` documentation for a description of the parameters.
    """
    axis: typing.Optional[str] = None
    factor: int = 1


class DownsampleState(ez.State):
    cur_settings: DownsampleSettings
    gen: typing.Generator


class Downsample(ez.Unit):
    """
    :obj:`Unit` for :obj:`downsample`.
    """
    SETTINGS: DownsampleSettings
    STATE: DownsampleState

    INPUT_SETTINGS = ez.InputStream(DownsampleSettings)
    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    def construct_generator(self):
        self.STATE.gen = downsample(axis=self.STATE.cur_settings.axis, factor=self.STATE.cur_settings.factor)

    def initialize(self) -> None:
        self.STATE.cur_settings = self.SETTINGS
        self.construct_generator()

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: DownsampleSettings) -> None:
        self.STATE.cur_settings = msg
        self.construct_generator()

    @ez.subscriber(INPUT_SIGNAL, zero_copy=True)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_signal(self, msg: AxisArray) -> typing.AsyncGenerator:
        if self.STATE.cur_settings.factor < 1:
            raise ValueError("Downsample factor must be at least 1 (no downsampling)")

        try:
            out_msg = self.STATE.gen.send(msg)
            if out_msg.data.size > 0:
                yield self.OUTPUT_SIGNAL, out_msg
        except (StopIteration, GeneratorExit):
            ez.logger.debug(f"Downsample closed in {self.address}")
        except Exception:
            ez.logger.info(traceback.format_exc())
