from dataclasses import replace

import ezmsg.core as ez
import numpy as np
import numpy.typing as npt

from ezmsg.util.messages.axisarray import AxisArray

from typing import AsyncGenerator, Optional, Tuple, List


class WindowSettings(ez.Settings):
    axis: Optional[str] = None
    newaxis: Optional[
        str
    ] = None  # Optional new axis for output.  If "None" - no new axes on output
    window_dur: Optional[
        float
    ] = None  # Sec.  If "None" -- passthrough; window_shift is ignored.
    window_shift: Optional[float] = None  # Sec.  If "None", activate "1:1 mode"


class WindowState(ez.State):
    cur_settings: WindowSettings

    samp_shape: Optional[Tuple[int, ...]] = None  # Shape of individual sample
    out_fs: Optional[float] = None
    buffer: Optional[npt.NDArray] = None
    window_samples: Optional[int] = None
    window_shift_samples: Optional[int] = None


class Window(ez.Unit):
    STATE: WindowState
    SETTINGS: WindowSettings

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)
    INPUT_SETTINGS = ez.InputStream(WindowSettings)

    def initialize(self) -> None:
        self.STATE.cur_settings = self.SETTINGS

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: WindowSettings) -> None:
        self.STATE.cur_settings = msg
        self.STATE.out_fs = None  # This should trigger a reallocation

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_signal(self, msg: AxisArray) -> AsyncGenerator:
        if self.STATE.cur_settings.window_dur is None:
            yield self.OUTPUT_SIGNAL, msg
            return

        axis_name = self.STATE.cur_settings.axis
        if axis_name is None:
            axis_name = msg.dims[0]
        axis_idx = msg.get_axis_idx(axis_name)
        axis = msg.get_axis(axis_name)
        fs = 1.0 / axis.gain

        # Create a view of data with time axis at dim 0
        time_view = np.moveaxis(msg.data, axis_idx, 0)
        samp_shape = time_view.shape[1:]

        # Pre(re?)allocate buffer
        window_samples = int(self.STATE.cur_settings.window_dur * fs)
        if (
            (self.STATE.samp_shape != samp_shape)
            or (self.STATE.out_fs != fs)
            or self.STATE.buffer is None
        ):
            self.STATE.buffer = np.zeros(tuple([window_samples] + list(samp_shape)))

        self.STATE.window_samples = window_samples
        self.STATE.samp_shape = samp_shape
        self.STATE.out_fs = fs

        self.STATE.window_shift_samples = None
        if self.STATE.cur_settings.window_shift is not None:
            self.STATE.window_shift_samples = int(
                fs * self.STATE.cur_settings.window_shift
            )

        # Currently we just concatenate the new time samples and clip the output
        # np.roll actually returns a copy, and there's no way to construct a
        # rolling view of the data.  In current numpy implementations, np.concatenate
        # is generally faster than np.roll and slicing anyway, but this could still
        # be a performance bottleneck for large memory arrays.
        self.STATE.buffer = np.concatenate((self.STATE.buffer, time_view), axis=0)

        buffer_offset = np.arange(self.STATE.buffer.shape[0] + time_view.shape[0])
        buffer_offset -= self.STATE.buffer.shape[0] + 1
        buffer_offset = (buffer_offset * axis.gain) + axis.offset

        outputs: List[Tuple[npt.NDArray, float]] = []

        if self.STATE.window_shift_samples is None:  # one-to-one mode
            self.STATE.buffer = self.STATE.buffer[-self.STATE.window_samples :, ...]
            buffer_offset = buffer_offset[-self.STATE.window_samples :]
            outputs.append((self.STATE.buffer, buffer_offset[0]))

        else:
            yieldable_size = self.STATE.window_samples + self.STATE.window_shift_samples
            while self.STATE.buffer.shape[0] >= yieldable_size:
                outputs.append(
                    (
                        self.STATE.buffer[: self.STATE.window_samples, ...],
                        buffer_offset[0],
                    )
                )
                self.STATE.buffer = self.STATE.buffer[
                    self.STATE.window_shift_samples :, ...
                ]
                buffer_offset = buffer_offset[self.STATE.window_shift_samples :]

        for out_view, offset in outputs:
            out_view = np.moveaxis(out_view, 0, axis_idx)

            if (
                self.STATE.cur_settings.newaxis is not None
                and self.STATE.cur_settings.newaxis != self.STATE.cur_settings.axis
            ):
                new_gain = 0.0
                if self.STATE.window_shift_samples is not None:
                    new_gain = axis.gain * self.STATE.window_shift_samples

                out_axis = replace(axis, unit=axis.unit, gain=new_gain, offset=offset)
                out_axes = {**msg.axes, **{self.STATE.cur_settings.newaxis: out_axis}}
                out_dims = [self.STATE.cur_settings.newaxis] + msg.dims
                out_view = out_view[np.newaxis, ...]

                yield self.OUTPUT_SIGNAL, replace(
                    msg, data=out_view, dims=out_dims, axes=out_axes
                )

            else:
                if axis_name in msg.axes:
                    out_axes = msg.axes
                    out_axes[axis_name] = replace(axis, offset=offset)
                    yield self.OUTPUT_SIGNAL, replace(msg, data=out_view, axes=out_axes)
                else:
                    yield self.OUTPUT_SIGNAL, replace(msg, data=out_view)
