from dataclasses import dataclass, replace

import ezmsg.core as ez
import numpy as np

from ezmsg.util.messages import view2d, shape2d
from ezmsg.sigproc.messages import TSMessage

from typing import (
    AsyncGenerator,
    Optional,
    Tuple
)

@dataclass( frozen = True )
class WindowSettingsMessage:
    window_dur: Optional[float] = None  # Sec.  If "None" -- passthrough; window_shift is ignored.
    window_shift: Optional[float] = None  # Sec.  If "None", activate "1:1 mode"


class WindowSettings(WindowSettingsMessage, ez.Settings):
    ...


class WindowState(ez.State):
    cur_settings: Optional[ WindowSettingsMessage ] = None

    samp_shape: Optional[Tuple[int, ...]] = None  # Shape of individual sample
    out_fs: Optional[float] = None
    buffer: Optional[np.ndarray] = None
    window_samples: Optional[int] = None
    window_shift_samples: Optional[int] = None


class Window(ez.Unit):

    STATE: WindowState
    SETTINGS: WindowSettings

    INPUT_SIGNAL = ez.InputStream(TSMessage)
    OUTPUT_SIGNAL = ez.OutputStream(TSMessage)
    INPUT_SETTINGS = ez.InputStream(WindowSettingsMessage)

    def initialize(self) -> None:
        self.STATE.cur_settings = self.SETTINGS

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: WindowSettingsMessage) -> None:
        self.STATE.cur_settings = msg
        self.STATE.out_fs = None # This should trigger a reallocation

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_signal(self, msg: TSMessage) -> AsyncGenerator:

        if self.STATE.cur_settings.window_dur is None:
            yield self.OUTPUT_SIGNAL, msg
            return

        # Create a view of data with time axis at dim 0
        time_view = np.moveaxis(msg.data, msg.time_dim, 0)
        samp_shape = time_view.shape[1:]

        if (self.STATE.samp_shape != samp_shape) or (self.STATE.out_fs != msg.fs):
            # Pre(re?)allocate window data
            self.STATE.samp_shape = samp_shape
            self.STATE.out_fs = msg.fs
            self.STATE.window_samples = int(
                self.STATE.cur_settings.window_dur * self.STATE.out_fs
            )

            if self.STATE.cur_settings.window_shift is not None:
                self.STATE.window_shift_samples = int(
                    self.STATE.out_fs * self.STATE.cur_settings.window_shift
                )

            if self.STATE.buffer is None:
                self.STATE.buffer = np.zeros(tuple([self.STATE.window_samples] + list(self.STATE.samp_shape)))
            elif self.STATE.buffer.shape[0] > self.STATE.window_samples:
                self.STATE.buffer = self.STATE.buffer[:self.STATE.window_samples, ...]
            elif self.STATE.buffer.shape[0] < self.STATE.window_samples:
                extra_samples = self.STATE.window_samples - self.STATE.buffer.shape[0]
                extra_samples = np.zeros(tuple([extra_samples] + list(self.STATE.samp_shape)))
                self.STATE.buffer = np.concatenate((extra_samples, self.STATE.buffer), axis=0)

        # Currently we just concatenate the new time samples and clip the output
        # np.roll actually returns a copy, and there's no way to construct a
        # rolling view of the data.  In current numpy implementations, np.concatenate
        # is generally faster than np.roll and slicing anyway, but this could still
        # be a performance bottleneck for large memory arrays.
        self.STATE.buffer = np.concatenate((self.STATE.buffer, time_view), axis=0)

        if self.STATE.window_shift_samples is None:  # one-to-one mode

            self.STATE.buffer = self.STATE.buffer[-self.STATE.window_samples:, ...]

            # Finally, move time axis back into location before yielding
            out_view = np.moveaxis(self.STATE.buffer, 0, msg.time_dim)
            yield (self.OUTPUT_SIGNAL, replace(msg, data=out_view))

        else:  # slightly more complicated window shifting

            yieldable_size = self.STATE.window_samples + self.STATE.window_shift_samples
            while self.STATE.buffer.shape[0] >= yieldable_size:

                # Yield if possible
                out_view = self.STATE.buffer[:self.STATE.window_samples, ...]
                out_view = np.moveaxis(out_view, 0, msg.time_dim)
                yield (self.OUTPUT_SIGNAL, replace(msg, data=out_view))

                # Shift window
                self.STATE.buffer = self.STATE.buffer[self.STATE.window_shift_samples:, ...]
