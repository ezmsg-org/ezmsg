from dataclasses import dataclass, replace

import ezmsg.core as ez
import numpy as np

from .messages import TSMessage as TimeSeriesMessage

from typing import (
    AsyncGenerator,
    Optional,
)

@dataclass( frozen = True )
class DownsampleSettingsMessage:
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
    INPUT_SIGNAL = ez.InputStream(TimeSeriesMessage)
    OUTPUT_SIGNAL = ez.OutputStream(TimeSeriesMessage)

    def initialize(self) -> None:
        self.STATE.cur_settings = self.SETTINGS

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: DownsampleSettingsMessage) -> None:
        self.STATE.cur_settings = msg

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_signal(self, msg: TimeSeriesMessage) -> AsyncGenerator:

        if self.STATE.cur_settings.factor < 1:
            raise ValueError("Downsample factor must be at least 1 (no downsampling)")

        samples = np.arange(msg.n_time) + self.STATE.s_idx
        samples = samples % self.STATE.cur_settings.factor
        self.STATE.s_idx = samples[-1] + 1

        pub_samples = np.where(samples == 0)[0]

        if len(pub_samples) != 0:

            time_view = np.moveaxis(msg.data, msg.time_dim, 0)
            data_down = time_view[pub_samples, ...]
            data_down = np.moveaxis(data_down, 0, -(msg.time_dim))

            yield (
                self.OUTPUT_SIGNAL,
                replace(
                    msg,
                    data=data_down,
                    fs=msg.fs / self.SETTINGS.factor
                )
            )
