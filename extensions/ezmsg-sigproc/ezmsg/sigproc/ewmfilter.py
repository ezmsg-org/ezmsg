import asyncio
from dataclasses import field, replace

import ezmsg.core as ez
import numpy as np

from .window import Window, WindowSettings
from .messages import TSMessage

from typing import (
    AsyncGenerator,
    Optional
)


class EWMSettings(ez.Settings):
    zero_offset: bool = True  # If true, we assume zero DC offset


class EWMState(ez.State):
    last_signal: Optional[TSMessage] = None
    buffer_queue: "asyncio.Queue[ TSMessage ]" = field(default_factory=asyncio.Queue)

    pows: Optional[np.ndarray] = None
    scale_arr: Optional[np.ndarray] = None
    pw0: Optional[np.ndarray] = None


class EWM(ez.Unit):
    """
    Exponentially Weighted Moving Average Standardization

    References https://stackoverflow.com/a/42926270
    FIXME: Assumes time axis is on dimension 0
    """
    SETTINGS: EWMSettings
    STATE: EWMState

    INPUT_SIGNAL = ez.InputStream(TSMessage)
    INPUT_BUFFER = ez.InputStream(TSMessage)
    OUTPUT_SIGNAL = ez.OutputStream(TSMessage)

    @ez.subscriber(INPUT_SIGNAL)
    async def on_signal(self, message: TSMessage) -> None:
        self.STATE.last_signal = message

    @ez.subscriber(INPUT_BUFFER)
    async def on_buffer(self, message: TSMessage) -> None:
        buffer_len = message.n_time
        block_len = self.STATE.last_signal.n_time
        window = buffer_len - block_len

        alpha = 2 / (window + 1.0)
        alpha_rev = 1 - alpha

        self.STATE.pows = alpha_rev ** (np.arange(buffer_len + 1))
        self.STATE.scale_arr = 1 / self.STATE.pows[:-1]
        self.STATE.pw0 = alpha * alpha_rev ** (buffer_len - 1)

        self.STATE.buffer_queue.put_nowait(message)

    @ez.publisher(OUTPUT_SIGNAL)
    async def sync_output(self) -> AsyncGenerator:

        def ewma(data: np.ndarray) -> np.ndarray:
            """ Assumes time axis is dim 0 """
            mult: np.ndarray = self.STATE.scale_arr[:, np.newaxis] * data * self.STATE.pw0
            out = self.STATE.scale_arr[::-1, np.newaxis] * mult.cumsum(axis=0)

            if not self.SETTINGS.zero_offset:
                out = (data[0, :, np.newaxis] * self.STATE.pows[1:]).T + out

            return out

        while True:
            buffer = await self.STATE.buffer_queue.get()  # includes signal
            signal = self.STATE.last_signal  # necessarily not "None" once there's a buffer.

            block_len = signal.n_time

            mean = ewma(buffer.data)
            std: np.ndarray = ((buffer.data - mean) ** 2.0)
            std = ewma(std)

            standardized: np.ndarray = (buffer.data - mean) / np.sqrt(std).clip(1e-4)

            yield (
                self.OUTPUT_SIGNAL, 
                replace(signal, data=standardized[-block_len:, ...])
            )


class EWMFilterSettings(ez.Settings):
    history_dur: float  # previous data to accumulate for standardization


class EWMFilter(ez.Collection):

    SETTINGS: EWMFilterSettings

    INPUT_SIGNAL = ez.InputStream(TSMessage)
    OUTPUT_SIGNAL = ez.OutputStream(TSMessage)

    WINDOW = Window()
    EWM = EWM()

    def configure(self) -> None:
        self.WINDOW.apply_settings(
            WindowSettings(
                window_dur=self.SETTINGS.history_dur,
                window_shift=None  # 1:1 mode
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.INPUT_SIGNAL, self.WINDOW.INPUT_SIGNAL),
            (self.WINDOW.OUTPUT_SIGNAL, self.EWM.INPUT_BUFFER),
            (self.INPUT_SIGNAL, self.EWM.INPUT_SIGNAL),
            (self.EWM.OUTPUT_SIGNAL, self.OUTPUT_SIGNAL)
        )
