import enum

from dataclasses import dataclass

import numpy as np
import ezmsg.core as ez

from ezmsg.util.messages.axisarray import AxisArray

from typing import Optional, AsyncGenerator


class WindowFunction(enum.Enum):
    NONE = enum.auto()
    HAMMING = enum.auto()
    HANNING = enum.auto()
    BARTLETT = enum.auto()
    BLACKMAN = enum.auto()


WINDOWS = {
    WindowFunction.NONE: np.ones,
    WindowFunction.HAMMING: np.hamming,
    WindowFunction.HANNING: np.hanning,
    WindowFunction.BARTLETT: np.bartlett,
    WindowFunction.BLACKMAN: np.blackman
}

class SpectralTransform(enum.Enum):
    RAW_COMPLEX = enum.auto()
    REL_POWER = enum.auto()
    REL_DB = enum.auto()


class SpectralOutput(enum.Enum):
    FULL = enum.auto()
    POSITIVE = enum.auto()
    NEGATIVE = enum.auto()


@dataclass
class SpectrumSettingsMessage:
    axis: Optional[str] = None
    # n: Optional[int] = None # n parameter for fft
    out_axis: Optional[str] = 'freq' # If none; don't change dim name
    window: WindowFunction = WindowFunction.HAMMING
    transform: SpectralTransform = SpectralTransform.REL_DB
    output: SpectralOutput = SpectralOutput.POSITIVE


class SpectrumSettings(ez.Settings, SpectrumSettingsMessage):
    ...


class SpectrumState(ez.State):
    cur_settings: SpectrumSettingsMessage


class Spectrum(ez.Unit):
    SETTINGS: SpectrumSettings
    STATE: SpectrumState

    INPUT_SETTINGS = ez.InputStream(SpectrumSettingsMessage)
    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    def initialize(self) -> None:
        self.STATE.cur_settings = self.SETTINGS

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: SpectrumSettingsMessage):
        self.STATE.cur_settings = msg

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_data(self, message: AxisArray) -> AsyncGenerator:

        axis_name = self.STATE.cur_settings.axis
        if axis_name is None:
            axis_name = message.dims[0]
        axis_idx = message.get_axis_idx(axis_name)
        axis = message.get_axis(axis_name)

        spectrum = np.moveaxis(message.data, axis_idx, -1)

        n_time = message.data.shape[axis_idx]
        window = WINDOWS[self.STATE.cur_settings.window](n_time)

        spectrum = np.fft.fft(spectrum*window) / n_time
        freqs = np.fft.fftfreq(n_time, d = axis.gain)
        spectrum = np.moveaxis(spectrum, axis_idx, -1)

        if self.STATE.cur_settings.transform is not SpectralTransform.RAW_COMPLEX:

            scale = np.sum(window ** 2.0) / axis.gain
            spectrum = (2.0 * (np.abs(spectrum) ** 2.0)) / scale

            if self.STATE.cur_settings.transform is SpectralTransform.REL_DB:
                spectrum = 10 * np.log10(spectrum)
        
        if self.STATE.cur_settings.output is SpectralOutput.POSITIVE:
            ...
        elif self.STATE.cur_settings.output is SpectralOutput.NEGATIVE:
            ...

        # AXIS

        # AxisArray()
        
        yield self.OUTPUT_SIGNAL, None


from ezmsg.sigproc.synth import (
    Oscillator, 
    OscillatorSettings, 
    PinkNoise, 
    PinkNoiseSettings,
    Clock,
    ClockSettings,
    Add
)

from ezmsg.sigproc.window import Window, WindowSettings
from ezmsg.testing.debuglog import DebugLog

class SpectralTestSettings(ez.Settings):
    fs: float = 500.0 # Hz
    n_time: int = 100
    alpha: float = 10.5 # Hz

class SpectralTest(ez.Collection):
    SETTINGS: SpectralTestSettings

    CLOCK = Clock()
    NOISE = PinkNoise()
    OSC = Oscillator()
    ADD = Add()
    WINDOW = Window()
    SPECT = Spectrum()
    LOG = DebugLog()

    def configure(self) -> None:

        self.CLOCK.apply_settings(
            ClockSettings(
                dispatch_rate = self.SETTINGS.fs / self.SETTINGS.n_time
            )
        )

        self.OSC.apply_settings(
            OscillatorSettings( 
                n_time = self.SETTINGS.n_time, 
                fs = self.SETTINGS.fs, 
                n_ch = 4, 
                dispatch_rate = 'ext_clock', 
                freq = 10.5
            )
        )

        self.NOISE.apply_settings(
            PinkNoiseSettings(
                n_time = self.SETTINGS.n_time,
                fs = self.SETTINGS.fs,
                n_ch = 4,
                dispatch_rate = 'ext_clock'
            )
        )

        self.WINDOW.apply_settings(
            WindowSettings(
                axis = 'time',
                window_dur = 2.0,
                window_shift = 0.5
            )
        )

        self.SPECT.apply_settings(
            SpectrumSettings(axis = 'time')
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.CLOCK.OUTPUT_CLOCK, self.OSC.INPUT_CLOCK),
            (self.CLOCK.OUTPUT_CLOCK, self.NOISE.INPUT_CLOCK),
            (self.OSC.OUTPUT_SIGNAL, self.ADD.INPUT_SIGNAL_A),
            (self.NOISE.OUTPUT_SIGNAL, self.ADD.INPUT_SIGNAL_B),
            (self.ADD.OUTPUT_SIGNAL, self.WINDOW.INPUT_SIGNAL),
            (self.WINDOW.OUTPUT_SIGNAL, self.SPECT.INPUT_SIGNAL),
            (self.SPECT.OUTPUT_SIGNAL, self.LOG.INPUT)
        )

if __name__ == '__main__':
    test = SpectralTest()
    ez.run(test)