import asyncio
import time
from dataclasses import replace

import ezmsg.core as ez
import numpy as np

from ezmsg.util.messages import AxisArray, time_axis, DimensionalAxis

from .butterworthfilter import ButterworthFilter, ButterworthFilterSettings

from typing import Optional, AsyncGenerator, Union


class CounterSettings(ez.Settings):
    """
    TODO: Adapt this to use ezmsg.util.rate?
    NOTE: This module uses asyncio.sleep to delay appropriately in realtime mode.
    This method of sleeping/yielding execution priority has quirky behavior with
    sub-millisecond sleep periods which may result in unexpected behavior (e.g.
    fs = 2000, n_time = 1, realtime = True -- may result in ~1400 msgs/sec)
    """
    n_time: int  # Number of samples to output per block
    fs: float  # Sampling rate of signal output in Hz
    n_ch: int = 1 # Number of channels to synthesize

    # Message dispatch rate (Hz), 'realtime', or None (fast as possible)
    dispatch_rate: Optional[Union[float, str]] = None

    # If set to an integer, counter will rollover
    mod: Optional[int] = None


class CounterState(ez.State):
    samp: int = 0  # current sample counter


class Counter(ez.Unit):
    """ Generates monotonically increasing counter """

    SETTINGS: CounterSettings
    STATE: CounterState

    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    def initialize(self) -> None:
        if isinstance(self.SETTINGS.dispatch_rate, str) and self.SETTINGS.dispatch_rate != 'realtime':
            raise ValueError(f'Unknown dispatch_rate: {self.SETTINGS.dispatch_rate}')

    @ez.publisher(OUTPUT_SIGNAL)
    async def publish(self) -> AsyncGenerator:

        block_samp = np.arange(self.SETTINGS.n_time)[:, np.newaxis]
        block_dur = self.SETTINGS.n_time / self.SETTINGS.fs

        while True:

            t_samp = block_samp + self.STATE.samp
            self.STATE.samp = t_samp[-1] + 1

            if self.SETTINGS.mod is not None:
                t_samp %= self.SETTINGS.mod
                self.STATE.samp %= self.SETTINGS.mod

            t_samp = np.tile( t_samp, ( 1, self.SETTINGS.n_ch ) )

            out = AxisArray(
                t_samp,
                dims = ['time', 'ch'],
                axes = dict( 
                    time = time_axis(
                        fs = self.SETTINGS.fs, 
                        offset = time.time()
                    )
                )
            )

            yield self.OUTPUT_SIGNAL, out

            if self.SETTINGS.dispatch_rate is not None:
                if isinstance(self.SETTINGS.dispatch_rate, str):
                    if self.SETTINGS.dispatch_rate == 'realtime':
                        await asyncio.sleep(block_dur)
                else:
                    await asyncio.sleep(1.0 / self.SETTINGS.dispatch_rate)


class SinGeneratorSettings(ez.Settings):
    time_axis: Optional[str] = 'time'
    freq: float = 1.0  # Oscillation frequency in Hz
    amp: float = 1.0  # Amplitude
    phase: float = 0.0  # Phase offset (in radians)


class SinGeneratorState(ez.State):
    ang_freq: float # pre-calculated angular frequency in radians


class SinGenerator(ez.Unit):
    SETTINGS: SinGeneratorSettings
    STATE: SinGeneratorState

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    def initialize(self) -> None:
        self.STATE.ang_freq = 2.0 * np.pi * self.SETTINGS.freq

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SIGNAL)
    async def generate(self, msg: AxisArray) -> AsyncGenerator:
        """
        msg is assumed to be a monotonically increasing counter ..
        .. or at least a counter with an intelligently chosen modulus
        """
        axis_name = self.SETTINGS.time_axis
        if axis_name is None:
            axis_name = msg.dims[0]
        axis = msg.axes.get(axis_name, None)
        fs = 1.0 / axis.gain if isinstance(axis, DimensionalAxis) else 1.0
        t_sec = msg.data / fs
        w = self.STATE.ang_freq * t_sec
        out_data = self.SETTINGS.amp * np.sin(w + self.SETTINGS.phase)
        yield (self.OUTPUT_SIGNAL, replace(msg, data=out_data))


class OscillatorSettings(ez.Settings):
    n_time: int  # Number of samples to output per block
    fs: float  # Sampling rate of signal output in Hz
    n_ch: int = 1 # Number of channels to output per block
    dispatch_rate: Optional[Union[float, str]] = None  # (Hz)
    freq: float = 1.0  # Oscillation frequency in Hz
    amp: float = 1.0  # Amplitude
    phase: float = 0.0  # Phase offset (in radians)
    sync: bool = False  # Adjust `freq` to sync with sampling rate


class Oscillator(ez.Collection):

    SETTINGS: OscillatorSettings

    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    COUNTER = Counter()
    SIN = SinGenerator()

    def configure(self) -> None:

        # Calculate synchronous settings if necessary
        freq = self.SETTINGS.freq
        mod = None
        if self.SETTINGS.sync:
            period = 1.0 / self.SETTINGS.freq
            mod = round(period * self.SETTINGS.fs)
            freq = 1.0 / (mod / self.SETTINGS.fs)

        self.COUNTER.apply_settings(
            CounterSettings(
                n_time=self.SETTINGS.n_time,
                fs=self.SETTINGS.fs,
                n_ch=self.SETTINGS.n_ch,
                dispatch_rate=self.SETTINGS.dispatch_rate,
                mod=mod
            )
        )

        self.SIN.apply_settings(
            SinGeneratorSettings(
                freq=freq,
                amp=self.SETTINGS.amp,
                phase=self.SETTINGS.phase
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.COUNTER.OUTPUT_SIGNAL, self.SIN.INPUT_SIGNAL),
            (self.SIN.OUTPUT_SIGNAL, self.OUTPUT_SIGNAL)
        )


class RandomGenerator(ez.Unit):
    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SIGNAL)
    async def generate(self, msg: AxisArray) -> AsyncGenerator:
        random_data = np.random.normal(size=msg.shape)
        yield (self.OUTPUT_SIGNAL, replace(msg, data=random_data))


class NoiseSettings(ez.Settings):
    n_time: int  # Number of samples to output per block
    fs: float  # Sampling rate of signal output in Hz
    n_ch: int = 1 # Number of channels to output
    dispatch_rate: Optional[Union[float, str]] = None  # (Hz)

WhiteNoiseSettings = NoiseSettings

class WhiteNoise(ez.Collection):

    SETTINGS: NoiseSettings

    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    COUNTER = Counter()
    RANDOM = RandomGenerator()

    def configure(self) -> None:

        self.COUNTER.apply_settings(
            CounterSettings(
                n_time=self.SETTINGS.n_time,
                fs=self.SETTINGS.fs,
                n_ch=self.SETTINGS.n_ch,
                dispatch_rate=self.SETTINGS.dispatch_rate,
                mod=None
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.COUNTER.OUTPUT_SIGNAL, self.RANDOM.INPUT_SIGNAL),
            (self.RANDOM.OUTPUT_SIGNAL, self.OUTPUT_SIGNAL)
        )

class PinkNoise(ez.Collection):

    SETTINGS: WhiteNoiseSettings

    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    WHITE_NOISE = WhiteNoise()
    FILTER = ButterworthFilter()

    def configure(self) -> None:

        self.WHITE_NOISE.apply_settings( self.SETTINGS )
        self.FILTER.apply_settings(
            ButterworthFilterSettings(
                order = 1,
                cutoff = self.SETTINGS.fs * 0.01 # Hz
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.WHITE_NOISE.OUTPUT_SIGNAL, self.FILTER.INPUT_SIGNAL),
            (self.FILTER.OUTPUT_SIGNAL, self.OUTPUT_SIGNAL)
        )