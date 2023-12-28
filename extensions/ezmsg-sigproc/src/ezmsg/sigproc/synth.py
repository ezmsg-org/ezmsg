import asyncio
import time
from dataclasses import dataclass, replace, field

import ezmsg.core as ez
import numpy as np

from ezmsg.util.messages.axisarray import AxisArray

from .butterworthfilter import ButterworthFilter, ButterworthFilterSettings

from typing import Optional, AsyncGenerator, Union


class ClockSettings(ez.Settings):
    # Message dispatch rate (Hz), or None (fast as possible)
    dispatch_rate: Optional[float]


class ClockState(ez.State):
    cur_settings: ClockSettings


class Clock(ez.Unit):
    SETTINGS: ClockSettings
    STATE: ClockState

    INPUT_SETTINGS = ez.InputStream(ClockSettings)
    OUTPUT_CLOCK = ez.OutputStream(ez.Flag)

    def initialize(self) -> None:
        self.STATE.cur_settings = self.SETTINGS

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: ClockSettings) -> None:
        self.STATE.cur_settings = msg

    @ez.publisher(OUTPUT_CLOCK)
    async def generate(self) -> AsyncGenerator:
        t_0 = time.time()
        n_dispatch = 0
        while True:
            if self.STATE.cur_settings.dispatch_rate is not None:
                n_dispatch += 1
                t_next = t_0 + n_dispatch / self.STATE.cur_settings.dispatch_rate
                await asyncio.sleep(t_next - time.time())
            yield self.OUTPUT_CLOCK, ez.Flag


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
    n_ch: int = 1  # Number of channels to synthesize

    # Message dispatch rate (Hz), 'realtime', 'ext_clock', or None (fast as possible)
    #  Note: if dispatch_rate is a float then time offsets will be synthetic and the
    #  system will run faster or slower than wall clock time.
    dispatch_rate: Optional[Union[float, str]] = None

    # If set to an integer, counter will rollover
    mod: Optional[int] = None


class CounterState(ez.State):
    cur_settings: CounterSettings
    counter_start: int = 0  # next sample's first value
    clock_zero: float = field(default_factory=time.time)  # time associated with first sample
    n_sent: int = 0  # It is convenient to know how many samples we have sent.
    clock_event: asyncio.Event


class Counter(ez.Unit):
    """Generates monotonically increasing counter"""

    SETTINGS: CounterSettings
    STATE: CounterState

    INPUT_CLOCK = ez.InputStream(ez.Flag)
    INPUT_SETTINGS = ez.InputStream(CounterSettings)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    def initialize(self) -> None:
        self.STATE.clock_event = asyncio.Event()
        self.STATE.clock_event.clear()
        self.STATE.clock_zero = time.time()
        self.validate_settings(self.SETTINGS)

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: CounterSettings) -> None:
        self.validate_settings(msg)

    def validate_settings(self, settings: CounterSettings) -> None:
        if isinstance(
            settings.dispatch_rate, str
        ) and self.SETTINGS.dispatch_rate not in ["realtime", "ext_clock"]:
            raise ValueError(f"Unknown dispatch_rate: {self.SETTINGS.dispatch_rate}")

        self.STATE.cur_settings = settings

    @ez.subscriber(INPUT_CLOCK)
    async def on_clock(self, _: ez.Flag):
        self.STATE.clock_event.set()

    @ez.publisher(OUTPUT_SIGNAL)
    async def publish(self) -> AsyncGenerator:
        while True:
            # This generator behaves differently depending on `dispatch_rate`

            # 0. Parse dispatch_rate to get bool flags
            b_realtime = False
            b_ext_clock = False
            b_manual_dispatch = False
            dispatch_rate = self.STATE.cur_settings.dispatch_rate
            if dispatch_rate is not None:
                if isinstance(dispatch_rate, str):
                    if dispatch_rate == "realtime":
                        b_realtime = True
                    elif dispatch_rate == "ext_clock":
                        b_ext_clock = True
                else:
                    b_manual_dispatch = True

            # 1. Sleep, if necessary, until we are at the end of the current block's data production
            t_now = time.time()
            if b_realtime:
                n_next = self.STATE.n_sent + self.STATE.cur_settings.n_time
                t_next = self.STATE.clock_zero + n_next / self.STATE.cur_settings.fs
                await asyncio.sleep(t_next - t_now)
            elif b_ext_clock:
                await self.STATE.clock_event.wait()
                self.STATE.clock_event.clear()
            elif b_manual_dispatch:
                n_disp_next = 1 + self.STATE.n_sent / self.STATE.cur_settings.n_time
                t_disp_next = self.STATE.clock_zero + n_disp_next / dispatch_rate
                await asyncio.sleep(t_disp_next - t_now)
                
            # 2. Prepare counters. Same for all timing methods.
            block_samp = np.arange(self.STATE.cur_settings.n_time)[:, np.newaxis]
            block_samp += self.STATE.counter_start
            if self.STATE.cur_settings.mod is not None:
                block_samp %= self.STATE.cur_settings.mod
            block_samp = np.tile(block_samp, (1, self.STATE.cur_settings.n_ch))

            # 3. Prepare offset - the time associated with block_samp[0]
            if b_realtime:
                offset = t_next - self.STATE.cur_settings.n_time / self.STATE.cur_settings.fs
            elif b_ext_clock:
                offset = time.time() - (self.STATE.cur_settings.n_time - 1) / self.STATE.cur_settings.fs
            else:
                # Purely synthetic.
                offset = self.STATE.n_sent / self.STATE.cur_settings.fs
                # offset += self.STATE.clock_zero  # ??

            # 4. Update state for next iteration
            self.STATE.counter_start = block_samp[-1, 0] + 1
            # if self.STATE.cur_settings.mod is not None:
            #     self.STATE.counter_start is purposely not modded
            self.STATE.n_sent += self.STATE.cur_settings.n_time

            # 5. Send block
            out = AxisArray(
                block_samp,
                dims=["time", "ch"],
                axes=dict(
                    time=AxisArray.Axis.TimeAxis(
                        fs=self.STATE.cur_settings.fs,
                        offset=offset
                    )
                ),
            )

            yield self.OUTPUT_SIGNAL, out


class SinGeneratorSettings(ez.Settings):
    time_axis: Optional[str] = "time"
    freq: float = 1.0  # Oscillation frequency in Hz
    amp: float = 1.0  # Amplitude
    phase: float = 0.0  # Phase offset (in radians)


class SinGeneratorState(ez.State):
    ang_freq: float  # pre-calculated angular frequency in radians


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
        fs = 1.0 / msg.get_axis(axis_name).gain
        t_sec = msg.data / fs
        w = self.STATE.ang_freq * t_sec
        out_data = self.SETTINGS.amp * np.sin(w + self.SETTINGS.phase)
        yield (self.OUTPUT_SIGNAL, replace(msg, data=out_data))


class OscillatorSettings(ez.Settings):
    n_time: int  # Number of samples to output per block
    fs: float  # Sampling rate of signal output in Hz
    n_ch: int = 1  # Number of channels to output per block
    dispatch_rate: Optional[Union[float, str]] = None  # (Hz) | 'realtime' | 'ext_clock'
    freq: float = 1.0  # Oscillation frequency in Hz
    amp: float = 1.0  # Amplitude
    phase: float = 0.0  # Phase offset (in radians)
    sync: bool = False  # Adjust `freq` to sync with sampling rate


class Oscillator(ez.Collection):
    SETTINGS: OscillatorSettings

    INPUT_CLOCK = ez.InputStream(ez.Flag)
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
                mod=mod,
            )
        )

        self.SIN.apply_settings(
            SinGeneratorSettings(
                freq=freq, amp=self.SETTINGS.amp, phase=self.SETTINGS.phase
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.INPUT_CLOCK, self.COUNTER.INPUT_CLOCK),
            (self.COUNTER.OUTPUT_SIGNAL, self.SIN.INPUT_SIGNAL),
            (self.SIN.OUTPUT_SIGNAL, self.OUTPUT_SIGNAL),
        )


class RandomGeneratorSettings(ez.Settings):
    loc: float = 0.0
    scale: float = 1.0


class RandomGenerator(ez.Unit):
    SETTINGS: RandomGeneratorSettings

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SIGNAL)
    async def generate(self, msg: AxisArray) -> AsyncGenerator:
        random_data = np.random.normal(
            size=msg.shape, loc=self.SETTINGS.loc, scale=self.SETTINGS.scale
        )

        yield self.OUTPUT_SIGNAL, replace(msg, data=random_data)


class NoiseSettings(ez.Settings):
    n_time: int  # Number of samples to output per block
    fs: float  # Sampling rate of signal output in Hz
    n_ch: int = 1  # Number of channels to output
    dispatch_rate: Optional[
        Union[float, str]
    ] = None  # (Hz), 'realtime', or 'ext_clock'
    loc: float = 0.0  # DC offset
    scale: float = 1.0  # Scale (in standard deviations)


WhiteNoiseSettings = NoiseSettings


class WhiteNoise(ez.Collection):
    SETTINGS: NoiseSettings

    INPUT_CLOCK = ez.InputStream(ez.Flag)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    COUNTER = Counter()
    RANDOM = RandomGenerator()

    def configure(self) -> None:
        self.RANDOM.apply_settings(
            RandomGeneratorSettings(loc=self.SETTINGS.loc, scale=self.SETTINGS.scale)
        )

        self.COUNTER.apply_settings(
            CounterSettings(
                n_time=self.SETTINGS.n_time,
                fs=self.SETTINGS.fs,
                n_ch=self.SETTINGS.n_ch,
                dispatch_rate=self.SETTINGS.dispatch_rate,
                mod=None,
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.INPUT_CLOCK, self.COUNTER.INPUT_CLOCK),
            (self.COUNTER.OUTPUT_SIGNAL, self.RANDOM.INPUT_SIGNAL),
            (self.RANDOM.OUTPUT_SIGNAL, self.OUTPUT_SIGNAL),
        )


PinkNoiseSettings = NoiseSettings


class PinkNoise(ez.Collection):
    SETTINGS: PinkNoiseSettings

    INPUT_CLOCK = ez.InputStream(ez.Flag)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    WHITE_NOISE = WhiteNoise()
    FILTER = ButterworthFilter()

    def configure(self) -> None:
        self.WHITE_NOISE.apply_settings(self.SETTINGS)
        self.FILTER.apply_settings(
            ButterworthFilterSettings(
                axis="time", order=1, cutoff=self.SETTINGS.fs * 0.01  # Hz
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.INPUT_CLOCK, self.WHITE_NOISE.INPUT_CLOCK),
            (self.WHITE_NOISE.OUTPUT_SIGNAL, self.FILTER.INPUT_SIGNAL),
            (self.FILTER.OUTPUT_SIGNAL, self.OUTPUT_SIGNAL),
        )


class AddState(ez.State):
    queue_a: "asyncio.Queue[AxisArray]" = field(default_factory=asyncio.Queue)
    queue_b: "asyncio.Queue[AxisArray]" = field(default_factory=asyncio.Queue)


class Add(ez.Unit):
    """Add two signals together.  Assumes compatible/similar axes/dimensions."""

    STATE: AddState

    INPUT_SIGNAL_A = ez.InputStream(AxisArray)
    INPUT_SIGNAL_B = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    @ez.subscriber(INPUT_SIGNAL_A)
    async def on_a(self, msg: AxisArray) -> None:
        self.STATE.queue_a.put_nowait(msg)

    @ez.subscriber(INPUT_SIGNAL_B)
    async def on_b(self, msg: AxisArray) -> None:
        self.STATE.queue_b.put_nowait(msg)

    @ez.publisher(OUTPUT_SIGNAL)
    async def output(self) -> AsyncGenerator:
        while True:
            a = await self.STATE.queue_a.get()
            b = await self.STATE.queue_b.get()

            yield (self.OUTPUT_SIGNAL, replace(a, data=a.data + b.data))


class EEGSynthSettings(ez.Settings):
    fs: float = 500.0  # Hz
    n_time: int = 100
    alpha_freq: float = 10.5  # Hz
    n_ch: int = 8


class EEGSynth(ez.Collection):
    SETTINGS: EEGSynthSettings

    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    CLOCK = Clock()
    NOISE = PinkNoise()
    OSC = Oscillator()
    ADD = Add()

    def configure(self) -> None:
        self.CLOCK.apply_settings(
            ClockSettings(dispatch_rate=self.SETTINGS.fs / self.SETTINGS.n_time)
        )

        self.OSC.apply_settings(
            OscillatorSettings(
                n_time=self.SETTINGS.n_time,
                fs=self.SETTINGS.fs,
                n_ch=self.SETTINGS.n_ch,
                dispatch_rate="ext_clock",
                freq=self.SETTINGS.alpha_freq,
            )
        )

        self.NOISE.apply_settings(
            PinkNoiseSettings(
                n_time=self.SETTINGS.n_time,
                fs=self.SETTINGS.fs,
                n_ch=self.SETTINGS.n_ch,
                dispatch_rate="ext_clock",
                scale=5.0,
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.CLOCK.OUTPUT_CLOCK, self.OSC.INPUT_CLOCK),
            (self.CLOCK.OUTPUT_CLOCK, self.NOISE.INPUT_CLOCK),
            (self.OSC.OUTPUT_SIGNAL, self.ADD.INPUT_SIGNAL_A),
            (self.NOISE.OUTPUT_SIGNAL, self.ADD.INPUT_SIGNAL_B),
            (self.ADD.OUTPUT_SIGNAL, self.OUTPUT_SIGNAL),
        )