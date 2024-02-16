import asyncio
from collections import deque
from dataclasses import dataclass, replace, field
import time
from typing import Optional, Generator, AsyncGenerator, Union

import numpy as np
import ezmsg.core as ez
from ezmsg.util.generator import consumer, GenAxisArray
from ezmsg.util.messages.axisarray import AxisArray

from .butterworthfilter import ButterworthFilter, ButterworthFilterSettings


# CLOCK -- generate events at a specified rate #
def clock(
    dispatch_rate: Optional[float]
) -> Generator[ez.Flag, None, None]:
    n_dispatch = -1
    t_0 = time.time()
    while True:
        if dispatch_rate is not None:
            n_dispatch += 1
            t_next = t_0 + n_dispatch / dispatch_rate
            time.sleep(max(0, t_next - time.time()))
        yield ez.Flag()


async def aclock(
    dispatch_rate: Optional[float]
) -> AsyncGenerator[ez.Flag, None]:
    t_0 = time.time()
    n_dispatch = -1
    while True:
        if dispatch_rate is not None:
            n_dispatch += 1
            t_next = t_0 + n_dispatch / dispatch_rate
            await asyncio.sleep(t_next - time.time())
        yield ez.Flag()


class ClockSettings(ez.Settings):
    # Message dispatch rate (Hz), or None (fast as possible)
    dispatch_rate: Optional[float]


class ClockState(ez.State):
    cur_settings: ClockSettings
    gen: AsyncGenerator


class Clock(ez.Unit):
    SETTINGS: ClockSettings
    STATE: ClockState

    INPUT_SETTINGS = ez.InputStream(ClockSettings)
    OUTPUT_CLOCK = ez.OutputStream(ez.Flag)

    def initialize(self) -> None:
        self.STATE.cur_settings = self.SETTINGS
        self.construct_generator()

    def construct_generator(self):
        self.STATE.gen = aclock(self.STATE.cur_settings.dispatch_rate)

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: ClockSettings) -> None:
        self.STATE.cur_settings = msg
        self.construct_generator()

    @ez.publisher(OUTPUT_CLOCK)
    async def generate(self) -> AsyncGenerator:
        async for msg in self.STATE.gen:
            yield self.OUTPUT_CLOCK, msg


# COUNTER - Generate incrementing integer. fs and dispatch_rate parameters combine to give many options. #
async def acounter(
    n_time: int,  # Number of samples to output per block
    fs: Optional[float],  # Sampling rate of signal output in Hz
    n_ch: int = 1,  # Number of channels to synthesize

    # Message dispatch rate (Hz), 'realtime', 'ext_clock', or None (fast as possible)
    #  Note: if dispatch_rate is a float then time offsets will be synthetic and the
    #  system will run faster or slower than wall clock time.
    dispatch_rate: Optional[Union[float, str]] = None,

    # If set to an integer, counter will rollover at this number.
    mod: Optional[int] = None,
) -> AsyncGenerator[AxisArray, Optional[ez.Flag]]:

    # TODO: Adapt this to use ezmsg.util.rate?

    counter_start: int = 0  # next sample's first value

    b_realtime = False
    b_ext_clock = False
    b_manual_dispatch = False
    if dispatch_rate is not None:
        if isinstance(dispatch_rate, str):
            if dispatch_rate.lower() == "realtime":
                b_realtime = True
            elif dispatch_rate.lower() == "ext_clock":
                b_ext_clock = True
                # TODO: Warn if n_time > 1
                # TODO: Warn that fs is ignored.
        else:
            b_manual_dispatch = True

    # Two entirely separate branches. 1 - ext_clock; 2 - others.
    if b_ext_clock:
        msg_in: Optional[ez.Flag] = None
        msg_out: Optional[AxisArray] = None
        offset_queue = deque()
        b_new_data: bool = False
        while True:
            msg_in = yield msg_out

            msg_out = None
            if msg_in is not None:
                # If we have msg_in, this was an event.
                offset_queue.append(time.time())
            elif len(offset_queue) > 0:
                # This was a request for data.
                block_samp = np.arange(counter_start, counter_start + n_time)[:, np.newaxis]
                if mod is not None:
                    block_samp %= mod
                msg_out = AxisArray(
                    np.tile(block_samp, (1, n_ch)),
                    dims=["time", "ch"],
                    axes={"time": AxisArray.Axis.TimeAxis(
                        fs=fs, offset=offset_queue.popleft() - (n_time - 1) / fs)
                    },
                )
                # Next message's start
                counter_start = block_samp[-1, 0] + 1  # do not % mod

    else:
        n_sent: int = 0  # It is convenient to know how many samples we have sent.
        clock_zero: float = time.time()  # time associated with first sample

        while True:
            # 1. Sleep, if necessary, until we are at the end of the current block
            if b_realtime:
                n_next = n_sent + n_time
                t_next = clock_zero + n_next / fs
                await asyncio.sleep(t_next - time.time())
            elif b_manual_dispatch:
                n_disp_next = 1 + n_sent / n_time
                t_disp_next = clock_zero + n_disp_next / dispatch_rate
                await asyncio.sleep(t_disp_next - time.time())

            # 2. Prepare counter data.
            block_samp = np.arange(counter_start, counter_start + n_time)[:, np.newaxis]
            if mod is not None:
                block_samp %= mod
            block_samp = np.tile(block_samp, (1, n_ch))

            # 3. Prepare offset - the time associated with block_samp[0]
            if b_realtime:
                offset = t_next - n_time / fs
            else:
                # Purely synthetic.
                offset = n_sent / fs
                # offset += clock_zero  # ??

            # 4. yield output
            yield AxisArray(
                block_samp,
                dims=["time", "ch"],
                axes={"time": AxisArray.Axis.TimeAxis(fs=fs, offset=offset)},
            )

            # 5. Update state for next iteration (after next yield)
            counter_start = block_samp[-1, 0] + 1  # do not % mod
            n_sent += n_time


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
    gen: AsyncGenerator[AxisArray, Optional[ez.Flag]]
    cur_settings: CounterSettings


class Counter(ez.Unit):
    """Generates monotonically increasing counter"""

    SETTINGS: CounterSettings
    STATE: CounterState

    INPUT_CLOCK = ez.InputStream(ez.Flag)
    INPUT_SETTINGS = ez.InputStream(CounterSettings)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    def initialize(self) -> None:
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
        self.construct_generator()

    def construct_generator(self):
        self.STATE.gen = acounter(
            self.STATE.cur_settings.n_time,
            self.STATE.cur_settings.fs,
            n_ch=self.STATE.cur_settings.n_ch,
            dispatch_rate=self.STATE.cur_settings.dispatch_rate,
            mod=self.STATE.cur_settings.mod
        )

    @ez.subscriber(INPUT_CLOCK)
    async def on_clock(self, clock: ez.Flag):
        self.STATE.gen.asend(clock)

    @ez.publisher(OUTPUT_SIGNAL)
    async def publish(self) -> AsyncGenerator:
        while True:
            yield self.OUTPUT_SIGNAL, await anext(self.STATE.gen)


@consumer
def sin(
    axis: Optional[str] = "time",
    freq: float = 1.0,  # Oscillation frequency in Hz
    amp: float = 1.0,  # Amplitude
    phase: float = 0.0,  # Phase offset (in radians)
) -> Generator[AxisArray, AxisArray, None]:
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out = AxisArray(np.array([]), dims=[""])

    ang_freq = 2.0 * np.pi * freq

    while True:
        axis_arr_in = yield axis_arr_out
        # axis_arr_in is expected to be sample counts

        axis_name = axis
        if axis_name is None:
            axis_name = axis_arr_in.dims[0]

        w = (ang_freq * axis_arr_in.get_axis(axis_name).gain) * axis_arr_in.data
        out_data = amp * np.sin(w + phase)
        axis_arr_out = replace(axis_arr_in, data=out_data)


class SinGeneratorSettings(ez.Settings):
    time_axis: Optional[str] = "time"
    freq: float = 1.0  # Oscillation frequency in Hz
    amp: float = 1.0  # Amplitude
    phase: float = 0.0  # Phase offset (in radians)


class SinGenerator(GenAxisArray):
    SETTINGS: SinGeneratorSettings

    def construct_generator(self):
        self.STATE.gen = sin(
            axis=self.SETTINGS.time_axis,
            freq=self.SETTINGS.freq,
            amp=self.SETTINGS.amp,
            phase=self.SETTINGS.phase
        )


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
