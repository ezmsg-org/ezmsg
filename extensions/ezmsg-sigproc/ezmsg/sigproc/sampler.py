from dataclasses import dataclass, replace, field
import time

import ezmsg.core as ez
import numpy as np

from ezmsg.util.messages.axisarray import AxisArray

from typing import Optional, Any, Tuple, List, Dict, AsyncGenerator


@dataclass(unsafe_hash = True)
class SampleTriggerMessage:
    timestamp: float = field(default_factory=time.time)
    period: Optional[Tuple[float, float]] = None
    value: Any = None


@dataclass
class SampleMessage:
    trigger: SampleTriggerMessage
    sample: AxisArray


class SamplerSettings(ez.Settings):
    buffer_dur: float
    axis: Optional[str] = None
    period: Optional[
        Tuple[float, float]
    ] = None  # Optional default period if unspecified in SampleTriggerMessage
    value: Any = None  # Optional default value if unspecified in SampleTriggerMessage

    estimate_alignment: bool = True
    # If true, use message timestamp fields and reported sampling rate to estimate
    # sample-accurate alignment for samples.
    # If false, sampling will be limited to incoming message rate -- "Block timing"
    # NOTE: For faster-than-realtime playback --  Incoming timestamps must reflect
    # "realtime" operation for estimate_alignment to operate correctly.


class SamplerState(ez.State):
    cur_settings: SamplerSettings
    triggers: Dict[SampleTriggerMessage, int] = field(default_factory=dict)
    last_msg: Optional[AxisArray] = None
    buffer: Optional[np.ndarray] = None


class Sampler(ez.Unit):
    SETTINGS: SamplerSettings
    STATE: SamplerState

    INPUT_TRIGGER = ez.InputStream(SampleTriggerMessage)
    INPUT_SETTINGS = ez.InputStream(SamplerSettings)
    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SAMPLE = ez.OutputStream(SampleMessage)

    def initialize(self) -> None:
        self.STATE.cur_settings = self.SETTINGS

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: SamplerSettings) -> None:
        self.STATE.cur_settings = msg

    @ez.subscriber(INPUT_TRIGGER)
    async def on_trigger(self, msg: SampleTriggerMessage) -> None:
        if self.STATE.last_msg is not None:
            axis_name = self.STATE.cur_settings.axis
            if axis_name is None:
                axis_name = self.STATE.last_msg.dims[0]
            axis = self.STATE.last_msg.get_axis(axis_name)
            axis_idx = self.STATE.last_msg.get_axis_idx(axis_name)

            fs = 1.0 / axis.gain
            last_msg_timestamp = axis.offset + (
                self.STATE.last_msg.shape[axis_idx] / fs
            )

            period = (
                msg.period if msg.period is not None else self.STATE.cur_settings.period
            )
            value = (
                msg.value if msg.value is not None else self.STATE.cur_settings.value
            )

            if period is None:
                ez.logger.warning(f"Sampling failed: period not specified")
                return

            # Check that period is valid
            start_offset = int(period[0] * fs)
            stop_offset = int(period[1] * fs)
            if (stop_offset - start_offset) <= 0:
                ez.logger.warning(f"Sampling failed: invalid period requested")
                return

            # Check that period is compatible with buffer duration
            max_buf_len = int(self.STATE.cur_settings.buffer_dur * fs)
            req_buf_len = int((period[1] - period[0]) * fs)
            if req_buf_len >= max_buf_len:
                ez.logger.warning(
                    f"Sampling failed: {period=} >= {self.STATE.cur_settings.buffer_dur=}"
                )
                return

            offset: int = 0
            if self.STATE.cur_settings.estimate_alignment:
                # Do what we can with the wall clock to determine sample alignment
                wall_delta = msg.timestamp - last_msg_timestamp
                offset = int(wall_delta * fs)

            # Check that current buffer accumulation allows for offset - period start
            if (
                self.STATE.buffer is None
                or -min(offset + start_offset, 0) >= self.STATE.buffer.shape[0]
            ):
                ez.logger.warning(
                    "Sampling failed: insufficient buffer accumulation for requested sample period"
                )
                return

            self.STATE.triggers[replace(msg, period=period, value=value)] = offset

        else:
            ez.logger.warning("Sampling failed: no signal to sample yet")

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SAMPLE)
    async def on_signal(self, msg: AxisArray) -> AsyncGenerator:
        axis_name = self.STATE.cur_settings.axis
        if axis_name is None:
            axis_name = msg.dims[0]
        axis = msg.get_axis(axis_name)

        fs = 1.0 / axis.gain

        if self.STATE.last_msg is None:
            self.STATE.last_msg = msg

        # Easier to deal with timeseries on axis 0
        last_msg = self.STATE.last_msg
        msg_data = np.moveaxis(msg.data, msg.get_axis_idx(axis_name), 0)
        last_msg_data = np.moveaxis(last_msg.data, last_msg.get_axis_idx(axis_name), 0)
        last_msg_axis = last_msg.get_axis(axis_name)
        last_msg_fs = 1.0 / last_msg_axis.gain

        # Check if signal properties have changed in a breaking way
        if fs != last_msg_fs or msg_data.shape[1:] != last_msg_data.shape[1:]:
            # Data stream changed meaningfully -- flush buffer, stop sampling
            if len(self.STATE.triggers) > 0:
                ez.logger.warning("Sampling failed: Discarding all triggers")
            ez.logger.warning("Flushing buffer: signal properties changed")
            self.STATE.buffer = None
            self.STATE.triggers = dict()

        # Accumulate buffer ( time dim => dim 0 )
        self.STATE.buffer = (
            msg_data
            if self.STATE.buffer is None
            else np.concatenate((self.STATE.buffer, msg_data), axis=0)
        )

        buffer_offset = np.arange(self.STATE.buffer.shape[0] + msg_data.shape[0])
        buffer_offset -= self.STATE.buffer.shape[0] + 1
        buffer_offset = (buffer_offset * axis.gain) + axis.offset

        pub_samples: List[SampleMessage] = []
        remaining_triggers: Dict[SampleTriggerMessage, int] = dict()
        for trigger, offset in self.STATE.triggers.items():
            if trigger.period is None:
                continue

            # trigger_offset points to t = 0 within buffer
            offset -= msg_data.shape[0]
            start = offset + int(trigger.period[0] * fs)
            stop = offset + int(trigger.period[1] * fs)

            if stop < 0:  # We should be able to dispatch a sample
                sample_data = self.STATE.buffer[start:stop, ...]
                sample_data = np.moveaxis(sample_data, msg.get_axis_idx(axis_name), 0)

                sample_offset = buffer_offset[start]
                sample_axis = replace(axis, offset=sample_offset)
                sample_axes = {**msg.axes, **{axis_name: sample_axis}}

                pub_samples.append(
                    SampleMessage(
                        trigger=trigger,
                        sample=replace(msg, data=sample_data, axes=sample_axes),
                    )
                )

            else:
                remaining_triggers[trigger] = offset

        for sample in pub_samples:
            yield self.OUTPUT_SAMPLE, sample

        self.STATE.triggers = remaining_triggers

        buf_len = int(self.STATE.cur_settings.buffer_dur * fs)
        self.STATE.buffer = self.STATE.buffer[-buf_len:, ...]
        self.STATE.last_msg = msg


## Dev/test apparatus
import asyncio

from ezmsg.testing.debuglog import DebugLog
from ezmsg.sigproc.synth import Oscillator, OscillatorSettings

from typing import AsyncGenerator


class TriggerGeneratorSettings(ez.Settings):
    period: Tuple[float, float]  # sec
    prewait: float = 0.5  # sec
    publish_period: float = 5.0  # sec


class TriggerGenerator(ez.Unit):
    SETTINGS: TriggerGeneratorSettings

    OUTPUT_TRIGGER = ez.OutputStream(SampleTriggerMessage)

    @ez.publisher(OUTPUT_TRIGGER)
    async def generate(self) -> AsyncGenerator:
        await asyncio.sleep(self.SETTINGS.prewait)

        output = 0
        while True:
            yield self.OUTPUT_TRIGGER, SampleTriggerMessage(
                period=self.SETTINGS.period, value=output
            )

            await asyncio.sleep(self.SETTINGS.publish_period)
            output += 1


class SamplerTestSystemSettings(ez.Settings):
    sampler_settings: SamplerSettings
    trigger_settings: TriggerGeneratorSettings


class SamplerTestSystem(ez.Collection):
    SETTINGS: SamplerTestSystemSettings

    OSC = Oscillator()
    SAMPLER = Sampler()
    TRIGGER = TriggerGenerator()
    DEBUG = DebugLog()

    def configure(self) -> None:
        self.SAMPLER.apply_settings(self.SETTINGS.sampler_settings)
        self.TRIGGER.apply_settings(self.SETTINGS.trigger_settings)

        self.OSC.apply_settings(
            OscillatorSettings(
                n_time=2,  # Number of samples to output per block
                fs=10,  # Sampling rate of signal output in Hz
                dispatch_rate="realtime",
                freq=2.0,  # Oscillation frequency in Hz
                amp=1.0,  # Amplitude
                phase=0.0,  # Phase offset (in radians)
                sync=True,  # Adjust `freq` to sync with sampling rate
            )
        )

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.OSC.OUTPUT_SIGNAL, self.SAMPLER.INPUT_SIGNAL),
            (self.TRIGGER.OUTPUT_TRIGGER, self.SAMPLER.INPUT_TRIGGER),
            (self.TRIGGER.OUTPUT_TRIGGER, self.DEBUG.INPUT),
            (self.SAMPLER.OUTPUT_SAMPLE, self.DEBUG.INPUT),
        )


if __name__ == "__main__":
    settings = SamplerTestSystemSettings(
        sampler_settings=SamplerSettings(buffer_dur=5.0),
        trigger_settings=TriggerGeneratorSettings(
            period=(1.0, 2.0), prewait=0.5, publish_period=5.0
        ),
    )

    system = SamplerTestSystem(settings)

    ez.run(SYSTEM = system)
