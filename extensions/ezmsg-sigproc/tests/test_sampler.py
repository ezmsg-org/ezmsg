import os
from typing import Optional, List

import numpy as np

import ezmsg.core as ez
from ezmsg.util.messagecodec import message_log
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messagelogger import MessageLogger, MessageLoggerSettings
from ezmsg.sigproc.sampler import (
    Sampler, SamplerSettings,
    TriggerGenerator, TriggerGeneratorSettings,
    SampleTriggerMessage, SampleMessage,
    sampler
)
from ezmsg.sigproc.synth import Oscillator, OscillatorSettings
from ezmsg.util.terminate import TerminateOnTotal, TerminateOnTotalSettings
from ezmsg.util.debuglog import DebugLog

from util import get_test_fn


def test_sampler_gen():
    data_dur = 10.0
    chunk_period = 0.1
    fs = 500.
    n_chans = 3

    # The sampler is a bit complicated as it requires 2 different inputs: signal and triggers
    # Prepare signal data
    n_data = int(data_dur * fs)
    data = np.arange(n_chans *n_data).reshape(n_chans, n_data)
    offsets = np.arange(n_data) / fs
    n_chunks = int(np.ceil(data_dur / chunk_period))
    n_per_chunk = int(np.ceil(n_data / n_chunks))
    signal_msgs = [
        AxisArray(
            data=data[:, ix * n_per_chunk:(ix + 1) * n_per_chunk],
            dims=["ch", "time"],
            axes={"time": AxisArray.Axis.TimeAxis(fs=fs, offset=offsets[ix * n_per_chunk])}
        )
        for ix in range(n_chunks)
    ]
    # Prepare triggers
    n_trigs = 7
    trig_ts = np.linspace(0.1, data_dur - 1.0, n_trigs) + np.random.randn(n_trigs) / fs
    period = (-0.01, 0.74)
    trigger_msgs = [
        SampleTriggerMessage(
            timestamp=_ts,
            period=period,
            value=["Start", "Stop"][_ix % 2]
        )
        for _ix, _ts in enumerate(trig_ts)
    ]
    # Mix the messages and sort by time
    msg_ts = [_.axes["time"].offset for _ in signal_msgs] + [_.timestamp for _ in trigger_msgs]
    mix_msgs = signal_msgs + trigger_msgs
    mix_msgs = [mix_msgs[_] for _ in np.argsort(msg_ts)]

    # Create the sample-generator
    period_dur = period[1] - period[0]
    buffer_dur = 2 * max(period_dur, period[1])
    gen = sampler(buffer_dur, axis="time", period=None, value=None, estimate_alignment=True)

    # Run the messages through the generator and collect samples.
    samples = []
    for msg in mix_msgs:
        samples.extend(gen.send(msg))

    assert len(samples) == n_trigs
    # Check sample data size
    assert all([_.sample.data.shape == (n_chans, int(fs * period_dur)) for _ in samples])
    # Compare the sample window slice against the trigger timestamps
    latencies = [_.sample.axes["time"].offset - (_.trigger.timestamp + _.trigger.period[0]) for _ in samples]
    assert all([0 <= _ < 1 / fs for _ in latencies])
    # Check the sample trigger value matches the trigger input.
    assert all([_.trigger.value == ["Start", "Stop"][ix % 2] for ix, _ in enumerate(samples)])


class SamplerSystemSettings(ez.Settings):
    # num_msgs: int
    osc_settings: OscillatorSettings
    trigger_settings: TriggerGeneratorSettings
    sampler_settings: SamplerSettings
    log_settings: MessageLoggerSettings
    term_settings: TerminateOnTotalSettings


class SamplerSystem(ez.Collection):
    SETTINGS: SamplerSystemSettings

    OSC = Oscillator()
    TRIGGER = TriggerGenerator()
    SAMPLER = Sampler()
    LOG = MessageLogger()
    DEBUG = DebugLog()
    TERM = TerminateOnTotal()

    def configure(self) -> None:
        self.OSC.apply_settings(self.SETTINGS.osc_settings)
        self.LOG.apply_settings(self.SETTINGS.log_settings)
        self.SAMPLER.apply_settings(self.SETTINGS.sampler_settings)
        self.TRIGGER.apply_settings(self.SETTINGS.trigger_settings)
        self.TERM.apply_settings(self.SETTINGS.term_settings)

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.OSC.OUTPUT_SIGNAL, self.SAMPLER.INPUT_SIGNAL),
            (self.SAMPLER.OUTPUT_SAMPLE, self.LOG.INPUT_MESSAGE),
            (self.LOG.OUTPUT_MESSAGE, self.TERM.INPUT_MESSAGE),
            # Trigger branch
            (self.TRIGGER.OUTPUT_TRIGGER, self.SAMPLER.INPUT_TRIGGER),
            # Debug branches
            (self.TRIGGER.OUTPUT_TRIGGER, self.DEBUG.INPUT),
            (self.SAMPLER.OUTPUT_SAMPLE, self.DEBUG.INPUT),
        )


def test_sampler_system(test_name: Optional[str] = None):
    freq = 40.0
    period = (0.5, 1.5)
    n_msgs = 4

    sample_dur = (period[1] - period[0])
    publish_period = sample_dur * 2.0

    test_filename = get_test_fn(test_name)
    ez.logger.info(test_filename)

    settings = SamplerSystemSettings(
        osc_settings=OscillatorSettings(
            n_time=2,  # Number of samples to output per block
            fs=freq,  # Sampling rate of signal output in Hz
            dispatch_rate="realtime",
            freq=2.0,  # Oscillation frequency in Hz
            amp=1.0,  # Amplitude
            phase=0.0,  # Phase offset (in radians)
            sync=True,  # Adjust `freq` to sync with sampling rate
        ),
        trigger_settings=TriggerGeneratorSettings(
            period=period,
            prewait=0.5,
            publish_period=publish_period
        ),
        sampler_settings=SamplerSettings(buffer_dur=publish_period + 1.0),
        log_settings=MessageLoggerSettings(output=test_filename),
        term_settings=TerminateOnTotalSettings(total=n_msgs),
    )

    system = SamplerSystem(settings)

    ez.run(SYSTEM=system)
    messages: List[SampleTriggerMessage] = [_ for _ in message_log(test_filename)]
    os.remove(test_filename)
    ez.logger.info(f"Analyzing recording of {len(messages)} messages...")
    assert len(messages) == n_msgs
    assert all([_.sample.data.shape == (int(freq * sample_dur), 1) for _ in messages])
    # Test the sample window slice vs the trigger timestamps
    latencies = [_.sample.axes["time"].offset - (_.trigger.timestamp + _.trigger.period[0]) for _ in messages]
    assert all([0 <= _ < 1/freq for _ in latencies])
    # Given that the input is a pure sinusoid, we could test that the signal has expected characteristics.
