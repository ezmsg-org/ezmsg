import os
import json

import pytest
import numpy as np

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messagegate import MessageGate, MessageGateSettings
from ezmsg.util.messagelogger import MessageLogger, MessageLoggerSettings
from ezmsg.util.messagecodec import message_log
from ezmsg.sigproc.synth import WhiteNoise, WhiteNoiseSettings
from ezmsg.sigproc.butterworthfilter import ButterworthFilter, ButterworthFilterSettings

from util import get_test_fn
from ezmsg.util.terminate import TerminateOnTimeout as TerminateTest
from ezmsg.util.terminate import TerminateOnTimeoutSettings as TerminateTestSettings

from typing import Optional, List


class ButterworthSystemSettings(ez.Settings):
    noise_settings: WhiteNoiseSettings
    gate_settings: MessageGateSettings
    butter_settings: ButterworthFilterSettings
    log_settings: MessageLoggerSettings
    term_settings: TerminateTestSettings


class ButterworthSystem(ez.Collection):
    NOISE = WhiteNoise()
    GATE = MessageGate()
    BUTTER = ButterworthFilter()
    LOG = MessageLogger()
    TERM = TerminateTest()

    SETTINGS: ButterworthSystemSettings

    def configure(self) -> None:
        self.NOISE.apply_settings(self.SETTINGS.noise_settings)
        self.GATE.apply_settings(self.SETTINGS.gate_settings)
        self.BUTTER.apply_settings(self.SETTINGS.butter_settings)
        self.LOG.apply_settings(self.SETTINGS.log_settings)
        self.TERM.apply_settings(self.SETTINGS.term_settings)

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.NOISE.OUTPUT_SIGNAL, self.GATE.INPUT),
            (self.GATE.OUTPUT, self.BUTTER.INPUT_SIGNAL),
            (self.BUTTER.OUTPUT_SIGNAL, self.LOG.INPUT_MESSAGE),
            (self.LOG.OUTPUT_MESSAGE, self.TERM.INPUT),
        )


@pytest.mark.parametrize(
    "cutoff, cuton",
    [
        (30.0, None),  # lowpass
        (None, 30.0),  # highpass
        (45.0, 30.0),  # bandpass
        (30.0, 45.0),  # bandstop
    ],
)
def test_butterworth_system(
    cutoff: float, cuton: float, test_name: Optional[str] = None
):
    in_fs = 128.0
    block_size = 128

    # in_fs / block_size = 1 second of data
    seconds_of_data = 10.0
    num_msgs = int((in_fs / block_size) * seconds_of_data)

    test_filename = get_test_fn(test_name)
    ez.logger.info(test_filename)

    settings = ButterworthSystemSettings(
        noise_settings=WhiteNoiseSettings(
            n_time=block_size,
            fs=in_fs,
            dispatch_rate=None,
        ),
        gate_settings=MessageGateSettings(
            start_open=True, default_open=False, default_after=num_msgs
        ),
        butter_settings=ButterworthFilterSettings(order=5, cutoff=cutoff, cuton=cuton),
        log_settings=MessageLoggerSettings(output=test_filename),
        term_settings=TerminateTestSettings(time=1.0),
    )

    system = ButterworthSystem(settings)

    ez.run(SYSTEM = system)

    messages: List[AxisArray] = []
    for msg in message_log(test_filename):
        messages.append(msg)

    os.remove(test_filename)

    ez.logger.info(f"Analyzing recording of { len( messages ) } messages...")

    data = np.concatenate([msg.data for msg in messages], axis=0)

    # Assert that graph has correct values
    freqs = np.fft.fftfreq(data.size, d=(1 / in_fs))
    fft_vals = np.log10(np.abs(np.fft.fft(data, axis=0)))
    all_vals = list(zip(freqs, fft_vals))

    specs = settings.butter_settings.filter_specs()
    assert specs is not None
    btype, cut = specs
    ez.logger.info(f"Testing {btype}...")

    if btype == "lowpass":
        zeroed_values = [val[1] for val in all_vals if val[0] > cutoff]
        white_values = [val[1] for val in all_vals if not val[0] > cutoff]
    if btype == "highpass":
        zeroed_values = [val[1] for val in all_vals if val[0] < cuton]
        white_values = [val[1] for val in all_vals if not val[0] < cuton]
    if btype == "bandpass":
        zeroed_values = [
            val[1] for val in all_vals if val[0] < cuton or val[0] > cutoff
        ]
        white_values = [
            val[1] for val in all_vals if not val[0] < cuton and not val[0] > cutoff
        ]
    if btype == "bandstop":
        zeroed_values = [
            val[1] for val in all_vals if val[0] < cuton and val[0] > cutoff
        ]
        white_values = [
            val[1] for val in all_vals if not val[0] < cuton or not val[0] > cutoff
        ]

    assert np.mean(zeroed_values) < np.mean(white_values)

    ez.logger.info("Test Complete.")


if __name__ == "__main__":
    test_butterworth_system(20, None, test_name="test_butterworth_system")
