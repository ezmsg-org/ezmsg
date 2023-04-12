import os
import json

import pytest
import numpy as np

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messagegate import MessageGate, MessageGateSettings
from ezmsg.util.messagelogger import MessageLogger, MessageLoggerSettings
from ezmsg.util.messagecodec import message_log
from ezmsg.sigproc.downsample import Downsample, DownsampleSettings
from ezmsg.sigproc.synth import Oscillator, OscillatorSettings

from util import get_test_fn
from ezmsg.util.terminate import TerminateOnTimeout as TerminateTest
from ezmsg.util.terminate import TerminateOnTimeoutSettings as TerminateTestSettings
from ezmsg.util.debuglog import DebugLog

from typing import Optional, List


class DownsampleSystemSettings(ez.Settings):
    num_msgs: int
    osc_settings: OscillatorSettings
    down_settings: DownsampleSettings
    log_settings: MessageLoggerSettings
    term_settings: TerminateTestSettings


class DownsampleSystem(ez.Collection):
    OSC = Oscillator()
    GATE = MessageGate()
    DOWN = Downsample()
    LOG = MessageLogger()
    TERM = TerminateTest()

    DEBUG = DebugLog()

    SETTINGS: DownsampleSystemSettings

    def configure(self) -> None:
        self.OSC.apply_settings(self.SETTINGS.osc_settings)
        self.GATE.apply_settings(
            MessageGateSettings(
                start_open=True,
                default_open=False,
                default_after=self.SETTINGS.num_msgs,
            )
        )
        self.DOWN.apply_settings(self.SETTINGS.down_settings)
        self.LOG.apply_settings(self.SETTINGS.log_settings)
        self.TERM.apply_settings(self.SETTINGS.term_settings)

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.OSC.OUTPUT_SIGNAL, self.GATE.INPUT),
            # ( self.OSC.OUTPUT_SIGNAL, self.DEBUG.INPUT ),
            (self.GATE.OUTPUT, self.DOWN.INPUT_SIGNAL),
            # ( self.GATE.OUTPUT, self.DEBUG.INPUT ),
            (self.DOWN.OUTPUT_SIGNAL, self.LOG.INPUT_MESSAGE),
            # ( self.DOWN.OUTPUT_SIGNAL, self.DEBUG.INPUT ),
            (self.LOG.OUTPUT_MESSAGE, self.TERM.INPUT),
            # ( self.LOG.OUTPUT_MESSAGE, self.DEBUG.INPUT ),
        )


@pytest.mark.parametrize("block_size", [1, 5, 10, 20])
@pytest.mark.parametrize("factor", [1, 2, 3])
def test_downsample_system(
    block_size: int, factor: int, test_name: Optional[str] = None
):
    in_fs = 10.0

    # Ensure 4 seconds of data
    num_msgs = int((in_fs / block_size) * 4.0)

    test_filename = get_test_fn(test_name)
    ez.logger.info(test_filename)

    settings = DownsampleSystemSettings(
        num_msgs=num_msgs,
        osc_settings=OscillatorSettings(
            n_time=block_size, freq=1.0, fs=in_fs, dispatch_rate=20.0, sync=True  # Hz
        ),
        down_settings=DownsampleSettings(factor=factor),
        log_settings=MessageLoggerSettings(output=test_filename),
        term_settings=TerminateTestSettings(time=1.0),
    )

    system = DownsampleSystem(settings)

    ez.run(SYSTEM = system)

    messages: List[AxisArray] = []
    for msg in message_log(test_filename):
        messages.append(msg)

    os.remove(test_filename)

    ez.logger.info(f"Analyzing recording of { len( messages ) } messages...")

    fs: Optional[float] = None
    dims: Optional[List[str]] = None
    data: List[np.ndarray] = []
    for msg in messages:
        # In this test, fs should change by factor
        msg_fs = 1.0 / msg.axes["time"].gain
        if fs is None:
            fs = msg_fs
        assert fs == msg_fs

        # In this test, we should have consistent time dimension
        if dims is None:
            dims = msg.dims
        else:
            assert dims == msg.dims

        data.append(msg.data)

    assert fs is not None
    assert fs - (in_fs / factor) < 0.01

    ez.logger.info("Consistent metadata!")

    # TODO: Write meaningful analyses of the recording to test functionality

    ez.logger.info("Test Complete.")


if __name__ == "__main__":
    test_downsample_system(10, 2, test_name="test_window_system")
