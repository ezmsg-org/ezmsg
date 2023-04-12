from dataclasses import field

import os
import json

import pytest
import numpy as np
import ezmsg.core as ez

from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messagegate import MessageGate, MessageGateSettings
from ezmsg.util.messagelogger import MessageLogger, MessageLoggerSettings
from ezmsg.util.messagecodec import message_log
from ezmsg.sigproc.synth import Counter, CounterSettings
from ezmsg.sigproc.window import Window, WindowSettings

from util import get_test_fn
from ezmsg.util.terminate import TerminateOnTimeout as TerminateTest
from ezmsg.util.terminate import TerminateOnTimeoutSettings as TerminateTestSettings
from ezmsg.util.debuglog import DebugLog

from typing import Optional, Dict, Any, List


class WindowSystemSettings(ez.Settings):
    num_msgs: int
    counter_settings: CounterSettings
    window_settings: WindowSettings
    log_settings: MessageLoggerSettings
    term_settings: TerminateTestSettings = field(default_factory=TerminateTestSettings)


class WindowSystem(ez.Collection):
    COUNTER = Counter()
    GATE = MessageGate()
    WIN = Window()
    LOG = MessageLogger()
    TERM = TerminateTest()

    DEBUG = DebugLog()

    SETTINGS: WindowSystemSettings

    def configure(self) -> None:
        self.COUNTER.apply_settings(self.SETTINGS.counter_settings)
        self.GATE.apply_settings(
            MessageGateSettings(
                start_open=True,
                default_open=False,
                default_after=self.SETTINGS.num_msgs,
            )
        )
        self.WIN.apply_settings(self.SETTINGS.window_settings)
        self.LOG.apply_settings(self.SETTINGS.log_settings)
        self.TERM.apply_settings(self.SETTINGS.term_settings)

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.COUNTER.OUTPUT_SIGNAL, self.GATE.INPUT),
            # ( self.COUNTER.OUTPUT_SIGNAL, self.DEBUG.INPUT ),
            (self.GATE.OUTPUT, self.WIN.INPUT_SIGNAL),
            # ( self.GATE.OUTPUT, self.DEBUG.INPUT ),
            (self.WIN.OUTPUT_SIGNAL, self.LOG.INPUT_MESSAGE),
            # ( self.WIN.OUTPUT_SIGNAL, self.DEBUG.INPUT ),
            (self.LOG.OUTPUT_MESSAGE, self.TERM.INPUT),
            # ( self.LOG.OUTPUT_MESSAGE, self.DEBUG.INPUT ),
        )


@pytest.mark.parametrize("block_size", [1, 5, 10, 20])
@pytest.mark.parametrize("win_dur", [0.2, 1.0])
@pytest.mark.parametrize("win_shift", [None, 0.1, 1.0])
def test_window_system(
    block_size: int,
    win_dur: float,
    win_shift: Optional[float],
    test_name: Optional[str] = None,
):
    in_fs = 10.0  # Hz
    num_msgs = int((in_fs / block_size) * 4.0)  # Ensure 4 seconds of data

    test_filename = get_test_fn(test_name)
    ez.logger.info(test_filename)

    settings = WindowSystemSettings(
        num_msgs=num_msgs,
        counter_settings=CounterSettings(
            n_time=block_size, fs=in_fs, dispatch_rate=20.0
        ),
        window_settings=WindowSettings(window_dur=win_dur, window_shift=win_shift),
        log_settings=MessageLoggerSettings(output=test_filename),
        term_settings=TerminateTestSettings(time=1.0),  # sec
    )

    system = WindowSystem(settings)

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
        # In this test, fs should never change
        msg_fs = 1.0 / msg.axes["time"].gain
        if fs is None:
            fs = msg_fs
        assert fs == msg_fs

        # In this test, we should have consistent dimensions
        if dims is None:
            dims = msg.dims
        else:
            assert dims == msg.dims

        data.append(msg.data)

        # Window should always output the same shape data
        assert data[0].shape == msg.shape

    ez.logger.info("Consistent metadata!")

    # If this test was performed in "one-to-one" mode, we should
    # have one window output per message pushed to Window
    if win_shift is None:
        assert len(data) == num_msgs

    ez.logger.info("Test Complete.")


if __name__ == "__main__":
    test_window_system(5, 0.6, None, test_name="test_window_system")
