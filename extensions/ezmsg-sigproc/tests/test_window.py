from dataclasses import field

import os
import json

import pytest
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
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

from typing import Optional, Dict, Any, List, Tuple


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
@pytest.mark.parametrize("newaxis", [None, "step"])
@pytest.mark.parametrize("win_dur", [0.2, 1.0])
@pytest.mark.parametrize("win_shift", [None, 0.1, 1.0])
def test_window_system(
    block_size: int,
    newaxis: Optional[str],
    win_dur: float,
    win_shift: Optional[float],
    test_name: Optional[str] = None,
):
    in_fs = 10.0  # Hz
    zero_pad_until = "full"  # See WindowSettings for other options
    # Calculate expected dimensions.
    win_len = int(win_dur * in_fs)
    shift_len = int(win_shift * in_fs) if win_shift is not None else block_size
    # num_msgs should be the greater value between (2 full windows + a shift) or 4.0 seconds
    num_msgs = int(np.ceil(max(2 * win_len + shift_len - 1, 4.0 * in_fs) / block_size))

    test_filename = get_test_fn(test_name)
    ez.logger.info(test_filename)

    settings = WindowSystemSettings(
        num_msgs=num_msgs,
        counter_settings=CounterSettings(
            n_time=block_size,
            fs=in_fs,
            dispatch_rate=float(num_msgs),  # Get through them in about 1 second.
        ),
        window_settings=WindowSettings(
            axis=None,
            newaxis=newaxis,
            window_dur=win_dur,
            window_shift=win_shift,
            zero_pad_until=zero_pad_until
        ),
        log_settings=MessageLoggerSettings(output=test_filename),
        term_settings=TerminateTestSettings(time=1.0),  # sec
    )

    system = WindowSystem(settings)
    ez.run(SYSTEM=system)

    messages: List[AxisArray] = [_ for _ in message_log(test_filename)]
    os.remove(test_filename)
    ez.logger.info(f"Analyzing recording of { len( messages ) } messages...")

    # Within a test config, the metadata should not change across messages.
    dims: List[str] = messages[0].dims
    n_ch = messages[0].shape[messages[0].get_axis_idx("ch")]
    for msg in messages:
        # In this test, fs should never change
        assert 1.0 / msg.axes["time"].gain == in_fs
        # In this test, we should have consistent dimensions
        assert dims == msg.dims
        # Window should always output the same shape data
        assert msg.shape[msg.get_axis_idx("ch")] == n_ch
        assert msg.shape[msg.get_axis_idx("time")] == win_len

    ez.logger.info("Consistent metadata!")

    # Collect the outputs we want to test
    data: List[np.ndarray] = [msg.data for msg in messages]
    offsets: np.ndarray = np.array([msg.axes[newaxis or "time"].offset for msg in messages])

    # If this test was performed in "one-to-one" mode, we should
    # have one window output per message pushed to Window
    if win_shift is None:
        assert len(data) == num_msgs

    # Turn the data into a ndarray.
    concat_ax = 0
    if newaxis is not None:
        concat_ax = messages[0].get_axis_idx(newaxis)
        data = np.concatenate(data, axis=concat_ax)
    else:
        data = np.stack(data)

    # We need to calculate how much zero padding there was so we know what to expect in the output.
    if zero_pad_until == "input":
        # As we are using zero_pad_until = "input", our padding is win_len - block_size
        zero_pad = max(0, win_len - block_size)
    else:  # "full"
        zero_pad = win_len

    # Check data
    assert data.shape[concat_ax] == len(messages)
    expected_counts = np.arange(num_msgs * block_size + zero_pad).astype(float) - zero_pad
    expected_data = sliding_window_view(expected_counts, (win_len,))
    if win_shift is None:
        expected_data = expected_data[max(0, block_size - win_len)::block_size]
    else:
        expected_data = expected_data[::shift_len][:data.shape[concat_ax]]
    assert np.array_equal(data, np.clip(expected_data[..., None], 0, None))

    # Check offsets
    expected_offsets = expected_data[:, 0] / in_fs
    assert np.allclose(offsets, expected_offsets)

    ez.logger.info("Test Complete.")


if __name__ == "__main__":
    test_window_system(5, 0.6, None, test_name="test_window_system")
