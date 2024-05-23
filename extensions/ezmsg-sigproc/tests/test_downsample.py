import os
import json

import pytest
import numpy as np

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messagegate import MessageGate, MessageGateSettings
from ezmsg.util.messagelogger import MessageLogger, MessageLoggerSettings
from ezmsg.util.messagecodec import message_log
from ezmsg.sigproc.downsample import downsample, Downsample, DownsampleSettings
from ezmsg.sigproc.synth import Counter, CounterSettings

from util import get_test_fn
from ezmsg.util.terminate import TerminateOnTimeout as TerminateTest
from ezmsg.util.terminate import TerminateOnTimeoutSettings as TerminateTestSettings
from ezmsg.util.debuglog import DebugLog

from typing import Optional, List


@pytest.mark.parametrize("block_size", [1, 5, 10, 20])
@pytest.mark.parametrize("factor", [1, 2, 3])
def test_downsample_core(block_size: int, factor: int):
    in_fs = 19.0
    test_dur = 4.0
    n_channels = 2
    n_features = 3
    num_samps = int(np.ceil(test_dur * in_fs))
    num_msgs = int(np.ceil(num_samps / block_size))
    sig = np.arange(num_samps * n_channels * n_features).reshape(num_samps, n_channels, n_features)
    tvec = np.arange(num_samps) / in_fs

    def msg_generator():
        for msg_ix in range(num_msgs):
            msg_sig = sig[msg_ix*block_size:(msg_ix+1)*block_size]
            msg_idx = msg_sig[0, 0, 0] / (n_channels * n_features)
            msg_offs = msg_idx / in_fs
            msg = AxisArray(
                data=msg_sig,
                dims=["time", "ch", "feat"],
                axes={"time": AxisArray.Axis.TimeAxis(fs=in_fs, offset=msg_offs)}
            )
            yield msg

    proc = downsample(axis="time", factor=factor)
    out_msgs = []
    for msg in msg_generator():
        res = proc.send(msg)
        if res.data.size:
            out_msgs.append(res)

    # Assert correctness of gain
    assert all(msg.axes["time"].gain == factor / in_fs for msg in out_msgs)

    # Assert messages have the correct timestamps
    expected_offsets = np.cumsum([0] + [_.data.shape[0] for _ in out_msgs]) * factor / in_fs
    actual_offsets = np.array([_.axes["time"].offset for _ in out_msgs])
    assert np.allclose(actual_offsets, expected_offsets[:-1])

    # Compare returned values to expected values.
    allres_msg = AxisArray.concatenate(*out_msgs, dim="time")
    assert np.array_equal(allres_msg.data, sig[::factor])


class DownsampleSystemSettings(ez.Settings):
    num_msgs: int
    counter_settings: CounterSettings
    down_settings: DownsampleSettings
    log_settings: MessageLoggerSettings
    term_settings: TerminateTestSettings


class DownsampleSystem(ez.Collection):
    COUNT = Counter()
    GATE = MessageGate()
    DOWN = Downsample()
    LOG = MessageLogger()
    TERM = TerminateTest()

    DEBUG = DebugLog()

    SETTINGS: DownsampleSystemSettings

    def configure(self) -> None:
        self.COUNT.apply_settings(self.SETTINGS.counter_settings)
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
            (self.COUNT.OUTPUT_SIGNAL, self.GATE.INPUT),
            # ( self.COUNT.OUTPUT_SIGNAL, self.DEBUG.INPUT ),
            (self.GATE.OUTPUT, self.DOWN.INPUT_SIGNAL),
            # ( self.GATE.OUTPUT, self.DEBUG.INPUT ),
            (self.DOWN.OUTPUT_SIGNAL, self.LOG.INPUT_MESSAGE),
            # ( self.DOWN.OUTPUT_SIGNAL, self.DEBUG.INPUT ),
            (self.LOG.OUTPUT_MESSAGE, self.TERM.INPUT),
            # ( self.LOG.OUTPUT_MESSAGE, self.DEBUG.INPUT ),
        )


@pytest.mark.parametrize("block_size", [10])
@pytest.mark.parametrize("factor", [3])
def test_downsample_system(
    block_size: int, factor: int, test_name: Optional[str] = None
):
    in_fs = 19.0
    num_msgs = int(4.0 / (block_size / in_fs))  # Ensure 4 seconds of data

    test_filename = get_test_fn(test_name)
    ez.logger.info(test_filename)

    settings = DownsampleSystemSettings(
        num_msgs=num_msgs,
        counter_settings=CounterSettings(
            n_time=block_size, fs=in_fs, dispatch_rate=20.0,
        ),
        down_settings=DownsampleSettings(factor=factor),
        log_settings=MessageLoggerSettings(output=test_filename),
        term_settings=TerminateTestSettings(time=1.0),
    )

    system = DownsampleSystem(settings)

    ez.run(SYSTEM = system)

    messages: List[AxisArray] = [_ for _ in message_log(test_filename)]
    os.remove(test_filename)
    ez.logger.info(f"Analyzing recording of { len( messages ) } messages...")

    # Check fs
    out_fs = in_fs / factor
    assert np.allclose(
        np.array([1 / msg.axes["time"].gain for msg in messages]),
        np.ones(len(messages,)) * out_fs
    )

    # Check data
    time_ax_idx = messages[0].get_axis_idx("time")
    data = np.concatenate([_.data for _ in messages], axis=time_ax_idx)
    expected_data = np.arange(data.shape[time_ax_idx]) * factor
    assert np.array_equal(data, expected_data[:, None])

    # Grab first sample from each message. We will use their values to get the offsets.
    #  This works because the input is Counter and we validated it above.
    first_samps = [np.take(msg.data, [0], axis=time_ax_idx) for msg in messages]

    # Check that the shape of each message is the same -- the set of shapes will be reduced to a single item.
    assert len(set([_.shape for _ in first_samps])) == 1

    # Check offsets
    first_samps = np.concatenate(first_samps, axis=time_ax_idx)
    expected_offsets = first_samps.squeeze() / out_fs / factor
    assert np.allclose(
        np.array([msg.axes["time"].offset for msg in messages]),
        expected_offsets
    )

    ez.logger.info("Test Complete.")


if __name__ == "__main__":
    test_downsample_system(10, 2, test_name="test_window_system")
