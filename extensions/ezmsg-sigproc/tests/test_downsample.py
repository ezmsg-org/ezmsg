import os
import json
import logging

import pytest
import numpy as np

import ezmsg.core as ez
from ezmsg.util.messagegate import MessageGate, MessageGateSettings
from ezmsg.util.messagelogger import MessageDecoder, MessageLogger, MessageLoggerSettings
from ezmsg.sigproc.downsample import Downsample, DownsampleSettings
from ezmsg.sigproc.synth import Oscillator, OscillatorSettings

from ezmsg.testing import get_test_fn
from ezmsg.testing.terminate import TerminateTest, TerminateTestSettings
from ezmsg.testing.debuglog import DebugLog

from typing import Optional, List

logger = logging.getLogger('ezmsg')


class DownsampleSystemSettings(ez.Settings):
    num_msgs: int
    osc_settings: OscillatorSettings
    down_settings: DownsampleSettings
    log_settings: MessageLoggerSettings
    term_settings: TerminateTestSettings


class DownsampleSystem(ez.System):

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
                default_after=self.SETTINGS.num_msgs
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
    block_size: int,
    factor: int,
    test_name: Optional[str] = None
):

    in_fs = 10.0

    # Ensure 4 seconds of data
    num_msgs = int((in_fs / block_size) * 4.0)

    test_filename = get_test_fn(test_name)
    logger.info(test_filename)

    settings = DownsampleSystemSettings(
        num_msgs=num_msgs,
        osc_settings=OscillatorSettings(
            n_time=block_size,
            freq=1.0,  # Hz
            fs=in_fs,
            dispatch_rate=20.0,
            sync=True
        ),
        down_settings=DownsampleSettings(
            factor=factor
        ),
        log_settings=MessageLoggerSettings(
            output=test_filename
        ),
        term_settings=TerminateTestSettings(
            time=1.0
        )
    )

    system = DownsampleSystem(settings)

    ez.run_system(system)

    messages = []
    with open(test_filename, "r") as file:
        for line in file:
            messages.append(json.loads(line, cls=MessageDecoder))

    os.remove(test_filename)

    logger.info(f'Analyzing recording of { len( messages ) } messages...')

    fs: Optional[float] = None
    time_dim: Optional[int] = None
    data: List[np.ndarray] = []
    for msg in messages:

        # In this test, fs should change by factor
        if fs is None:
            fs = msg.get('fs')
        assert fs == msg.get('fs')
        assert fs == in_fs / factor

        # In this test, we should have consistent time dimension
        if time_dim is None:
            time_dim = msg.get('time_dim')
        else:
            assert time_dim == msg.get('time_dim')

        data.append(msg.get('data'))

    logger.info('Consistent metadata!')

    # TODO: Write meaningful analyses of the recording to test functionality

    logger.info('Test Complete.')


if __name__ == '__main__':
    test_downsample_system(10, 2, test_name='test_window_system')
