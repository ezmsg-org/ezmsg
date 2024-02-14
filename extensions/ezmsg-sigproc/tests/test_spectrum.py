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
from ezmsg.sigproc.spectrum import spectrum

from util import get_test_fn
from ezmsg.util.terminate import TerminateOnTimeout as TerminateTest
from ezmsg.util.terminate import TerminateOnTimeoutSettings as TerminateTestSettings

from typing import Optional, List


def test_spectrum_gen():
    gen = spectrum()
    result = gen.send(test_input)
    assert np.allclose(result, expected_result)
