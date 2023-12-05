import os
from typing import Optional, List
from dataclasses import field
import numpy as np

from ezmsg.util.messages.axisarray import AxisArray

from ezmsg.sigproc.scaler import scaler, scaler_np

# For test system
from util import get_test_fn
from ezmsg.sigproc.scaler import AdaptiveStandardScalerSettings, AdaptiveStandardScaler
import ezmsg.core as ez
from ezmsg.sigproc.synth import Counter, CounterSettings
from ezmsg.util.terminate import TerminateOnTotalSettings, TerminateOnTotal
from ezmsg.util.messagelogger import MessageLogger, MessageLoggerSettings
from ezmsg.util.messagecodec import message_log


def test_adaptive_standard_scaler_river():
    # Test data values taken from river:
    # https://github.com/online-ml/river/blob/main/river/preprocessing/scale.py#L511-L536C17
    data = np.array([5.278, 5.050, 6.550, 7.446, 9.472, 10.353, 11.784, 11.173])
    expected_result = np.array([0.0, -0.816, 0.812, 0.695, 0.754, 0.598, 0.651, 0.124])

    test_input = AxisArray(np.tile(data, (2, 1)), dims=["ch", "time"], axes={"time": AxisArray.Axis()})

    tau = 1.0914  # The River example used alpha = 0.6. tau = -gain / np.log(1 - alpha) and here we're using gain = 1.0
    _scaler = scaler(time_constant=tau, axis="time")
    output = _scaler.send(test_input)
    assert np.allclose(output.data[0], expected_result, atol=1e-3)

    _scaler_np = scaler_np(time_constant=tau, axis="time")
    output = _scaler_np.send(test_input)
    assert np.allclose(output.data[0], expected_result, atol=1e-3)


class ScalerTestSystemSettings(ez.Settings):
    counter_settings: CounterSettings
    scaler_settings: AdaptiveStandardScalerSettings
    log_settings: MessageLoggerSettings
    term_settings: TerminateOnTotalSettings = field(default_factory=TerminateOnTotalSettings)


class ScalerTestSystem(ez.Collection):
    SETTINGS: ScalerTestSystemSettings

    COUNTER = Counter()
    SCALER = AdaptiveStandardScaler()
    LOG = MessageLogger()
    TERM = TerminateOnTotal()

    def configure(self) -> None:
        self.COUNTER.apply_settings(self.SETTINGS.counter_settings)
        self.SCALER.apply_settings(self.SETTINGS.scaler_settings)
        self.LOG.apply_settings(self.SETTINGS.log_settings)
        self.TERM.apply_settings(self.SETTINGS.term_settings)

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.COUNTER.OUTPUT_SIGNAL, self.SCALER.INPUT),
            (self.SCALER.OUTPUT, self.LOG.INPUT_MESSAGE),
            (self.LOG.OUTPUT_MESSAGE, self.TERM.INPUT_MESSAGE)
        )


def test_scaler_system(
        tau: float = 1.0,
        fs: float = 10.0,
        duration: float = 2.0,
        test_name: Optional[str] = None,
):
    """
    For this test, we assume that Counter and scaler_np are functioning properly.
    The purpose of this test is exclusively to test that the AdaptiveStandardScaler and AdaptiveStandardScalerSettings
    generated classes are wrapping scaler_np and exposing its parameters.
    This test passing should only be considered a success if test_adaptive_standard_scaler_river also passed.
    """
    block_size: int = 4
    test_filename = get_test_fn(test_name)
    ez.logger.info(test_filename)
    settings = ScalerTestSystemSettings(
        counter_settings=CounterSettings(
            n_time=block_size,
            fs=fs,
            n_ch=1,
            dispatch_rate=duration,  # Simulation duration in 1.0 seconds
            mod=None,
        ),
        scaler_settings=AdaptiveStandardScalerSettings(
            time_constant=tau,
            axis="time"
        ),
        log_settings=MessageLoggerSettings(
            output=test_filename,
        ),
        term_settings=TerminateOnTotalSettings(
            total=int(duration * fs / block_size),
        )
    )
    system = ScalerTestSystem(settings)
    ez.run(SYSTEM=system)

    # Collect result
    messages: List[AxisArray] = [_ for _ in message_log(test_filename)]
    os.remove(test_filename)

    data = np.concatenate([_.data for _ in messages]).squeeze()

    expected_input = AxisArray(np.arange(len(data))[None, :],
                               dims=["ch", "time"], axes={"time": AxisArray.Axis(gain=1/fs, offset=0.0)})
    _scaler = scaler_np(time_constant=tau, axis="time")
    expected_output = _scaler.send(expected_input)
    assert np.allclose(expected_output.data.squeeze(), data)
