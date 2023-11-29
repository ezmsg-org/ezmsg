import numpy as np

from ezmsg.util.messages.axisarray import AxisArray

# from ezmsg.wyss.adaptive_standard_scaler import AdaptiveStandardScalerSettings, AdaptiveStandardScaler
from ezmsg.wyss.scaler import scaler, scaler_np


def test_adaptive_standard_scaler_river():
    # Test data values taken from river:
    # https://github.com/online-ml/river/blob/main/river/preprocessing/scale.py#L511-L536C17
    data = np.array([5.278, 5.050, 6.550, 7.446, 9.472, 10.353, 11.784, 11.173])
    expected_result = np.array([0.0, -0.816, 0.812, 0.695, 0.754, 0.598, 0.651, 0.124])

    input = AxisArray(np.tile(data, (2, 1)), dims=["ch", "time"], axes={"time": AxisArray.Axis()})

    tau = 1.0914  # The River example used alpha = 0.6. tau = -gain / np.log(1 - alpha) and here we're using gain = 1.0
    _scaler = scaler(time_constant=tau, axis="time")
    output = _scaler.send(input)
    assert np.allclose(output.data[0], expected_result, atol=1e-3)

    _scaler_np = scaler_np(time_constant=tau, axis="time")
    output = _scaler_np.send(input)  # Note: about 6.3e-5 s / 8-sample call
    assert np.allclose(output.data[0], expected_result, atol=1e-3)
