import warnings
import time

import numpy.typing as npt

from ezmsg.util.messages.axisarray import AxisArray

from typing import Optional

# UPCOMING: TSMessage Deprecation
# TSMessage is deprecated because it doesn't handle multiple time axes well.
# AxisArray has an incompatible API but supports a superset of functionality.
warnings.warn(
    "TimeSeriesMessage/TSMessage is deprecated.  Please use ezmsg.utils.AxisArray",
    DeprecationWarning,
    stacklevel=2,
)


def TSMessage(
    data: npt.NDArray,
    fs: float = 1.0,
    time_dim: int = 0,
    timestamp: Optional[float] = None,
) -> AxisArray:
    dims = [f"dim_{i}" for i in range(data.ndim)]
    dims[time_dim] = "time"
    offset = time.time() if timestamp is None else timestamp
    offset_adj = data.shape[time_dim] / fs  # offset corresponds to idx[0] on time_dim
    axis = AxisArray.Axis.TimeAxis(fs, offset=offset - offset_adj)
    return AxisArray(data, dims=dims, axes=dict(time=axis))
