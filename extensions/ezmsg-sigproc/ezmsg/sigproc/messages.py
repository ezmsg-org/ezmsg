import warnings
import time

import numpy.typing as npt

from ezmsg.util.messages import AxisArray, time_axis

from typing import Optional

# UPCOMING: TSMessage Deprecation
# TSMessage is deprecated because it doesn't handle multiple time axes well.
# AxisArray has an incompatible API but supports a superset of functionality
# including messages with multiple time axes and dimensional or categorical axes.
warnings.warn( 
    "TimeSeriesMessage/TSMessage is deprecated.  Please use ezmsg.utils.AxisArray", 
    DeprecationWarning, 
    stacklevel=2 
)

def TSMessage( data: npt.NDArray, fs: float = 1.0, time_dim: int = 0, timestamp: Optional[float] = None ) -> AxisArray:
    dims = [f'dim_{i}' for i in range(data.ndim)]
    dims[time_dim] = 'time'
    offset = time.time() if timestamp is None else timestamp
    axis = time_axis(fs, offset = offset)
    return AxisArray( data, dims = dims, axes = dict( time = axis ) )

