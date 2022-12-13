import warnings
import time

from dataclasses import dataclass, field
from ezmsg.util.messages import AxisArray, TimeAxis


# UPCOMING: TSMessage Deprecation
# TSMessage is deprecated because it doesn't handle multiple time axes well.
# AxisArray has an incompatible API but supports a superset of functionality
# including messages with multiple time axes and dimensional or categorical axes.
# warnings.warn( 
#     "TimeSeriesMessage/TSMessage is deprecated.  Please use ezmsg.utils.AxisArray", 
#     DeprecationWarning, 
#     stacklevel=2 
# )

@dataclass
class TSMessage(AxisArray):
    """
    NOTE: UPCOMING DEPRECATION. Please use ezmsg.utils.AxisArray
    This class remains as a backwards-compatible API for AxisArray

    TS(TimeSeries)Message:
    
    Base message type for timeseries data within ezmsg.sigproc
    TSMessages have one time dimension, and a sampling rate along that one time axis.
    Any higher dimensions are treated as "channels"
    """

    fs: float = 1.0
    time_dim: int = 0
    timestamp: float = field(default_factory = time.time)

    def __post_init__(self):
        super().__post_init__()
        self.axes[self.dims[self.time_dim]] = TimeAxis(fs=self.fs)

    @property
    def n_time(self) -> int:
        """ Number of time values in the message """
        return self.data.shape[self.time_dim]

    @property
    def n_ch(self) -> int:
        """ Number of channels in the message """
        return self.shape2d(self.dims[self.time_dim])[1]

    @property
    def _timestamp(self) -> float:
        return self.timestamp
