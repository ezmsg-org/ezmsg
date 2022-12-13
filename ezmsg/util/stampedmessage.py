import time
from dataclasses import dataclass, field
import warnings

warnings.warn( 
    "StampedMessage is deprecated.  Please add timestamp fields into your own message dataclasses", 
    DeprecationWarning, 
    stacklevel=2 
)

@dataclass
class StampedMessage:
    _timestamp: float = field(default_factory=time.time, init=False)
