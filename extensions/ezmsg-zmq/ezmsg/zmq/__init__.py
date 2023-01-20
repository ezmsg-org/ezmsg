__all__ = [
    "__version__",
    "ZMQMessage",
]

from .__version__ import __version__

from dataclasses import dataclass
from pickle import PickleBuffer

class ZeroCopyBytes(bytes):

    def __reduce_ex__(self, protocol):
        if protocol >= 5:
            return type(self)._reconstruct, (PickleBuffer(self),), None
        else:
            # PickleBuffer is forbidden with pickle protocols <= 4.
            return type(self)._reconstruct, (bytes(self),)

    @classmethod
    def _reconstruct(cls, obj):
        with memoryview(obj) as m:
            # Get a handle over the original buffer object
            obj = m.obj
            if isinstance(obj, cls):
                # Original buffer object is a ZeroCopyBytes, return it
                # as-is.
                return obj
            else:
                return cls(obj)

@dataclass
class ZMQMessage:
    data: bytes
    