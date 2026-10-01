"""
Wire elision of coordinate axes a receiver already holds.

An :class:`~ezmsg.util.messages.axisarray.AxisArray` stream sends the same
non-stream coordinate axes (channel labels, positions, ...) in every message,
often more bytes than the data itself. Within a process that costs nothing --
messages share the axis objects -- but across one, every message serializes,
copies and unpickles them again, and the receiver gets a new axis object per
message, so consumers can only compare it by value.

Here the publisher sends each such axis in full once (an :class:`_AxisDef`:
token plus axis) and thereafter as a 16-byte :class:`_AxisRef`. The receiving
channel keeps the axes it was given, copied out of the transport's memory, and
substitutes them for the references, so every message of a stream shares one
axis object on the far side too.

The token is derived from :attr:`CoordinateAxis.fingerprint` (a 64-bit content
digest), so equal axes share a token no matter which object carried them.

Only the axes of a top-level ``AxisArray`` message are elided, and never its
stream axis, whose values change every message. Everything else pickles as
before. Set ``EZMSG_DISABLE_AXIS_ELISION`` to turn the feature off.
"""

import hashlib
import os
import pickle
import typing

from collections import OrderedDict

ELISION_ENABLED = "EZMSG_DISABLE_AXIS_ELISION" not in os.environ

# Stream dimension assumed for a message that does not declare one; mirrors
# ezmsg-baseproc's default, so a per-message "time" axis is never elided.
FALLBACK_STREAM_DIM = "time"

# Publisher: forget what was announced (forcing definitions again) past this
# many distinct axes. Receiver: keep at most this many.
MAX_ANNOUNCED = 256
MAX_TABLE = 256

_AxisArray: typing.Any = None
_CoordinateAxis: typing.Any = None


def _types() -> tuple[typing.Any, typing.Any]:
    # Imported lazily: ezmsg.util.messages imports ezmsg.core.
    global _AxisArray, _CoordinateAxis
    if _AxisArray is None:
        from ..util.messages.axisarray import AxisArray, CoordinateAxis

        _AxisArray, _CoordinateAxis = AxisArray, CoordinateAxis
    return _AxisArray, _CoordinateAxis


class MissingAxis(Exception):
    """A message referenced an axis this receiver does not hold."""


class _AxisRef:
    """Stands in, on the wire, for an axis the receiver already holds."""

    __slots__ = ("token",)

    def __init__(self, token: bytes) -> None:
        self.token = token

    def __reduce__(self):
        return (_AxisRef, (self.token,))


class _AxisDef:
    """Carries an axis on the wire along with the token later messages will use."""

    __slots__ = ("token", "axis")

    def __init__(self, token: bytes, axis: typing.Any) -> None:
        self.token = token
        self.axis = axis

    def __reduce__(self):
        return (_AxisDef, (self.token, self.axis))


def wire_token(axis: typing.Any) -> bytes | None:
    """The axis's token, computed once per axis object; None if it has no
    fingerprint (contents that cannot be digested are never elided)."""
    d = axis.__dict__
    token = d.get("_wire_token")
    if token is None:
        fp = axis.fingerprint
        if fp is None:
            return None
        # The fingerprint holds a numpy dtype, slow to unpickle; a fixed-size
        # digest of it is what travels.
        token = d["_wire_token"] = hashlib.blake2b(pickle.dumps(fp, protocol=5), digest_size=16).digest()
    return token


def _stream_dim(d: dict) -> str | None:
    stream = d.get("stream_dim")
    if stream is None and FALLBACK_STREAM_DIM in d["dims"]:
        stream = FALLBACK_STREAM_DIM
    return stream


class AxisElision:
    """Publisher side: which axes have been announced to the current channels."""

    __slots__ = ("announced",)

    def __init__(self) -> None:
        self.announced: set[bytes] = set()

    def reset(self) -> None:
        """Send every axis in full again (a channel joined, or asked)."""
        self.announced.clear()

    def wire(self, obj: typing.Any) -> typing.Any:
        """``obj``, or a shallow stand-in whose eligible axes are elided."""
        AxisArray, CoordinateAxis = _types()
        if not isinstance(obj, AxisArray):
            return obj
        d = obj.__dict__
        axes = d["axes"]
        stream = _stream_dim(d)
        new_axes = None
        for dim, axis in axes.items():
            if dim == stream or type(axis) is not CoordinateAxis:
                continue
            token = wire_token(axis)
            if token is None:
                continue
            if new_axes is None:
                new_axes = dict(axes)
            if token in self.announced:
                new_axes[dim] = _AxisRef(token)
            else:
                if len(self.announced) >= MAX_ANNOUNCED:
                    self.announced.clear()
                self.announced.add(token)
                new_axes[dim] = _AxisDef(token, axis)
        if new_axes is None:
            return obj
        # A bare copy of the message (no __init__, so no validation) differing
        # only in its axes dict; the caller's message is untouched.
        wired = object.__new__(type(obj))
        wired.__dict__.update(d)
        wired.__dict__["axes"] = new_axes
        return wired


def _owned(axis: typing.Any) -> typing.Any:
    """A copy of ``axis`` whose data owns its memory (it may arrive as a view
    into a channel's shared memory, which must not be pinned or read after the
    slot is reused), keeping its cached fingerprint and token."""
    import numpy as np

    data = axis.data
    if isinstance(data, np.ndarray) and data.flags.owndata:
        return axis
    out = object.__new__(type(axis))
    out.__dict__.update(axis.__dict__)
    out.__dict__["data"] = np.array(data, copy=True)
    return out


class AxisTable:
    """Receiver side: the axes this channel has been given, by token."""

    __slots__ = ("_axes",)

    def __init__(self) -> None:
        self._axes: "OrderedDict[bytes, typing.Any]" = OrderedDict()

    def __len__(self) -> int:
        return len(self._axes)

    def resolve(self, obj: typing.Any) -> typing.Any:
        """Replace an ``AxisArray``'s elided axes in place; record new ones.

        :raises MissingAxis: if a reference names an axis not held here.
        """
        AxisArray, _ = _types()
        if not isinstance(obj, AxisArray):
            return obj
        axes = obj.__dict__["axes"]
        for dim, axis in axes.items():
            kind = type(axis)
            if kind is _AxisRef:
                try:
                    axes[dim] = self._axes[axis.token]
                except KeyError:
                    raise MissingAxis(axis.token.hex()) from None
            elif kind is _AxisDef:
                held = self._axes.get(axis.token)
                if held is None:
                    held = self._axes[axis.token] = _owned(axis.axis)
                    if len(self._axes) > MAX_TABLE:
                        self._axes.popitem(last=False)
                else:
                    self._axes.move_to_end(axis.token)
                axes[dim] = held
        return obj
