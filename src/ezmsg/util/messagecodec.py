"""
Message encoding and decoding utilities for ezmsg logging.

This module provides JSON serialization support for complex objects including:

- Dataclass objects with type preservation
- NumPy arrays with efficient binary encoding
- Arbitrary objects via pickle fallback

The MessageEncoder and MessageDecoder classes handle automatic conversion
between Python objects and JSON representations suitable for file logging.
"""

from collections.abc import Iterable, Generator
import json
import pickle
import base64
import typing
import importlib

from pathlib import Path
from dataclasses import dataclass, is_dataclass, fields
from functools import reduce

try:
    import numpy as np
    import numpy.typing as npt
except ImportError:
    np = None

TYPE = "_type"
TIMESTAMP_ATTR = "_log_timestamp"
PICKLE_TYPE = "_pickle"
PICKLE_DATA = "data"
NDARRAY_TYPE = "_ndarray"
NDARRAY_DTYPE = "dtype"
NDARRAY_SHAPE = "shape"
NDARRAY_DATA = "data"


@dataclass
class LogStart: ...


def type_str(obj: typing.Any) -> str:
    """
    Get a string representation of an object's type for serialization.

    :param obj: Object to get type string for
    :type obj: typing.Any
    :return: String representation in format 'module:qualname'
    :rtype: str
    """
    t = type(obj)
    name = getattr(t, "__qualname__", t.__name__)
    return f"{t.__module__}:{name}"


def import_type(typestr: str) -> type:
    """
    Import a type from a string representation.

    :param typestr: String representation in format 'module:qualname'
    :type typestr: str
    :return: The imported type
    :rtype: type
    :raises ImportError: If typestr does not resolve to a valid type
    """
    module, name = typestr.split(":")
    module = importlib.import_module(module)
    ty = reduce(lambda t, n: getattr(t, n), [module] + name.split("."))

    if not isinstance(ty, type):
        raise ImportError(f"{typestr} does not resolve to type")

    return ty


class MessageEncoder(json.JSONEncoder):
    """
    JSON encoder for ezmsg messages with support for dataclasses, numpy arrays, and arbitrary objects.

    This encoder extends the standard JSON encoder to handle:

    - Dataclass objects (serialized as dictionaries with type information)
    - NumPy arrays (serialized as base64-encoded data with metadata)
    - Other objects via pickle (as fallback)
    """

    def default(self, o: typing.Any):
        if is_dataclass(o):
            return {
                **{f.name: getattr(o, f.name) for f in fields(o)},
                **{TYPE: type_str(o)},
            }

        elif np and isinstance(o, np.ndarray):
            contig_obj = np.ascontiguousarray(o)
            buf = base64.b64encode(contig_obj.data)
            return {
                TYPE: NDARRAY_TYPE,
                NDARRAY_DTYPE: str(o.dtype),
                NDARRAY_DATA: buf.decode("ascii"),
                NDARRAY_SHAPE: o.shape,
            }

        try:
            return json.JSONEncoder.default(self, o)

        except TypeError:
            buf = base64.b64encode(pickle.dumps(o))
            return {
                TYPE: PICKLE_TYPE,
                PICKLE_DATA: buf.decode("ascii"),
            }


class StampedMessage(typing.NamedTuple):
    """
    A message with an associated timestamp.

    :param msg: The message object
    :type msg: typing.Any
    :param timestamp: Optional timestamp for the message
    :type timestamp: float | None
    """

    msg: typing.Any
    timestamp: float | None


def _object_hook(obj: dict[str, typing.Any]) -> typing.Any:
    """
    JSON object hook for decoding ezmsg messages.

    Handles reconstruction of dataclasses, numpy arrays, and pickled objects
    from their JSON representations.

    :param obj: Dictionary from JSON decoder
    :type obj: dict[str, typing.Any]
    :return: Reconstructed object
    :rtype: typing.Any
    """
    obj_type: str | None = obj.get(TYPE)

    out_obj: typing.Any = obj

    if obj_type is not None:
        if np and obj_type == NDARRAY_TYPE:
            data_bytes: str | None = obj.get(NDARRAY_DATA)
            data_shape: Iterable[int] | None = obj.get(NDARRAY_SHAPE)
            data_dtype: npt.DTypeLike | None = obj.get(NDARRAY_DTYPE)

            if (
                isinstance(data_bytes, str)
                and data_shape is not None
                and data_dtype is not None
            ):
                buf = base64.b64decode(data_bytes.encode("ascii"))
                out_obj = np.frombuffer(buf, dtype=data_dtype).reshape(data_shape)

        elif obj_type == PICKLE_TYPE:
            data_bytes: str | None = obj.get(PICKLE_DATA)
            if isinstance(data_bytes, str):
                buf = base64.b64decode(data_bytes.encode("ascii"))
                out_obj = pickle.loads(buf)

        else:
            cls = import_type(obj_type)
            del obj[TYPE]
            out_obj = cls(**obj)

    return out_obj


class MessageDecoder(json.JSONDecoder):
    """
    JSON decoder for ezmsg messages.

    Automatically reconstructs dataclasses, numpy arrays, and pickled objects
    from their JSON representations using the _object_hook function.
    """

    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=_object_hook, *args, **kwargs)


def message_log(
    fname: Path, return_object: bool = True
) -> Generator[typing.Any, None, None]:
    """
    Generator function to read messages from a log file created by MessageLogger.

    :param fname: Path to the log file
    :type fname: Path
    :param return_object: If True, yield only the message objects; if False, yield complete log entries
    :type return_object: bool
    :return: Generator yielding messages or log entries
    :rtype: collections.abc.Generator[typing.Any, None, None]
    """
    with open(fname, "r") as f:
        for line in f:
            obj = json.loads(line, cls=MessageDecoder)
            if isinstance(obj["obj"], LogStart):
                continue
            if return_object is True:
                yield obj["obj"]
            else:
                yield obj
