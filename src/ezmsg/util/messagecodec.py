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
    t = type(obj)
    name = getattr(t, "__qualname__", t.__name__)
    return f"{t.__module__}:{name}"


def import_type(typestr: str) -> type:
    module, name = typestr.split(":")
    module = importlib.import_module(module)
    ty = reduce(lambda t, n: getattr(t, n), [module] + name.split("."))

    if not isinstance(ty, type):
        raise ImportError(f"{typestr} does not resolve to type")

    return ty


class MessageEncoder(json.JSONEncoder):
    def default(self, obj: typing.Any):
        if is_dataclass(obj):
            return {
                **{f.name: getattr(obj, f.name) for f in fields(obj)},
                **{TYPE: type_str(obj)},
            }

        elif np and isinstance(obj, np.ndarray):
            contig_obj = np.ascontiguousarray(obj)
            buf = base64.b64encode(contig_obj.data)
            return {
                TYPE: NDARRAY_TYPE,
                NDARRAY_DTYPE: str(obj.dtype),
                NDARRAY_DATA: buf.decode("ascii"),
                NDARRAY_SHAPE: obj.shape,
            }

        try:
            return json.JSONEncoder.default(self, obj)

        except TypeError:
            buf = base64.b64encode(pickle.dumps(obj))
            return {
                TYPE: PICKLE_TYPE,
                PICKLE_DATA: buf.decode("ascii"),
            }


class StampedMessage(typing.NamedTuple):
    msg: typing.Any
    timestamp: typing.Optional[float]


class MessageDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj: typing.Dict[str, typing.Any]) -> typing.Any:
        obj_type: typing.Optional[str] = obj.get(TYPE)

        out_obj: typing.Any = obj

        if obj_type is not None:
            if np and obj_type == NDARRAY_TYPE:
                data_bytes: typing.Optional[str] = obj.get(NDARRAY_DATA)
                data_shape: typing.Optional[typing.Iterable[int]] = obj.get(
                    NDARRAY_SHAPE
                )
                data_dtype: typing.Optional[npt.DTypeLike] = obj.get(NDARRAY_DTYPE)

                if (
                    isinstance(data_bytes, str)
                    and data_shape is not None
                    and data_dtype is not None
                ):
                    buf = base64.b64decode(data_bytes.encode("ascii"))
                    out_obj = np.frombuffer(buf, dtype=data_dtype).reshape(data_shape)

            elif obj_type == PICKLE_TYPE:
                data_bytes: typing.Optional[str] = obj.get(PICKLE_DATA)
                if isinstance(data_bytes, str):
                    buf = base64.b64decode(data_bytes.encode("ascii"))
                    out_obj = pickle.loads(buf)

            else:
                cls = import_type(obj_type)
                del obj[TYPE]
                out_obj = cls(**obj)

        return out_obj


def message_log(
    fname: Path, return_object: bool = True
) -> typing.Generator[typing.Any, None, None]:
    with open(fname, "r") as f:
        for line in f:
            obj = json.loads(line, cls=MessageDecoder)
            if isinstance(obj["obj"], LogStart):
                continue
            if return_object is True:
                yield obj["obj"]
            else:
                yield obj
