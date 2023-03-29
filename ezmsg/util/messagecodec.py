import json
import base64
import importlib

from pathlib import Path
from dataclasses import is_dataclass, fields
from functools import reduce

from typing import Optional, Any, Dict, Iterable, Generator

try:
    import numpy as np
    import numpy.typing as npt
except ImportError:
    np = None

TYPE = "_type"
NDARRAY_TYPE = "_ndarray"
NDARRAY_DTYPE = "dtype"
NDARRAY_SHAPE = "shape"
NDARRAY_DATA = "data"


def type_str(obj: Any) -> str:
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
    def default(self, obj: Any):
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

        return json.JSONEncoder.default(self, obj)


class MessageDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj: Dict[str, Any]) -> Any:
        obj_type: Optional[str] = obj.get(TYPE)

        if obj_type is None:
            return obj

        if np and obj_type == NDARRAY_TYPE:
            data_bytes: Optional[str] = obj.get(NDARRAY_DATA)
            data_shape: Optional[Iterable[int]] = obj.get(NDARRAY_SHAPE)
            data_dtype: Optional[npt.DTypeLike] = obj.get(NDARRAY_DTYPE)

            if (
                isinstance(data_bytes, str)
                and data_shape is not None
                and data_dtype is not None
            ):
                buf = base64.b64decode(data_bytes.encode("ascii"))
                return np.frombuffer(buf, dtype=data_dtype).reshape(data_shape)

        else:
            cls = import_type(obj_type)
            del obj[TYPE]
            return cls(**obj)

        return obj


def message_log(fname: Path) -> Generator[Any, None, None]:
    with open(fname, "r") as f:
        for l in f:
            obj = json.loads(l, cls=MessageDecoder)
            yield obj
