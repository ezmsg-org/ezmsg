import sys
import typing

# This content ripped from xarray
# https://github.com/pydata/xarray/blob/8e899adcd72295a138228056643a78cac6e2de57/xarray/core/utils.py

# See GH5624, this is a convoluted way to allow type-checking to use `TypeGuard` without
# requiring typing_extensions as a required dependency to _run_ the code (it is required
# to type-check).
try:
    if sys.version_info >= (3, 10):
        from typing import TypeGuard
    else:
        from typing_extensions import TypeGuard
except ImportError:
    if typing.TYPE_CHECKING:
        raise

T = typing.TypeVar("T")

def is_dict_like(value: typing.Any) -> TypeGuard[typing.Mapping]:
    return hasattr(value, "keys") and hasattr(value, "__getitem__")

def either_dict_or_kwargs(
    pos_kwargs: typing.Optional[typing.Mapping[str, T]],
    kw_kwargs: typing.Mapping[str, T],
    func_name: str,
) -> typing.Mapping[str, T]:
    if pos_kwargs is None or pos_kwargs == {}:
        # Need an explicit cast to appease mypy due to invariance; see
        # https://github.com/python/mypy/issues/6228
        return typing.cast(typing.Mapping[str, T], kw_kwargs)

    if not is_dict_like(pos_kwargs):
        raise ValueError(f"the first argument to .{func_name} must be a dictionary")
    if kw_kwargs:
        raise ValueError(
            f"cannot specify both keyword and positional arguments to .{func_name}"
        )
    return pos_kwargs
