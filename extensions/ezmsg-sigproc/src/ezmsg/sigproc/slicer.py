from dataclasses import replace
from typing import Generator

import numpy as np
from ezmsg.util.messages.axisarray import AxisArray, slice_along_axis
from ezmsg.util.gen_to_unit import gen_to_unit
from ezmsg.util.generator import consumer


"""
Slicer:Select a subset of data along a particular axis.
"""


def parse_slice(s: str) -> tuple[slice]:
    """
    Parses a string representation of a slice and returns a tuple of slice objects.

    Args:
        s (str): The string representation of the slice.

    Returns:
        tuple[slice]: A tuple of slice objects.
    """
    if s.lower() in ["", ":", "none"]:
        return (slice(None),)
    if "," not in s:
        parts = [part.strip() for part in s.split(":")]
        if len(parts) == 1:
            return (slice(int(parts[0]), int(parts[0]) + 1),)
        return (slice(*(int(part.strip()) if part else None for part in parts)),)
    l = [parse_slice(_) for _ in s.split(",")]
    return tuple([item for sublist in l for item in sublist])


@consumer
def slicer(
    selection: str = "", axis: str | None = None
) -> Generator[AxisArray, AxisArray, None]:
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out = AxisArray(np.array([]), dims=[""])
    _slice = None

    while True:
        axis_arr_in = yield axis_arr_out

        if axis is None:
            axis = axis_arr_in.dims[-1]
        axis_idx = axis_arr_in.get_axis_idx(axis)

        if _slice is None:
            _slices = parse_slice(selection)
            if len(_slices) == 1:
                _slice = _slices[0]
            else:
                indices = np.arange(axis_arr_in.data.shape[axis_idx])
                indices = np.hstack([indices[_] for _ in _slices])
                _slice = np.s_[indices]

        axis_arr_out = replace(
            axis_arr_in,
            data=slice_along_axis(axis_arr_in.data, _slice, axis_idx),
        )


SlicerSettings, Slicer = gen_to_unit(slicer)
