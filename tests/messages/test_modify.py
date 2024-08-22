import copy
import typing

import numpy as np
import pytest

from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messages.modify import modify_axis


@pytest.mark.parametrize("name_map", [None, {"step": "time"}])
def test_modify_axis(name_map: typing.Optional[typing.Dict[str, str]]):
    input_ax_arr = AxisArray(
        data=np.arange(60).reshape(3, 5, 4),
        dims=["step", "freq", "ch"],
        axes={"step": AxisArray.Axis.TimeAxis(fs=10.0, offset=0.0)},
    )
    backup = copy.deepcopy(input_ax_arr)

    gen = modify_axis(name_map)
    res = gen.send(input_ax_arr)

    # Make sure the input hasn't changed
    assert np.array_equal(input_ax_arr.data, backup.data)
    assert input_ax_arr.dims == backup.dims
    assert list(input_ax_arr.axes.keys()) == list(backup.axes.keys())
    for k, v in input_ax_arr.axes.items():
        assert v == backup.axes[k]

    assert res.data is input_ax_arr.data
    if name_map is None:
        assert res is input_ax_arr
    else:
        for k, v in name_map.items():
            assert k not in res.axes
            assert v in res.axes
            assert input_ax_arr.axes[k] is res.axes[v]
