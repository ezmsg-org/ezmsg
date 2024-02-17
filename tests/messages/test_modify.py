import numpy as np
import pytest
import typing

from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messages.modify import modify_axis, ModifyAxis, ModifyAxisSettings


@pytest.mark.parametrize("name_map", [None, {"step": "time"}])
def test_modify_axis(name_map: typing.Optional[dict[str, str]]):
    input_ax_arr = AxisArray(
        data=np.arange(60).reshape(3, 5, 4),
        dims=["step", "freq", "ch"],
        axes={"step": AxisArray.Axis.TimeAxis(fs=10.0, offset=0.0)}
    )

    gen = modify_axis(name_map)
    res = gen.send(input_ax_arr)
    if name_map is None:
        assert res is input_ax_arr
    else:
        for k, v in name_map.items():
            assert k not in res.axes
            assert v in res.axes
            assert input_ax_arr.axes[k] is res.axes[v]
