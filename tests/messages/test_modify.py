import copy

import numpy as np
import pytest

from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messages.modify import modify_axis


@pytest.mark.parametrize("name_map", [None, {"step": "time"}])
def test_modify_axis(name_map: dict[str, str | None] | None):
    input_ax_arr = AxisArray(
        data=np.arange(60).reshape(3, 5, 4),
        dims=["step", "freq", "ch"],
        axes={
            "step": AxisArray.TimeAxis(fs=10.0, offset=0.0),
            "freq": AxisArray.LinearAxis(gain=1.0, offset=0.0),
            "ch": AxisArray.CoordinateAxis(data=np.arange(4).astype(str), dims=["ch"]),
        },
        key="test_modify_axis",
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


@pytest.mark.parametrize("targ_dim_len", [1, 3])
def test_drop_axis(targ_dim_len: int):
    input_ax_arr = AxisArray(
        data=np.arange(targ_dim_len * 5 * 4).reshape(targ_dim_len, 5, 4),
        dims=["step", "freq", "ch"],
        axes={
            "step": AxisArray.TimeAxis(fs=10.0, offset=0.0),
            "freq": AxisArray.LinearAxis(gain=1.0, offset=0.0),
            "ch": AxisArray.CoordinateAxis(data=np.arange(4).astype(str), dims=["ch"]),
        },
        key="test_drop_axis",
    )
    gen = modify_axis({"step": None})
    if targ_dim_len != 1:
        with pytest.raises(ValueError):
            res = gen.send(input_ax_arr)
    else:
        res = gen.send(input_ax_arr)
        assert "step" not in res.dims
        assert "step" not in res.axes
        assert "freq" in res.dims
        assert "freq" in res.axes
        assert "ch" in res.dims
        assert "ch" in res.axes
        assert res.data.shape == (5, 4)


class TestChunkDimIsRemapped:
    """`chunk_dim` names a dimension, so renaming that dimension has to move it.

    Left alone it would either point at a name no longer in `dims` -- which
    AxisArray rejects at construction -- or, worse, silently name whichever
    other dimension inherited the old name.
    """

    @staticmethod
    def _msg(chunk_dim: str | None, win_len: int = 3):
        return AxisArray(
            data=np.arange(win_len * 4 * 2).reshape(win_len, 4, 2),
            dims=["win", "time", "ch"],
            axes={
                "win": AxisArray.TimeAxis(fs=10.0),
                "time": AxisArray.TimeAxis(fs=100.0),
                "ch": AxisArray.CoordinateAxis(data=np.array(["a", "b"]), dims=["ch"]),
            },
            key="test_chunk_dim",
            chunk_dim=chunk_dim,
        )

    def test_renaming_the_chunk_dim_moves_it(self):
        res = modify_axis({"win": "batch"}).send(self._msg("win"))
        assert res.dims == ["batch", "time", "ch"]
        assert res.chunk_dim == "batch"

    def test_a_swap_follows_the_dimension_not_the_name(self):
        """The case that motivated this: a windowing stage emits `win` as the
        chunk dimension and a later stage swaps the two time-like names."""
        res = modify_axis({"win": "time", "time": "sample"}).send(self._msg("win"))
        assert res.dims == ["time", "sample", "ch"]
        assert res.chunk_dim == "time"

    def test_renaming_another_dim_leaves_it_alone(self):
        res = modify_axis({"ch": "channel"}).send(self._msg("win"))
        assert res.dims == ["win", "time", "channel"]
        assert res.chunk_dim == "win"

    def test_undeclared_stays_undeclared(self):
        assert modify_axis({"win": "batch"}).send(self._msg(None)).chunk_dim is None

    def test_dropping_the_chunk_dim_clears_it(self):
        res = modify_axis({"win": None}).send(self._msg("win", win_len=1))
        assert res.dims == ["time", "ch"]
        assert res.chunk_dim is None
