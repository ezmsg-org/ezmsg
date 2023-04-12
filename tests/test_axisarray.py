import pytest
import numpy as np

from dataclasses import dataclass, field

from ezmsg.util.messages.axisarray import AxisArray, shape2d

from typing import Generator, List

DATA = np.ones((2, 5, 4, 4))


def test_simple() -> None:
    AxisArray(DATA, dims=["ch", "time", "x", "y"])


@dataclass
class MultiChannelData(AxisArray):
    ch_names: List[str] = field(default_factory=list)


def test_axes() -> None:
    MultiChannelData(
        DATA,
        dims=["ch", "time", "x", "y"],
        axes={
            "time": AxisArray.Axis.TimeAxis(fs=5.0),
            "x": AxisArray.Axis(unit="mm", gain=0.2, offset=-13.0),
            "y": AxisArray.Axis(unit="mm", gain=0.2, offset=-13.0),
        },
        ch_names=["a", "b"],
    )


def msg_gen(
    fs: float, x_size: int = 4, y_size: int = 4
) -> Generator[AxisArray, None, None]:
    sidx = 0
    while True:
        yield MultiChannelData(
            np.ones((2, 1, x_size, y_size)) * sidx,
            dims=["ch", "time", "x", "y"],
            axes=dict(
                time=AxisArray.Axis.TimeAxis(fs=fs, offset=sidx / fs),
                x=AxisArray.Axis(unit="mm"),
                y=AxisArray.Axis(unit="mm"),
            ),
            ch_names=["a", "b"],
        )

        sidx += 1


def test_concat() -> None:
    x_size, y_size = 4, 4
    fs = 10.0
    gen = msg_gen(fs, x_size, y_size)
    batch_size = 10
    num_batches = 5

    batches: List[AxisArray] = list()
    for _ in range(num_batches):
        win: List[AxisArray] = list()
        for msg, _ in zip(gen, range(batch_size)):
            win.append(msg)
        batches.append(AxisArray.concatenate(*win, dim="time"))

    t_cat = AxisArray.concatenate(*batches, dim="time")
    assert t_cat.shape[t_cat.get_axis_idx("time")] == (batch_size * num_batches)

    x_cat = AxisArray.concatenate(*batches, dim="x")
    assert x_cat.shape[x_cat.get_axis_idx("x")] == (x_size * num_batches)

    batch_cat = AxisArray.concatenate(
        *batches, dim="batch", axis=AxisArray.Axis.TimeAxis(fs / batch_size)
    )
    assert batch_cat.dims[0] == "batch"
    assert batch_cat.shape[0] == num_batches

    assert isinstance(batch_cat, MultiChannelData)


@pytest.mark.parametrize(
    "data",
    [
        np.array(5.0),
        np.random.randn(16),
        np.random.randn(16, 32),
        np.random.randn(16, 32, 42),
        np.random.randn(16, 32, 42, 73),
    ],
)
def test_view2d(data: np.ndarray):
    dims = [f"dim_{i}" for i in range(data.ndim)]
    for time_dim in range(len(data.shape)):
        _dims = dims[:]
        _dims[time_dim] = "time"
        msg = AxisArray(
            data.copy(),
            dims=_dims,
            axes=dict(
                time=AxisArray.Axis.TimeAxis(fs=5.0),
            ),
        )

        with msg.view2d("time") as arr:
            should_share_memory = time_dim == 0 or time_dim == (data.ndim - 1)
            assert np.shares_memory(msg.data, arr) == should_share_memory
            assert arr.shape == shape2d(msg.data, time_dim)
            arr[:] = arr + 1

        assert np.allclose(msg.data, data + 1)
        assert msg.data.shape == data.shape


def test_sel():
    gain = 0.25
    offset = -50
    data = (np.arange(400) * gain) + offset
    aa = AxisArray(
        data, dims=["dim0"], axes=dict(dim0=AxisArray.Axis(gain=gain, offset=offset))
    )

    aa_sl = aa.sel(dim0=slice(None, -10.75, 1.5))  # slice based on axis info
    aa_idx = aa.isel(dim0=-1) # index slice of last index
    assert aa_idx.data == data[-1]
    
