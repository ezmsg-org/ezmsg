import pytest
import numpy as np

from dataclasses import dataclass, field

from ezmsg.util.messages.axisarray import (
    AxisArray,
    shape2d,
    slice_along_axis,
    sliding_win_oneaxis,
)

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
            "time": AxisArray.TimeAxis(fs=5.0),
            "x": AxisArray.LinearAxis(unit="mm", gain=0.2, offset=-13.0),
            "y": AxisArray.LinearAxis(unit="mm", gain=0.2, offset=-13.0),
        },
        key="spatial_sensor_array",
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
                time=AxisArray.TimeAxis(fs=fs, offset=sidx / fs),
                x=AxisArray.LinearAxis(unit="mm"),
                y=AxisArray.LinearAxis(unit="mm"),
            ),
            key="spatial_sensor_array",
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
        *batches, dim="batch", axis=AxisArray.TimeAxis(fs / batch_size)
    )
    assert batch_cat.dims[0] == "batch"
    assert batch_cat.shape[0] == num_batches

    assert isinstance(batch_cat, MultiChannelData)

    # Test filtering based on key
    gen = msg_gen(fs, x_size, y_size)
    single_batch = [next(gen) for _ in range(batch_size)]
    # All messages pass
    single_batch_cat = AxisArray.concatenate(
        *single_batch, dim="time", filter_key="spatial_sensor_array"
    )
    assert single_batch_cat.key == "spatial_sensor_array"
    assert single_batch_cat.shape[single_batch_cat.get_axis_idx("time")] == batch_size
    # Exclude one message based on filter_key
    single_batch[0].key = "wrong key"
    filter_batch_cat = AxisArray.concatenate(
        *single_batch, dim="time", filter_key="spatial_sensor_array"
    )
    assert filter_batch_cat.key == "spatial_sensor_array"
    assert filter_batch_cat.shape[filter_batch_cat.get_axis_idx("time")] == (
        batch_size - 1
    )
    # No filtering, but key is reset because it is not consistent
    nofilter_batch_cat = AxisArray.concatenate(*single_batch, dim="time")
    assert nofilter_batch_cat.key == ""
    assert (
        nofilter_batch_cat.shape[nofilter_batch_cat.get_axis_idx("time")] == batch_size
    )


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
                time=AxisArray.TimeAxis(fs=5.0),
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
        data,
        dims=["dim0"],
        axes=dict(dim0=AxisArray.LinearAxis(gain=gain, offset=offset)),
    )

    aa_sl = aa.sel(dim0=slice(-10.75, 1.5))  # slice based on axis info
    assert np.allclose(
        aa_sl.data,
        data[np.argmin(np.abs(data - -10.75)) : np.argmin(np.abs(data - 1.5))],
    )
    aa_idx = aa.isel(dim0=-1)  # index slice of last index
    assert aa_idx.data == data[-1]


@pytest.mark.parametrize("axis", [0, 1, 2, -1, 3, -4])
@pytest.mark.parametrize(
    "sl",
    [
        3,
        slice(None, None, 2),
        slice(2, 4, None),
        slice(-3, -1, None),
        slice(3, 10, None),
    ],
)
def test_slice_along_axis(axis: int, sl):
    dims = [4, 5, 6]
    data = np.arange(np.prod(dims)).reshape(dims)

    if axis >= len(dims) or axis < -len(dims):
        with pytest.raises(ValueError):
            res = slice_along_axis(data, sl=sl, axis=axis)
        return

    res = slice_along_axis(data, sl=sl, axis=axis)
    if isinstance(sl, int):
        assert res.ndim == len(dims) - 1
    else:
        assert res.ndim == len(dims)

    if axis in [0, -len(dims)]:
        expected = data[sl]
    elif axis in [1, 1 - len(dims)]:
        expected = data[:, sl]
    elif axis in [2, 2 - len(dims)]:
        expected = data[:, :, sl]
    assert np.array_equal(res, expected)
    assert np.shares_memory(res, expected)


@pytest.mark.parametrize("nwin", [0, 3, 8])
@pytest.mark.parametrize("axis", [0, 1, 2, -1, 3, -4])
@pytest.mark.parametrize("step", [1, 2])
def test_sliding_win_oneaxis(nwin: int, axis: int, step: int):
    import numpy.lib.stride_tricks as nps

    dims = [4, 5, 6]
    data = np.arange(np.prod(dims)).reshape(dims)

    if axis < -len(dims) or axis >= len(dims):
        with pytest.raises(IndexError):
            sliding_win_oneaxis(data, nwin, axis, step)
        return

    if nwin > dims[axis]:
        with pytest.raises(ValueError):
            sliding_win_oneaxis(data, nwin, axis, step)
        return

    res = sliding_win_oneaxis(data, nwin, axis, step)

    if nwin == 0:
        assert res.size == 0
        return

    expected = nps.sliding_window_view(data, nwin, axis)
    # Note: sliding window inserted at end, and trimmed axis left in place.
    dest_ax = axis if axis >= 0 else len(dims) + axis
    expected = np.moveaxis(expected, -1, dest_ax + 1)
    if step > 1:
        expected = slice_along_axis(expected, slice(None, None, step), dest_ax)
    assert np.array_equal(res, expected)
    assert np.shares_memory(res, expected)

def xarray_available():
    try:
        import xarray
        return True 
    except ImportError:
        return False

@pytest.mark.skipif(not xarray_available(), reason = "Optional dependency 'xarray' not installed")
def test_to_xr_dataarray():
    
    quality = ((np.arange(np.prod(DATA.shape[-2:])) % 3).reshape(DATA.shape[-2:]) + 1) / 3
    aa = MultiChannelData(
        DATA,
        dims=["ch", "time", "x", "y"],
        axes={
            "time": AxisArray.TimeAxis(fs=5.0),
            "x": AxisArray.LinearAxis(unit="mm", gain=0.2, offset=-13.0),
            "y": AxisArray.LinearAxis(unit="mm", gain=0.2, offset=-13.0),
            "quality": AxisArray.CoordinateAxis(unit = '%', data = quality, dims = ['x', 'y'])
        },
        key="spatial_sensor_array_with_sensor_quality_metric",
        ch_names=["a", "b"],
    )
        
    da = aa.to_xr_dataarray()
    assert da.shape == aa.shape
    assert da.dims == ('ch', 'time', 'x', 'y')
    assert np.allclose(da.time.data, np.array([0.0, 0.2, 0.4, 0.6, 0.8]))

    quality_data = da.where(da.quality == 1.0).stack(pixel = ['x', 'y']).dropna('pixel')
    assert np.allclose(quality_data.x.data, np.array([-13.0, -12.8, -12.6, -12.6, -12.4]))
    assert np.allclose(quality_data.y.data, np.array([-12.6, -12.8, -13.0, -12.4, -12.6]))
