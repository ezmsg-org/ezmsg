import pytest
import numpy as np

from dataclasses import dataclass, field

import ezmsg.util.messages.axisarray as ezaa

from typing import Generator, List

DATA = np.ones((2, 5, 4, 4))


def test_simple() -> None:
    ezaa.AxisArray(DATA, dims=["ch", "time", "x", "y"])


@dataclass
class MultiChannelData(ezaa.AxisArray):
    ch_names: List[str] = field(default_factory=list)


def test_axes() -> None:
    MultiChannelData(
        DATA,
        dims=["ch", "time", "x", "y"],
        axes={
            "time": ezaa.AxisArray.TimeAxis(fs=5.0),
            "x": ezaa.AxisArray.LinearAxis(unit="mm", gain=0.2, offset=-13.0),
            "y": ezaa.AxisArray.LinearAxis(unit="mm", gain=0.2, offset=-13.0),
        },
        key="spatial_sensor_array",
        ch_names=["a", "b"],
    )


def msg_gen(
    fs: float, x_size: int = 4, y_size: int = 4
) -> Generator[ezaa.AxisArray, None, None]:
    sidx = 0
    while True:
        yield MultiChannelData(
            np.ones((2, 1, x_size, y_size)) * sidx,
            dims=["ch", "time", "x", "y"],
            axes=dict(
                time=ezaa.AxisArray.TimeAxis(fs=fs, offset=sidx / fs),
                x=ezaa.AxisArray.LinearAxis(unit="mm"),
                y=ezaa.AxisArray.LinearAxis(unit="mm"),
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

    batches: List[ezaa.AxisArray] = list()
    for _ in range(num_batches):
        win: List[ezaa.AxisArray] = list()
        for msg, _ in zip(gen, range(batch_size)):
            win.append(msg)
        batches.append(ezaa.concatenate(*win, dim="time"))

    t_cat = ezaa.concatenate(*batches, dim="time")
    assert t_cat.shape[t_cat.get_axis_idx("time")] == (batch_size * num_batches)

    x_cat = ezaa.concatenate(*batches, dim="x")
    assert x_cat.shape[x_cat.get_axis_idx("x")] == (x_size * num_batches)

    batch_cat = ezaa.concatenate(
        *batches, dim="batch", axis=ezaa.AxisArray.TimeAxis(fs / batch_size)
    )
    assert batch_cat.dims[0] == "batch"
    assert batch_cat.shape[0] == num_batches

    assert isinstance(batch_cat, MultiChannelData)

    # Test filtering based on key
    gen = msg_gen(fs, x_size, y_size)
    single_batch = [next(gen) for _ in range(batch_size)]
    # All messages pass
    single_batch_cat = ezaa.concatenate(
        *single_batch, dim="time", filter_key="spatial_sensor_array"
    )
    assert single_batch_cat.key == "spatial_sensor_array"
    assert single_batch_cat.shape[single_batch_cat.get_axis_idx("time")] == batch_size
    # Exclude one message based on filter_key
    single_batch[0].key = "wrong key"
    filter_batch_cat = ezaa.concatenate(
        *single_batch, dim="time", filter_key="spatial_sensor_array"
    )
    assert filter_batch_cat.key == "spatial_sensor_array"
    assert filter_batch_cat.shape[filter_batch_cat.get_axis_idx("time")] == (
        batch_size - 1
    )
    # No filtering, but key is reset because it is not consistent
    nofilter_batch_cat = ezaa.concatenate(*single_batch, dim="time")
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
        msg = ezaa.AxisArray(
            data.copy(),
            dims=_dims,
            axes=dict(
                time=ezaa.AxisArray.TimeAxis(fs=5.0),
            ),
        )

        with msg.view2d("time") as arr:
            should_share_memory = time_dim == 0 or time_dim == (data.ndim - 1)
            assert np.shares_memory(msg.data, arr) == should_share_memory
            assert arr.shape == ezaa.shape2d(msg.data, time_dim)
            arr[:] = arr + 1

        assert np.allclose(msg.data, data + 1)
        assert msg.data.shape == data.shape


def test_sel():
    gain = 0.25 # Make sure this is not 1.0 to test gain effect
    offset = -50.0
    n_pts = 400
    # Create data such that index i has value (i * gain) + offset
    data_1d = (np.arange(n_pts) * gain) + offset
    aa = ezaa.AxisArray(
        data_1d,
        dims=["dim0"],
        axes=dict(dim0=ezaa.AxisArray.LinearAxis(gain=gain, offset=offset)),
    )

    # Test sel with a slice that selects multiple elements
    coord_slice_val = slice(-10.75, 1.5) # Values in coordinate space
    aa_sl_sel_multi = aa.sel(dim0=coord_slice_val)
    assert np.allclose(
        aa_sl_sel_multi.data,
        data_1d[aa.axes['dim0'].index(coord_slice_val.start) : aa.axes['dim0'].index(coord_slice_val.stop)] # type: ignore
    )
    assert aa_sl_sel_multi.dims == ["dim0"] # Dimension preserved

    # Test sel with a slice that selects a single element, drop=False (default)
    single_coord_val = data_1d[10] # e.g., -47.5
    # Adjust slice to be slightly wider to avoid rint(10.5)==10 issue
    coord_slice_single = slice(single_coord_val, single_coord_val + 0.9 * gain) 
    
    aa_sl_sel_single_nodrop = aa.sel(dim0=coord_slice_single, drop=False)
    assert aa_sl_sel_single_nodrop.data.shape == (1,)
    assert np.allclose(aa_sl_sel_single_nodrop.data[0], data_1d[10])
    assert aa_sl_sel_single_nodrop.dims == ["dim0"] # Dimension preserved
    assert "dim0" in aa_sl_sel_single_nodrop.axes
    assert aa_sl_sel_single_nodrop.axes["dim0"].offset == single_coord_val

    # Test sel with a slice that selects a single element, drop=True.
    # Dimension should be preserved as singleton because the indexer passed to isel is a slice.
    aa_sl_sel_single_drop = aa.sel(dim0=coord_slice_single, drop=True)
    assert aa_sl_sel_single_drop.data.shape == (1,)
    assert np.allclose(aa_sl_sel_single_drop.data, data_1d[10])
    assert aa_sl_sel_single_drop.dims == ["dim0"] # Dimension preserved
    assert "dim0" in aa_sl_sel_single_drop.axes
    assert aa_sl_sel_single_drop.axes["dim0"].offset == single_coord_val

    # Test isel with integer indexer, drop=False (default)
    aa_idx_isel_nodrop = aa.isel(dim0=-1, drop=False)
    assert aa_idx_isel_nodrop.data.shape == (1,)
    assert aa_idx_isel_nodrop.data[0] == data_1d[-1]
    assert aa_idx_isel_nodrop.dims == ["dim0"]
    assert "dim0" in aa_idx_isel_nodrop.axes
    assert aa_idx_isel_nodrop.axes["dim0"].offset == data_1d[-1]

    # Test isel with integer indexer, drop=True
    aa_idx_isel_drop = aa.isel(dim0=-1, drop=True)
    assert aa_idx_isel_drop.data.ndim == 0
    assert aa_idx_isel_drop.data == data_1d[-1]
    assert "dim0" not in aa_idx_isel_drop.dims
    assert "dim0" not in aa_idx_isel_drop.axes

    # Test isel with slice that results in single element.
    # isel's `drop` flag only applies to integer indexers, not slices.
    # So, the dimension is preserved as a singleton.
    aa_slice_isel_singleton = aa.isel(dim0=slice(5,6), drop=True) # drop has no effect here
    assert aa_slice_isel_singleton.data.ndim == 1 
    assert aa_slice_isel_singleton.data.shape == (1,)
    assert aa_slice_isel_singleton.data[0] == data_1d[5]
    assert "dim0" in aa_slice_isel_singleton.dims
    assert aa_slice_isel_singleton.axes["dim0"].offset == data_1d[5]


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
            res = ezaa.slice_along_axis(data, sl=sl, axis=axis)
        return

    res = ezaa.slice_along_axis(data, sl=sl, axis=axis)
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
            ezaa.sliding_win_oneaxis(data, nwin, axis, step)
        return

    if nwin > dims[axis]:
        with pytest.raises(ValueError):
            ezaa.sliding_win_oneaxis(data, nwin, axis, step)
        return

    res = ezaa.sliding_win_oneaxis(data, nwin, axis, step)

    if nwin == 0:
        assert res.size == 0
        return

    expected = nps.sliding_window_view(data, nwin, axis)
    # Note: sliding window inserted at end, and trimmed axis left in place.
    dest_ax = axis if axis >= 0 else len(dims) + axis
    expected = np.moveaxis(expected, -1, dest_ax + 1)
    if step > 1:
        expected = ezaa.slice_along_axis(expected, slice(None, None, step), dest_ax)
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
            "time": ezaa.AxisArray.TimeAxis(fs=5.0),
            "x": ezaa.AxisArray.LinearAxis(unit="mm", gain=0.2, offset=-13.0),
            "y": ezaa.AxisArray.LinearAxis(unit="mm", gain=0.2, offset=-13.0),
            "quality": ezaa.AxisArray.CoordinateAxis(unit = '%', data = quality, dims = ['x', 'y'])
        },
        key="spatial_sensor_array_with_sensor_quality_metric",
        ch_names=["a", "b"],
    )
        
    da = aa.to_xr_dataarray()
    assert da.shape == aa.shape
    assert da.dims == ('ch', 'time', 'x', 'y')
    assert np.allclose(da.time.data, np.array([0.0, 0.2, 0.4, 0.6, 0.8]))

    quality_data = da.where(da.quality == 1.0).stack(pixel = ['x', 'y']).dropna('pixel')
    assert np.allclose(quality_data.x.data, np.array([-13.0, -12.8, -12.6, -12.6, -12.4])) # type: ignore
    assert np.allclose(quality_data.y.data, np.array([-12.6, -12.8, -13.0, -12.4, -12.6])) # type: ignore


@pytest.fixture
def sample_axis_array() -> ezaa.AxisArray:
    return ezaa.AxisArray(
        data=np.arange(2 * 3 * 4).reshape(2, 3, 4),
        dims=["dim0", "dim1", "dim2"],
        axes={
            "dim0": ezaa.LinearAxis(unit="A", gain=1.0),
            "dim1": ezaa.LinearAxis(unit="B", gain=2.0, offset=1.0),
        },
        attrs={"info": "sample_data", "version": 1},
        key="sample_key",
    )


def test_modify_dims_rename(sample_axis_array: ezaa.AxisArray):
    modified_aa = sample_axis_array.modify_dims({"dim0": "new_dim0", "dim2": "new_dim2"})
    assert modified_aa.dims == ["new_dim0", "dim1", "new_dim2"]
    assert "new_dim0" in modified_aa.axes
    assert "dim0" not in modified_aa.axes
    assert modified_aa.axes["new_dim0"] == sample_axis_array.axes["dim0"]
    assert "new_dim2" not in modified_aa.axes # dim2 had no axis
    assert np.array_equal(modified_aa.data, sample_axis_array.data)
    assert modified_aa.attrs == sample_axis_array.attrs
    assert modified_aa.key == sample_axis_array.key


def test_modify_dims_drop_singleton(sample_axis_array: ezaa.AxisArray):
    # Create a singleton dimension
    singleton_aa = sample_axis_array.isel(dim0=0, drop=True) # Shape (3,4), dims ["dim1", "dim2"]
    # Add it back as a new dimension
    singleton_aa = ezaa.AxisArray(
        data = np.expand_dims(singleton_aa.data, axis=0),
        dims = ["singleton_dim"] + singleton_aa.dims,
        axes = {"singleton_dim": ezaa.LinearAxis(unit="S"), **singleton_aa.axes},
        attrs= singleton_aa.attrs
    )
    assert singleton_aa.shape[0] == 1

    modified_aa = singleton_aa.modify_dims({"singleton_dim": None})
    assert modified_aa.dims == ["dim1", "dim2"]
    assert "singleton_dim" not in modified_aa.dims
    assert "singleton_dim" not in modified_aa.axes
    assert modified_aa.data.shape == (3, 4)


def test_modify_dims_drop_nonsingleton_raises(sample_axis_array: ezaa.AxisArray):
    with pytest.raises(ValueError, match="Cannot drop dimension 'dim0'"):
        sample_axis_array.modify_dims({"dim0": None})


def test_modify_dims_empty_map(sample_axis_array: ezaa.AxisArray):
    modified_aa = sample_axis_array.modify_dims({})
    assert modified_aa is sample_axis_array # Should return self if no changes


def test_update_axes(sample_axis_array: ezaa.AxisArray):
    new_dim0_axis = ezaa.LinearAxis(unit="X", gain=0.5)
    new_dim2_axis = ezaa.CoordinateAxis(data=np.array([10, 20, 30, 40]), dims=["dim2"])

    modified_aa = sample_axis_array.update_axes(dim0=new_dim0_axis, dim2=new_dim2_axis)

    assert modified_aa.axes["dim0"] == new_dim0_axis
    assert modified_aa.axes["dim1"] == sample_axis_array.axes["dim1"] # Unchanged
    assert modified_aa.axes["dim2"] == new_dim2_axis
    assert np.array_equal(modified_aa.data, sample_axis_array.data)
    assert modified_aa.dims == sample_axis_array.dims
    assert modified_aa.attrs == sample_axis_array.attrs


def test_update_axes_nonexistent_dim_raises(sample_axis_array: ezaa.AxisArray):
    with pytest.raises(ValueError, match="Dimension 'non_existent_dim' not found"):
        sample_axis_array.update_axes(non_existent_dim=ezaa.LinearAxis())


def test_update_axes_invalid_type_raises(sample_axis_array: ezaa.AxisArray):
    with pytest.raises(TypeError, match="must be an instance of AxisBase"):
        sample_axis_array.update_axes(dim0="not_an_axis_object") # type: ignore


def test_update_axes_empty_kwargs(sample_axis_array: ezaa.AxisArray):
    modified_aa = sample_axis_array.update_axes()
    assert modified_aa is sample_axis_array # Should return self


def test_update_attrs(sample_axis_array: ezaa.AxisArray):
    modified_aa = sample_axis_array.update_attrs(
        info="updated_info", new_attr="new_value", version=2
    )
    assert modified_aa.attrs["info"] == "updated_info"
    assert modified_aa.attrs["new_attr"] == "new_value"
    assert modified_aa.attrs["version"] == 2
    assert np.array_equal(modified_aa.data, sample_axis_array.data)
    assert modified_aa.dims == sample_axis_array.dims
    assert modified_aa.axes == sample_axis_array.axes


def test_update_attrs_empty_kwargs(sample_axis_array: ezaa.AxisArray):
    # Note: update_attrs currently doesn't return self on no-op, it creates a new instance with copied attrs.
    # This is fine, just a note.
    modified_aa = sample_axis_array.update_attrs()
    assert modified_aa.attrs == sample_axis_array.attrs
    assert modified_aa is not sample_axis_array
    assert modified_aa == sample_axis_array
