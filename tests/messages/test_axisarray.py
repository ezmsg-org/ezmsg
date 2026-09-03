import importlib.util
import pytest
import numpy as np

from dataclasses import dataclass, field

from ezmsg.util.messages.axisarray import (
    AxisArray,
    CoordinateAxis,
    LinearAxis,
    replace,
    shape2d,
    slice_along_axis,
    sliding_win_oneaxis,
)

from collections.abc import Generator

DATA = np.ones((2, 5, 4, 4))


def test_simple() -> None:
    AxisArray(DATA, dims=["ch", "time", "x", "y"])


@dataclass
class MultiChannelData(AxisArray):
    ch_names: list[str] = field(default_factory=list)


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

    batches: list[AxisArray] = list()
    for _ in range(num_batches):
        win: list[AxisArray] = list()
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


def test_concat_with_coordinate_axis():
    # Create two AxisArray objects with a CoordinateAxis
    n_a = 2
    n_b1 = 3
    aa1 = AxisArray(
        np.arange(n_a * n_b1).reshape(n_a, n_b1),
        dims=["a", "b"],
        axes={"b": AxisArray.CoordinateAxis(data=np.arange(1, 1 + n_b1), dims=["b"])},
    )

    n_b2 = 4
    aa2 = AxisArray(
        np.arange(n_a * n_b1, n_a * (n_b1 + n_b2)).reshape(n_a, n_b2),
        dims=["a", "b"],
        axes={
            "b": AxisArray.CoordinateAxis(
                data=np.arange(1 + n_b1, 1 + n_b1 + n_b2), dims=["b"]
            )
        },
    )

    # Concatenate along the CoordinateAxis
    concatenated = AxisArray.concatenate(aa1, aa2, dim="b")

    # Check the shape of the concatenated array
    assert concatenated.shape == (n_a, n_b1 + n_b2)

    # Check the data of the concatenated CoordinateAxis
    expected_axis_data = np.arange(1, 1 + n_b1 + n_b2)
    assert np.array_equal(concatenated.axes["b"].data, expected_axis_data)

    # Check that the other axes are preserved
    assert "a" in concatenated.dims

    # Check that the concatenated data is correct
    expected_data = np.hstack((aa1.data, aa2.data))
    assert np.array_equal(concatenated.data, expected_data)


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


import platform

_has_mlx = importlib.util.find_spec("mlx") is not None
requires_mlx = pytest.mark.skipif(
    not _has_mlx or platform.machine() != "arm64" or platform.system() != "Darwin",
    reason="Requires MLX on Apple Silicon",
)


@requires_mlx
@pytest.mark.parametrize("nwin", [0, 3, 8])
@pytest.mark.parametrize("axis", [0, 1, 2, -1, 3, -4])
@pytest.mark.parametrize("step", [1, 2])
def test_sliding_win_oneaxis_mlx(nwin: int, axis: int, step: int):
    """Test the strided path using MLX arrays."""
    import mlx.core as mx

    dims = [4, 5, 6]
    np_data = np.arange(np.prod(dims)).reshape(dims)
    mx_data = mx.array(np_data)

    if axis < -len(dims) or axis >= len(dims):
        with pytest.raises(IndexError):
            sliding_win_oneaxis(mx_data, nwin, axis, step)
        return

    if nwin > dims[axis]:
        with pytest.raises(ValueError):
            sliding_win_oneaxis(mx_data, nwin, axis, step)
        return

    res = sliding_win_oneaxis(mx_data, nwin, axis, step)

    if nwin == 0:
        assert np.asarray(res).size == 0
        return

    # Compare against the numpy strided result.
    expected = sliding_win_oneaxis(np_data, nwin, axis, step)
    np.testing.assert_array_equal(np.asarray(res), expected)


@pytest.mark.parametrize("nwin", [0, 3, 8])
@pytest.mark.parametrize("axis", [0, 1, 2, -1])
@pytest.mark.parametrize("step", [1, 2])
def test_sliding_win_oneaxis_generic(nwin: int, axis: int, step: int):
    """Test the generic (take+reshape) fallback path using numpy arrays."""
    from ezmsg.util.messages.axisarray import _sliding_win_generic

    dims = [4, 5, 6]
    data = np.arange(np.prod(dims)).reshape(dims)

    # Normalize axis the same way sliding_win_oneaxis does before calling _generic.
    norm_axis = axis if axis >= 0 else len(dims) + axis

    if nwin > dims[norm_axis]:
        with pytest.raises(ValueError):
            _sliding_win_generic(data, nwin, norm_axis, step, xp=np)
        return

    res = _sliding_win_generic(data, nwin, norm_axis, step, xp=np)

    if nwin == 0:
        assert res.size == 0
        return

    # Compare against the strided numpy result.
    expected = sliding_win_oneaxis(data, nwin, axis, step)
    np.testing.assert_array_equal(res, expected)


@pytest.mark.benchmark(group="sliding_win")
@pytest.mark.parametrize(
    "shape,axis,nwin,step",
    [
        ((100, 64), 0, 50, 1),  # (time, channels) — typical EEG window
        ((1000, 32), 0, 256, 64),  # large time axis with step
        ((8, 1000, 16), 1, 100, 10),  # middle axis
    ],
    ids=["100x64_win50", "1000x32_win256_step64", "8x1000x16_win100_step10"],
)
class TestSlidingWinBenchmark:
    def test_strided(self, benchmark, shape, axis, nwin, step):
        data = np.random.randn(*shape)
        benchmark(sliding_win_oneaxis, data, nwin, axis, step)

    def test_generic(self, benchmark, shape, axis, nwin, step):
        from ezmsg.util.messages.axisarray import _sliding_win_generic

        data = np.random.randn(*shape)
        norm_axis = axis if axis >= 0 else len(shape) + axis
        benchmark(_sliding_win_generic, data, nwin, norm_axis, step, xp=np)

    @requires_mlx
    def test_mlx_strided(self, benchmark, shape, axis, nwin, step):
        import mlx.core as mx

        data = mx.array(np.random.randn(*shape))
        benchmark(sliding_win_oneaxis, data, nwin, axis, step)


def xarray_available() -> bool:
    return importlib.util.find_spec("xarray") is not None


@pytest.mark.skipif(
    not xarray_available(), reason="Optional dependency 'xarray' not installed"
)
def test_to_xr_dataarray():
    quality = (
        (np.arange(np.prod(DATA.shape[-2:])) % 3).reshape(DATA.shape[-2:]) + 1
    ) / 3
    aa = MultiChannelData(
        DATA,
        dims=["ch", "time", "x", "y"],
        axes={
            "time": AxisArray.TimeAxis(fs=5.0),
            "x": AxisArray.LinearAxis(unit="mm", gain=0.2, offset=-13.0),
            "y": AxisArray.LinearAxis(unit="mm", gain=0.2, offset=-13.0),
            "quality": AxisArray.CoordinateAxis(
                unit="%", data=quality, dims=["x", "y"]
            ),
        },
        key="spatial_sensor_array_with_sensor_quality_metric",
        ch_names=["a", "b"],
    )

    da = aa.to_xr_dataarray()
    assert da.shape == aa.shape
    assert da.dims == ("ch", "time", "x", "y")
    assert np.allclose(da.time.data, np.array([0.0, 0.2, 0.4, 0.6, 0.8]))

    quality_data = da.where(da.quality == 1.0).stack(pixel=["x", "y"]).dropna("pixel")
    assert np.allclose(
        quality_data.x.data, np.array([-13.0, -12.8, -12.6, -12.6, -12.4])
    )
    assert np.allclose(
        quality_data.y.data, np.array([-12.6, -12.8, -13.0, -12.4, -12.6])
    )


class TestCoordinateAxisFingerprint:
    """``CoordinateAxis.fingerprint`` is derived from the contents, not assigned.

    It exists so a consumer that caches something resolved from coordinate
    *values* can notice those values changing under a fixed shape, without
    paying an O(bytes) comparison per consumer per message.
    """

    @staticmethod
    def _axis(labels, **kwargs):
        return CoordinateAxis(data=np.array(labels), dims=["ch"], **kwargs)

    def test_equal_contents_agree(self):
        """Two separately built but equal axes must agree, or every consumer
        would reset on a source that rebuilds its axis per message."""
        assert (
            self._axis(["A", "B", "C"]).fingerprint
            == self._axis(["A", "B", "C"]).fingerprint
        )

    def test_different_contents_differ(self):
        assert (
            self._axis(["A", "B", "C"]).fingerprint
            != self._axis(["X", "Y", "Z"]).fingerprint
        )

    def test_reorder_is_detected(self):
        assert (
            self._axis(["A", "B", "C"]).fingerprint
            != self._axis(["B", "A", "C"]).fingerprint
        )

    def test_unit_and_dims_are_included(self):
        assert (
            self._axis(["A", "B"]).fingerprint
            != self._axis(["A", "B"], unit="label").fingerprint
        )
        square = np.array([["A", "B"], ["C", "D"]])
        one = CoordinateAxis(data=square, dims=["ch", "x"])
        two = CoordinateAxis(data=square, dims=["ch", "y"])
        assert one.fingerprint != two.fingerprint

    def test_dtype_change_alone_is_detected(self):
        wide = CoordinateAxis(data=np.array([1, 2], dtype=np.int64), dims=["ch"])
        narrow = CoordinateAxis(data=np.array([1, 2], dtype=np.int32), dims=["ch"])
        assert wide.fingerprint != narrow.fingerprint

    def test_structured_dtype(self):
        dt = np.dtype([("label", "U8"), ("bank", "U2")])
        first = np.array([("e1", "A"), ("e2", "B")], dtype=dt)
        same = np.array([("e1", "A"), ("e2", "B")], dtype=dt)
        other = np.array([("e1", "A"), ("e2", "C")], dtype=dt)
        assert (
            CoordinateAxis(data=first, dims=["ch"]).fingerprint
            == CoordinateAxis(data=same, dims=["ch"]).fingerprint
        )
        assert (
            CoordinateAxis(data=first, dims=["ch"]).fingerprint
            != CoordinateAxis(data=other, dims=["ch"]).fingerprint
        )

    def test_object_dtype_is_content_based(self):
        """An object array's buffer holds pointers, so digesting it directly
        would make two equal axes disagree and reset consumers every message."""
        one = CoordinateAxis(data=np.array(["c1", "c2"], dtype=object), dims=["ch"])
        two = CoordinateAxis(
            data=np.array(["".join(("c", str(i))) for i in (1, 2)], dtype=object),
            dims=["ch"],
        )
        assert one.fingerprint == two.fingerprint
        three = CoordinateAxis(data=np.array(["c1", "c9"], dtype=object), dims=["ch"])
        assert one.fingerprint != three.fingerprint

    def test_non_contiguous_data(self):
        """A strided view has no C-contiguous buffer; the digest must gather."""
        strided = np.array(["a", "X", "b", "X", "c", "X"])[::2]
        assert not strided.flags["C_CONTIGUOUS"]
        assert (
            CoordinateAxis(data=strided, dims=["ch"]).fingerprint
            == self._axis(["a", "b", "c"]).fingerprint
        )

    def test_undigestable_contents_report_none(self):
        """None means 'unknown', so a caller falls back to comparing rather
        than treating two unknowns as equal."""

        class NoStringForm:
            def __str__(self):
                raise ValueError("cannot stringify")

            __repr__ = __str__

        axis = self._axis(["a", "b"])
        axis.data = np.array([NoStringForm(), NoStringForm()], dtype=object)
        axis.__dict__.pop("_fingerprint", None)
        assert axis.fingerprint is None

    def test_cached_on_the_instance(self):
        axis = self._axis([f"e{i:03d}" for i in range(64)])
        assert axis.fingerprint is axis.fingerprint
        assert "_fingerprint" in axis.__dict__

    def test_is_hashable(self):
        """Consumers fold it into hash((key, shape, fingerprint))."""
        hash(("key", (30, 3), self._axis(["A", "B", "C"]).fingerprint))

    def test_survives_pickling(self):
        """The cached value crosses a process boundary with the data, so the
        far side never recomputes it."""
        import pickle

        axis = self._axis([f"e{i:03d}" for i in range(64)])
        expected = axis.fingerprint
        msg = AxisArray(
            np.zeros((4, 64)),
            dims=["time", "ch"],
            axes={"time": AxisArray.TimeAxis(fs=10.0), "ch": axis},
            key="k",
        )
        restored = pickle.loads(pickle.dumps(msg)).axes["ch"]
        assert restored is not axis
        assert restored.__dict__.get("_fingerprint") is not None  # arrived precomputed
        assert restored.fingerprint == expected

    def test_replace_yields_a_fresh_fingerprint(self):
        """``replace`` builds a new axis, so the cache cannot leak across."""
        axis = self._axis(["A", "B"])
        _ = axis.fingerprint
        updated = replace(axis, data=np.array(["X", "Y"]))
        assert updated.fingerprint != axis.fingerprint


class TestCoordinateAxisEquality:
    """``CoordinateAxis`` compares its coordinate values, not just its unit.

    It inherits an ``__eq__`` from two dataclasses; the MRO puts ``AxisBase``
    (which compares only ``unit``) ahead of ``ArrayWithNamedDims`` (which
    compares contents), so without an explicit ``__eq__`` any two axes sharing a
    unit compared equal.
    """

    @staticmethod
    def _axis(labels, **kwargs):
        return CoordinateAxis(data=np.array(labels), dims=["ch"], **kwargs)

    def test_equal_values(self):
        assert self._axis(["A", "B"]) == self._axis(["A", "B"])

    def test_different_values(self):
        assert self._axis(["A", "B"]) != self._axis(["X", "Y"])

    def test_reordered_values(self):
        assert self._axis(["A", "B"]) != self._axis(["B", "A"])

    def test_different_length(self):
        assert self._axis(["A", "B"]) != self._axis(["A", "B", "C"])

    def test_different_unit(self):
        assert self._axis(["A", "B"]) != self._axis(["A", "B"], unit="label")

    def test_different_dims(self):
        assert self._axis(["A", "B"]) != CoordinateAxis(
            data=np.array(["A", "B"]), dims=["x"]
        )

    def test_identity(self):
        axis = self._axis(["A", "B"])
        assert axis == axis  # noqa: PLR0124 -- exercises the `self is other` fast path

    def test_other_axis_type(self):
        assert self._axis(["A", "B"]) != LinearAxis(gain=1.0)

    def test_linear_axis_equality_is_unaffected(self):
        assert LinearAxis(gain=2.0, offset=1.0) == LinearAxis(gain=2.0, offset=1.0)
        assert LinearAxis(gain=2.0) != LinearAxis(gain=3.0)

    def test_axisarray_sees_a_relabelled_channel_axis(self):
        """The consequence that motivated the fix: ``AxisArray.__eq__`` tests
        ``self.axes == other.axes``, so a shadowed axis comparison made two
        messages differing only in channel labels compare equal."""

        def msg(labels):
            return AxisArray(
                np.zeros((4, 2)),
                dims=["time", "ch"],
                axes={"time": AxisArray.TimeAxis(fs=100.0), "ch": self._axis(labels)},
                key="k",
            )

        assert msg(["A", "B"]) == msg(["A", "B"])
        assert msg(["A", "B"]) != msg(["X", "Y"])


class TestChunkDim:
    """`chunk_dim` names the dimension successive messages append along.

    Its extent is whatever arrived this time, so consumers that cache state
    keyed on the message layout have to treat it differently from the
    dimensions that describe the stream's configuration.
    """

    @staticmethod
    def _msg(**kwargs):
        return AxisArray(
            np.zeros((4, 2)),
            dims=["time", "ch"],
            axes={"time": AxisArray.TimeAxis(fs=100.0)},
            **kwargs,
        )

    def test_defaults_to_none(self):
        """Undeclared, so existing producers are unaffected."""
        assert self._msg().chunk_dim is None

    def test_round_trips(self):
        assert self._msg(chunk_dim="time").chunk_dim == "time"

    def test_must_name_an_actual_dim(self):
        with pytest.raises(ValueError, match="chunk_dim 'nope' is not one of dims"):
            self._msg(chunk_dim="nope")

    def test_replace_carries_it(self):
        """A transformer that only changes data keeps the same layout."""
        original = self._msg(chunk_dim="time")
        assert replace(original, data=np.ones((4, 2))).chunk_dim == "time"

    def test_a_rename_must_update_it(self):
        """Renaming the chunk dimension without updating chunk_dim is caught."""
        original = self._msg(chunk_dim="time")
        with pytest.raises(ValueError, match="must update chunk_dim"):
            replace(original, dims=["win", "ch"])
        assert replace(original, dims=["win", "ch"], chunk_dim="win").chunk_dim == "win"

    def test_survives_pickling(self):
        import pickle

        assert (
            pickle.loads(pickle.dumps(self._msg(chunk_dim="time"))).chunk_dim == "time"
        )

    def test_positional_construction_is_unaffected(self):
        """chunk_dim is last, so existing positional calls still work."""
        msg = AxisArray(np.zeros((4, 2)), ["time", "ch"], {}, {}, "key")
        assert msg.key == "key" and msg.chunk_dim is None
