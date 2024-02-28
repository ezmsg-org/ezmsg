# from typing import Optional
import numpy as np
import pytest
import scipy.signal
from ezmsg.util.messages.axisarray import AxisArray

from ezmsg.sigproc.butterworthfilter import (
    ButterworthFilterSettings as LegacyButterSettings,
)
from ezmsg.sigproc.filter import butter
from ezmsg.sigproc.butterworthfilter import ButterworthFilterSettings as LegacyButterSettings


@pytest.mark.parametrize(
    "cutoff, cuton",
    [
        (30.0, None),  # lowpass
        (None, 30.0),  # highpass
        (45.0, 30.0),  # bandpass
        (30.0, 45.0),  # bandstop
    ],
)
@pytest.mark.parametrize("order", [2, 4, 8])
def test_butterworth_legacy_filter_settings(cutoff: float, cuton: float, order: int):
    """
    Test the butterworth legacy filter settings generation of btype and Wn.
    We test them explicitly because we assume they are correct when used in our later settings.

    Parameters:
        cutoff (float): The cutoff frequency for the filter. Can be None for highpass filters.
        cuton (float): The cuton frequency for the filter. Can be None for lowpass filters.
            If cuton is larger than cutoff we assume bandstop.
        order (int): The order of the filter.
    """
    settings_obj = LegacyButterSettings(
        axis="time", fs=500, order=order, cuton=cuton, cutoff=cutoff
    )
    btype, Wn = settings_obj.filter_specs()
    if cuton is None:
        assert btype == "lowpass"
        assert Wn == cutoff
    elif cutoff is None:
        assert btype == "highpass"
        assert Wn == cuton
    elif cuton <= cutoff:
        assert btype == "bandpass"
        assert Wn == (cuton, cutoff)
    else:
        assert btype == "bandstop"
        assert Wn == (cutoff, cuton)


@pytest.mark.parametrize(
    "cutoff, cuton",
    [
        (30.0, None),  # lowpass
        (None, 30.0),  # highpass
        (45.0, 30.0),  # bandpass
        (30.0, 45.0),  # bandstop
    ],
)
@pytest.mark.parametrize("order", [0, 2, 5, 8])  # 0 = passthrough
# All fs entries must be greater than 2x the largest of cutoff | cuton
@pytest.mark.parametrize("fs", [200.0])
@pytest.mark.parametrize("n_chans", [3])
@pytest.mark.parametrize("n_dims, time_ax", [(1, 0), (3, 0), (3, 1), (3, 2)])
@pytest.mark.parametrize("coef_type", ["ba", "sos"])
def test_butterworth(
    cutoff: float,
    cuton: float,
    order: int,
    fs: float,
    n_chans: int,
    n_dims: int,
    time_ax: int,
    coef_type: str,
):
    dur = 2.0
    n_freqs = 5
    n_splits = 4

    n_times = int(dur * fs)
    if n_dims == 1:
        dat_shape = [n_times]
        dat_dims = ["time"]
        other_axes = {}
    else:
        dat_shape = [n_freqs, n_chans]
        dat_shape.insert(time_ax, n_times)
        dat_dims = ["freq", "ch"]
        dat_dims.insert(time_ax, "time")
        other_axes = {"freq": AxisArray.Axis(unit="Hz"), "ch": AxisArray.Axis()}
    in_dat = np.arange(np.prod(dat_shape), dtype=float).reshape(*dat_shape)

    # Calculate Expected Result
    btype, Wn = LegacyButterSettings(
        axis="time", fs=500, order=order, cuton=cuton, cutoff=cutoff
    ).filter_specs()
    coefs = scipy.signal.butter(order, Wn, btype=btype, output=coef_type, fs=fs)
    tmp_dat = np.moveaxis(in_dat, time_ax, -1)
    if coef_type == "ba":
        if order == 0:
            # butter does not return correct coefs under these conditions; Set manually.
            coefs = (np.array([1.0, 0.0]),) * 2
        zi = scipy.signal.lfilter_zi(*coefs)
        if n_dims == 3:
            zi = np.tile(zi[None, None, :], (n_freqs, n_chans, 1))
        out_dat, _ = scipy.signal.lfilter(*coefs, tmp_dat, zi=zi)
    elif coef_type == "sos":
        zi = scipy.signal.sosfilt_zi(coefs)
        if n_dims == 3:
            zi = np.tile(zi[:, None, None, :], (1, n_freqs, n_chans, 1))
        out_dat, _ = scipy.signal.sosfilt(coefs, tmp_dat, zi=zi)
    out_dat = np.moveaxis(out_dat, -1, time_ax)

    # Split the data into multiple messages
    n_seen = 0
    messages = []
    for split_dat in np.array_split(in_dat, n_splits, axis=time_ax):
        _time_axis = AxisArray.Axis.TimeAxis(fs=fs, offset=n_seen / fs)
        messages.append(
            AxisArray(split_dat, dims=dat_dims, axes={**other_axes, "time": _time_axis})
        )
        n_seen += split_dat.shape[time_ax]

    # Test axis_name `None` when target axis idx is 0.
    axis_name = "time" if time_ax != 0 else None
    gen = butter(
        axis=axis_name,
        order=order,
        cuton=cuton,
        cutoff=cutoff,
        coef_type=coef_type,
    )

    result = np.concatenate([gen.send(_).data for _ in messages], axis=time_ax)
    assert np.allclose(result, out_dat)