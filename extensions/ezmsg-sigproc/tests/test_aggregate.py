from functools import partial

import numpy as np
import pytest
from ezmsg.util.messages.axisarray import AxisArray

from ezmsg.sigproc.aggregate import ranged_aggregate, AggregationFunction


@pytest.mark.parametrize("agg_func", [AggregationFunction.MEAN, AggregationFunction.MEDIAN, AggregationFunction.STD])
def test_aggregate(agg_func: AggregationFunction):
    n_chans = 20
    n_freqs = 100
    data_dur = 30.0
    fs = 1024.0
    bands = [(5.0, 20.0), (30.0, 50.0)]
    targ_ax = "freq"

    n_samples = int(data_dur * fs)
    data = np.arange(n_samples * n_chans * n_freqs).reshape(n_samples, n_chans, n_freqs)
    n_msgs = int(data_dur / 2)

    offset = 0
    messages = []
    for arr in np.array_split(data, n_samples // n_msgs):
        messages.append(
            AxisArray(
                arr,
                dims=["time", "ch", "freq"],
                axes={
                    "time": AxisArray.Axis.TimeAxis(fs=fs, offset=offset),
                    "freq": AxisArray.Axis(gain=1.0, offset=0.0, unit="Hz")
                }
            )
        )
        offset += arr.shape[0] / fs

    gen = ranged_aggregate(axis=targ_ax, bands=bands, operation=agg_func)
    results = [gen.send(_) for _ in messages]

    assert all([type(_) is AxisArray for _ in results])

    # Check output axis
    for res in results:
        ax = res.axes[targ_ax]
        assert ax.offset == np.mean(bands[0])
        if len(bands) > 1:
            assert ax.gain == np.mean(bands[1]) - np.mean(bands[0])
        assert ax.unit == messages[0].axes[targ_ax].unit

    # Check data
    targ_ax = messages[0].axes[targ_ax]
    targ_ax_vec = targ_ax.offset + np.arange(data.shape[-1]) * targ_ax.gain
    agg_func = {
        AggregationFunction.MEAN: partial(np.mean, axis=-1, keepdims=True),
        AggregationFunction.MEDIAN: partial(np.median, axis=-1, keepdims=True),
        AggregationFunction.STD: partial(np.std, axis=-1, keepdims=True)
    }[agg_func]
    expected_data = np.concatenate([
        agg_func(data[..., np.logical_and(targ_ax_vec >= start, targ_ax_vec <= stop)])
        for (start, stop) in bands
    ], axis=-1)
    received_data = AxisArray.concatenate(*results, dim="time").data
    assert np.allclose(received_data, expected_data)
