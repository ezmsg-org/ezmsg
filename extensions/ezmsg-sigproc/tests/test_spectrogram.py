import typing

import numpy as np

from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.sigproc.spectrum import WindowFunction, SpectralTransform, SpectralOutput
from ezmsg.sigproc.spectrogram import spectrogram


def _debug_plot(
        ax_arr: AxisArray,
        targ_fsets: typing.Optional[typing.Tuple[typing.List[float], typing.List[float]]] = None
):
    import matplotlib.pyplot as plt

    t_vec = ax_arr.axes["time"].offset + np.arange(ax_arr.data.shape[1] * ax_arr.axes["time"].gain)
    t_vec -= ax_arr.axes["time"].gain / 2
    f_vec = ax_arr.axes["freq"].offset + np.arange(ax_arr.data.shape[1] * ax_arr.axes["freq"].gain)
    f_vec -= ax_arr.axes["freq"].gain / 2
    plt.imshow(
        ax_arr.data[..., 0].T,
        origin="lower",
        aspect="auto",
        extent=(t_vec[0], t_vec[-1], f_vec[0], f_vec[-1])
    )
    plt.xlabel("Time")
    plt.ylabel("Frequency")

    if targ_fsets is not None:
        n_splits = len(targ_fsets)
        per_split = len(t_vec) // n_splits
        for split_ix in range(n_splits):
            x_vec = split_ix * per_split + np.arange(per_split)
            for yy in targ_fsets[split_ix]:
                plt.plot(x_vec, yy * np.ones_like(x_vec), linestyle="--", color="r", linewidth=1.0)


def test_spectrogram():
    win_dur = 1.0
    win_step_dur = 0.5

    # Test data is 10 seconds in duration:
    #  First 5 seconds -- sum of sinusoids at specified frequencies
    #  Second 5 seconds -- sum of different sinusoids
    f_sets = (
        [10., 20., 70.],
        [14., 35., 300.]
    )
    seg_dur = 5.0
    fs = 1000.
    t_seg = np.arange(int(seg_dur * fs)) / fs
    data = np.array([])
    for f_set in f_sets:
        data = np.hstack((
            data,
            np.sum(np.vstack(
                [1.0 * np.sin(2 * np.pi * f * t_seg + 0) for f in f_set]
            ), axis=0)
        ))

    n_msgs = 30
    offset = 0.0
    messages = []
    for split_dat in np.array_split(data[:, None], n_msgs, axis=0):
        _time_axis = AxisArray.Axis.TimeAxis(fs=fs, offset=offset)
        messages.append(
            AxisArray(split_dat, dims=["time", "ch"], axes={"time": _time_axis})
        )
        offset += split_dat.shape[0] / fs

    gen = spectrogram(
        window_dur=win_dur,
        window_shift=win_step_dur,
        window=WindowFunction.HANNING,
        transform=SpectralTransform.REL_DB,
        output=SpectralOutput.POSITIVE,
    )

    results = [gen.send(msg) for msg in messages]
    results = [_ for _ in results if _ is not None]  # Drop None

    # Check that the windows span the expected times.
    total_dur = seg_dur * len(f_sets)
    expected_delta = total_dur - win_step_dur
    delta_win_starts = (results[-1].axes["time"].offset - results[0].axes["time"].offset)
    assert np.abs(delta_win_starts - expected_delta) < 1e-9
    all_deltas = np.diff([_.axes["time"].offset for _ in results])
    assert np.allclose(all_deltas, win_step_dur + np.zeros((len(results) - 1)))

    result = AxisArray.concatenate(*results, dim="time")
    # TODO: Check spectral peaks change over time. _debug_plot is useful if not automatic.
    # _debug_plot(result, targ_fsets=f_sets)
