import numpy as np
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.sigproc.bandpower import bandpower, SpectrogramSettings

from util import create_messages_with_periodic_signal


def _debug_plot(result):
    import matplotlib.pyplot as plt

    t_vec = result.axes["time"].offset + np.arange(result.data.shape[0]) * result.axes["time"].gain
    plt.plot(t_vec, result.data[..., 0])


def test_bandpower():
    win_dur = 1.0
    fs = 1000.
    bands = [(9, 11), (70, 90), (134, 136)]

    sin_params = [
        {"f": 10.0, "a": 3.0, "dur": 4.0, "offset": 1.0},
        {"f": 10.0, "a": 1.0, "dur": 3.0, "offset": 5.0},
        {"f": 135.0, "a": 4.0, "dur": 4.0, "offset": 1.0},
        {"f": 135.0, "a": 2.0, "dur": 3.0, "offset": 5.0},
    ]
    messages = create_messages_with_periodic_signal(
        sin_params=sin_params,
        fs=fs,
        msg_dur=0.4,
        win_step_dur=None  # The spectrogram will do the windowing
    )

    gen = bandpower(
        spectrogram_settings=SpectrogramSettings(
            window_dur=win_dur,
            window_shift=0.1,
        ),
        bands=bands
    )
    results = [gen.send(msg) for msg in messages]

    result = AxisArray.concatenate(*results, dim="time")
    # _debug_plot(result)

    # Check the amplitudes at the midpoints of each of our sinusoids.
    t_vec = result.axes["time"].offset + np.arange(result.data.shape[0]) * result.axes["time"].gain
    mags = []
    for s_p in sin_params[:2]:
        ix = np.argmin(np.abs(t_vec - (s_p["offset"] + s_p["dur"]/2)))
        mags.append(result.data[ix, 0, 0])
    for s_p in sin_params[2:]:
        ix = np.argmin(np.abs(t_vec - (s_p["offset"] + s_p["dur"]/2)))
        mags.append(result.data[ix, 2, 0])
    # The sorting of the measured magnitudes should match the sorting of the parameter magnitudes.
    assert np.array_equal(np.argsort(mags), np.argsort([_["a"] for _ in sin_params]))
