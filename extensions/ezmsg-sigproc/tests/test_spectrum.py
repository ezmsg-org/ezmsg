import pytest
import numpy as np

from ezmsg.util.messages.axisarray import AxisArray, slice_along_axis
from ezmsg.sigproc.spectrum import spectrum, SpectralTransform, SpectralOutput, WindowFunction
from util import get_test_fn, create_messages_with_periodic_signal


def _debug_plot_welch(raw: AxisArray, result: AxisArray, welch_db: bool = True):
    import scipy.signal
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(2, 1)

    t_ax = raw.axes["time"]
    t_vec = np.arange(raw.data.shape[raw.get_axis_idx("time")]) * t_ax.gain + t_ax.offset
    ch0_raw = raw.data[..., :, 0]
    if ch0_raw.ndim > 1:
        # For multi-win inputs
        ch0_raw = ch0_raw[0]
    ax[0].plot(t_vec, ch0_raw)
    ax[0].set_xlabel("Time (s)")

    f_ax = result.axes["freq"]
    f_vec = np.arange(result.data.shape[result.get_axis_idx("freq")]) * f_ax.gain + f_ax.offset
    ch0_spec = result.data[..., :, 0]
    if ch0_spec.ndim > 1:
        ch0_spec = ch0_spec[0]
    ax[1].plot(f_vec, ch0_spec, label="calculated", linewidth=2.0)
    ax[1].set_xlabel("Frequency (Hz)")

    f, Pxx = scipy.signal.welch(ch0_raw, fs=1 / raw.axes["time"].gain, window="hamming", nperseg=len(ch0_raw))
    if welch_db:
        Pxx = 10 * np.log10(Pxx)
    ax[1].plot(f, Pxx, label="welch", color="tab:orange", linestyle="--")
    ax[1].set_ylabel("dB" if welch_db else "V**2/Hz")
    ax[1].legend()

    plt.tight_layout()
    plt.show()


@pytest.mark.parametrize("window", [WindowFunction.HANNING, WindowFunction.HAMMING])
@pytest.mark.parametrize("transform", [SpectralTransform.REL_DB, SpectralTransform.REL_POWER])
@pytest.mark.parametrize("output", [SpectralOutput.POSITIVE, SpectralOutput.NEGATIVE, SpectralOutput.FULL])
def test_spectrum_gen_multiwin(
    window: WindowFunction,
    transform: SpectralTransform,
    output: SpectralOutput
):
    win_dur = 1.0
    win_step_dur = 0.5
    fs = 1000.0
    sin_params = [
        {"a": 1.0, "f": 10.0, "p": 0.0, "dur": 20.0},
        {"a": 0.5, "f": 20.0, "p": np.pi / 7, "dur": 20.0},
        {"a": 0.2, "f": 200.0, "p": np.pi / 11, "dur": 20.0},
    ]
    win_len = int(win_dur * fs)

    messages = create_messages_with_periodic_signal(
        sin_params=sin_params,
        fs=fs,
        msg_dur=win_dur,
        win_step_dur=win_step_dur
    )
    input_multiwin = AxisArray.concatenate(*messages, dim="win")
    input_multiwin.axes["win"] = AxisArray.Axis.TimeAxis(offset=0, fs=1/win_step_dur)

    gen = spectrum(axis="time", window=window, transform=transform, output=output)
    result = gen.send(input_multiwin)
    # _debug_plot_welch(input_multiwin, result, welch_db=True)
    assert isinstance(result, AxisArray)
    assert "time" not in result.dims
    assert "time" not in result.axes
    assert "ch" in result.dims
    assert "win" in result.dims
    assert "freq" in result.axes
    assert result.axes["freq"].gain == 1 / win_dur
    assert "freq" in result.dims
    fax_ix = result.get_axis_idx("freq")
    f_len = win_len if output == SpectralOutput.FULL else win_len // 2
    assert result.data.shape[fax_ix] == f_len
    f_vec = result.axes["freq"].gain * np.arange(f_len) + result.axes["freq"].offset
    if output == SpectralOutput.NEGATIVE:
        f_vec = np.abs(f_vec)
    for s_p in sin_params:
        f_ix = np.argmin(np.abs(f_vec - s_p["f"]))
        peak_inds = np.argmax(slice_along_axis(result.data, slice(f_ix-3, f_ix+3), axis=fax_ix), axis=fax_ix)
        assert np.all(peak_inds == 3)


@pytest.mark.parametrize("window", [WindowFunction.HANNING, WindowFunction.HAMMING])
@pytest.mark.parametrize("transform", [SpectralTransform.REL_DB, SpectralTransform.REL_POWER])
@pytest.mark.parametrize("output", [SpectralOutput.POSITIVE, SpectralOutput.NEGATIVE, SpectralOutput.FULL])
def test_spectrum_gen(
    window: WindowFunction,
    transform: SpectralTransform,
    output: SpectralOutput
):
    win_dur = 1.0
    win_step_dur = 0.5
    fs = 1000.0
    sin_params = [
        {"a": 1.0, "f": 10.0, "p": 0.0, "dur": 20.0},
        {"a": 0.5, "f": 20.0, "p": np.pi / 7, "dur": 20.0},
        {"a": 0.2, "f": 200.0, "p": np.pi / 11, "dur": 20.0},
    ]
    messages = create_messages_with_periodic_signal(
        sin_params=sin_params,
        fs=fs,
        msg_dur=win_dur,
        win_step_dur=win_step_dur
    )
    gen = spectrum(axis="time", window=window, transform=transform, output=output)
    results = [gen.send(msg) for msg in messages]

    assert "freq" in results[0].dims
    assert "ch" in results[0].dims
    assert "win" not in results[0].dims
    # _debug_plot_welch(messages[0], results[0], welch_db=True)
