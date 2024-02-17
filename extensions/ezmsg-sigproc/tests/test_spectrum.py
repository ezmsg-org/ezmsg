import os
import json

import pytest
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray, slice_along_axis
from ezmsg.util.messagegate import MessageGate, MessageGateSettings
from ezmsg.util.messagelogger import MessageLogger, MessageLoggerSettings
from ezmsg.util.messagecodec import message_log
from ezmsg.sigproc.synth import sin
from ezmsg.sigproc.spectrum import spectrum, SpectralTransform, SpectralOutput, WindowFunction

from util import get_test_fn
from ezmsg.util.terminate import TerminateOnTimeout as TerminateTest
from ezmsg.util.terminate import TerminateOnTimeoutSettings as TerminateTestSettings

from typing import Optional, List


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
@pytest.mark.parametrize("output", [SpectralOutput.FULL, SpectralOutput.POSITIVE, SpectralOutput.NEGATIVE])
def test_spectrum_gen(
    window: WindowFunction,
    transform: SpectralTransform,
    output: SpectralOutput
):
    afps = [(1.0, 10.0, 0.0), (0.5, 20.0, np.pi/7), (0.2, 200.0, np.pi/11)]
    test_dur = 20.0
    win_dur = 1.0
    win_step_dur = 0.5
    fs = 1000.0

    # Prepare inputs
    win_len = int(win_dur * fs)
    win_step = int(win_step_dur * fs)
    t = np.arange(int(test_dur * fs)) / fs
    data = np.sum(np.vstack(
        [a * np.sin(2 * np.pi * f * t + p) for a, f, p in afps]
    ), axis=0)
    win_data = sliding_window_view(data, (win_len,), axis=0)[::win_step]
    input_wins = AxisArray(
        win_data[..., None],
        dims=["win", "time", "ch"],
        axes={
            "win": AxisArray.Axis.TimeAxis(offset=0, fs=1/win_step_dur),
            "time": AxisArray.Axis.TimeAxis(offset=0, fs=fs),
        }
    )

    gen = spectrum(axis="time", window=window, transform=transform, output=output)
    result = gen.send(input_wins)
    # _debug_plot_welch(input_wins, result, welch_db=True)
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
    for a, f, p in afps:
        f_ix = np.argmin(np.abs(f_vec - f))
        peak_inds = np.argmax(slice_along_axis(result.data, slice(f_ix-3, f_ix+3), axis=fax_ix), axis=fax_ix)
        assert np.all(peak_inds == 3)

    # single-windowed input
    input_single = AxisArray(win_data[0, ..., None], dims=["time", "ch"], axes={"time": input_wins.axes["time"]})
    gen = spectrum(axis="time", window=window, transform=transform, output=output)
    result = gen.send(input_single)
    assert "freq" in result.dims
    assert "ch" in result.dims
    # _debug_plot_welch(input_single, result, welch_db=True)
