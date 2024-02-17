from dataclasses import field, replace
import typing

import numpy as np

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.generator import compose, consumer, GenAxisArray
from ezmsg.util.messages.modify import modify_axis, ModifyAxis, ModifyAxisSettings
from ezmsg.sigproc.window import windowing, Window, WindowSettings
from ezmsg.sigproc.spectrum import (
    spectrum, Spectrum, SpectrumSettings,
    WindowFunction, SpectralTransform, SpectralOutput
)


@consumer
def spectrogram(
    window_dur: typing.Optional[float] = None,
    window_shift: typing.Optional[float] = None,
    window: WindowFunction = WindowFunction.HANNING,
    transform: SpectralTransform = SpectralTransform.REL_DB,
    output: SpectralOutput = SpectralOutput.POSITIVE
) -> typing.Generator[typing.Optional[AxisArray], AxisArray, None]:

    # pipeline = compose(
    f_win = windowing(axis="time", newaxis="step", window_dur=window_dur, window_shift=window_shift)
    f_spec = spectrum(axis="time", window=window, transform=transform, output=output)
    f_modify = modify_axis(name_map={"step": "time"})
    # )

    # State variables
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out: typing.Optional[AxisArray] = None

    while True:
        axis_arr_in = yield axis_arr_out

        axis_arr_out = None
        wins = f_win.send(axis_arr_in)
        if len(wins):
            specs = f_spec.send(wins[0])
            if specs is not None:
                axis_arr_out = f_modify.send(specs)
        # axis_arr_out = pipeline(axis_arr_in)


class SpectrogramSettings(ez.Settings):
    window_dur: float | None = None  # window duration in seconds
    window_shift: float | None = None  # window step in seconds. If None, window_shift == window_dur
    # See SpectrumSettings for details of following settings:
    window: WindowFunction = WindowFunction.HAMMING
    transform: SpectralTransform = SpectralTransform.REL_DB
    output: SpectralOutput = SpectralOutput.POSITIVE


class SpectrogramState(ez.State):
    cur_settings: SpectrogramSettings


class Spectrogram(ez.Collection):
    SETTINGS: SpectrogramSettings
    STATE: SpectrogramState

    # Ports
    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    # Subunits
    WINDOW = Window()
    SPECTRUM = Spectrum()
    MODIFYAXIS = ModifyAxis()

    def configure(self) -> None:
        self.WINDOW.apply_settings(WindowSettings(
            axis="time",
            newaxis="step",
            window_dur=self.SETTINGS.window_dur,
            window_shift=self.SETTINGS.window_shift
        ))
        self.SPECTRUM.apply_settings(SpectrumSettings(
            axis="time",
            window=self.SETTINGS.window,
            transform=self.SETTINGS.transform,
            output=self.SETTINGS.output
        ))
        self.MODIFYAXIS.apply_settings(ModifyAxisSettings(name_map={"step": "time"}))

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.INPUT_SIGNAL, self.WINDOW.INPUT_SIGNAL),
            (self.WINDOW.OUTPUT_SIGNAL, self.SPECTRUM.INPUT_SIGNAL),
            (self.SPECTRUM.OUTPUT_SIGNAL, self.MODIFYAXIS.INPUT_SIGNAL),
            (self.MODIFYAXIS.OUTPUT_SIGNAL, self.OUTPUT_SIGNAL)
        )
