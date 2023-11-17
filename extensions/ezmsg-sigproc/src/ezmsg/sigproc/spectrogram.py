from dataclasses import field, replace
from typing import AsyncGenerator

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.sigproc.window import Window, WindowSettings
from ezmsg.sigproc.spectral import Spectrum, SpectrumSettings, WindowFunction, SpectralTransform, SpectralOutput


class ModifyAxisSettings(ez.Settings):
    name_map: dict | None


class ModifyAxis(ez.Unit):
    SETTINGS: ModifyAxisSettings

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_data(self, msg: AxisArray) -> AsyncGenerator:
        if self.SETTINGS.name_map is not None:
            new_dims = msg.dims
            new_axes = msg.axes
            for k, v in self.SETTINGS.name_map.items():
                if k in new_dims:
                    new_axes[v] = new_axes.pop(k)
                    new_dims[new_dims.index(k)] = v
        yield self.OUTPUT_SIGNAL, replace(msg, dims=new_dims, axes=new_axes)


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
