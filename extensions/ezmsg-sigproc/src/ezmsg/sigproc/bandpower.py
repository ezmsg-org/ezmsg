from dataclasses import field

import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray

from .spectrogram import SpectrogramSettings, Spectrogram
from .aggregate import RangedAggregate, RangedAggregateSettings


class BandPowerSettings(ez.Settings):
    spectrogram_settings: SpectrogramSettings = field(default_factory=SpectrogramSettings)
    bands: list[tuple] | None = field(default_factory=lambda: [(17, 30), (70, 170)])


class BandPower(ez.Collection):
    SETTINGS: BandPowerSettings

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    # Subunits
    SPECGRAM = Spectrogram()
    BANDS = RangedAggregate()

    def configure(self) -> None:
        self.SPECGRAM.apply_settings(self.SETTINGS.spectrogram_settings)
        self.BANDS.apply_settings(RangedAggregateSettings(
            axis="freq",
            bands=self.SETTINGS.bands,
        ))

    def network(self) -> ez.NetworkDefinition:
        return (
            (self.INPUT_SIGNAL, self.SPECGRAM.INPUT_SIGNAL),
            (self.SPECGRAM.OUTPUT_SIGNAL, self.BANDS.INPUT_SIGNAL),
            (self.BANDS.OUTPUT_SIGNAL, self.OUTPUT_SIGNAL)
        )
