from dataclasses import field, replace
from typing import AsyncGenerator

import numpy as np
import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.sigproc.spectral import OptionsEnum
from .spectrogram import SpectrogramSettings, Spectrogram


class AggregationFunction(OptionsEnum):
    NONE = "None (all)"
    MAX = "max"
    MIN = "min"
    MEAN = "mean"
    MEDIAN = "median"
    STD = "std"
    NANMAX = "nanmax"
    NANMIN = "nanmin"
    NANMEAN = "nanmean"
    NANMEDIAN = "nanmedian"
    NANSTD = "nanstd"


AGGREGATORS = {
    AggregationFunction.NONE: np.all,
    AggregationFunction.MAX: np.max,
    AggregationFunction.MIN: np.min,
    AggregationFunction.MEAN: np.mean,
    AggregationFunction.MEDIAN: np.median,
    AggregationFunction.STD: np.std,
    AggregationFunction.NANMAX: np.nanmax,
    AggregationFunction.NANMIN: np.nanmin,
    AggregationFunction.NANMEAN: np.nanmean,
    AggregationFunction.NANMEDIAN: np.nanmedian,
    AggregationFunction.NANSTD: np.nanstd
}


class RangedAggregateSettings(ez.Settings):
    axis: str | None = None
    bands: list[tuple] | None = None
    operation: AggregationFunction = AggregationFunction.MEAN


class RangedAggregateState(ez.State):
    cur_settings: RangedAggregateSettings


class RangedAggregate(ez.Unit):
    SETTINGS: RangedAggregateSettings
    STATE: RangedAggregateState

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    def initialize(self) -> None:
        self.STATE.cur_settings = self.SETTINGS
        self._target_axis: AxisArray.Axis = field(default_factory=AxisArray.Axis)
        self._out_axis: AxisArray.Axis = field(default_factory=AxisArray.Axis)
        self._slices: list[tuple] | None = None

    def _recalc_slices(self, axis: AxisArray.Axis, axlen: int):
        self._target_axis = axis
        ax_vec = axis.offset + np.arange(axlen) * axis.gain
        slices = []
        mids = []
        for (start, stop) in self.STATE.cur_settings.bands:
            inds = np.where(np.logical_and(ax_vec >= start, ax_vec <= stop))[0]
            mids.append(np.mean(inds) * axis.gain + axis.offset)
            slices.append(np.s_[inds[0]:inds[-1] + 1])
        self._slices = slices
        self._out_axis = AxisArray.Axis(
            unit=axis.unit, offset=mids[0], gain=(mids[1] - mids[0]) if len(mids) > 1 else 1.0
        )

    # TODO: if settings have changed then self._recalc_slices(self._target_axis)

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_data(self, msg: AxisArray) -> AsyncGenerator:
        if self.STATE.cur_settings.bands is not None:
            axis_name = self.STATE.cur_settings.axis or msg.dims[0]
            ax_idx = msg.get_axis_idx(axis_name)
            if self._slices is None or self._target_axis != msg.get_axis(axis_name):
                self._recalc_slices(msg.get_axis(axis_name), msg.data.shape[ax_idx])
            in_data = np.moveaxis(msg.data, ax_idx, 0)
            out_data = []
            agg_func = AGGREGATORS[self.STATE.cur_settings.operation]
            for sl in self._slices:
                out_data.append(agg_func(in_data[sl], axis=0))
            new_axes = {
                k: (v if k != axis_name else self._out_axis)
                for k, v in msg.axes.items()
            }
            msg = replace(msg, data=np.stack(out_data, axis=ax_idx), axes=new_axes)
        yield self.OUTPUT_SIGNAL, msg


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
