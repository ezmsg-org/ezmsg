import ezmsg.core as ez
import scipy.signal
import numpy as np

from .filter import Filter, FilterState, FilterSettingsBase

from typing import Optional, Tuple, Union


class ButterworthFilterSettings(FilterSettingsBase):
    order: int = 0
    cuton: Optional[float] = None  # Hz
    cutoff: Optional[float] = None  # Hz

    def filter_specs(self) -> Optional[Tuple[str, Union[float, Tuple[float, float]]]]:
        if self.cuton is None and self.cutoff is None:
            return None
        elif self.cuton is None and self.cutoff is not None:
            return "lowpass", self.cutoff
        elif self.cuton is not None and self.cutoff is None:
            return "highpass", self.cuton
        elif self.cuton is not None and self.cutoff is not None:
            if self.cuton <= self.cutoff:
                return "bandpass", (self.cuton, self.cutoff)
            else:
                return "bandstop", (self.cutoff, self.cuton)


@consumer
def butter(
    axis: typing.Optional[str],
    order: int = 0,
    cuton: typing.Optional[float] = None,
    cutoff: typing.Optional[float] = None,
    coef_type: str = "ba",
) -> typing.Generator[AxisArray, AxisArray, None]:
    # IO
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out = AxisArray(np.array([]), dims=[""])

    btype, cutoffs = ButterworthFilterSettings(
        order=order, cuton=cuton, cutoff=cutoff
    ).filter_specs()

    # We cannot calculate coefs yet because we do not know input sample rate
    coefs = None
    filter_gen = filtergen(axis, coefs, coef_type)  # Passthrough.

    while True:
        axis_arr_in = yield axis_arr_out
        if coefs is None and order > 0:
            fs = 1 / axis_arr_in.axes[axis or axis_arr_in.dims[0]].gain
            coefs = scipy.signal.butter(
                order, Wn=cutoffs, btype=btype, fs=fs, output=coef_type
            )
            filter_gen = filtergen(axis, coefs, coef_type)

        axis_arr_out = filter_gen.send(axis_arr_in)


class ButterworthFilterState(FilterState):
    design: ButterworthFilterSettings


class ButterworthFilter(Filter):
    SETTINGS: ButterworthFilterSettings
    STATE: ButterworthFilterState

    INPUT_FILTER = ez.InputStream(ButterworthFilterSettings)

    def initialize(self) -> None:
        self.STATE.design = self.SETTINGS
        self.STATE.filt_designed = True
        super().initialize()

    def design_filter(self) -> Optional[Tuple[np.ndarray, np.ndarray]]:
        specs = self.STATE.design.filter_specs()
        if self.STATE.design.order > 0 and specs is not None:
            btype, cut = specs
            return scipy.signal.butter(
                self.STATE.design.order,
                Wn=cut,
                btype=btype,
                fs=self.STATE.fs,
                output="ba",
            )
        else:
            return None

    @ez.subscriber(INPUT_FILTER)
    async def redesign(self, message: ButterworthFilterSettings) -> None:
        if type(message) is not ButterworthFilterSettings:
            return

        if self.STATE.design.order != message.order:
            self.STATE.zi = None
        self.STATE.design = message
        self.update_filter()
