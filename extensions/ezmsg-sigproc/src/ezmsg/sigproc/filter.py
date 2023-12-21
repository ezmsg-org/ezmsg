from dataclasses import dataclass, replace, field

import ezmsg.core as ez
import scipy.signal
import numpy as np
import asyncio

from ezmsg.util.messages.axisarray import AxisArray

from typing import AsyncGenerator, Optional, Tuple


@dataclass
class FilterCoefficients:
    b: np.ndarray = field(default_factory=lambda: np.array([1.0, 0.0]))
    a: np.ndarray = field(default_factory=lambda: np.array([1.0, 0.0]))


class FilterSettingsBase(ez.Settings):
    axis: Optional[str] = None
    fs: Optional[float] = None


class FilterSettings(FilterSettingsBase):
    # If you'd like to statically design a filter, define it in settings
    filt: Optional[FilterCoefficients] = None


class FilterState(ez.State):
    axis: Optional[str] = None
    zi: Optional[np.ndarray] = None
    filt_designed: bool = False
    filt: Optional[FilterCoefficients] = None
    filt_set: asyncio.Event = field(default_factory=asyncio.Event)
    samp_shape: Optional[Tuple[int, ...]] = None
    fs: Optional[float] = None  # Hz


class Filter(ez.Unit):
    SETTINGS: FilterSettingsBase
    STATE: FilterState

    INPUT_FILTER = ez.InputStream(FilterCoefficients)
    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    def design_filter(self) -> Optional[Tuple[np.ndarray, np.ndarray]]:
        raise NotImplementedError("Must implement 'design_filter' in Unit subclass!")

    # Set up filter with static initialization if specified
    def initialize(self) -> None:
        if self.SETTINGS.axis is not None:
            self.STATE.axis = self.SETTINGS.axis

        if isinstance(self.SETTINGS, FilterSettings):
            if self.SETTINGS.filt is not None:
                self.STATE.filt = self.SETTINGS.filt
                self.STATE.filt_set.set()
        else:
            self.STATE.filt_set.clear()

        if self.SETTINGS.fs is not None:
            try:
                self.update_filter()
            except NotImplementedError:
                ez.logger.debug("Using filter coefficients.")

    @ez.subscriber(INPUT_FILTER)
    async def redesign(self, message: FilterCoefficients):
        self.STATE.filt = message

    def update_filter(self):
        try:
            coefs = self.design_filter()
            self.STATE.filt = (
                FilterCoefficients() if coefs is None else FilterCoefficients(*coefs)
            )
            self.STATE.filt_set.set()
            self.STATE.filt_designed = True
        except NotImplementedError as e:
            raise e
        except Exception as e:
            ez.logger.warning(f"Error when designing filter: {e}")

    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_SIGNAL)
    async def apply_filter(self, msg: AxisArray) -> AsyncGenerator:
        axis_name = msg.dims[0] if self.STATE.axis is None else self.STATE.axis
        axis_idx = msg.get_axis_idx(axis_name)
        axis = msg.get_axis(axis_name)
        fs = 1.0 / axis.gain

        if self.STATE.fs != fs and self.STATE.filt_designed is True:
            self.STATE.fs = fs
            self.update_filter()

        # Ensure filter is defined
        # TODO: Maybe have me be a passthrough filter until coefficients are received
        if self.STATE.filt is None:
            self.STATE.filt_set.clear()
            ez.logger.info("Awaiting filter coefficients...")
            await self.STATE.filt_set.wait()
            ez.logger.info("Filter coefficients received.")

        assert self.STATE.filt is not None

        arr_in = msg.data

        # If the array is one dimensional, add a temporary second dimension so that the math works out
        one_dimensional = False
        if arr_in.ndim == 1:
            arr_in = np.expand_dims(arr_in, axis=1)
            one_dimensional = True

        # We will perform filter with time dimension as last axis
        arr_in = np.moveaxis(arr_in, axis_idx, -1)
        samp_shape = arr_in[..., 0].shape

        # Re-calculate/reset zi if necessary
        if self.STATE.zi is None or samp_shape != self.STATE.samp_shape:
            zi: np.ndarray = scipy.signal.lfilter_zi(
                self.STATE.filt.b, self.STATE.filt.a
            )
            self.STATE.samp_shape = samp_shape
            self.STATE.zi = np.array([zi] * np.prod(self.STATE.samp_shape))
            self.STATE.zi = self.STATE.zi.reshape(
                tuple(list(self.STATE.samp_shape) + [zi.shape[0]])
            )

        arr_out, self.STATE.zi = scipy.signal.lfilter(
            self.STATE.filt.b, self.STATE.filt.a, arr_in, zi=self.STATE.zi
        )

        arr_out = np.moveaxis(arr_out, -1, axis_idx)

        # Remove temporary first dimension if necessary
        if one_dimensional:
            arr_out = np.squeeze(arr_out, axis=1)

        yield self.OUTPUT_SIGNAL, replace(msg, data=arr_out),
