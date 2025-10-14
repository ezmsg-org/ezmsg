from collections.abc import AsyncGenerator, Generator
import traceback
import typing

import numpy as np

import ezmsg.core as ez
from .axisarray import AxisArray, replace
from ..generator import consumer, GenState


@consumer
def modify_axis(
    name_map: dict[str, str | None] | None = None,
) -> Generator[AxisArray, AxisArray, None]:
    """
    Modify an AxisArray's axes and dims according to a name_map.

    :param name_map: A dictionary where the keys are the names of the old dims and the values are the new names.
        Use None as a value to drop the dimension. If the dropped dimension is not len==1 then an error is raised.
    :type name_map: dict[str, str | None] | None
    :return: A primed generator object ready to yield an AxisArray with modified axes for each .send(axis_array).
    :rtype: collections.abc.Generator[AxisArray, AxisArray, None]
    """
    # State variables
    axis_arr_in = AxisArray(np.array([]), dims=[""])
    axis_arr_out = AxisArray(np.array([]), dims=[""])

    while True:
        axis_arr_in = yield axis_arr_out

        if name_map is not None:
            new_dims = [name_map.get(old_k, old_k) for old_k in axis_arr_in.dims]
            new_axes = {
                name_map.get(old_k, old_k): v for old_k, v in axis_arr_in.axes.items()
            }
            drop_ax_ix = [
                ix
                for ix, old_dim in enumerate(axis_arr_in.dims)
                if new_dims[ix] is None
            ]
            if len(drop_ax_ix) > 0:
                # Rewrite new_dims and new_axes without the dropped axes
                new_dims = [_ for _ in new_dims if _ is not None]
                new_axes.pop(None, None)
                # Reshape data
                # np.squeeze will raise ValueError if not len==1
                axis_arr_out = replace(
                    axis_arr_in,
                    data=np.squeeze(axis_arr_in.data, axis=tuple(drop_ax_ix)),
                    dims=new_dims,
                    axes=new_axes,
                )
            else:
                axis_arr_out = replace(axis_arr_in, dims=new_dims, axes=new_axes)
        else:
            axis_arr_out = axis_arr_in


class ModifyAxisSettings(ez.Settings):
    """
    Settings for ModifyAxis unit.
    
    Configuration for modifying axis names and dimensions of AxisArray messages.

    :param name_map: A dictionary where the keys are the names of the old dims and the values are the new names.
        Use None as a value to drop the dimension. If the dropped dimension is not len==1 then an error is raised.
    :type name_map: dict[str, str | None] | None
    """
    name_map: dict[str, str | None] | None = None


class ModifyAxis(ez.Unit):
    """
    Unit for modifying axis names and dimensions of AxisArray messages.
    
    Renames dimensions and axes according to a name mapping, with support for
    dropping dimensions. Uses zero-copy operations for efficient processing.
    """
    STATE = GenState
    SETTINGS = ModifyAxisSettings

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)
    INPUT_SETTINGS = ez.InputStream(ModifyAxisSettings)

    async def initialize(self) -> None:
        """
        Initialize the ModifyAxis unit.
        
        Sets up the generator for axis modification operations.
        """
        self.construct_generator()

    def construct_generator(self):
        """
        Construct the axis-modifying generator with current settings.
        
        Creates a new modify_axis generator instance using the unit's name mapping.
        """
        self.STATE.gen = modify_axis(name_map=self.SETTINGS.name_map)

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: ez.Settings) -> None:
        """
        Handle incoming settings updates.
        
        :param msg: New settings to apply.
        :type msg: ez.Settings
        """
        self.apply_settings(msg)
        self.construct_generator()

    @ez.subscriber(INPUT_SIGNAL, zero_copy=True)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_message(self, message: AxisArray) -> AsyncGenerator:
        """
        Process incoming AxisArray messages and modify their axes.
        
        Uses zero-copy operations to efficiently modify axis names and dimensions
        while preserving data integrity.
        
        :param message: Input AxisArray to modify.
        :type message: AxisArray
        :return: Async generator yielding AxisArray with modified axes.
        :rtype: collections.abc.AsyncGenerator
        """
        try:
            ret = self.STATE.gen.send(message)
            if ret is not None:
                yield self.OUTPUT_SIGNAL, ret
        except (StopIteration, GeneratorExit):
            ez.logger.debug(f"ModifyAxis closed in {self.address}")
        except Exception:
            ez.logger.info(traceback.format_exc())
