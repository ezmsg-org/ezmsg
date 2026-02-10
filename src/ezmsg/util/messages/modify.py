import warnings
from collections.abc import AsyncGenerator

import numpy as np

import ezmsg.core as ez
from .axisarray import AxisArray, replace


class ModifyAxisSettings(ez.Settings):
    """
    Settings for ModifyAxisTransformer and ModifyAxis unit.

    :param name_map: A dictionary where the keys are the names of the old dims and the values are the new names.
        Use None as a value to drop the dimension. If the dropped dimension is not len==1 then an error is raised.
    :type name_map: dict[str, str | None] | None
    """

    name_map: dict[str, str | None] | None = None


class ModifyAxisTransformer:
    """
    Modify an AxisArray's axes and dims according to a name_map.

    This is a stateless transformer that renames dimensions and axes, with support
    for dropping length-1 dimensions.

    :param settings: Settings for the transformer.
    :type settings: ModifyAxisSettings
    """

    def __init__(self, settings: ModifyAxisSettings = ModifyAxisSettings()):
        self.settings = settings

    def __call__(self, message: AxisArray) -> AxisArray:
        name_map = self.settings.name_map
        if name_map is None:
            return message

        new_dims = [name_map.get(old_k, old_k) for old_k in message.dims]
        new_axes = {
            name_map.get(old_k, old_k): v for old_k, v in message.axes.items()
        }
        drop_ax_ix = [
            ix
            for ix, old_dim in enumerate(message.dims)
            if new_dims[ix] is None
        ]
        if len(drop_ax_ix) > 0:
            new_dims = [d for d in new_dims if d is not None]
            new_axes.pop(None, None)
            return replace(
                message,
                data=np.squeeze(message.data, axis=tuple(drop_ax_ix)),
                dims=new_dims,
                axes=new_axes,
            )
        return replace(message, dims=new_dims, axes=new_axes)

    _send_warned: bool = False

    def send(self, message: AxisArray) -> AxisArray:
        """Alias for __call__. Deprecated — use ``__call__`` directly instead."""
        if not ModifyAxisTransformer._send_warned:
            ModifyAxisTransformer._send_warned = True
            warnings.warn(
                "ModifyAxisTransformer.send() is deprecated. Use __call__() instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        return self(message)


def modify_axis(
    name_map: dict[str, str | None] | None = None,
) -> ModifyAxisTransformer:
    """
    Create a :class:`ModifyAxisTransformer` instance.

    This function exists for backward compatibility with code that previously used
    the generator-based ``modify_axis``. The returned object supports ``.send(msg)``.

    :param name_map: A dictionary where the keys are the names of the old dims and the values are the new names.
        Use None as a value to drop the dimension. If the dropped dimension is not len==1 then an error is raised.
    :type name_map: dict[str, str | None] | None
    :return: A ModifyAxisTransformer instance.
    :rtype: ModifyAxisTransformer
    """
    return ModifyAxisTransformer(ModifyAxisSettings(name_map=name_map))


class ModifyAxis(ez.Unit):
    """
    Unit for modifying axis names and dimensions of AxisArray messages.

    Renames dimensions and axes according to a name mapping, with support for
    dropping dimensions.
    """

    SETTINGS = ModifyAxisSettings

    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)
    INPUT_SETTINGS = ez.InputStream(ModifyAxisSettings)

    _transformer: ModifyAxisTransformer

    async def initialize(self) -> None:
        self._transformer = ModifyAxisTransformer(self.SETTINGS)

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: ez.Settings) -> None:
        self.apply_settings(msg)
        self._transformer = ModifyAxisTransformer(self.SETTINGS)

    @ez.subscriber(INPUT_SIGNAL, zero_copy=True)
    @ez.publisher(OUTPUT_SIGNAL)
    async def on_message(self, message: AxisArray) -> AsyncGenerator:
        ret = self._transformer(message)
        if ret is not None:
            yield self.OUTPUT_SIGNAL, ret
