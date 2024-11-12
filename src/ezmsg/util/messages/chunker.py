import asyncio
import traceback
import typing

import numpy as np
import numpy.typing as npt

import ezmsg.core as ez
from .axisarray import AxisArray, slice_along_axis, replace
from ..generator import GenState


def array_chunker(
    data: npt.ArrayLike,
    chunk_len: int,
    axis: int = 0,
    fs: float = 1000.0,
    tzero: float = 0.0,
) -> typing.Generator[AxisArray, None, None]:
    """
    Create a generator that yields chunks of an array along a specified axis.
    The generator should be useful for quick offline analyses, tests, or examples.
    This generator probably is not useful for online streaming applications.

    Parameters:
        data: An array_like object to iterate over, chunk-by-chunk.
        chunk_len: The length of the chunk returned in each iteration (except the last).
        axis: The axis along which to chunk the array.
        fs: The sampling frequency of the data. Will only be used to make the time axis.
        tzero: The time offset of the first chunk. Will only be used to make the time axis.

    Returns:
        A generator that yields AxisArrays containing chunks of the input array.
    """

    if not type(data) == np.ndarray:
        data = np.array(data)
    n_chunks = int(np.ceil(data.shape[axis] / chunk_len))
    tvec = np.arange(n_chunks, dtype=float) * chunk_len / fs + tzero
    out_dims = [f"d{_}" for _ in range(data.ndim)]
    out_dims[axis] = "time"
    template = AxisArray(
        slice_along_axis(data, slice(None, 0), axis),
        dims=out_dims,
        axes={"time": AxisArray.TimeAxis(fs=fs, offset=tvec[0])},
    )

    for chunk_ix in range(n_chunks):
        view = slice_along_axis(
            data, slice(chunk_ix * chunk_len, (chunk_ix + 1) * chunk_len), axis
        )
        axis_arr_out = replace(
            template,
            data=view,
            axes={"time": AxisArray.TimeAxis(fs=fs, offset=tvec[chunk_ix])},
        )
        yield axis_arr_out


class ArrayChunkerSettings(ez.Settings):
    data: npt.ArrayLike
    chunk_len: int
    axis: int = 0
    fs: float = 1000.0
    tzero: float = 0.0


class ArrayChunker(ez.Unit):
    SETTINGS = ArrayChunkerSettings
    STATE = GenState

    INPUT_SETTINGS = ez.InputStream(ArrayChunkerSettings)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    async def initialize(self) -> None:
        self.construct_generator()

    def construct_generator(self):
        self.STATE.gen = array_chunker(
            data=self.SETTINGS.data,
            chunk_len=self.SETTINGS.chunk_len,
            axis=self.SETTINGS.axis,
            fs=self.SETTINGS.fs,
            tzero=self.SETTINGS.tzero,
        )

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: ez.Settings) -> None:
        self.apply_settings(msg)
        self.construct_generator()

    @ez.publisher(OUTPUT_SIGNAL)
    async def send_chunk(self) -> typing.AsyncGenerator:
        try:
            while True:
                yield self.OUTPUT_SIGNAL, next(self.STATE.gen)
                await asyncio.sleep(0)
        except (StopIteration, GeneratorExit):
            ez.logger.debug(f"ArrayChunker closed in {self.address}")
        except Exception:
            ez.logger.info(traceback.format_exc())
