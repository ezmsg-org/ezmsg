import asyncio
from collections.abc import Generator, AsyncGenerator
import traceback

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
) -> Generator[AxisArray, None, None]:
    """
    Create a generator that yields AxisArrays containing chunks of an array
    along a specified axis.

    The generator should be useful for quick offline analyses, tests, or examples.
    This generator probably is not useful for online streaming applications.

    :param data: An array_like object to iterate over, chunk-by-chunk.
    :type data: npt.ArrayLike
    :param chunk_len: The length of the chunk returned in each iteration (except the last).
    :type chunk_len: int
    :param axis: The axis along which to chunk the array.
    :type axis: int
    :param fs: The sampling frequency of the data. Will only be used to make the time axis.
    :type fs: float
    :param tzero: The time offset of the first chunk. Will only be used to make the time axis.
    :type tzero: float
    :return: A generator that yields AxisArrays containing chunks of the input array.
    :rtype: collections.abc.Generator[AxisArray, None, None]
    """

    if not type(data) == np.ndarray:  # noqa: E721  # hot path optimization
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
    """
    Settings for ArrayChunker unit.

    Configuration for chunking array data along a specified axis with timing information.

    :param data: An array_like object to iterate over, chunk-by-chunk.
    :type data: npt.ArrayLike
    :param chunk_len: The length of the chunk returned in each iteration (except the last).
    :type chunk_len: int
    :param axis: The axis along which to chunk the array.
    :type axis: int
    :param fs: The sampling frequency of the data. Will only be used to make the time axis.
    :type fs: float
    :param tzero: The time offset of the first chunk. Will only be used to make the time axis.
    :type tzero: float
    """

    data: npt.ArrayLike
    chunk_len: int
    axis: int = 0
    fs: float = 1000.0
    tzero: float = 0.0


class ArrayChunker(ez.Unit):
    """
    Unit for chunking array data along a specified axis.

    Converts array data into sequential chunks along a specified axis,
    with proper timing axis information for streaming applications.
    """

    SETTINGS = ArrayChunkerSettings
    STATE = GenState

    INPUT_SETTINGS = ez.InputStream(ArrayChunkerSettings)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    async def initialize(self) -> None:
        """
        Initialize the ArrayChunker unit.

        Sets up the generator for chunking operations based on current settings.
        """
        self.construct_generator()

    def construct_generator(self):
        """
        Construct the chunking generator with current settings.

        Creates a new array_chunker generator instance using the unit's settings.
        """
        self.STATE.gen = array_chunker(
            data=self.SETTINGS.data,
            chunk_len=self.SETTINGS.chunk_len,
            axis=self.SETTINGS.axis,
            fs=self.SETTINGS.fs,
            tzero=self.SETTINGS.tzero,
        )

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: ez.Settings) -> None:
        """
        Handle incoming settings updates.

        :param msg: New settings to apply.
        :type msg: ez.Settings
        """
        self.apply_settings(msg)
        self.construct_generator()

    @ez.publisher(OUTPUT_SIGNAL)
    async def send_chunk(self) -> AsyncGenerator:
        """
        Publisher method that yields data chunks.

        Continuously yields chunks from the generator until exhausted,
        with proper exception handling for completion cases.

        :return: Async generator yielding AxisArray chunks.
        :rtype: collections.abc.AsyncGenerator
        """
        try:
            while True:
                yield self.OUTPUT_SIGNAL, next(self.STATE.gen)
                await asyncio.sleep(0)
        except (StopIteration, GeneratorExit):
            ez.logger.debug(f"ArrayChunker closed in {self.address}")
        except Exception:
            ez.logger.info(traceback.format_exc())
