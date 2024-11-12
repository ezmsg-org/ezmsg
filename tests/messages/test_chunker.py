import time
import pytest

import numpy as np

from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messages.chunker import array_chunker


@pytest.mark.parametrize("axis", [0, 1, 2])
def test_array_chunker(axis: int):
    tzero = time.time()
    dshape = [3, 4, 5]
    dshape[axis] = 10_000
    dshape = tuple(dshape)
    fs = 1_000.0
    chunk_len = 19
    data = np.arange(np.prod(dshape)).reshape(dshape)
    tvec = np.arange(dshape[axis]) / fs + tzero

    chunker = array_chunker(data, chunk_len=chunk_len, axis=axis, fs=fs, tzero=tzero)
    chunks = list(chunker)

    assert len(chunks) == int(np.ceil(dshape[axis] / chunk_len))
    assert all(isinstance(chunk, AxisArray) for chunk in chunks)
    assert all(chunk.data.ndim == data.ndim for chunk in chunks)
    assert all(chunk.data.shape[axis] == chunk_len for chunk in chunks[:-1])
    assert chunks[-1].data.shape[axis] == dshape[axis] % chunk_len
    assert np.array_equal([_.axes["time"].offset for _ in chunks], tvec[::chunk_len])
    assert np.array_equal(
        np.concatenate([chunk.data for chunk in chunks], axis=axis), data
    )
    assert all(np.may_share_memory(chunk.data, data) for chunk in chunks)
