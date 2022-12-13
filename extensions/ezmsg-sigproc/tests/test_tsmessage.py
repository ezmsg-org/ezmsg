
import pytest

from ezmsg.sigproc.messages import TSMessage
from ezmsg.util.messages import view2d

import numpy as np

@pytest.mark.parametrize("data", [
    np.array( 5.0 ),
    np.random.randn(16),
    np.random.randn(16, 32),
    np.random.randn(16, 32, 42),
    np.random.randn(16, 32, 42, 73),
] )
def test_view2d( data: np.ndarray ):
    for time_dim in range(len(data.shape)):
        msg = TSMessage(data.copy(), fs = 5.0, time_dim = time_dim)

        with view2d( msg.data, axis = msg.time_dim ) as arr:
            print(f'{np.shares_memory(msg.data, arr)=}', f'{arr.shape=}')
            assert arr.shape == (msg.n_time, msg.n_ch)
            arr[:] = arr + 1

        assert np.allclose(msg.data, data + 1)

        assert msg.data.shape == data.shape

