import pytest

import ezmsg.core as ez
from ezmsg.core.unit import THREAD_ATTR


def test_thread_decorator_warns_and_sets_attribute():
    def fn(_):
        return None

    with pytest.warns(
        DeprecationWarning,
        match=r"`@ez\.thread` is deprecated and will be removed in a future release",
    ):
        decorated = ez.thread(fn)

    assert decorated is fn
    assert hasattr(decorated, THREAD_ATTR)
