"""Tests for ezmsg.util.messages.util.replace."""

import numpy as np
import pytest

from ezmsg.util.messages.axisarray import AxisArray, CoordinateAxis
from ezmsg.util.messages.util import fast_replace, slow_replace


def _axis(labels):
    return CoordinateAxis(data=np.array(labels), dims=["ch"])


@pytest.mark.parametrize(
    "replace_fn", [fast_replace, slow_replace], ids=["fast", "slow"]
)
class TestReplaceWithDerivedCaches:
    """A lazily derived cache on the instance must not reach the constructor.

    ``fast_replace`` copies ``__dict__`` straight into ``__class__(**kwargs)``,
    so a cached attribute that is not a dataclass field would raise TypeError.
    It must also not be *carried over*: a value derived from the old field
    values would be wrong on a copy that changes them.
    """

    def test_replace_after_fingerprint_access(self, replace_fn):
        axis = _axis(["A", "B"])
        assert axis.fingerprint is not None  # populates the cache
        updated = replace_fn(axis, data=np.array(["X", "Y"]))
        assert list(updated.data) == ["X", "Y"]

    def test_stale_fingerprint_is_not_carried_over(self, replace_fn):
        axis = _axis(["A", "B"])
        before = axis.fingerprint
        updated = replace_fn(axis, data=np.array(["X", "Y"]))
        assert updated.fingerprint != before

    def test_unrelated_field_change_still_refreshes(self, replace_fn):
        """``unit`` is part of the fingerprint, so it has to be recomputed even
        though the data did not change."""
        axis = _axis(["A", "B"])
        before = axis.fingerprint
        updated = replace_fn(axis, unit="label")
        assert updated.fingerprint != before

    def test_axisarray_replace_is_unaffected(self, replace_fn):
        axis = _axis(["A", "B"])
        _ = axis.fingerprint
        msg = AxisArray(
            np.zeros((4, 2)),
            dims=["time", "ch"],
            axes={"time": AxisArray.TimeAxis(fs=10.0), "ch": axis},
            key="k",
        )
        updated = replace_fn(msg, data=np.ones((4, 2)))
        assert updated.key == "k"
        # The axis object is passed through by reference, cache intact.
        assert updated.axes["ch"] is axis
        assert updated.axes["ch"].fingerprint == axis.fingerprint
