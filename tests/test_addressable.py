import pytest

from ezmsg.core.addressable import Addressable


@pytest.fixture
def my_addressable():
    """Returns a Addressable instance"""
    return Addressable()


@pytest.mark.parametrize(
    "location, name",
    [
        (["node", "topic"], "preston"),
    ],
)
def test_address(my_addressable, location, name):
    my_addressable._set_name(name)
    my_addressable._set_location(location)
    assert my_addressable.address == f"node/topic/{name}"


def test_name(my_addressable):
    with pytest.raises(AssertionError):
        my_addressable.name


def test_location(my_addressable):
    with pytest.raises(AssertionError):
        my_addressable.location
