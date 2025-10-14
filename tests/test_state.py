import asyncio
import pytest
from contextlib import nullcontext as does_not_raise

from dataclasses import field

import ezmsg.core as ez


class ToyState(ez.State):
    integer: int
    string: str
    opt_integer_default: int | None = field(default=None)
    list_default_factory: list = field(default_factory=list)


class ToySettings(ez.Settings):
    override_integer: int | None = None
    override_string: str | None = None
    override_opt_integer_default: int | None = None
    override_list_default_factory: list | None = None


class ToyUnit(ez.Unit):
    STATE = ToyState
    SETTINGS = ToySettings

    async def initialize(self) -> None:
        if self.SETTINGS.override_integer is not None:
            self.STATE.integer = self.SETTINGS.override_integer
        if self.SETTINGS.override_string is not None:
            self.STATE.string = self.SETTINGS.override_string
        if self.SETTINGS.override_opt_integer_default is not None:
            self.STATE.opt_integer_default = self.SETTINGS.override_opt_integer_default
        if self.SETTINGS.override_list_default_factory is not None:
            self.STATE.list_default_factory = (
                self.SETTINGS.override_list_default_factory
            )


@pytest.mark.parametrize("override_integer", [None, 2])
@pytest.mark.parametrize("override_string", [None, "test"])
@pytest.mark.parametrize("override_opt_integer_default", [None, 3])
@pytest.mark.parametrize("override_list_default_factory", [None, ["a", "b"]])
def test_state_configurations(
    override_integer: int | None,
    override_string: str | None,
    override_opt_integer_default: int | None,
    override_list_default_factory: list | None,
):
    ctx = does_not_raise()
    if override_integer is None or override_string is None:
        ctx = pytest.raises(AttributeError)

    with ctx:
        settings = ToySettings(
            override_integer=override_integer,
            override_string=override_string,
            override_opt_integer_default=override_opt_integer_default,
            override_list_default_factory=override_list_default_factory,
        )
        unit = ToyUnit(settings)
        unit._set_name()
        unit._set_location()
        asyncio.run(unit.setup())


if __name__ == "__main__":
    test_state_configurations(
        override_integer=None,
        override_string=None,
        override_opt_integer_default=None,
        override_list_default_factory=None,
    )
