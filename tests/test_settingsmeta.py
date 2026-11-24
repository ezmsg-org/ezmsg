import enum
import io
import pickle
from dataclasses import dataclass, field
from typing import Any

import ezmsg.core as ez
from ezmsg.core.settingsmeta import (
    _sanitize,
    settings_repr_value,
    settings_schema_from_type,
    settings_structured_value,
)

# Exactly the types a JSON encoder accepts. Membership is checked by identity
# rather than isinstance so that a mixin enum -- whose members are instances of
# int or str -- does not pass as its mixed-in builtin.
_JSON_TYPES = (type(None), bool, int, float, str, list, dict)


def _assert_wire_safe(value: Any, path: str = "<root>") -> None:
    """Assert nothing but plain builtins survived sanitization.

    Anything else means the defining package has to be importable wherever the
    payload is unpickled, which is precisely what sanitization exists to avoid.
    """
    assert type(value) in _JSON_TYPES, (
        f"{path}: {type(value)!r} is not wire-safe ({value!r})"
    )
    if isinstance(value, dict):
        for key, val in value.items():
            assert type(key) is str, f"{path}: key {key!r} is {type(key)!r}, not str"
            _assert_wire_safe(val, f"{path}.{key}")
    elif isinstance(value, list):
        for idx, val in enumerate(value):
            _assert_wire_safe(val, f"{path}[{idx}]")


class Rate(enum.IntEnum):
    SLOW = 1
    FAST = 5


class Mode(str, enum.Enum):
    """A str-mixin enum, spelled the pre-3.11 way so this runs on 3.10.

    ``enum.StrEnum`` is 3.11+, but it is this mixin that matters here: members
    are instances of ``str`` either way.
    """

    IDLE = "idle"
    BUSY = "busy"


class Flags(enum.IntFlag):
    NONE = 0
    READ = 1
    WRITE = 2


class Plain(enum.Enum):
    X = "x"


@dataclass
class NestedSettings:
    rate: Rate = Rate.SLOW
    modes: list[Mode] = field(default_factory=lambda: [Mode.IDLE])


class MixinEnumSettings(ez.Settings):
    rate: Rate = Rate.FAST
    mode: Mode = Mode.BUSY
    flags: Flags = Flags.READ | Flags.WRITE
    plain: Plain = Plain.X
    by_rate: dict[Rate, Mode] = field(default_factory=lambda: {Rate.SLOW: Mode.IDLE})
    nested: NestedSettings = field(default_factory=NestedSettings)


def test_sanitize_unwraps_mixin_enums():
    """IntEnum/StrEnum/IntFlag members must reduce to their values, not pass through.

    They are instances of int/str, so a primitives check that runs before the
    enum check returns the member itself.
    """
    assert _sanitize(Rate.FAST) == 5
    assert type(_sanitize(Rate.FAST)) is int
    assert _sanitize(Mode.BUSY) == "busy"
    assert type(_sanitize(Mode.BUSY)) is str
    assert _sanitize(Flags.READ | Flags.WRITE) == 3
    assert type(_sanitize(Flags.READ | Flags.WRITE)) is int
    assert _sanitize(Plain.X) == "x"


def test_sanitize_unwraps_enums_in_containers():
    assert _sanitize([Rate.SLOW, Mode.IDLE]) == [1, "idle"]
    assert _sanitize(NestedSettings()) == {"rate": 1, "modes": ["idle"]}


def test_sanitize_renders_enum_keys_by_value():
    """Enum mapping keys must render the same on every supported interpreter.

    ``str()`` of an IntEnum member changed in 3.11 -- ``'Rate.SLOW'`` before,
    ``'1'` after -- so keying by ``str(key)`` alone makes the payload depend on
    the Python version the graph happens to run under.
    """
    assert _sanitize({Rate.SLOW: Mode.IDLE}) == {"1": "idle"}
    assert _sanitize({Mode.BUSY: 1}) == {"busy": 1}
    assert _sanitize({Plain.X: 1}) == {"x": 1}


def test_structured_value_is_wire_safe():
    structured = settings_structured_value(MixinEnumSettings())
    _assert_wire_safe(structured)
    assert structured == {
        "rate": 5,
        "mode": "busy",
        "flags": 3,
        "plain": "x",
        "by_rate": {"1": "idle"},
        "nested": {"rate": 1, "modes": ["idle"]},
    }


def test_repr_value_is_wire_safe():
    _assert_wire_safe(settings_repr_value(MixinEnumSettings()))


def test_schema_defaults_and_choices_are_wire_safe():
    schema = settings_schema_from_type(MixinEnumSettings)
    assert schema is not None
    defaults = {f.name: f.default for f in schema.fields}
    for name, default in defaults.items():
        _assert_wire_safe(default, f"<default {name}>")
    assert defaults["rate"] == 5
    assert defaults["mode"] == "busy"

    for f in schema.fields:
        if f.choices is not None:
            _assert_wire_safe(f.choices, f"<choices {f.name}>")


class _ObserverUnpickler(pickle.Unpickler):
    """Unpickles as an observer that has only ezmsg installed.

    Every settings payload crosses to clients that deliberately do not depend on
    the graph's own packages, so anything a payload forces them to import is a
    leak. Refusing every module but ``ezmsg`` reproduces the ``ModuleNotFoundError``
    such a client would hit, without needing a second interpreter.
    """

    def find_class(self, module: str, name: str) -> Any:
        if module != "builtins" and not module.startswith("ezmsg."):
            raise ModuleNotFoundError(f"payload requires {module}.{name}")
        return super().find_class(module, name)


def test_sanitized_payloads_need_no_package_but_ezmsg():
    settings = MixinEnumSettings()
    payloads = [
        settings_structured_value(settings),
        settings_repr_value(settings),
        settings_schema_from_type(MixinEnumSettings),
    ]
    for payload in payloads:
        _ObserverUnpickler(io.BytesIO(pickle.dumps(payload))).load()
