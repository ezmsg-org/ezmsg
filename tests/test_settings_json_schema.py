"""Settings JSON Schema metadata and authoritative field coercion.

Covers the `schema` extra's three behaviors — a standard JSON Schema riding
``SettingsSchemaMetadata``, pydantic-JSON structured snapshot values, and
validation/coercion of dynamic settings field updates in the owning process
— plus the soft-dependency contract: with pydantic stubbed out, schemas are
None and updates apply raw, exactly as before the extra existed.
"""

import enum
from dataclasses import dataclass, field

import pytest

from ezmsg.core import settingsmeta
from ezmsg.core.settingsmeta import (
    SettingsCoercionError,
    coerce_settings_field_value,
    settings_json_schema,
    settings_schema_from_type,
    settings_schema_from_value,
    settings_structured_value,
)


class Flavor(enum.Enum):
    VANILLA = "vanilla"
    MINT = "mint"


@dataclass
class SubSettings:
    depth: int = 3


@dataclass
class DemoSettings:
    gain: float = 1.0
    taps: tuple[float, ...] = ()
    weights: dict[str, float] = field(default_factory=dict)
    flavor: Flavor = Flavor.VANILLA
    label: str | None = None
    sub: SubSettings = field(default_factory=SubSettings)


class Opaque:
    pass


@dataclass
class UnmodelableSettings:
    handle: Opaque = None  # type: ignore[assignment]


class TestSchemaAttachment:
    def test_dataclass_metadata_carries_json_schema(self):
        meta = settings_schema_from_type(DemoSettings)
        assert meta is not None and meta.provider == "dataclass"
        schema = meta.json_schema
        assert schema is not None and schema["type"] == "object"
        assert schema["properties"]["gain"]["type"] == "number"
        assert schema["properties"]["taps"]["items"] == {"type": "number"}
        assert schema["$defs"]["Flavor"]["enum"] == ["vanilla", "mint"]
        assert schema["$defs"]["SubSettings"]["properties"]["depth"]["type"] == "integer"

    def test_schema_from_value_carries_it_too(self):
        meta = settings_schema_from_value(DemoSettings())
        assert meta is not None and meta.json_schema is not None

    def test_unmodelable_type_keeps_fields_but_no_json_schema(self):
        meta = settings_schema_from_type(UnmodelableSettings)
        assert meta is not None
        assert [f.name for f in meta.fields] == ["handle"]
        assert meta.json_schema is None
        assert settings_json_schema(UnmodelableSettings) is None

    def test_without_pydantic_schema_is_none(self, monkeypatch):
        monkeypatch.setattr(settingsmeta, "_TYPE_ADAPTER", None)
        assert settings_json_schema(DemoSettings) is None
        meta = settings_schema_from_type(DemoSettings)
        assert meta is not None and meta.json_schema is None


class TestStructuredValue:
    def test_pydantic_json_mode_dump(self):
        structured = settings_structured_value(DemoSettings(taps=(1.0, 2.0), flavor=Flavor.MINT))
        assert structured == {
            "gain": 1.0,
            "taps": [1.0, 2.0],
            "weights": {},
            "flavor": "mint",
            "label": None,
            "sub": {"depth": 3},
        }

    def test_falls_back_for_unmodelable_values(self):
        structured = settings_structured_value(UnmodelableSettings())
        # Legacy sanitize path: the opaque handle renders as repr.
        assert structured is not None and set(structured) == {"handle"}

    def test_falls_back_without_pydantic(self, monkeypatch):
        monkeypatch.setattr(settingsmeta, "_TYPE_ADAPTER", None)
        structured = settings_structured_value(DemoSettings(flavor=Flavor.MINT))
        assert structured is not None and structured["flavor"] == "mint"


class TestFieldCoercion:
    def test_int_promotes_to_declared_float(self):
        coerced = coerce_settings_field_value(DemoSettings, "gain", 2)
        assert coerced == 2.0 and isinstance(coerced, float)

    def test_list_becomes_the_declared_variadic_tuple(self):
        coerced = coerce_settings_field_value(DemoSettings, "taps", [1, 2.5])
        assert coerced == (1.0, 2.5) and isinstance(coerced, tuple)

    def test_enum_reconstructs_by_value(self):
        assert coerce_settings_field_value(DemoSettings, "flavor", "mint") is Flavor.MINT

    def test_enum_member_name_is_refused(self):
        # The wire convention is member VALUES; a name is a refusal the
        # caller sees, not a silent raw publish.
        with pytest.raises(SettingsCoercionError):
            coerce_settings_field_value(DemoSettings, "flavor", "MINT")

    def test_nested_dataclass_path(self):
        assert coerce_settings_field_value(DemoSettings, "sub.depth", "4") == 4

    def test_dict_value_path(self):
        coerced = coerce_settings_field_value(DemoSettings, "weights.alpha", "0.5")
        assert coerced == 0.5 and isinstance(coerced, float)

    def test_refused_value_raises_with_field_path(self):
        with pytest.raises(SettingsCoercionError, match="'gain'"):
            coerce_settings_field_value(DemoSettings, "gain", "loud")

    def test_unknown_field_passes_raw(self):
        assert coerce_settings_field_value(DemoSettings, "nope", "x") == "x"

    def test_unresolvable_annotation_passes_raw(self):
        assert coerce_settings_field_value(UnmodelableSettings, "handle", "x") == "x"

    def test_without_pydantic_everything_passes_raw(self, monkeypatch):
        monkeypatch.setattr(settingsmeta, "_TYPE_ADAPTER", None)
        assert coerce_settings_field_value(DemoSettings, "gain", "loud") == "loud"
