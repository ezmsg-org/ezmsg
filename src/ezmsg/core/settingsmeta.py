from __future__ import annotations

from dataclasses import MISSING, asdict, fields as dataclass_fields, is_dataclass
import enum
from collections.abc import Mapping
from typing import Any, get_args, get_origin

from .graphmeta import SettingsFieldMetadata, SettingsSchemaMetadata


def _type_name(tp: object) -> str:
    if isinstance(tp, type):
        return f"{tp.__module__}.{tp.__qualname__}"
    return str(tp)


def _sanitize(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, enum.Enum):
        return _sanitize(value.value)
    if isinstance(value, Mapping):
        return {str(key): _sanitize(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_sanitize(val) for val in value]
    if is_dataclass(value):
        try:
            return _sanitize(asdict(value))
        except Exception:
            return repr(value)
    return repr(value)


def settings_structured_value(value: object) -> dict[str, Any] | None:
    if value is None:
        return None

    if is_dataclass(value):
        try:
            asdict_value = asdict(value)
            if isinstance(asdict_value, dict):
                return _sanitize(asdict_value)
        except Exception:
            pass

    if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
        try:
            dumped = value.model_dump()  # type: ignore[attr-defined]
            if isinstance(dumped, dict):
                return _sanitize(dumped)
        except Exception:
            pass

    if hasattr(value, "dict") and callable(getattr(value, "dict")):
        try:
            dumped = value.dict()  # type: ignore[attr-defined]
            if isinstance(dumped, dict):
                return _sanitize(dumped)
        except Exception:
            pass

    if isinstance(value, Mapping):
        return _sanitize(dict(value))

    if hasattr(value, "param"):
        param_ns = getattr(value, "param")
        if hasattr(param_ns, "values") and callable(param_ns.values):
            try:
                values = param_ns.values()
                if isinstance(values, dict):
                    return _sanitize(values)
            except Exception:
                pass

    return None


def settings_repr_value(value: object) -> dict[str, Any] | str:
    structured = settings_structured_value(value)
    if structured is not None:
        return structured
    return repr(value)


def _widget_hint(
    *,
    field_type: str,
    choices: list[Any] | None,
    bounds: tuple[float | None, float | None] | None,
) -> str | None:
    field_type_lower = field_type.lower()
    if choices:
        return "select"
    if "bool" in field_type_lower:
        return "checkbox"
    if bounds is not None and ("int" in field_type_lower or "float" in field_type_lower):
        return "slider"
    if "int" in field_type_lower:
        return "int_input"
    if "float" in field_type_lower:
        return "float_input"
    if "str" in field_type_lower:
        return "text_input"
    return None


def _choices_from_annotation(annotation: Any) -> list[Any] | None:
    origin = get_origin(annotation)
    if origin is None:
        return None
    origin_name = getattr(origin, "__name__", str(origin))
    if origin_name != "Literal":
        return None
    return [_sanitize(val) for val in get_args(annotation)]


def _extract_bounds(obj: object) -> tuple[float | None, float | None] | None:
    lower = None
    upper = None
    for attr in ("ge", "gt", "min_length"):
        if hasattr(obj, attr):
            bound_val = getattr(obj, attr)
            if isinstance(bound_val, (int, float)):
                lower = float(bound_val)
                break
    for attr in ("le", "lt", "max_length"):
        if hasattr(obj, attr):
            bound_val = getattr(obj, attr)
            if isinstance(bound_val, (int, float)):
                upper = float(bound_val)
                break
    if lower is None and upper is None:
        return None
    return (lower, upper)


def settings_schema_from_type(settings_type: object) -> SettingsSchemaMetadata | None:
    if not isinstance(settings_type, type):
        return None

    if is_dataclass(settings_type):
        fields: list[SettingsFieldMetadata] = []
        for f in dataclass_fields(settings_type):
            required = f.default is MISSING and f.default_factory is MISSING
            default_val: Any | None = None
            if not required:
                if f.default is not MISSING:
                    default_val = _sanitize(f.default)
                elif f.default_factory is not MISSING:
                    try:
                        default_val = _sanitize(f.default_factory())
                    except Exception:
                        default_val = "<factory>"
            metadata = f.metadata if isinstance(f.metadata, Mapping) else {}
            description = metadata.get("description")
            choices = metadata.get("choices")
            if isinstance(choices, (list, tuple, set)):
                choices = [_sanitize(val) for val in choices]
            else:
                choices = _choices_from_annotation(f.type)
            bounds = None
            ge = metadata.get("ge", metadata.get("min"))
            le = metadata.get("le", metadata.get("max"))
            if isinstance(ge, (int, float)) or isinstance(le, (int, float)):
                bounds = (
                    float(ge) if isinstance(ge, (int, float)) else None,
                    float(le) if isinstance(le, (int, float)) else None,
                )
            field_type = _type_name(f.type)
            fields.append(
                SettingsFieldMetadata(
                    name=f.name,
                    field_type=field_type,
                    required=required,
                    default=default_val,
                    description=description if isinstance(description, str) else None,
                    bounds=bounds,
                    choices=choices if isinstance(choices, list) else None,
                    widget_hint=_widget_hint(
                        field_type=field_type,
                        choices=choices if isinstance(choices, list) else None,
                        bounds=bounds,
                    ),
                )
            )
        return SettingsSchemaMetadata(
            provider="dataclass",
            settings_type=_type_name(settings_type),
            fields=fields,
        )

    if hasattr(settings_type, "model_fields"):
        model_fields = getattr(settings_type, "model_fields")
        if isinstance(model_fields, dict):
            fields: list[SettingsFieldMetadata] = []
            for name, field_info in model_fields.items():
                annotation = getattr(field_info, "annotation", Any)
                is_required_attr = getattr(field_info, "is_required", None)
                required = (
                    bool(is_required_attr())
                    if callable(is_required_attr)
                    else bool(is_required_attr)
                )
                default_val = None
                if not required:
                    default = getattr(field_info, "default", None)
                    default_val = _sanitize(default)
                description = getattr(field_info, "description", None)
                choices = _choices_from_annotation(annotation)
                bounds = _extract_bounds(field_info)
                field_type = _type_name(annotation)
                fields.append(
                    SettingsFieldMetadata(
                        name=name,
                        field_type=field_type,
                        required=required,
                        default=default_val,
                        description=description if isinstance(description, str) else None,
                        bounds=bounds,
                        choices=choices,
                        widget_hint=_widget_hint(
                            field_type=field_type, choices=choices, bounds=bounds
                        ),
                    )
                )
            return SettingsSchemaMetadata(
                provider="pydantic",
                settings_type=_type_name(settings_type),
                fields=fields,
            )

    if hasattr(settings_type, "__fields__"):
        model_fields = getattr(settings_type, "__fields__")
        if isinstance(model_fields, dict):
            fields: list[SettingsFieldMetadata] = []
            for name, field_info in model_fields.items():
                annotation = getattr(field_info, "outer_type_", Any)
                required = bool(getattr(field_info, "required", False))
                default_val = None if required else _sanitize(getattr(field_info, "default", None))
                fi = getattr(field_info, "field_info", None)
                description = getattr(fi, "description", None) if fi is not None else None
                choices = _choices_from_annotation(annotation)
                bounds = _extract_bounds(fi if fi is not None else field_info)
                field_type = _type_name(annotation)
                fields.append(
                    SettingsFieldMetadata(
                        name=name,
                        field_type=field_type,
                        required=required,
                        default=default_val,
                        description=description if isinstance(description, str) else None,
                        bounds=bounds,
                        choices=choices,
                        widget_hint=_widget_hint(
                            field_type=field_type, choices=choices, bounds=bounds
                        ),
                    )
                )
            return SettingsSchemaMetadata(
                provider="pydantic",
                settings_type=_type_name(settings_type),
                fields=fields,
            )

    param_ns = getattr(settings_type, "param", None)
    if param_ns is not None and hasattr(param_ns, "objects"):
        try:
            objects = param_ns.objects("existing")
        except Exception:
            try:
                objects = param_ns.objects()
            except Exception:
                objects = None
        if isinstance(objects, dict):
            fields: list[SettingsFieldMetadata] = []
            for name, param_obj in objects.items():
                if name == "name":
                    continue
                choices_obj = getattr(param_obj, "objects", None)
                choices = None
                if isinstance(choices_obj, Mapping):
                    choices = [_sanitize(choice) for choice in choices_obj.keys()]
                elif isinstance(choices_obj, (list, tuple, set)):
                    choices = [_sanitize(choice) for choice in choices_obj]
                bounds_obj = getattr(param_obj, "bounds", None)
                bounds = None
                if (
                    isinstance(bounds_obj, tuple)
                    and len(bounds_obj) == 2
                    and all(
                        bound is None or isinstance(bound, (int, float))
                        for bound in bounds_obj
                    )
                ):
                    bounds = (
                        float(bounds_obj[0]) if isinstance(bounds_obj[0], (int, float)) else None,
                        float(bounds_obj[1]) if isinstance(bounds_obj[1], (int, float)) else None,
                    )
                default_val = _sanitize(getattr(param_obj, "default", None))
                description = getattr(param_obj, "doc", None)
                field_type = _type_name(type(param_obj))
                fields.append(
                    SettingsFieldMetadata(
                        name=name,
                        field_type=field_type,
                        required=False,
                        default=default_val,
                        description=description if isinstance(description, str) else None,
                        bounds=bounds,
                        choices=choices,
                        widget_hint=_widget_hint(
                            field_type=field_type, choices=choices, bounds=bounds
                        ),
                    )
                )
            return SettingsSchemaMetadata(
                provider="param",
                settings_type=_type_name(settings_type),
                fields=fields,
            )

    return None


def settings_schema_from_value(value: object) -> SettingsSchemaMetadata | None:
    if value is None:
        return None
    return settings_schema_from_type(type(value))

