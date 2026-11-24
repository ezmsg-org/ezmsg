"""Resolve inherited generic annotations without changing shared stream objects."""

import types
from functools import reduce
from operator import or_
from typing import TypeVar, Union, get_args, get_origin


def _substitute(annotation, bindings):
    if isinstance(annotation, TypeVar):
        seen = set()
        while isinstance(annotation, TypeVar) and annotation in bindings and annotation not in seen:
            seen.add(annotation)
            annotation = bindings[annotation]
        return annotation
    args = get_args(annotation)
    if not args:
        return annotation
    resolved = tuple(_substitute(arg, bindings) for arg in args)
    if resolved == args:
        return annotation
    if hasattr(annotation, "copy_with"):
        return annotation.copy_with(resolved)
    origin = get_origin(annotation)
    if origin in (Union, types.UnionType):
        return reduce(or_, resolved)
    try:
        return origin[resolved[0] if len(resolved) == 1 else resolved]
    except (TypeError, AttributeError):
        return annotation


def resolve_stream_type(component_type: type, annotation):
    """Substitute explicit generic base arguments; leave unbound types unresolved."""
    candidates = {}

    def walk(cls, inherited):
        for parent in cls.__dict__.get("__orig_bases__", cls.__bases__):
            origin = get_origin(parent) or parent
            args = tuple(_substitute(arg, inherited) for arg in get_args(parent))
            bindings = {**inherited, **dict(zip(getattr(origin, "__parameters__", ()), args))}
            for variable, value in bindings.items():
                if variable != value:
                    candidates.setdefault(variable, []).append(value)
            if isinstance(origin, type) and origin is not object:
                walk(origin, bindings)

    walk(component_type, {})
    # Ambiguous multiple inheritance must not advertise an invented concrete type.
    bindings = {
        variable: values[0]
        for variable, values in candidates.items()
        if all(value == values[0] for value in values)
    }
    return _substitute(annotation, bindings)
