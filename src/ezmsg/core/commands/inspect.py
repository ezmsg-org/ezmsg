"""``ezmsg inspect`` — describe an app's components without running it.

Everything reported here is read from CLASS-level state the component
metaclasses already maintain (``__streams__``, ``__settings_type__``,
``__components__``), so nothing is instantiated, no settings are required,
and no graph connection is involved: the target module is imported and its
component classes are described as declared. Because a child's attribute
name is exactly the segment ezmsg uses for its runtime address, the
``path`` reported for each nested component matches the address it will
register under at run time (below whatever root name ``ez.run`` is given —
the class name stands in for that root segment here).

Two things are invisible to a static description, by design: components a
Collection creates dynamically in ``configure()``, and stream addresses
(assigned at graph build). Settings schemas ride along in both the
field-list form and, when the ``schema`` extra is installed, the standard
JSON Schema — the same payload the graph metadata carries for a running
app, so consumers can share one contract for "at rest" and "live".
"""

import argparse
import dataclasses
import importlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

from ..collection import Collection
from ..component import Component
from ..settingsmeta import settings_schema_from_type
from ..stream import InputStream

INSPECT_SCHEMA_VERSION = 1


def _type_name(tp: object) -> str:
    if isinstance(tp, type):
        return f"{tp.__module__}.{tp.__qualname__}"
    return str(tp)


def _load_module(source: str) -> Any:
    """Import ``source`` as a file path or a dotted module name."""
    path = Path(source).expanduser()
    if path.suffix == ".py" or path.exists():
        spec = importlib.util.spec_from_file_location(path.stem, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load a module from {source!r}")
        module = importlib.util.module_from_spec(spec)
        # Registered so dataclass/typing resolution that imports the module
        # by name (get_type_hints, pydantic) can find it.
        sys.modules[path.stem] = module
        spec.loader.exec_module(module)
        return module
    return importlib.import_module(source)


def _streams_description(component_cls: type) -> list[dict[str, Any]]:
    streams = getattr(component_cls, "__streams__", {})
    return [
        {
            "name": name,
            "kind": type(stream).__name__,
            "msg_type": _type_name(getattr(stream, "msg_type", None)),
        }
        for name, stream in streams.items()
    ]


def _component_description(name: str, path: str, component_cls: type) -> dict[str, Any]:
    settings_type = getattr(component_cls, "__settings_type__", None)
    schema = settings_schema_from_type(settings_type) if settings_type is not None else None
    input_settings = getattr(component_cls, "__streams__", {}).get("INPUT_SETTINGS")
    description: dict[str, Any] = {
        "name": name,
        "path": path,
        "component_type": _type_name(component_cls),
        "collection": issubclass(component_cls, Collection),
        "settings_type": _type_name(settings_type) if settings_type is not None else None,
        "settings_schema": dataclasses.asdict(schema) if schema is not None else None,
        # Same rule the graph metadata applies to a running component: only
        # an InputStream inlet named INPUT_SETTINGS accepts dynamic updates.
        "dynamic_settings": isinstance(input_settings, InputStream),
        "streams": _streams_description(component_cls),
    }
    children = getattr(component_cls, "__components__", None)
    if issubclass(component_cls, Collection) and isinstance(children, dict):
        description["components"] = [
            _component_description(child_name, f"{path}/{child_name}", type(child))
            for child_name, child in children.items()
        ]
    return description


def _root_component_classes(module: Any, component_name: str | None) -> list[type]:
    if component_name is not None:
        target = getattr(module, component_name, None)
        if not (isinstance(target, type) and issubclass(target, Component)):
            raise SystemExit(
                f"{component_name!r} is not an ezmsg Component class in {module.__name__!r}"
            )
        return [target]
    roots = [
        value
        for value in vars(module).values()
        if isinstance(value, type)
        and issubclass(value, Component)
        and value.__module__ == module.__name__
    ]
    if not roots:
        raise SystemExit(f"No ezmsg Component classes are defined in {module.__name__!r}")
    return roots


def handle_inspect(args: argparse.Namespace) -> None:
    try:
        module = _load_module(args.source)
    except SystemExit:
        raise
    except BaseException as exc:
        raise SystemExit(f"Could not import {args.source!r}: {type(exc).__name__}: {exc}") from exc

    roots = _root_component_classes(module, args.component)
    result = {
        "inspect_schema_version": INSPECT_SCHEMA_VERSION,
        "source": args.source,
        "components": [
            _component_description(root.__name__, root.__name__, root) for root in roots
        ],
    }
    print(json.dumps(result, indent=args.indent))


def setup_inspect_cmdline(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "inspect",
        help="describe a module's components and settings schemas without running them",
    )
    parser.add_argument(
        "source",
        help="path to a .py file, or a dotted module name, defining ezmsg Components",
    )
    parser.add_argument(
        "--component",
        default=None,
        help="describe only this Component class (default: every Component the module defines)",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=None,
        help="pretty-print the JSON with this indent (default: one line)",
    )
    parser.set_defaults(_handler=handle_inspect)
