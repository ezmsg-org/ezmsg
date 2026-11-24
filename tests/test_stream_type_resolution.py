from typing import Generic, TypeVar

import pytest

import ezmsg.core as ez
from ezmsg.core.backend import ExecutionContext, GraphRunner
from ezmsg.core.type_resolution import resolve_stream_type

T = TypeVar("T")
U = TypeVar("U")
S = TypeVar("S")


class GenericUnit(ez.Unit, Generic[S, T, U]):
    INPUT_SETTINGS = ez.InputStream(S)
    INPUT = ez.InputStream(T)
    OUTPUT = ez.OutputStream(U)


class Intermediate(GenericUnit[ez.Settings, T, list[T]], Generic[T]):
    pass


class Concrete(Intermediate[int]):
    pass


def test_specialized_stream_and_settings_metadata():
    unit = Concrete()
    ExecutionContext.setup({"UNIT": unit})
    metadata = GraphRunner(components={"UNIT": unit})._component_metadata().components["UNIT"]
    assert metadata.streams["INPUT"].msg_type == "builtins.int"
    assert metadata.streams["OUTPUT"].msg_type == "list[int]"
    assert metadata.dynamic_settings.settings_type == "ezmsg.core.settings.Settings"
    assert GenericUnit.__streams__["INPUT"].msg_type is T
    assert unit.INPUT.msg_type is T  # Metadata does not mutate stream declarations.


def test_unspecialized_variables_remain_unresolved():
    assert resolve_stream_type(GenericUnit, T) is T


def test_independent_specializations():
    class Text(Intermediate[str]):
        pass

    assert resolve_stream_type(Text, U) == list[str]
    assert resolve_stream_type(Concrete, U) == list[int]


@pytest.mark.parametrize(
    "annotation, expected",
    [
        (int, "builtins.int"),
        (list, "builtins.list"),
        (list[int], "list[int]"),
        (dict[str, list[int]], "dict[str, list[int]]"),
        (tuple[int, str], "tuple[int, str]"),
    ],
)
def test_parameterized_stream_metadata_preserves_arguments(annotation, expected):
    class TypedUnit(ez.Unit):
        OUTPUT = ez.OutputStream(annotation)

    unit = TypedUnit()
    ExecutionContext.setup({"UNIT": unit})
    metadata = GraphRunner(components={"UNIT": unit})._component_metadata().components["UNIT"]
    assert metadata.streams["OUTPUT"].msg_type == expected
