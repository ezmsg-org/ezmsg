"""``ezmsg inspect``: static component/settings description, no run needed."""

import json

import pytest

from ezmsg.core.command import cmdline

DEMO_MODULE = '''
import ezmsg.core as ez
from ezmsg.util.messages.axisarray import AxisArray


class GainSettings(ez.Settings):
    gain: float = 1.0
    label: str = ""


class GainUnit(ez.Unit):
    SETTINGS = GainSettings

    INPUT_SETTINGS = ez.InputStream(GainSettings)
    INPUT_SIGNAL = ez.InputStream(AxisArray)
    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: GainSettings) -> None:
        self.apply_settings(msg)


class TopicalSystem(ez.Collection):
    # An InputTopic named INPUT_SETTINGS: discoverable, but NOT dynamic
    # settings — a topic is not an InputStream (the SpikeRateFromRaw shape).
    SETTINGS = GainSettings

    INPUT_SETTINGS = ez.InputTopic(GainSettings)

    GAIN = GainUnit()

    def network(self) -> ez.NetworkDefinition:
        return ()


class DemoSystem(ez.Collection):
    SETTINGS = GainSettings

    GAIN = GainUnit()

    def network(self) -> ez.NetworkDefinition:
        return ()
'''


@pytest.fixture
def demo_module_path(tmp_path):
    path = tmp_path / "inspect_demo_system.py"
    path.write_text(DEMO_MODULE)
    return path


def _inspect(capsys, *argv):
    cmdline(argv=["inspect", *argv])
    return json.loads(capsys.readouterr().out)


def _by_name(components, name):
    return next(component for component in components if component["name"] == name)


class TestInspectCommand:
    def test_describes_every_component_class_in_the_module(self, capsys, demo_module_path):
        result = _inspect(capsys, str(demo_module_path))
        assert result["inspect_schema_version"] == 1
        assert {component["name"] for component in result["components"]} == {
            "GainUnit",
            "TopicalSystem",
            "DemoSystem",
        }

    def test_collection_children_carry_runtime_address_paths(self, capsys, demo_module_path):
        result = _inspect(capsys, str(demo_module_path))
        system = _by_name(result["components"], "DemoSystem")
        assert system["collection"] is True
        children = {child["name"]: child for child in system["components"]}
        assert children["GAIN"]["path"] == "DemoSystem/GAIN"
        assert children["GAIN"]["component_type"].endswith("GainUnit")
        assert children["GAIN"]["collection"] is False

    def test_dynamic_settings_requires_an_input_stream_inlet(self, capsys, demo_module_path):
        result = _inspect(capsys, str(demo_module_path))
        system = _by_name(result["components"], "DemoSystem")
        children = {child["name"]: child for child in system["components"]}
        assert children["GAIN"]["dynamic_settings"] is True
        # An InputTopic named INPUT_SETTINGS does not accept dynamic updates —
        # the same rule the running graph's metadata applies.
        topical = _by_name(result["components"], "TopicalSystem")
        assert topical["dynamic_settings"] is False

    def test_settings_schema_and_json_schema_ride_along(self, capsys, demo_module_path):
        result = _inspect(capsys, str(demo_module_path))
        unit = _by_name(result["components"], "GainUnit")
        schema = unit["settings_schema"]
        assert unit["settings_type"].endswith("GainSettings")
        assert {field["name"] for field in schema["fields"]} == {"gain", "label"}
        json_schema = schema["json_schema"]
        assert json_schema is not None
        assert json_schema["properties"]["gain"]["type"] == "number"

    def test_streams_are_listed_with_kind_and_message_type(self, capsys, demo_module_path):
        result = _inspect(capsys, str(demo_module_path))
        unit = _by_name(result["components"], "GainUnit")
        streams = {stream["name"]: stream for stream in unit["streams"]}
        assert streams["INPUT_SIGNAL"]["kind"] == "InputStream"
        assert streams["INPUT_SIGNAL"]["msg_type"].endswith("AxisArray")
        assert streams["OUTPUT_SIGNAL"]["kind"] == "OutputStream"

    def test_component_flag_selects_one_root(self, capsys, demo_module_path):
        result = _inspect(capsys, str(demo_module_path), "--component", "DemoSystem")
        assert [component["name"] for component in result["components"]] == ["DemoSystem"]

    def test_unknown_component_name_is_a_clear_error(self, demo_module_path):
        with pytest.raises(SystemExit, match="Nope"):
            cmdline(argv=["inspect", str(demo_module_path), "--component", "Nope"])

    def test_module_without_components_is_a_clear_error(self):
        with pytest.raises(SystemExit, match="No ezmsg Component classes"):
            cmdline(argv=["inspect", "json"])

    def test_broken_module_reports_the_import_failure(self, tmp_path):
        path = tmp_path / "broken_module.py"
        path.write_text("this is not python(\n")
        with pytest.raises(SystemExit, match="Could not import"):
            cmdline(argv=["inspect", str(path)])
