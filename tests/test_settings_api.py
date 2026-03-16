import asyncio
import pickle
import time
from dataclasses import dataclass

import pytest

import ezmsg.core as ez
from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.graphmeta import (
    ComponentMetadata,
    DynamicSettingsMetadata,
    GraphMetadata,
    ProcessControlErrorCode,
    ProcessControlResponse,
    SettingsFieldUpdateRequest,
    SettingsEventType,
    SettingsSnapshotValue,
)
from ezmsg.core.graphserver import GraphService
from ezmsg.core.processclient import ProcessControlClient


def _metadata_with_component(component_address: str) -> GraphMetadata:
    return GraphMetadata(
        schema_version=1,
        root_name="SYS",
        components={
            component_address: ComponentMetadata(
                address=component_address,
                name="UNIT",
                component_type="example.Unit",
                settings_type="example.Settings",
                initial_settings=(None, {"alpha": 1}),
                dynamic_settings=DynamicSettingsMetadata(
                    enabled=True,
                    input_topic=f"{component_address}/INPUT_SETTINGS",
                    settings_type="example.Settings",
                ),
                settings_schema=None,
            )
        },
    )


@pytest.mark.asyncio
async def test_settings_snapshot_and_events_from_metadata_registration():
    graph_server = GraphService().create_server()
    address = graph_server.address

    owner = GraphContext(address, auto_start=False)
    observer = GraphContext(address, auto_start=False)

    await owner.__aenter__()
    await observer.__aenter__()

    try:
        component_address = "SYS/UNIT_A"
        await owner.register_metadata(_metadata_with_component(component_address))

        settings = await observer.settings_snapshot()
        assert component_address in settings
        assert settings[component_address].repr_value == {"alpha": 1}
        assert settings[component_address].structured_value == {"alpha": 1}
        assert settings[component_address].settings_schema is None

        events = await observer.settings_events(after_seq=0)
        matching = [
            event
            for event in events
            if event.component_address == component_address
            and event.event_type == SettingsEventType.INITIAL_SETTINGS
        ]
        assert matching
        assert matching[-1].source_session_id == str(owner._session_id)
    finally:
        await owner.__aexit__(None, None, None)
        await observer.__aexit__(None, None, None)
        graph_server.stop()


@dataclass
class _SettingsMsg:
    gain: int


class _SettingsSource(ez.Unit):
    OUTPUT = ez.OutputStream(_SettingsMsg)

    @ez.publisher(OUTPUT)
    async def emit(self):
        yield self.OUTPUT, _SettingsMsg(gain=7)
        raise ez.Complete


class _SettingsSink(ez.Unit):
    INPUT_SETTINGS = ez.InputStream(_SettingsMsg)

    @ez.subscriber(INPUT_SETTINGS)
    async def on_settings(self, msg: _SettingsMsg) -> None:
        raise ez.NormalTermination


class _SettingsSystem(ez.Collection):
    SRC = _SettingsSource()
    SINK = _SettingsSink()

    def network(self) -> ez.NetworkDefinition:
        return ((self.SRC.OUTPUT, self.SINK.INPUT_SETTINGS),)


class _SettingsOnlySystem(ez.Collection):
    SINK = _SettingsSink()

    def network(self) -> ez.NetworkDefinition:
        return ()


def test_input_settings_hook_reports_to_graphserver():
    graph_server = GraphService().create_server()
    address = graph_server.address
    try:
        ez.run(components={"SYS": _SettingsSystem()}, graph_address=address, force_single_process=True)

        async def observe() -> None:
            observer = GraphContext(address, auto_start=False)
            await observer.__aenter__()
            try:
                settings = await observer.settings_snapshot()
                sink_address = "SYS/SINK"
                assert sink_address in settings
                assert settings[sink_address].repr_value == {"gain": 7}
                assert settings[sink_address].structured_value == {"gain": 7}
                assert settings[sink_address].settings_schema is not None
                schema = settings[sink_address].settings_schema
                assert schema is not None
                assert schema.provider == "dataclass"
                assert any(
                    field.name == "gain" and "int" in field.field_type.lower()
                    for field in schema.fields
                )

                events = await observer.settings_events(after_seq=0)
                matching = [
                    event
                    for event in events
                    if event.component_address == sink_address
                    and event.event_type == SettingsEventType.SETTINGS_UPDATED
                ]
                assert matching
            finally:
                await observer.__aexit__(None, None, None)

        asyncio.run(observe())
    finally:
        graph_server.stop()


@pytest.mark.asyncio
async def test_graphcontext_update_settings_via_input_settings_topic():
    graph_server = GraphService().create_server()
    address = graph_server.address

    observer = GraphContext(address, auto_start=False)
    await observer.__aenter__()

    run_task = asyncio.create_task(
        asyncio.to_thread(
            ez.run,
            components={"SYS": _SettingsOnlySystem()},
            graph_address=address,
            force_single_process=True,
        )
    )

    try:
        for _ in range(40):
            try:
                await observer.settings_input_topic("SYS/SINK")
                break
            except RuntimeError:
                await asyncio.sleep(0.05)
        else:
            raise AssertionError("Timed out waiting for dynamic settings metadata")

        await observer.update_settings("SYS/SINK", _SettingsMsg(gain=11))
        await asyncio.wait_for(run_task, timeout=5.0)

        settings = await observer.settings_snapshot()
        assert settings["SYS/SINK"].repr_value == {"gain": 11}

    finally:
        if not run_task.done():
            await asyncio.wait_for(run_task, timeout=5.0)
        await observer.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_graphcontext_update_setting_field_routes_to_process():
    graph_server = GraphService().create_server()
    address = graph_server.address

    observer = GraphContext(address, auto_start=False)
    await observer.__aenter__()
    process = ProcessControlClient(address, process_id="proc-setting-patch")
    await process.connect()
    await process.register(["SYS/SINK"])

    try:
        async def handler(request) -> ProcessControlResponse:
            assert request.operation == "UPDATE_SETTING_FIELD"
            assert request.payload is not None
            update = pickle.loads(request.payload)
            assert isinstance(update, SettingsFieldUpdateRequest)
            assert update.field_path == "gain"
            assert update.value == 11
            return ProcessControlResponse(
                request_id=request.request_id,
                ok=True,
                payload=pickle.dumps(
                    SettingsSnapshotValue(serialized=None, repr_value={"gain": 11})
                ),
            )

        process.set_request_handler(handler)

        patched = await observer.update_setting("SYS/SINK", "gain", 11)
        assert patched.repr_value == {"gain": 11}
    finally:
        await process.close()
        await observer.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_graphcontext_update_setting_waits_and_propagates_process_failure():
    graph_server = GraphService().create_server()
    address = graph_server.address

    observer = GraphContext(address, auto_start=False)
    await observer.__aenter__()
    process = ProcessControlClient(address, process_id="proc-setting-fail")
    await process.connect()
    await process.register(["SYS/SINK"])

    try:
        async def handler(request) -> ProcessControlResponse:
            assert request.operation == "UPDATE_SETTING_FIELD"
            await asyncio.sleep(0.05)
            return ProcessControlResponse(
                request_id=request.request_id,
                ok=False,
                error="Simulated publish failure",
                error_code=ProcessControlErrorCode.HANDLER_ERROR,
            )

        process.set_request_handler(handler)

        start = time.perf_counter()
        with pytest.raises(RuntimeError, match="Simulated publish failure"):
            await observer.update_setting("SYS/SINK", "gain", 99, timeout=1.0)
        elapsed = time.perf_counter() - start
        assert elapsed >= 0.04
    finally:
        await process.close()
        await observer.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_reported_settings_update_visible_in_snapshot_and_events():
    graph_server = GraphService().create_server()
    address = graph_server.address

    observer = GraphContext(address, auto_start=False)
    await observer.__aenter__()

    process = ProcessControlClient(address, process_id="proc-settings")
    await process.connect()

    try:
        await process.register(["SYS/UNIT_B"])
        await process.report_settings_update(
            component_address="SYS/UNIT_B",
            value=SettingsSnapshotValue(serialized=None, repr_value={"gain": 2}),
        )

        settings = await observer.settings_snapshot()
        assert settings["SYS/UNIT_B"].repr_value == {"gain": 2}

        events = await observer.settings_events(after_seq=0)
        matching = [
            event
            for event in events
            if event.component_address == "SYS/UNIT_B"
            and event.event_type == SettingsEventType.SETTINGS_UPDATED
        ]
        assert matching
        assert matching[-1].source_process_id == "proc-settings"

        stream = observer.subscribe_settings_events(after_seq=0)
        streamed = await asyncio.wait_for(anext(stream), timeout=1.0)
        assert streamed.component_address == "SYS/UNIT_B"
        await stream.aclose()
    finally:
        await process.close()
        await observer.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_session_owned_settings_removed_when_session_drops():
    graph_server = GraphService().create_server()
    address = graph_server.address

    owner = GraphContext(address, auto_start=False)
    observer = GraphContext(address, auto_start=False)

    await owner.__aenter__()
    await observer.__aenter__()

    try:
        component_address = "SYS/UNIT_C"
        await owner.register_metadata(_metadata_with_component(component_address))
        settings = await observer.settings_snapshot()
        assert component_address in settings

        await owner._close_session()
        await asyncio.sleep(0.05)

        settings = await observer.settings_snapshot()
        assert component_address not in settings
    finally:
        await owner.__aexit__(None, None, None)
        await observer.__aexit__(None, None, None)
        graph_server.stop()
