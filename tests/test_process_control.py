import asyncio
import pickle

import pytest

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.graphmeta import ProcessRegistration
from ezmsg.core.processclient import ProcessControlClient
from ezmsg.core.graphserver import GraphService
from ezmsg.core.netprotocol import Command, close_stream_writer, read_str, uint64_to_bytes


@pytest.mark.asyncio
async def test_process_registration_visible_in_snapshot():
    graph_server = GraphService().create_server()
    address = graph_server.address

    observer = GraphContext(address, auto_start=False)
    await observer.__aenter__()

    process = ProcessControlClient(address, process_id="proc-A")
    await process.connect()

    try:
        await process.register(["SYS/U1", "SYS/U2"])
        await process.update_ownership(added_units=["SYS/U3"], removed_units=["SYS/U1"])

        snapshot = await observer.snapshot()
        assert len(snapshot.processes) == 1

        process_entry = next(iter(snapshot.processes.values()))
        assert process_entry.process_id == "proc-A"
        assert process_entry.pid is not None
        assert process_entry.host is not None
        assert process_entry.units == ["SYS/U2", "SYS/U3"]

    finally:
        await process.close()
        await asyncio.sleep(0.05)
        await observer.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_snapshot_entry_drops_on_disconnect():
    graph_server = GraphService().create_server()
    address = graph_server.address

    observer = GraphContext(address, auto_start=False)
    await observer.__aenter__()

    process = ProcessControlClient(address, process_id="proc-B")
    await process.connect()

    try:
        await process.register(["SYS/U1"])
        snapshot = await observer.snapshot()
        assert len(snapshot.processes) == 1

        await process.close()
        await asyncio.sleep(0.05)

        snapshot = await observer.snapshot()
        assert len(snapshot.processes) == 0

    finally:
        await process.close()
        await observer.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "command",
    [
        Command.PROCESS_REGISTER,
        Command.PROCESS_UPDATE_OWNERSHIP,
        Command.PROCESS_SETTINGS_UPDATE,
    ],
)
async def test_process_payload_parse_failures_return_error_ack(command: Command):
    graph_server = GraphService().create_server()
    address = graph_server.address

    reader, writer = await GraphService(address).open_connection()
    try:
        writer.write(Command.PROCESS.value)
        await writer.drain()
        _client_id = await read_str(reader)
        response = await reader.read(1)
        assert response == Command.COMPLETE.value

        # Non-pickled bytes intentionally trigger parse failure in process handlers.
        bad_payload = b"not-a-pickle-payload"
        writer.write(command.value)
        writer.write(uint64_to_bytes(len(bad_payload)))
        writer.write(bad_payload)
        await writer.drain()

        response = await reader.read(1)
        assert response == Command.ERROR.value
    finally:
        await close_stream_writer(writer)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_register_succeeds_after_error_ack():
    graph_server = GraphService().create_server()
    address = graph_server.address

    reader, writer = await GraphService(address).open_connection()
    try:
        writer.write(Command.PROCESS.value)
        await writer.drain()
        _client_id = await read_str(reader)
        response = await reader.read(1)
        assert response == Command.COMPLETE.value

        bad_payload = b"not-a-pickle-payload"
        writer.write(Command.PROCESS_REGISTER.value)
        writer.write(uint64_to_bytes(len(bad_payload)))
        writer.write(bad_payload)
        await writer.drain()
        response = await reader.read(1)
        assert response == Command.ERROR.value

        good_payload = pickle.dumps(
            ProcessRegistration(
                process_id="proc-after-error",
                pid=123,
                host="test-host",
                units=["SYS/U1"],
            )
        )
        writer.write(Command.PROCESS_REGISTER.value)
        writer.write(uint64_to_bytes(len(good_payload)))
        writer.write(good_payload)
        await writer.drain()
        response = await reader.read(1)
        assert response == Command.COMPLETE.value
    finally:
        await close_stream_writer(writer)
        graph_server.stop()
