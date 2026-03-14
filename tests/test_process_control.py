import asyncio

import pytest

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.processclient import ProcessControlClient
from ezmsg.core.graphserver import GraphService


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
