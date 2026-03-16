import asyncio

import pytest

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.graphmeta import TopologyChangedEvent, TopologyEventType
from ezmsg.core.graphserver import GraphService
from ezmsg.core.processclient import ProcessControlClient


async def _next_matching_event(
    stream, predicate, timeout: float = 1.0
) -> TopologyChangedEvent:
    async def _wait() -> TopologyChangedEvent:
        while True:
            event = await anext(stream)
            if predicate(event):
                return event

    return await asyncio.wait_for(_wait(), timeout=timeout)


@pytest.mark.asyncio
async def test_topology_subscription_reports_session_edge_changes():
    graph_server = GraphService().create_server()
    address = graph_server.address

    owner = GraphContext(address, auto_start=False)
    observer = GraphContext(address, auto_start=False)

    await owner.__aenter__()
    await observer.__aenter__()

    stream = observer.subscribe_topology_events(after_seq=0)
    try:
        await owner.connect("SRC", "DST")
        event = await _next_matching_event(
            stream,
            lambda e: (
                e.event_type == TopologyEventType.GRAPH_CHANGED
                and "DST" in e.changed_topics
            ),
            timeout=1.0,
        )
        assert event.source_session_id == str(owner._session_id)

        await owner.disconnect("SRC", "DST")
        event = await _next_matching_event(
            stream,
            lambda e: (
                e.event_type == TopologyEventType.GRAPH_CHANGED
                and "DST" in e.changed_topics
            ),
            timeout=1.0,
        )
        assert event.source_session_id == str(owner._session_id)
    finally:
        await stream.aclose()
        await owner.__aexit__(None, None, None)
        await observer.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_topology_subscription_reports_process_changes():
    graph_server = GraphService().create_server()
    address = graph_server.address

    observer = GraphContext(address, auto_start=False)
    await observer.__aenter__()

    process = ProcessControlClient(address, process_id="proc-topology")
    stream = observer.subscribe_topology_events(after_seq=0)

    try:
        await process.connect()
        await process.register(["SYS/U1"])

        registered = await _next_matching_event(
            stream,
            lambda e: (
                e.event_type == TopologyEventType.PROCESS_CHANGED
                and e.source_process_id == "proc-topology"
            ),
            timeout=1.0,
        )
        assert registered.source_session_id is None

        await process.update_ownership(added_units=["SYS/U2"], removed_units=["SYS/U1"])
        updated = await _next_matching_event(
            stream,
            lambda e: (
                e.event_type == TopologyEventType.PROCESS_CHANGED
                and e.source_process_id == "proc-topology"
            ),
            timeout=1.0,
        )
        assert updated.source_session_id is None
    finally:
        await stream.aclose()
        await process.close()
        await observer.__aexit__(None, None, None)
        graph_server.stop()
