import asyncio

import pytest

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.graphmeta import GraphMetadata, GraphSnapshot
from ezmsg.core.graphserver import GraphService


def _edge_exists(snapshot: GraphSnapshot, from_topic: str, to_topic: str) -> bool:
    return to_topic in snapshot.graph.get(from_topic, [])


@pytest.mark.asyncio
async def test_session_drop_cleans_owned_edges():
    graph_server = GraphService().create_server()
    address = graph_server.address

    owner = GraphContext(address, auto_start=False)
    observer = GraphContext(address, auto_start=False)

    await owner.__aenter__()
    await observer.__aenter__()

    try:
        await owner.connect("SRC", "DST")

        snapshot = await observer.snapshot()
        assert _edge_exists(snapshot, "SRC", "DST")

        await owner._close_session()
        await asyncio.sleep(0.05)

        snapshot = await observer.snapshot()
        assert not _edge_exists(snapshot, "SRC", "DST")
    finally:
        await owner.__aexit__(None, None, None)
        await observer.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_shared_edge_survives_until_last_session_drops():
    graph_server = GraphService().create_server()
    address = graph_server.address

    owner_a = GraphContext(address, auto_start=False)
    owner_b = GraphContext(address, auto_start=False)

    await owner_a.__aenter__()
    await owner_b.__aenter__()

    try:
        await owner_a.connect("SRC", "DST")
        await owner_b.connect("SRC", "DST")

        await owner_a._close_session()
        await asyncio.sleep(0.05)

        snapshot = await owner_b.snapshot()
        assert _edge_exists(snapshot, "SRC", "DST")

        await owner_b._close_session()
        await asyncio.sleep(0.05)

        dag = await GraphService(address).dag()
        assert "DST" not in dag.graph.get("SRC", set())
    finally:
        await owner_a.__aexit__(None, None, None)
        await owner_b.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_session_metadata_drops_with_session():
    graph_server = GraphService().create_server()
    address = graph_server.address

    owner = GraphContext(address, auto_start=False)
    observer = GraphContext(address, auto_start=False)

    await owner.__aenter__()
    await observer.__aenter__()

    try:
        metadata = GraphMetadata(
            schema_version=1,
            root_name="TEST",
            components={},
        )
        await owner.register_metadata(metadata)

        owner_session_id = str(owner._session_id)
        snapshot = await observer.snapshot()
        assert owner_session_id in snapshot.sessions
        assert snapshot.sessions[owner_session_id].metadata == metadata

        await owner._close_session()
        await asyncio.sleep(0.05)

        snapshot = await observer.snapshot()
        assert owner_session_id not in snapshot.sessions
    finally:
        await owner.__aexit__(None, None, None)
        await observer.__aexit__(None, None, None)
        graph_server.stop()
