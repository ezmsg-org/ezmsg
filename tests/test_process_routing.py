import asyncio

import pytest

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.graphmeta import ProcessControlRequest, ProcessControlResponse
from ezmsg.core.graphserver import GraphService
from ezmsg.core.processclient import ProcessControlClient


@pytest.mark.asyncio
async def test_process_routing_roundtrip():
    graph_server = GraphService().create_server()
    address = graph_server.address

    observer = GraphContext(address, auto_start=False)
    await observer.__aenter__()

    process = ProcessControlClient(address, process_id="proc-route")
    await process.connect()
    await process.register(["SYS/U1"])

    async def handler(request: ProcessControlRequest) -> ProcessControlResponse:
        assert request.unit_address == "SYS/U1"
        assert request.operation == "ECHO"
        return ProcessControlResponse(
            request_id=request.request_id,
            ok=True,
            payload=request.payload,
        )

    process.set_request_handler(handler)

    try:
        response = await observer.process_request(
            "SYS/U1",
            "ECHO",
            payload=b"hello",
            timeout=1.0,
        )
        assert response.ok
        assert response.payload == b"hello"
        assert response.process_id == "proc-route"
    finally:
        await process.close()
        await observer.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_routing_missing_owner_returns_error():
    graph_server = GraphService().create_server()
    address = graph_server.address

    observer = GraphContext(address, auto_start=False)
    await observer.__aenter__()

    try:
        response = await observer.process_request(
            "SYS/UNKNOWN",
            "PING",
            payload=b"",
            timeout=0.25,
        )
        assert not response.ok
        assert response.error is not None
        assert "No process owns unit" in response.error
    finally:
        await observer.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_routing_timeout_returns_error():
    graph_server = GraphService().create_server()
    address = graph_server.address

    observer = GraphContext(address, auto_start=False)
    await observer.__aenter__()

    process = ProcessControlClient(address, process_id="proc-timeout")
    await process.connect()
    await process.register(["SYS/U2"])

    block = asyncio.Event()

    async def blocking_handler(_request: ProcessControlRequest) -> ProcessControlResponse:
        await block.wait()
        return ProcessControlResponse(request_id="", ok=False)

    process.set_request_handler(blocking_handler)

    try:
        response = await observer.process_request(
            "SYS/U2",
            "SLOW",
            timeout=0.05,
        )
        assert not response.ok
        assert response.error is not None
        assert "Timed out waiting for process response" in response.error
        assert response.process_id == "proc-timeout"
    finally:
        await process.close()
        await observer.__aexit__(None, None, None)
        graph_server.stop()
