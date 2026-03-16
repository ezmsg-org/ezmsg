import asyncio

import pytest

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.graphmeta import (
    ProcessControlErrorCode,
    ProfilingTraceControl,
)
from ezmsg.core.graphserver import GraphService
from ezmsg.core.processclient import ProcessControlClient


@pytest.mark.asyncio
async def test_process_profiling_snapshot_collects_pub_sub_metrics():
    graph_server = GraphService().create_server()
    address = graph_server.address

    ctx = GraphContext(address, auto_start=False)
    await ctx.__aenter__()

    process = ProcessControlClient(address, process_id="proc-prof")
    await process.connect()
    await process.register(["SYS/U1"])

    pub = await ctx.publisher("TOPIC_PROF")
    sub = await ctx.subscriber("TOPIC_PROF")

    try:
        for idx in range(8):
            await pub.broadcast(idx)
            async with sub.recv_zero_copy() as _msg:
                await asyncio.sleep(0)

        snap = await ctx.process_profiling_snapshot("SYS/U1", timeout=1.0)
        assert snap.process_id == "proc-prof"
        assert snap.window_seconds > 0
        assert len(snap.publishers) >= 1
        assert len(snap.subscribers) >= 1

        pub_metrics = next(iter(snap.publishers.values()))
        assert pub_metrics.messages_published_total >= 8
        assert pub_metrics.publish_rate_hz_window >= 0.0

        sub_metrics = next(iter(snap.subscribers.values()))
        assert sub_metrics.messages_received_total >= 8
        assert sub_metrics.lease_time_ns_total > 0
        assert sub_metrics.lease_time_ns_avg_window >= 0.0
    finally:
        await process.close()
        await ctx.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_profiling_trace_control_and_batch():
    graph_server = GraphService().create_server()
    address = graph_server.address

    ctx = GraphContext(address, auto_start=False)
    await ctx.__aenter__()

    process = ProcessControlClient(address, process_id="proc-trace")
    await process.connect()
    await process.register(["SYS/U2"])

    pub = await ctx.publisher("TOPIC_TRACE")
    sub = await ctx.subscriber("TOPIC_TRACE")

    try:
        response = await ctx.process_set_profiling_trace(
            "SYS/U2",
            ProfilingTraceControl(
                enabled=True,
                sample_mod=1,
                publisher_topics=["TOPIC_TRACE"],
                subscriber_topics=["TOPIC_TRACE"],
            ),
            timeout=1.0,
        )
        assert response.ok

        for idx in range(5):
            await pub.broadcast(idx)
            async with sub.recv_zero_copy() as _msg:
                span_start_ns = sub.begin_profile()
                try:
                    await asyncio.sleep(0)
                finally:
                    sub.end_profile(span_start_ns, "taskA")

        batch = await ctx.process_profiling_trace_batch(
            "SYS/U2", max_samples=200, timeout=1.0
        )
        assert batch.process_id == "proc-trace"
        assert len(batch.samples) > 0

        disable_response = await ctx.process_set_profiling_trace(
            "SYS/U2",
            ProfilingTraceControl(enabled=False),
            timeout=1.0,
        )
        assert disable_response.ok
    finally:
        await process.close()
        await ctx.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_profiling_snapshot_all_and_unroutable_error_code():
    graph_server = GraphService().create_server()
    address = graph_server.address

    ctx = GraphContext(address, auto_start=False)
    await ctx.__aenter__()

    process = ProcessControlClient(address, process_id="proc-all")
    await process.connect()
    await process.register(["SYS/U3"])

    try:
        snapshots = await ctx.profiling_snapshot_all(timeout_per_process=0.5)
        assert "proc-all" in snapshots
        assert snapshots["proc-all"].process_id == "proc-all"

        response = await ctx.process_request(
            "SYS/MISSING",
            "GET_PROFILING_SNAPSHOT",
            timeout=0.2,
        )
        assert not response.ok
        assert response.error_code == ProcessControlErrorCode.UNROUTABLE_UNIT
    finally:
        await process.close()
        await ctx.__aexit__(None, None, None)
        graph_server.stop()
