import asyncio

import pytest

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.graphmeta import (
    ProcessControlErrorCode,
    ProfilingStreamControl,
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


@pytest.mark.asyncio
async def test_process_profiling_trace_subscription_push():
    graph_server = GraphService().create_server()
    address = graph_server.address

    ctx = GraphContext(address, auto_start=False)
    await ctx.__aenter__()

    process = ProcessControlClient(address, process_id="proc-stream")
    await process.connect()
    await process.register(["SYS/U4"])

    pub = await ctx.publisher("TOPIC_STREAM")
    sub = await ctx.subscriber("TOPIC_STREAM")
    stream = None

    try:
        response = await ctx.process_set_profiling_trace(
            "SYS/U4",
            ProfilingTraceControl(
                enabled=True,
                sample_mod=1,
                publisher_topics=["TOPIC_STREAM"],
                subscriber_topics=["TOPIC_STREAM"],
            ),
            timeout=1.0,
        )
        assert response.ok

        stream = ctx.subscribe_profiling_trace(
            ProfilingStreamControl(interval=0.02, max_samples=256)
        )

        for idx in range(8):
            await pub.broadcast(idx)
            async with sub.recv_zero_copy() as _msg:
                span_start_ns = sub.begin_profile()
                try:
                    await asyncio.sleep(0)
                finally:
                    sub.end_profile(span_start_ns, "taskA")

        batch = await asyncio.wait_for(anext(stream), timeout=1.0)
        assert batch.timestamp > 0.0
        assert "proc-stream" in batch.batches
        process_batch = batch.batches["proc-stream"]
        assert process_batch.process_id == "proc-stream"
        assert len(process_batch.samples) > 0
    finally:
        if stream is not None:
            await stream.aclose()
        await process.close()
        await ctx.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_profiling_trace_control_endpoint_metric_and_ttl():
    graph_server = GraphService().create_server()
    address = graph_server.address

    ctx = GraphContext(address, auto_start=False)
    await ctx.__aenter__()

    process = ProcessControlClient(address, process_id="proc-trace-filter")
    await process.connect()
    await process.register(["SYS/U5"])

    pub_a = await ctx.publisher("TOPIC_A")
    sub_a = await ctx.subscriber("TOPIC_A")
    pub_b = await ctx.publisher("TOPIC_B")
    sub_b = await ctx.subscriber("TOPIC_B")

    try:
        # Warm up and discover endpoint IDs for precise filter targeting.
        for idx in range(3):
            await pub_a.broadcast(idx)
            async with sub_a.recv_zero_copy() as _msg:
                await asyncio.sleep(0)
            await pub_b.broadcast(idx)
            async with sub_b.recv_zero_copy() as _msg:
                await asyncio.sleep(0)

        snapshot = await ctx.process_profiling_snapshot("SYS/U5", timeout=1.0)
        pub_a_endpoint = next(
            pub.endpoint_id
            for pub in snapshot.publishers.values()
            if pub.topic == "TOPIC_A"
        )

        response = await ctx.process_set_profiling_trace(
            "SYS/U5",
            ProfilingTraceControl(
                enabled=True,
                sample_mod=1,
                publisher_endpoint_ids=[pub_a_endpoint],
                metrics=["publish_delta_ns"],
            ),
            timeout=1.0,
        )
        assert response.ok

        for idx in range(8):
            await pub_a.broadcast(idx)
            async with sub_a.recv_zero_copy() as _msg:
                await asyncio.sleep(0)
            await pub_b.broadcast(idx)
            async with sub_b.recv_zero_copy() as _msg:
                await asyncio.sleep(0)

        batch = await ctx.process_profiling_trace_batch(
            "SYS/U5", max_samples=512, timeout=1.0
        )
        assert len(batch.samples) > 0
        assert all(sample.metric == "publish_delta_ns" for sample in batch.samples)
        assert all(sample.endpoint_id == pub_a_endpoint for sample in batch.samples)

        ttl_response = await ctx.process_set_profiling_trace(
            "SYS/U5",
            ProfilingTraceControl(
                enabled=True,
                sample_mod=1,
                publisher_endpoint_ids=[pub_a_endpoint],
                metrics=["publish_delta_ns"],
                ttl_seconds=0.01,
            ),
            timeout=1.0,
        )
        assert ttl_response.ok
        await asyncio.sleep(0.03)

        for idx in range(3):
            await pub_a.broadcast(idx)
            async with sub_a.recv_zero_copy() as _msg:
                await asyncio.sleep(0)

        expired_batch = await ctx.process_profiling_trace_batch(
            "SYS/U5", max_samples=512, timeout=1.0
        )
        assert len(expired_batch.samples) == 0
    finally:
        await process.close()
        await ctx.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_profiling_trace_subscription_stream_control():
    graph_server = GraphService().create_server()
    address = graph_server.address

    ctx = GraphContext(address, auto_start=False)
    await ctx.__aenter__()

    process_a = ProcessControlClient(address, process_id="proc-stream-a")
    await process_a.connect()
    await process_a.register(["SYS/U6"])

    stream = None
    try:
        stream = ctx.subscribe_profiling_trace(
            ProfilingStreamControl(
                interval=0.02,
                max_samples=64,
                process_ids=["proc-stream-a"],
                include_empty_batches=True,
            )
        )
        batch = await asyncio.wait_for(anext(stream), timeout=1.0)
        assert "proc-stream-a" in batch.batches
        assert len(batch.batches) == 1
    finally:
        if stream is not None:
            await stream.aclose()
        await process_a.close()
        await ctx.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_profiling_trace_subscription_does_not_starve_peer_subscribers():
    graph_server = GraphService().create_server()
    address = graph_server.address

    ctx = GraphContext(address, auto_start=False)
    await ctx.__aenter__()

    process = ProcessControlClient(address, process_id="proc-stream-multi")
    await process.connect()
    await process.register(["SYS/U7"])

    pub = await ctx.publisher("TOPIC_STREAM_MULTI")
    sub = await ctx.subscriber("TOPIC_STREAM_MULTI")
    stream_a = None
    stream_b = None

    try:
        response = await ctx.process_set_profiling_trace(
            "SYS/U7",
            ProfilingTraceControl(
                enabled=True,
                sample_mod=1,
                publisher_topics=["TOPIC_STREAM_MULTI"],
                subscriber_topics=["TOPIC_STREAM_MULTI"],
            ),
            timeout=1.0,
        )
        assert response.ok

        control = ProfilingStreamControl(interval=0.02, max_samples=256)
        stream_a = ctx.subscribe_profiling_trace(control)
        stream_b = ctx.subscribe_profiling_trace(control)

        for idx in range(12):
            await pub.broadcast(idx)
            async with sub.recv_zero_copy() as _msg:
                span_start_ns = sub.begin_profile()
                try:
                    await asyncio.sleep(0)
                finally:
                    sub.end_profile(span_start_ns, "taskA")

        batch_a = await asyncio.wait_for(anext(stream_a), timeout=1.0)
        batch_b = await asyncio.wait_for(anext(stream_b), timeout=1.0)

        assert "proc-stream-multi" in batch_a.batches
        assert "proc-stream-multi" in batch_b.batches
        assert len(batch_a.batches["proc-stream-multi"].samples) > 0
        assert len(batch_b.batches["proc-stream-multi"].samples) > 0
    finally:
        if stream_a is not None:
            await stream_a.aclose()
        if stream_b is not None:
            await stream_b.aclose()
        await process.close()
        await ctx.__aexit__(None, None, None)
        graph_server.stop()
