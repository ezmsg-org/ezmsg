import asyncio

import pytest

from ezmsg.core import profiling as profiling_core
from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.graphmeta import (
    ProcessControlErrorCode,
    ProfilingStreamControl,
    ProfilingTraceControl,
)
from ezmsg.core.graphserver import GraphService
from ezmsg.core.processclient import ProcessControlClient


def test_profiling_windows_age_out_during_idle_snapshots(monkeypatch: pytest.MonkeyPatch):
    publisher = profiling_core._PublisherMetrics(
        topic="TOPIC_IDLE",
        endpoint_id="TOPIC_IDLE:ep1",
        num_buffers=4,
    )
    subscriber = profiling_core._SubscriberMetrics(
        topic="TOPIC_IDLE",
        endpoint_id="TOPIC_IDLE:sub1",
    )

    publisher.record_publish(0, inflight=0)
    publisher.record_publish(int(0.1e9), inflight=0)
    subscriber.record_receive(
        int(0.1e9),
        lease_ns=int(0.2e6),
        channel_kind=profiling_core.ProfileChannelType.LOCAL,
    )

    now_ns = {"value": int(0.2e9)}
    monkeypatch.setattr(profiling_core, "PROFILE_TIME", lambda: now_ns["value"])

    active_pub = publisher.snapshot()
    active_sub = subscriber.snapshot()
    assert active_pub.messages_published_window == 2
    assert active_pub.publish_rate_hz_window > 0.0
    assert active_sub.messages_received_window == 1

    now_ns["value"] = int(30e9)
    idle_pub = publisher.snapshot()
    idle_sub = subscriber.snapshot()
    assert idle_pub.messages_published_window == 0
    assert idle_pub.publish_rate_hz_window == 0.0
    assert idle_pub.backpressure_wait_ns_window == 0
    assert idle_sub.messages_received_window == 0
    assert idle_sub.attributable_backpressure_ns_window == 0
    assert idle_sub.lease_time_ns_avg_window == 0.0


@pytest.mark.asyncio
async def test_process_profiling_snapshot_collects_pub_sub_metrics():
    graph_server = GraphService().create_server()
    address = graph_server.address

    ctx = GraphContext(address, auto_start=False)
    await ctx.__aenter__()

    process = ProcessControlClient(address)
    await process.connect()
    assert process.client_id is not None
    process_key = process.client_id
    await process.register(["SYS/U1"])

    pub = await ctx.publisher("TOPIC_PROF")
    sub = await ctx.subscriber("TOPIC_PROF")

    try:
        for idx in range(8):
            await pub.broadcast(idx)
            async with sub.recv_zero_copy() as _msg:
                await asyncio.sleep(0)

        snap = await ctx.process_profiling_snapshot("SYS/U1", timeout=1.0)
        assert snap.process_id == process_key
        assert snap.window_seconds > 0
        assert len(snap.publishers) >= 1
        assert len(snap.subscribers) >= 1

        pub_metrics = next(
            pub for pub in snap.publishers.values() if pub.topic == "TOPIC_PROF"
        )
        assert pub_metrics.messages_published_total >= 8
        assert pub_metrics.publish_rate_hz_window >= 0.0

        sub_metrics = next(
            sub for sub in snap.subscribers.values() if sub.topic == "TOPIC_PROF"
        )
        assert sub_metrics.messages_received_total >= 8
        assert sub_metrics.lease_time_ns_total > 0
        assert sub_metrics.lease_time_ns_avg_window >= 0.0
    finally:
        await process.close()
        await ctx.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_connect_does_not_clear_preexisting_profile_metrics():
    graph_server = GraphService().create_server()
    address = graph_server.address

    ctx = GraphContext(address, auto_start=False)
    await ctx.__aenter__()

    pub = await ctx.publisher("TOPIC_PRECONNECT")
    sub = await ctx.subscriber("TOPIC_PRECONNECT")
    for idx in range(6):
        await pub.broadcast(idx)
        async with sub.recv_zero_copy() as _msg:
            await asyncio.sleep(0)

    process = ProcessControlClient(address)
    await process.connect()
    await process.register(["SYS/U_PRE"])

    try:
        snap = await ctx.process_profiling_snapshot("SYS/U_PRE", timeout=1.0)
        assert len(snap.publishers) >= 1
        assert len(snap.subscribers) >= 1
        pub_metrics = next(
            pub for pub in snap.publishers.values() if pub.topic == "TOPIC_PRECONNECT"
        )
        sub_metrics = next(
            sub for sub in snap.subscribers.values() if sub.topic == "TOPIC_PRECONNECT"
        )
        assert pub_metrics.messages_published_total >= 6
        assert sub_metrics.messages_received_total >= 6
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

    process = ProcessControlClient(address)
    await process.connect()
    assert process.client_id is not None
    process_key = process.client_id
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
        assert batch.process_id == process_key
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

    process = ProcessControlClient(address)
    await process.connect()
    assert process.client_id is not None
    process_key = process.client_id
    await process.register(["SYS/U3"])

    try:
        snapshots = await ctx.profiling_snapshot_all(timeout_per_process=0.5)
        assert process_key in snapshots
        assert snapshots[process_key].process_id == process_key

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

    process = ProcessControlClient(address)
    await process.connect()
    assert process.client_id is not None
    process_key = process.client_id
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
        assert process_key in batch.batches
        process_batch = batch.batches[process_key]
        assert process_batch.process_id == process_key
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

    process = ProcessControlClient(address)
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

    process_a = ProcessControlClient(address)
    await process_a.connect()
    assert process_a.client_id is not None
    process_a_key = process_a.client_id
    await process_a.register(["SYS/U6"])

    stream = None
    try:
        stream = ctx.subscribe_profiling_trace(
            ProfilingStreamControl(
                interval=0.02,
                max_samples=64,
                process_ids=[process_a_key],
                include_empty_batches=True,
            )
        )
        batch = await asyncio.wait_for(anext(stream), timeout=1.0)
        assert process_a_key in batch.batches
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

    process = ProcessControlClient(address)
    await process.connect()
    assert process.client_id is not None
    process_key = process.client_id
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

        assert process_key in batch_a.batches
        assert process_key in batch_b.batches
        assert len(batch_a.batches[process_key].samples) > 0
        assert len(batch_b.batches[process_key].samples) > 0
    finally:
        if stream_a is not None:
            await stream_a.aclose()
        if stream_b is not None:
            await stream_b.aclose()
        await process.close()
        await ctx.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_profiling_trace_batch_interleaves_publisher_and_subscriber_samples():
    graph_server = GraphService().create_server()
    address = graph_server.address

    ctx = GraphContext(address, auto_start=False)
    await ctx.__aenter__()

    process = ProcessControlClient(address)
    await process.connect()
    await process.register(["SYS/U8"])

    pub = await ctx.publisher("TOPIC_TRACE_MIX")
    sub = await ctx.subscriber("TOPIC_TRACE_MIX")

    try:
        response = await ctx.process_set_profiling_trace(
            "SYS/U8",
            ProfilingTraceControl(
                enabled=True,
                sample_mod=1,
                publisher_topics=["TOPIC_TRACE_MIX"],
                subscriber_topics=["TOPIC_TRACE_MIX"],
                metrics=["publish_delta_ns", "lease_time_ns"],
            ),
            timeout=1.0,
        )
        assert response.ok

        for idx in range(64):
            await pub.broadcast(idx)
            async with sub.recv_zero_copy() as _msg:
                await asyncio.sleep(0)

        batch = await ctx.process_profiling_trace_batch(
            "SYS/U8", max_samples=32, timeout=1.0
        )
        metrics = {sample.metric for sample in batch.samples}
        assert "publish_delta_ns" in metrics
        assert "lease_time_ns" in metrics
    finally:
        await process.close()
        await ctx.__aexit__(None, None, None)
        graph_server.stop()


@pytest.mark.asyncio
async def test_process_profiling_trace_control_change_clears_stale_trace_samples():
    graph_server = GraphService().create_server()
    address = graph_server.address

    ctx = GraphContext(address, auto_start=False)
    await ctx.__aenter__()

    process = ProcessControlClient(address)
    await process.connect()
    await process.register(["SYS/U9"])

    pub_old = await ctx.publisher("TOPIC_TRACE_OLD")
    sub_old = await ctx.subscriber("TOPIC_TRACE_OLD")
    pub_new = await ctx.publisher("TOPIC_TRACE_NEW")
    sub_new = await ctx.subscriber("TOPIC_TRACE_NEW")

    try:
        old_response = await ctx.process_set_profiling_trace(
            "SYS/U9",
            ProfilingTraceControl(
                enabled=True,
                sample_mod=1,
                publisher_topics=["TOPIC_TRACE_OLD"],
                metrics=["publish_delta_ns"],
            ),
            timeout=1.0,
        )
        assert old_response.ok

        for idx in range(12):
            await pub_old.broadcast(idx)
            async with sub_old.recv_zero_copy() as _msg:
                await asyncio.sleep(0)

        new_response = await ctx.process_set_profiling_trace(
            "SYS/U9",
            ProfilingTraceControl(
                enabled=True,
                sample_mod=1,
                publisher_topics=["TOPIC_TRACE_NEW"],
                metrics=["publish_delta_ns"],
            ),
            timeout=1.0,
        )
        assert new_response.ok

        for idx in range(8):
            await pub_new.broadcast(idx)
            async with sub_new.recv_zero_copy() as _msg:
                await asyncio.sleep(0)

        batch = await ctx.process_profiling_trace_batch(
            "SYS/U9", max_samples=256, timeout=1.0
        )
        assert len(batch.samples) > 0
        assert all(sample.topic == "TOPIC_TRACE_NEW" for sample in batch.samples)
    finally:
        await process.close()
        await ctx.__aexit__(None, None, None)
        graph_server.stop()
