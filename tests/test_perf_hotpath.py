import asyncio
import pytest

from ezmsg.core.graphcontext import GraphContext
from ezmsg.core.processclient import ProcessControlClient
from ezmsg.util.perf.hotpath import HotPathCase, build_cases, run_hotpath_case

from ezmsg.core.graphserver import GraphServer
from ezmsg.core.graphmeta import ProfilingTraceControl


def test_build_cases_are_sorted_by_case_id():
    cases = build_cases(
        apis=["sync", "async"],
        transports=["local", "tcp", "shm"],
        payload_sizes=[1024, 64],
        num_buffers=1,
    )
    assert [case.case_id for case in cases] == sorted(case.case_id for case in cases)
    assert "async/shm/payload=64/buffers=1/trace=false" in {
        case.case_id for case in cases
    }
    assert "async/local/payload=64/buffers=1/trace=false" in {
        case.case_id for case in cases
    }


def test_run_hotpath_case_smoke():
    server = GraphServer()
    try:
        server.start(("127.0.0.1", 0))
    except PermissionError:
        pytest.skip("Local socket binding is unavailable in this environment")
    try:
        result = run_hotpath_case(
            HotPathCase(
                api="sync",
                transport="tcp",
                payload_size=64,
                num_buffers=1,
            ),
            count=8,
            warmup=2,
            samples=2,
            graph_address=server.address,
        )
    finally:
        server.stop()

    assert result.case.case_id == "sync/tcp/payload=64/buffers=1/trace=false"
    assert len(result.samples_seconds) == 2
    assert all(sample > 0 for sample in result.samples_seconds)
    assert result.summary.us_per_message_median > 0


def test_run_hotpath_case_trace_smoke():
    server = GraphServer()
    try:
        server.start(("127.0.0.1", 0))
    except PermissionError:
        pytest.skip("Local socket binding is unavailable in this environment")
    try:
        result = run_hotpath_case(
            HotPathCase(
                api="async",
                transport="local",
                payload_size=64,
                num_buffers=1,
                trace=True,
            ),
            count=8,
            warmup=2,
            samples=1,
            graph_address=server.address,
        )
    finally:
        server.stop()

    assert result.case.case_id == "async/local/payload=64/buffers=1/trace=true"
    assert len(result.samples_seconds) == 1
    assert result.samples_seconds[0] > 0


def test_build_cases_can_enable_trace_mode():
    cases = build_cases(
        apis=["async"],
        transports=["local"],
        payload_sizes=[64],
        num_buffers=1,
        trace=True,
    )

    assert len(cases) == 1
    assert cases[0].trace is True
    assert cases[0].case_id == "async/local/payload=64/buffers=1/trace=true"


@pytest.mark.asyncio
async def test_trace_profile_matches_dashboard_metrics():
    server = GraphServer()
    try:
        server.start(("127.0.0.1", 0))
    except PermissionError:
        pytest.skip("Local socket binding is unavailable in this environment")

    async with GraphContext(server.address, auto_start=False) as ctx:
        process = ProcessControlClient(server.address)
        await process.connect()
        process._trace_push_interval_s = 60.0
        await process.register(["EZMSG/PERF/HOTPATH"])

        topic = "/EZMSG/PERF/HOTPATH/TEST"
        pub = await ctx.publisher(topic)
        sub = await ctx.subscriber(topic)

        try:
            response = await ctx.process_set_profiling_trace(
                "EZMSG/PERF/HOTPATH",
                ProfilingTraceControl(
                    enabled=True,
                    sample_mod=1,
                    publisher_topics=[topic],
                    subscriber_topics=[topic],
                    metrics=["publish_delta_ns", "lease_time_ns", "user_span_ns"],
                ),
                timeout=1.0,
            )
            assert response.ok

            await pub.broadcast(b"1234")
            async with sub.recv_zero_copy() as _msg:
                span_start_ns = sub.begin_profile()
                try:
                    await asyncio.sleep(0)
                finally:
                    sub.end_profile(span_start_ns, "taskA")

            batch = await ctx.process_profiling_trace_batch(
                "EZMSG/PERF/HOTPATH", max_samples=100, timeout=1.0
            )
        finally:
            await process.close()
            server.stop()

    assert {sample.metric for sample in batch.samples} >= {
        "publish_delta_ns",
        "lease_time_ns",
        "user_span_ns",
    }
