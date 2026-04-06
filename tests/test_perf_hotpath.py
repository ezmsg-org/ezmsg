import pytest

from ezmsg.util.perf.hotpath import HotPathCase, build_cases, run_hotpath_case

from ezmsg.core.graphserver import GraphServer


def test_build_cases_are_sorted_by_case_id():
    cases = build_cases(
        apis=["sync", "async"],
        transports=["local", "tcp", "shm"],
        payload_sizes=[1024, 64],
        num_buffers=1,
    )
    assert [case.case_id for case in cases] == sorted(case.case_id for case in cases)
    assert "async/shm/payload=64/buffers=1" in {case.case_id for case in cases}
    assert "async/local/payload=64/buffers=1" in {case.case_id for case in cases}


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

    assert result.case.case_id == "sync/tcp/payload=64/buffers=1"
    assert len(result.samples_seconds) == 2
    assert all(sample > 0 for sample in result.samples_seconds)
    assert result.summary.us_per_message_median > 0
