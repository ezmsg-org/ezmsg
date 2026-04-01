from ezmsg.util.perf.ab import (
    build_hotpath_command,
    build_pair_order,
    summarize_ab_results,
)


def test_build_pair_order_is_balanced_and_reproducible():
    first = build_pair_order(6, seed=123)
    second = build_pair_order(6, seed=123)

    assert first == second
    assert len(first) == 6
    assert first.count(("A", "B")) == 3
    assert first.count(("B", "A")) == 3


def test_build_hotpath_command_contains_expected_args(tmp_path):
    cmd = build_hotpath_command(
        tmp_path / "out.json",
        count=100,
        warmup=10,
        payload_sizes=[64, 256],
        transports=["local", "shm"],
        apis=["async", "sync"],
        num_buffers=2,
        quiet=True,
    )

    assert cmd[:5] == ["uv", "run", "python", "-m", "ezmsg.util.perf.hotpath"]
    assert "--count" in cmd
    assert "--payload-sizes" in cmd
    assert "--quiet" in cmd


def test_summarize_ab_results_uses_b_vs_a_delta():
    paired_runs = [
        (
            {"async/shm/payload=64/buffers=1": 10.0},
            {"async/shm/payload=64/buffers=1": 12.0},
        ),
        (
            {"async/shm/payload=64/buffers=1": 8.0},
            {"async/shm/payload=64/buffers=1": 9.0},
        ),
    ]

    summary = summarize_ab_results(
        ref_a="dev",
        ref_b="CURRENT",
        rounds=2,
        seed=0,
        paired_runs=paired_runs,
    )

    assert len(summary.cases) == 1
    case = summary.cases[0]
    assert case.case_id == "async/shm/payload=64/buffers=1"
    assert case.a_us_per_message_median == 9.0
    assert case.b_us_per_message_median == 10.5
    assert case.delta_pct_median > 0
    assert case.b_faster_pairs == 0
