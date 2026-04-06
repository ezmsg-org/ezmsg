import json
from pathlib import Path

from ezmsg.util.messagecodec import MessageEncoder
from ezmsg.util.perf.analysis import (
    build_report_bundle,
    default_compare_html_path,
    default_report_html_path,
    write_html_report,
)
from ezmsg.util.perf.envinfo import TestEnvironmentInfo as PerfEnvironmentInfo
from ezmsg.util.perf.impl import (
    Metrics as PerfMetrics,
    TestLogEntry as PerfLogEntry,
    TestParameters as PerfParameters,
)
from ezmsg.util.perf.run import benchmark, output_paths_for_name


def _write_perf_log(
    path: Path, info: PerfEnvironmentInfo, entries: list[PerfLogEntry]
) -> None:
    with open(path, "w") as handle:
        handle.write(json.dumps(info, cls=MessageEncoder) + "\n")
        for entry in entries:
            handle.write(json.dumps(entry, cls=MessageEncoder) + "\n")


def _entry(
    *,
    config: str,
    comms: str,
    n_clients: int,
    msg_size: int,
    sample_rate: float,
    latency: float,
    data_rate: float,
) -> PerfLogEntry:
    return PerfLogEntry(
        params=PerfParameters(
            msg_size=msg_size,
            num_msgs=128,
            n_clients=n_clients,
            config=config,
            comms=comms,
            max_duration=1.0,
            num_buffers=1,
        ),
        results=PerfMetrics(
            num_msgs=128,
            sample_rate_mean=sample_rate,
            sample_rate_median=sample_rate * 0.98,
            latency_mean=latency,
            latency_median=latency * 0.97,
            latency_total=latency * 128,
            data_rate=data_rate,
        ),
    )


def test_report_and_compare_html_paths(tmp_path):
    perf = tmp_path / "perf_20260406_120000.txt"
    baseline = tmp_path / "perf_20260405_120000.txt"

    assert default_report_html_path(perf) == tmp_path / "perf_20260406_120000.html"
    assert default_compare_html_path(perf, baseline) == (
        tmp_path / "perf_20260406_120000.vs_perf_20260405_120000.html"
    )
    assert output_paths_for_name("smoke") == (
        Path("perf_smoke.txt"),
        Path("report_smoke.html"),
    )


def test_build_report_bundle_and_html_report(tmp_path):
    candidate = tmp_path / "candidate.txt"
    baseline = tmp_path / "baseline.txt"

    candidate_info = PerfEnvironmentInfo(git_branch="candidate", git_commit="abc123")
    baseline_info = PerfEnvironmentInfo(git_branch="baseline", git_commit="def456")

    _write_perf_log(
        candidate,
        candidate_info,
        [
            _entry(
                config="fanin",
                comms="local",
                n_clients=1,
                msg_size=64,
                sample_rate=1200.0,
                latency=0.0009,
                data_rate=2_400_000.0,
            ),
            _entry(
                config="relay",
                comms="tcp",
                n_clients=2,
                msg_size=256,
                sample_rate=700.0,
                latency=0.0024,
                data_rate=1_200_000.0,
            ),
        ],
    )
    _write_perf_log(
        baseline,
        baseline_info,
        [
            _entry(
                config="fanin",
                comms="local",
                n_clients=1,
                msg_size=64,
                sample_rate=1000.0,
                latency=0.0012,
                data_rate=2_000_000.0,
            ),
            _entry(
                config="relay",
                comms="tcp",
                n_clients=2,
                msg_size=256,
                sample_rate=900.0,
                latency=0.0018,
                data_rate=1_700_000.0,
            ),
        ],
    )

    bundle = build_report_bundle(candidate, baseline_path=baseline)
    assert bundle.relative is True
    assert bundle.delta_counts["improvement"] > 0
    assert bundle.delta_counts["regression"] > 0
    assert bundle.top_improvements
    assert bundle.top_regressions

    out_path = write_html_report(candidate, baseline_path=baseline, open_browser=False)
    html = out_path.read_text(encoding="utf-8")

    assert "Comparison Overview" in html
    assert "Biggest Regressions" in html
    assert "data-filter='config'" in html
    assert "display-mode" in html
    assert "metric-view" in html
    assert "candidate.vs_baseline.html" == out_path.name


def test_benchmark_writes_raw_output_and_html_report(tmp_path, monkeypatch):
    output_path = tmp_path / "perf_smoke.txt"
    html_path = tmp_path / "report_smoke.html"
    monkeypatch.chdir(tmp_path)

    class FakeGraphServer:
        def __init__(self):
            self.address = ("127.0.0.1", 0)

        def start(self):
            return None

        def stop(self):
            return None

    html_calls: list[tuple[Path, Path | None, bool]] = []

    monkeypatch.setattr("ezmsg.util.perf.run.GraphServer", FakeGraphServer)
    monkeypatch.setattr("ezmsg.util.perf.run.warmup", lambda _: None)
    monkeypatch.setattr(
        "ezmsg.util.perf.run.perform_test",
        lambda **_: PerfMetrics(
            num_msgs=8,
            sample_rate_mean=1000.0,
            sample_rate_median=950.0,
            latency_mean=0.001,
            latency_median=0.0009,
            latency_total=0.008,
            data_rate=1_000_000.0,
        ),
    )

    def _fake_write_html_report(
        perf_path: Path,
        baseline_path: Path | None = None,
        output_path: Path | None = None,
        open_browser: bool = False,
    ) -> Path:
        html_calls.append((perf_path, output_path, open_browser))
        target = output_path or perf_path.with_suffix(".html")
        target.write_text("<html></html>", encoding="utf-8")
        return target

    monkeypatch.setattr(
        "ezmsg.util.perf.analysis.write_html_report",
        _fake_write_html_report,
    )

    raw_path, report_path = benchmark(
        max_duration=0.01,
        num_msgs=8,
        num_buffers=1,
        iters=1,
        repeats=1,
        msg_sizes=[64],
        n_clients=[1],
        comms=["local"],
        configs=["fanin"],
        grid=True,
        warmup_dur=0.0,
        name="smoke",
        open_browser=False,
    )

    assert raw_path.name == output_path.name
    assert report_path is not None
    assert report_path.name == html_path.name
    assert output_path.exists()
    assert html_path.exists()
    assert html_calls == [(raw_path, report_path, False)]
