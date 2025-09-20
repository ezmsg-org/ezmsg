from pathlib import Path

from .analysis import summary, SummaryArgs
from .eval import perf_eval, PerfEvalArgs

def command() -> None:
    import argparse

    parser = argparse.ArgumentParser(description = 'ezmsg perf test utility')
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_run = subparsers.add_parser("run", help="run performance test")
    p_run.add_argument(
        "--duration",
        type=float,
        default=2.0,
        help="individual test duration in seconds (default = 2.0)",
    )
    p_run.add_argument(
        "--num-buffers",
        type=int,
        default=32,
        help="shared memory buffers (default = 32)",
    )

    p_run.set_defaults(_handler=lambda ns: perf_eval(
        PerfEvalArgs(
            duration = ns.duration, 
            num_buffers = ns.num_buffers
        )
    ))

    p_summary = subparsers.add_parser("summary", help = "summarise performance results")
    p_summary.add_argument(
        "perf",
        type=Path,
        help="perf test",
    )
    p_summary.add_argument(
        "--baseline",
        "-b",
        type=Path,
        default=None,
        help="baseline perf test for comparison",
    )

    p_summary.set_defaults(_handler=lambda ns: summary(
        SummaryArgs(
            perf = ns.perf,
            baseline = ns.baseline
        )
    ))

    ns = parser.parse_args()
    ns._handler(ns)
