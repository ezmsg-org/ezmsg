import argparse
import sys

from .ab import setup_ab_cmdline
from .analysis import setup_summary_cmdline
from .hotpath import setup_hotpath_cmdline
from .run import setup_run_cmdline


def setup_perf_cmdline(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("perf", help="performance test utilities")
    perf_subparsers = parser.add_subparsers(dest="perf_command", required=True)

    setup_run_cmdline(perf_subparsers)
    setup_hotpath_cmdline(perf_subparsers)
    setup_ab_cmdline(perf_subparsers)
    setup_summary_cmdline(perf_subparsers)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ezmsg perf test utility")
    subparsers = parser.add_subparsers(dest="command", required=True)
    setup_perf_cmdline(subparsers)
    return parser


def command(argv: list[str] | None = None) -> None:
    parser = build_parser()

    if argv is None:
        argv = ["perf", *sys.argv[1:]]

    ns = parser.parse_args(argv)
    ns._handler(ns)


if __name__ == "__main__":
    command()
