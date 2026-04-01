import argparse

from .ab import setup_ab_cmdline
from .analysis import setup_summary_cmdline
from .hotpath import setup_hotpath_cmdline
from .run import setup_run_cmdline


def command() -> None:
    parser = argparse.ArgumentParser(description="ezmsg perf test utility")
    subparsers = parser.add_subparsers(dest="command", required=True)

    setup_run_cmdline(subparsers)
    setup_hotpath_cmdline(subparsers)
    setup_ab_cmdline(subparsers)
    setup_summary_cmdline(subparsers)

    ns = parser.parse_args()
    ns._handler(ns)


if __name__ == "__main__":
    command()
