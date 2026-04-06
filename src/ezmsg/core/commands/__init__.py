import argparse

from .graphviz import setup_graphviz_cmdline
from .mermaid import setup_mermaid_cmdline
from .serve import setup_serve_cmdline
from .shutdown import setup_shutdown_cmdline
from .start import setup_start_cmdline


def setup_core_cmdline(subparsers: argparse._SubParsersAction) -> None:
    setup_serve_cmdline(subparsers)
    setup_start_cmdline(subparsers)
    setup_shutdown_cmdline(subparsers)
    setup_graphviz_cmdline(subparsers)
    setup_mermaid_cmdline(subparsers)
