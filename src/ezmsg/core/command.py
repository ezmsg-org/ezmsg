import argparse
import asyncio
import inspect

from ezmsg.util.perf.command import setup_perf_cmdline

from .commands import setup_core_cmdline
from .commands.graphviz import handle_graphviz
from .commands.mermaid import handle_mermaid, mermaid_url as mm
from .commands.serve import handle_serve
from .commands.shutdown import handle_shutdown
from .commands.start import handle_start
from .netprotocol import (
    Address,
    GRAPHSERVER_ADDR_ENV,
    GRAPHSERVER_PORT_DEFAULT,
    PUBLISHER_START_PORT_ENV,
    PUBLISHER_START_PORT_DEFAULT,
)


def build_parser() -> argparse.ArgumentParser:
    """
    Build the ezmsg core command-line parser.

    Each command gets its own subparser so command-specific options are not
    shared globally across unrelated commands.
    """
    parser = argparse.ArgumentParser(
        "ezmsg.core",
        description="start and stop core ezmsg server processes",
        epilog=f"""
            You can also change server configuration with environment variables.
            GraphServer will be hosted on ${GRAPHSERVER_ADDR_ENV} (default port: {GRAPHSERVER_PORT_DEFAULT}).  
            Publishers will be assigned available ports starting from {PUBLISHER_START_PORT_DEFAULT}. (Change with ${PUBLISHER_START_PORT_ENV})
        """,
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="command for ezmsg")

    setup_core_cmdline(subparsers)
    setup_perf_cmdline(subparsers)
    return parser


def cmdline(argv: list[str] | None = None) -> None:
    """
    Command-line interface for ezmsg core server management.

    Provides commands for starting, stopping, and managing ezmsg server
    processes including GraphServer and SHMServer, as well as utilities
    for graph visualization.
    """
    parser = build_parser()
    args = parser.parse_args(args=argv)

    result = args._handler(args)
    if inspect.isawaitable(result):
        asyncio.run(result)


async def run_command(
    cmd: str,
    graph_address: Address,
    target: str = "live",
    compact: int | None = None,
    nobrowser: bool = False,
) -> None:
    handlers = {
        "serve": handle_serve,
        "start": handle_start,
        "shutdown": handle_shutdown,
        "graphviz": handle_graphviz,
        "mermaid": handle_mermaid,
    }
    if cmd not in handlers:
        raise ValueError(f"Unknown ezmsg command '{cmd}'")
    args = argparse.Namespace(
        command=cmd,
        address=str(graph_address),
        target=target,
        compact=compact,
        nobrowser=nobrowser,
    )
    await handlers[cmd](args)
