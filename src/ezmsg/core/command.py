import argparse
import asyncio
import inspect

from .commands import setup_core_cmdline
from .commands.graphviz import handle_graphviz
from .commands.mermaid import handle_mermaid, mermaid_url as mm
from .commands.serve import handle_serve
from .commands.shutdown import handle_shutdown
from .commands.start import handle_start
from .netprotocol import (
    Address,
    DEFAULT_HOST,
    GRAPHSERVER_ADDR_ENV,
    GRAPHSERVER_PORT_DEFAULT,
    PUBLISHER_START_PORT_ENV,
    PUBLISHER_START_PORT_DEFAULT,
)
from .commands.dashboard import (
    DASHBOARD_ADDR_ENV,
    DASHBOARD_INSTALL_HINT,
    DASHBOARD_PORT_DEFAULT,
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
            Dashboard will be hosted on ${DASHBOARD_ADDR_ENV} (default: {DEFAULT_HOST}:{DASHBOARD_PORT_DEFAULT}, or graph port + 1).
            Publishers will be assigned available ports starting from {PUBLISHER_START_PORT_DEFAULT}. (Change with ${PUBLISHER_START_PORT_ENV})
        """,
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="command for ezmsg")

    setup_core_cmdline(subparsers)
    from ezmsg.util.perf.command import setup_perf_cmdline

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
        try:
            asyncio.run(result)
        except KeyboardInterrupt:
            # asyncio.run() re-raises KeyboardInterrupt after cancelling the main
            # task on Ctrl+C, even when command cleanup has already completed.
            pass


async def run_command(
    cmd: str,
    graph_address: Address,
    target: str = "live",
    compact: int | None = None,
    nobrowser: bool = False,
    dashboard: int | bool | None = None,
) -> None:
    handlers = {
        "dashboard": None,
        "serve": handle_serve,
        "start": handle_start,
        "shutdown": handle_shutdown,
        "graphviz": handle_graphviz,
        "mermaid": handle_mermaid,
    }
    if cmd not in handlers:
        raise ValueError(f"Unknown ezmsg command '{cmd}'")
    if cmd == "dashboard":
        try:
            from ezmsg.dashboard.server import handle_dashboard
        except ImportError as exc:
            raise RuntimeError(DASHBOARD_INSTALL_HINT) from exc
        handlers["dashboard"] = handle_dashboard
    args = argparse.Namespace(
        command=cmd,
        address=str(graph_address),
        graph_address=str(graph_address),
        target=target,
        compact=compact,
        nobrowser=nobrowser,
        dashboard=dashboard,
        host="127.0.0.1",
        port=8000,
        open_browser=False,
        log_level="info",
    )
    result = handlers[cmd](args)
    if inspect.isawaitable(result):
        await result
