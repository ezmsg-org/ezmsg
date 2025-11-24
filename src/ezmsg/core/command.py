import argparse
import asyncio
import base64
import json
import logging
import subprocess
import sys
import webbrowser
import zlib

from .graphserver import GraphService
from .netprotocol import (
    Address,
    GRAPHSERVER_ADDR_ENV,
    GRAPHSERVER_PORT_DEFAULT,
    PUBLISHER_START_PORT_ENV,
    PUBLISHER_START_PORT_DEFAULT,
    close_stream_writer,
)

logger = logging.getLogger("ezmsg")


def cmdline() -> None:
    """
    Command-line interface for ezmsg core server management.

    Provides commands for starting, stopping, and managing ezmsg server
    processes including GraphServer and SHMServer, as well as utilities
    for graph visualization.
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

    parser.add_argument(
        "command",
        help="command for ezmsg",
        choices=["serve", "start", "shutdown", "graphviz", "mermaid"],
    )

    parser.add_argument("--address", help="Address for GraphServer", default=None)

    parser.add_argument(
        "--target",
        help="Target for mermaid output. Options are 'ink', 'live', and 'play'.",
        default="live",
    )

    parser.add_argument(
        "-c",
        "--compact",
        help="""Use compact graph representation. Only used when `cmd` is 'mermaid' or 'graphviz'.
        Removes the lowest level of detail (typically streams). Can be stacked (eg. '-cc').
        Warning: this will also prune the graph of proxy topics (nodes that are both sources and targets).
        """,
        action="count",
    )

    parser.add_argument(
        "-n",
        "--nobrowser",
        help="Do not automatically open the browser for mermaid output. `--target` value will be ignored.",
        action="store_true",
    )

    class Args:
        command: str
        address: str | None
        target: str
        compact: int | None
        nobrowser: bool

    args = parser.parse_args(namespace=Args)

    graph_address = Address("127.0.0.1", GRAPHSERVER_PORT_DEFAULT)
    if args.address is not None:
        graph_address = Address.from_string(args.address)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(
        run_command(
            args.command,
            graph_address,
            args.target,
            args.compact,
            args.nobrowser,
        )
    )


async def run_command(
    cmd: str,
    graph_address: Address,
    target: str = "live",
    compact: int | None = None,
    nobrowser: bool = False,
) -> None:
    """
    Run an ezmsg command with the specified parameters.

    This function handles various ezmsg commands like 'serve', 'start', 'shutdown', etc.
    and manages the graph and shared memory services.

    :param cmd: The command to execute ('serve', 'start', 'shutdown', 'graphviz', 'mermaid')
    :type cmd: str
    :param graph_address: Address of the graph service
    :type graph_address: Address
    :param target: Target for visualization commands (default: 'live')
    :type target: str
    :param compact: Compactification level for visualization commands
    :type compact: int | None
    :param nobrowser: Whether to suppress browser opening for visualization
    :type nobrowser: bool
    """
    graph_service = GraphService(graph_address)

    if cmd == "serve":
        logger.info(f"GraphServer Address: {graph_address}")

        graph_server = graph_service.create_server()

        try:
            logger.info("Servers running...")
            graph_server.join()

        except KeyboardInterrupt:
            logger.info("Interrupt detected; shutting down servers")

        finally:
            if graph_server is not None:
                graph_server.stop()

    elif cmd == "start":
        popen = subprocess.Popen(
            [sys.executable, "-m", "ezmsg.core", "serve", f"--address={graph_address}"]
        )

        while True:
            try:
                _, writer = await graph_service.open_connection()
                await close_stream_writer(writer)
                break
            except ConnectionRefusedError:
                await asyncio.sleep(0.1)

        logger.info(f"Forked ezmsg servers in PID: {popen.pid}")

    elif cmd == "shutdown":
        try:
            await graph_service.shutdown()
            logger.info(
                f"Issued shutdown command to GraphServer @ {graph_service.address}"
            )

        except ConnectionRefusedError:
            logger.warning(
                f"Could not issue shutdown command to GraphServer @ {graph_service.address}; server not running?"
            )

    elif cmd in ["graphviz", "mermaid"]:
        graph_out = await graph_service.get_formatted_graph(
            fmt=cmd, compact_level=compact
        )
        print(graph_out)
        if cmd == "mermaid":
            if not nobrowser:
                if target == "live":
                    print(
                        "%% If the graph does not render immediately, try toggling the 'Pan & Zoom' button."
                    )
                webbrowser.open(mm(graph_out, target=target))


def mm(graph: str, target="live") -> str:
    """
    Generate a Mermaid visualization URL for the given graph.

    :param graph: Graph representation string to visualize.
    :type graph: str
    :param target: Target platform ('live' or 'ink').
    :type target: str
    :return: URL for graph visualization.
    :rtype: str
    """
    if target != "ink":
        jdict = {
            "code": graph,
            "mermaid": {"theme": "default"},
            "updateDiagram": True,
            "autoSync": True,
            "rough": False,
        }
        graph = json.dumps(jdict)
    graphbytes: bytes = graph.encode("utf8")

    if target != "ink":
        compress = zlib.compressobj(9, zlib.DEFLATED, 15, 8, zlib.Z_DEFAULT_STRATEGY)
        graphbytes = compress.compress(graphbytes)
        graphbytes += compress.flush()

    base64_bytes = base64.b64encode(graphbytes)
    base64_string = base64_bytes.decode("ascii")

    if target == "ink":
        prefix = "https://mermaid.ink/img/"
    elif target in ["live", "play"]:
        type_str = "pako"  # or "base64" if we skip compression above.
        if target == "live":
            prefix = f"https://mermaid.live/edit#{type_str}:"
        else:  # "play"
            prefix = f"https://www.mermaidchart.com/play#{type_str}:"
    else:
        raise ValueError(
            f"Unknown mermaid target '{target}'. Available options are 'ink', 'live', or 'play'."
        )
    return prefix + base64_string
