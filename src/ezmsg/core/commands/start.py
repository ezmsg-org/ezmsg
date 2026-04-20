import argparse
import asyncio
import logging
import subprocess
import sys

from ..graphserver import GraphService
from ..netprotocol import close_stream_writer
from .common import add_address_argument, graph_address_from_args
from .dashboard import (
    DashboardDependencyError,
    add_dashboard_argument,
    require_dashboard_dependency,
)

logger = logging.getLogger("ezmsg")


async def handle_start(args: argparse.Namespace) -> None:
    graph_address = graph_address_from_args(args)
    graph_service = GraphService(graph_address)
    cmd = [sys.executable, "-m", "ezmsg.core", "serve", f"--address={graph_address}"]
    if args.dashboard is not None:
        try:
            require_dashboard_dependency()
        except DashboardDependencyError as exc:
            logger.warning(str(exc))
            return
        cmd.append("--dashboard")
        if type(args.dashboard) is int:
            cmd.append(str(args.dashboard))

    popen = subprocess.Popen(cmd)

    while True:
        try:
            _, writer = await graph_service.open_connection()
            await close_stream_writer(writer)
            break
        except ConnectionRefusedError:
            await asyncio.sleep(0.1)

    logger.info(f"Forked ezmsg servers in PID: {popen.pid}")


def setup_start_cmdline(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("start")
    add_address_argument(parser)
    add_dashboard_argument(parser)
    parser.set_defaults(_handler=handle_start)
