import argparse
import asyncio
import logging
import subprocess
import sys

from ..graphserver import GraphService
from ..netprotocol import close_stream_writer
from .common import add_address_argument, graph_address_from_args

logger = logging.getLogger("ezmsg")


async def handle_start(args: argparse.Namespace) -> None:
    graph_address = graph_address_from_args(args)
    graph_service = GraphService(graph_address)

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


def setup_start_cmdline(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("start")
    add_address_argument(parser)
    parser.set_defaults(_handler=handle_start)
