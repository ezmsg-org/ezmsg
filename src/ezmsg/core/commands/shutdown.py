import argparse
import logging

from ..graphserver import GraphService
from .common import add_address_argument, graph_address_from_args

logger = logging.getLogger("ezmsg")


async def handle_shutdown(args: argparse.Namespace) -> None:
    graph_address = graph_address_from_args(args)
    graph_service = GraphService(graph_address)

    try:
        await graph_service.shutdown()
        logger.info(f"Issued shutdown command to GraphServer @ {graph_service.address}")
    except ConnectionRefusedError:
        logger.warning(
            f"Could not issue shutdown command to GraphServer @ {graph_service.address}; server not running?"
        )


def setup_shutdown_cmdline(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("shutdown")
    add_address_argument(parser)
    parser.set_defaults(_handler=handle_shutdown)
