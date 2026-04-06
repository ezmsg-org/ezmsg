import argparse
import asyncio
import logging

from ..graphserver import GraphService
from .common import add_address_argument, graph_address_from_args

logger = logging.getLogger("ezmsg")


async def handle_serve(args: argparse.Namespace) -> None:
    graph_address = graph_address_from_args(args)
    graph_service = GraphService(graph_address)

    logger.info(f"GraphServer Address: {graph_address}")
    graph_server = graph_service.create_server()

    try:
        logger.info("Servers running...")
        await asyncio.to_thread(graph_server.join)
    except KeyboardInterrupt:
        logger.info("Interrupt detected; shutting down servers")
    finally:
        graph_server.stop()


def setup_serve_cmdline(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("serve")
    add_address_argument(parser)
    parser.set_defaults(_handler=handle_serve)
