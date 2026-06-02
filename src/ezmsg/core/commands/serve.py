import argparse
import asyncio
import logging

from ..graphserver import GraphService
from .common import (
    add_address_argument,
    add_log_file_argument,
    graph_address_from_args,
    managed_log_file,
    resolve_log_file,
)
from .dashboard import (
    DashboardDependencyError,
    add_dashboard_argument,
    start_dashboard,
)

logger = logging.getLogger("ezmsg")


async def handle_serve(args: argparse.Namespace) -> None:
    graph_address = graph_address_from_args(args)
    with managed_log_file(resolve_log_file(args, graph_address)) as log_path:
        graph_service = GraphService(graph_address)

        logger.info(f"GraphServer Address: {graph_address}")
        logger.info(f"GraphServer Log File: {log_path}")
        graph_server = graph_service.create_server()
        dashboard_server = None

        try:
            if args.dashboard is not None:
                dashboard_port = args.dashboard if type(args.dashboard) is int else None
                dashboard_server = start_dashboard(
                    graph_service.address, dashboard_port=dashboard_port
                )
                logger.info(f"Dashboard Address: {dashboard_server.url}")
            logger.info("Servers running...")
            await asyncio.to_thread(graph_server.join)
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Interrupt detected; shutting down servers")
        except DashboardDependencyError as exc:
            logger.warning(str(exc))
        finally:
            if dashboard_server is not None:
                dashboard_server.stop()
            graph_server.stop()


def setup_serve_cmdline(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("serve")
    add_address_argument(parser)
    add_log_file_argument(parser)
    add_dashboard_argument(parser)
    parser.set_defaults(_handler=handle_serve)
