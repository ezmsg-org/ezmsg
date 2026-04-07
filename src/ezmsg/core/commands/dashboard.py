import argparse
import os
from typing import Any

from ..netprotocol import (
    Address,
    DEFAULT_HOST,
    GRAPHSERVER_ADDR_ENV,
    GRAPHSERVER_PORT_DEFAULT,
)

DASHBOARD_ADDR_ENV = "EZMSG_DASHBOARD_ADDR"
DASHBOARD_PORT_DEFAULT = GRAPHSERVER_PORT_DEFAULT + 1
DASHBOARD_INSTALL_HINT = (
    "Dashboard support requires the optional `ezmsg-dashboard` package. "
    "Install it with `pip install ezmsg-dashboard`."
)


class DashboardDependencyError(RuntimeError):
    pass


def add_dashboard_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dashboard",
        nargs="?",
        const=True,
        default=None,
        type=int,
        metavar="PORT",
        help=(
            "Serve the optional ezmsg dashboard alongside the graph server. "
            "If PORT is omitted, ezmsg uses the configured dashboard address or graph port + 1."
        ),
    )


def default_graph_address() -> Address:
    address_str = os.environ.get(
        GRAPHSERVER_ADDR_ENV, f"{DEFAULT_HOST}:{GRAPHSERVER_PORT_DEFAULT}"
    )
    return Address.from_string(address_str)


def dashboard_address(
    graph_address: Address | None = None, dashboard_port: int | None = None
) -> Address:
    if DASHBOARD_ADDR_ENV in os.environ:
        address = Address.from_string(os.environ[DASHBOARD_ADDR_ENV])
    else:
        resolved_graph_address = graph_address or default_graph_address()
        address = Address(resolved_graph_address.host, resolved_graph_address.port + 1)

    if dashboard_port is not None:
        return Address(address.host, dashboard_port)
    return address


def require_dashboard_dependency() -> Any:
    try:
        from ezmsg.dashboard.server import start_dashboard_server
    except ImportError as exc:
        raise DashboardDependencyError(DASHBOARD_INSTALL_HINT) from exc
    return start_dashboard_server


def start_dashboard(graph_address: Address, dashboard_port: int | None = None) -> Any:
    start_dashboard_server = require_dashboard_dependency()

    address = dashboard_address(graph_address, dashboard_port=dashboard_port)
    return start_dashboard_server(
        graph_address=graph_address,
        host=address.host,
        port=address.port,
        log_level="warning",
    )
