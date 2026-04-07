import argparse
import logging

from .dashboard import DASHBOARD_INSTALL_HINT

logger = logging.getLogger("ezmsg")


def _warn_dashboard_dependency_missing(_: argparse.Namespace) -> None:
    logger.warning(DASHBOARD_INSTALL_HINT)


def _setup_dashboard_fallback(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "dashboard",
        help="launch the optional ezmsg dashboard server",
        description="Launch the optional ezmsg dashboard server.",
    )
    parser.add_argument("--graph-address", default=None, help="Address of the ezmsg graph server.")
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind host for the dashboard.")
    parser.add_argument("--port", type=int, default=8000, help="HTTP bind port for the dashboard.")
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Open the dashboard in a browser after startup.",
    )
    parser.add_argument(
        "--log-level",
        default="info",
        choices=["critical", "error", "warning", "info", "debug", "trace"],
        help="Uvicorn log verbosity.",
    )
    parser.set_defaults(_handler=_warn_dashboard_dependency_missing)


def setup_dashboard_cmdline(subparsers: argparse._SubParsersAction) -> None:
    try:
        from ezmsg.dashboard.server import setup_dashboard_cmdline as setup_optional_dashboard
    except ImportError:
        _setup_dashboard_fallback(subparsers)
        return

    setup_optional_dashboard(subparsers)
