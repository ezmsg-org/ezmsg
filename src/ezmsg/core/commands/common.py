import argparse
from collections.abc import Iterator
from contextlib import contextmanager
import logging
import os
from datetime import datetime
from pathlib import Path

from ..graphserver import GraphService
from ..logconfig import create_ezmsg_log_formatter
from ..netprotocol import Address


def add_address_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--address", help="Address for GraphServer", default=None)


def add_log_file_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--log-file",
        help="Path to the ezmsg service log file",
        default=None,
    )


def add_compact_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-c",
        "--compact",
        help="""Use compact graph representation.
        Removes the lowest level of detail (typically streams). Can be stacked (eg. '-cc').
        Warning: this will also prune the graph of proxy topics (nodes that are both sources and targets).
        """,
        action="count",
    )


def graph_address_from_args(args: argparse.Namespace) -> Address:
    if args.address is None:
        return GraphService.default_address()
    return Address.from_string(args.address)


def resolve_log_file(args: argparse.Namespace, address: Address) -> Path:
    if args.log_file is not None:
        return Path(args.log_file).expanduser()

    env_log_file = os.environ.get("EZMSG_LOG_FILE")
    if env_log_file is not None:
        return Path(env_log_file).expanduser()

    if os.name == "nt":
        data_home = Path(
            os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")
        )
    else:
        data_home = Path(
            os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")
        )

    log_dir = data_home / "ezmsg" / "logs" / f"GraphServer-{address.port}"
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    return log_dir / f"{timestamp}.log"


def _configure_managed_log_file(log_file: Path) -> tuple[Path, logging.FileHandler | None]:
    log_path = log_file.expanduser().resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("ezmsg")
    if any(
        isinstance(handler, logging.FileHandler)
        and getattr(handler, "baseFilename", None) == str(log_path)
        for handler in logger.handlers
    ):
        return log_path, None

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(create_ezmsg_log_formatter())
    logger.addHandler(handler)

    return log_path, handler


def configure_log_file(log_file: Path) -> Path:
    log_path, _ = _configure_managed_log_file(log_file)

    return log_path


@contextmanager
def managed_log_file(log_file: Path) -> Iterator[Path]:
    log_path, handler = _configure_managed_log_file(log_file)
    try:
        yield log_path
    finally:
        if handler is not None:
            logger = logging.getLogger("ezmsg")
            logger.removeHandler(handler)
            handler.close()
