import argparse

from ..graphserver import GraphService
from ..netprotocol import Address


def add_address_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--address", help="Address for GraphServer", default=None)


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
