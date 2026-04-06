import argparse

from ..graphserver import GraphService
from .common import add_address_argument, add_compact_argument, graph_address_from_args


async def handle_graphviz(args: argparse.Namespace) -> None:
    graph_service = GraphService(graph_address_from_args(args))
    graph_out = await graph_service.get_formatted_graph(
        fmt="graphviz", compact_level=args.compact
    )
    print(graph_out)


def setup_graphviz_cmdline(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("graphviz")
    add_address_argument(parser)
    add_compact_argument(parser)
    parser.set_defaults(_handler=handle_graphviz)
