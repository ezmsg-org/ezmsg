import argparse
import base64
import json
import webbrowser
import zlib

from ..graphserver import GraphService
from .common import add_address_argument, add_compact_argument, graph_address_from_args


def mermaid_url(graph: str, target: str = "live") -> str:
    if target != "ink":
        graph = json.dumps(
            {
                "code": graph,
                "mermaid": {"theme": "default"},
                "updateDiagram": True,
                "autoSync": True,
                "rough": False,
            }
        )

    graphbytes = graph.encode("utf8")

    if target != "ink":
        compress = zlib.compressobj(9, zlib.DEFLATED, 15, 8, zlib.Z_DEFAULT_STRATEGY)
        graphbytes = compress.compress(graphbytes) + compress.flush()

    base64_string = base64.b64encode(graphbytes).decode("ascii")

    if target == "ink":
        prefix = "https://mermaid.ink/img/"
    elif target == "live":
        prefix = "https://mermaid.live/edit#pako:"
    elif target == "play":
        prefix = "https://www.mermaidchart.com/play#pako:"
    else:
        raise ValueError(
            f"Unknown mermaid target '{target}'. Available options are 'ink', 'live', or 'play'."
        )

    return prefix + base64_string


async def handle_mermaid(args: argparse.Namespace) -> None:
    graph_service = GraphService(graph_address_from_args(args))
    graph_out = await graph_service.get_formatted_graph(
        fmt="mermaid", compact_level=args.compact
    )
    print(graph_out)

    if args.nobrowser:
        return

    if args.target == "live":
        print(
            "%% If the graph does not render immediately, try toggling the 'Pan & Zoom' button."
        )
    webbrowser.open(mermaid_url(graph_out, target=args.target))


def setup_mermaid_cmdline(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("mermaid")
    add_address_argument(parser)
    add_compact_argument(parser)
    parser.add_argument(
        "--target",
        help="Target for mermaid output. Options are 'ink', 'live', and 'play'.",
        default="live",
    )
    parser.add_argument(
        "-n",
        "--nobrowser",
        help="Do not automatically open the browser for mermaid output. `--target` value will be ignored.",
        action="store_true",
    )
    parser.set_defaults(_handler=handle_mermaid)
