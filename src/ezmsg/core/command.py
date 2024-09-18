import os
import sys
import base64
import asyncio
import argparse
import logging
import subprocess
import typing
import webbrowser

from collections import defaultdict
from uuid import uuid4
from textwrap import indent

from .graphserver import GraphService
from .shmserver import SHMService
from .netprotocol import (
    Address,
    GRAPHSERVER_ADDR_ENV,
    GRAPHSERVER_PORT_DEFAULT,
    SHMSERVER_ADDR_ENV,
    SHMSERVER_PORT_DEFAULT,
    PUBLISHER_START_PORT_ENV,
    PUBLISHER_START_PORT_DEFAULT,
    close_stream_writer,
)
from .dag import DAG

logger = logging.getLogger("ezmsg")

IND = "  "


def cmdline() -> None:
    parser = argparse.ArgumentParser(
        "ezmsg.core",
        description="start and stop core ezmsg server processes",
        epilog=f"""
            You can also change server configuration with environment variables.
            GraphServer will be hosted on ${GRAPHSERVER_ADDR_ENV} (default port: {GRAPHSERVER_PORT_DEFAULT}).  
            SHMServer will be hosted on ${SHMSERVER_ADDR_ENV} (default port: {SHMSERVER_PORT_DEFAULT}).
            Publishers will be assigned available ports starting from {PUBLISHER_START_PORT_DEFAULT}. (Change with ${PUBLISHER_START_PORT_ENV})
        """,
    )

    parser.add_argument(
        "command",
        help="command for ezmsg",
        choices=["serve", "start", "shutdown", "graphviz", "mermaid"],
    )

    parser.add_argument("--address", help="Address for GraphServer", default=None)

    class Args:
        command: str
        address: typing.Optional[str]

    args = parser.parse_args(namespace=Args)

    graph_address = Address("127.0.0.1", GRAPHSERVER_PORT_DEFAULT)
    if args.address is not None:
        graph_address = Address.from_string(args.address)
    shm_address_str = os.environ.get(
        SHMSERVER_ADDR_ENV, f"127.0.0.1:{SHMSERVER_PORT_DEFAULT}"
    )
    shm_address = Address.from_string(shm_address_str)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(run_command(args.command, graph_address, shm_address))


async def run_command(cmd: str, graph_address: Address, shm_address: Address) -> None:
    shm_service = SHMService(shm_address)
    graph_service = GraphService(graph_address)

    if cmd == "serve":
        logger.info(f"GraphServer Address: {graph_address}")
        logger.info(f"SHMServer Address: {shm_address}")

        shm_server = shm_service.create_server()
        graph_server = graph_service.create_server()

        try:
            logger.info("Servers running...")
            graph_server.join()

        except KeyboardInterrupt:
            logger.info("Interrupt detected; shutting down servers")

        finally:
            if graph_server is not None:
                graph_server.stop()

            if shm_server is not None:
                shm_server.stop()

    elif cmd == "start":
        popen = subprocess.Popen(
            [sys.executable, "-m", "ezmsg.core", "serve", f"--address={graph_address}"]
        )

        while True:
            try:
                _, writer = await graph_service.open_connection()
                await close_stream_writer(writer)
                _, writer = await shm_service.open_connection()
                await close_stream_writer(writer)
                break
            except ConnectionRefusedError:
                await asyncio.sleep(0.1)

        logger.info(f"Forked ezmsg servers in PID: {popen.pid}")

    elif cmd == "shutdown":
        try:
            await graph_service.shutdown()
            logger.info(
                f"Issued shutdown command to GraphServer @ {graph_service.address}"
            )

        except ConnectionRefusedError:
            logger.warning(
                f"Could not issue shutdown command to GraphServer @ {graph_service.address}; server not running?"
            )

    elif cmd == "graphviz":
        try:
            dag: DAG = await graph_service.dag()
        except (ConnectionRefusedError, ConnectionResetError):
            logger.info(
                f"GraphServer not running @{graph_address}, or host is refusing connections"
            )
            return

        graph_connections = dag.graph.copy()
        # Let's eliminate proxy topics, i.e. connections with inputs and outputs.
        source_nodes = []
        for node, conns in graph_connections.items():
            if len(conns) > 0:
                source_nodes += [node]
        proxy_topics = []
        for conns in graph_connections.values():
            for conn in conns:
                if conn in source_nodes and conn not in proxy_topics:
                    proxy_topics += [conn]
        # Replace Proxy Topics with actual source and downstream
        for proxy_topic in proxy_topics:
            downstreams = graph_connections.pop(proxy_topic)
            logger.info(f"{proxy_topic} downstream connetions: {downstreams}")
            for node, conns in graph_connections.items():
                for conn in conns:
                    if conn == proxy_topic:
                        new_conns = conns.copy()
                        new_conns.remove(proxy_topic)
                        new_conns.union(downstreams)
                        logger.info(
                            f"Updating connections for {node} from {conns} to {new_conns}"
                        )
                        graph_connections[node] = new_conns

        graph_connections = dag.graph.copy()
        # Let's come up with UUID node names
        nodes = set(graph_connections.keys())
        node_map = {name: f'"{str(uuid4())}"' for name in nodes}

        # Construct the graph
        def tree():
            return defaultdict(tree)

        graph: defaultdict = tree()

        connections = ""
        for node, conns in graph_connections.items():
            subgraph = graph
            path = node.split("/")
            route = path[:-1]
            stream = path[-1]
            for seg in route:
                subgraph = subgraph[seg]
            subgraph[stream] = node

            for sub in conns:
                connections += f"{node_map[node]} -> {node_map[sub]};" + "\n"

        # Now convert to dot syntax
        def recurse_graph(g: defaultdict):
            out = ""
            for leaf in g:
                if isinstance(g[leaf], defaultdict):
                    out += indent(
                        "\n".join(
                            [
                                f"subgraph {leaf.lower()} {{",
                                indent("cluster = true;", IND),
                                indent(f'label = "{leaf}";', IND),
                                f"{recurse_graph(g[leaf])}}};",
                                "",
                            ]
                        ),
                        IND,
                    )
                elif isinstance(g[leaf], str):
                    out += indent(f"{node_map[g[leaf]]} [label={leaf}];" + "\n", IND)
            return out

        subgraph_tree = recurse_graph(graph)

        graphviz_out = "\n".join(
            [
                "digraph EZ {",
                indent('rankdir="LR"', IND),
                subgraph_tree,
                indent(connections, IND),
                "}",
            ]
        )

        print(graphviz_out)
    elif cmd == "mermaid":
        try:
            dag: DAG = await graph_service.dag()
        except (ConnectionRefusedError, ConnectionResetError):
            logger.info(
                f"GraphServer not running @{graph_address}, or host is refusing connections"
            )
            return

        graph_connections = dag.graph.copy()
        # Let's eliminate proxy topics, i.e. connections with inputs and outputs.
        source_nodes = []
        for node, conns in graph_connections.items():
            if len(conns) > 0:
                source_nodes += [node]
        proxy_topics = []
        for conns in graph_connections.values():
            for conn in conns:
                if conn in source_nodes and conn not in proxy_topics:
                    proxy_topics += [conn]
        # Replace Proxy Topics with actual source and downstream
        for proxy_topic in proxy_topics:
            downstreams = graph_connections.pop(proxy_topic)
            logger.info(f"{proxy_topic} downstream connetions: {downstreams}")
            for node, conns in graph_connections.items():
                for conn in conns:
                    if conn == proxy_topic:
                        new_conns = conns.copy()
                        new_conns.remove(proxy_topic)
                        new_conns.union(downstreams)
                        logger.info(
                            f"Updating connections for {node} from {conns} to {new_conns}"
                        )
                        graph_connections[node] = new_conns

        graph_connections = dag.graph.copy()
        # Let's come up with UUID node names
        nodes = set(graph_connections.keys())
        node_map = {name: f"{str(uuid4())}" for name in nodes}

        # Construct the graph
        def tree():
            return defaultdict(tree)

        graph: defaultdict = tree()

        connections = ""
        for node, conns in graph_connections.items():
            subgraph = graph
            path = node.split("/")
            route = path[:-1]
            stream = path[-1]
            for seg in route:
                subgraph = subgraph[seg]
            subgraph[stream] = node

            for sub in conns:
                connections += f"{node_map[node]} --> {node_map[sub]}" + "\n"

        # Now convert to dot syntax
        def recurse_graph(g: defaultdict):
            out = ""
            for leaf in g:
                if isinstance(g[leaf], defaultdict):
                    out += indent(
                        "\n".join(
                            [
                                f"subgraph {leaf.lower()} [{leaf}]",
                                # "direction LR",
                                f"{recurse_graph(g[leaf])}",
                                "end",
                                "",
                            ]
                        ),
                        IND,
                    )
                elif isinstance(g[leaf], str):
                    out += indent(f"{node_map[g[leaf]]}[{leaf}]" + "\n", IND)
            return out

        subgraph_tree = recurse_graph(graph)

        mermaid = "\n".join(
            [
                "flowchart LR",
                subgraph_tree,
                indent(connections, IND),
            ]
        )

        # print(mermaid)
        webbrowser.open(mm(mermaid))


def mm(graph):
    graphbytes = graph.encode("utf8")
    base64_bytes = base64.b64encode(graphbytes)
    base64_string = base64_bytes.decode("ascii")
    return f"https://mermaid.ink/img/{base64_string}"
