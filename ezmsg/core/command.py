import os
import asyncio
import argparse
import logging
import subprocess
import typing

from collections import defaultdict
from uuid import uuid4
from textwrap import indent

import ezmsg.core as ez

from .graphserver import GraphServer, GraphService
from .shmserver import SHMServer, SHMService
from .netprotocol import (
    Address,
    GRAPHSERVER_ADDR_ENV,
    GRAPHSERVER_PORT_DEFAULT,
    SHMSERVER_ADDR_ENV,
    SHMSERVER_PORT_DEFAULT,
    PUBLISHER_START_PORT_ENV,
    PUBLISHER_START_PORT_DEFAULT,
    close_stream_writer
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
        choices=["serve", "start", "graphviz"],
    )

    parser.add_argument(
        "--address",
        help="Address for GraphServer", 
        default=None
    )

    class Args:
        command: str
        address: typing.Optional[str]

    args = parser.parse_args(namespace=Args)

    graph_address = Address('127.0.0.1', GRAPHSERVER_PORT_DEFAULT)
    if args.address is not None:
        graph_address = Address.from_string(args.address)
    shm_address_str = os.environ.get(SHMSERVER_ADDR_ENV, f'127.0.0.1:{SHMSERVER_PORT_DEFAULT}')
    shm_address = Address.from_string(shm_address_str)

    asyncio.run(run_command(args.command, graph_address, shm_address))


async def run_command(cmd: str, graph_address: Address, shm_address: Address) -> None:
    ez.SHM = SHMService(shm_address)
    ez.GRAPH = GraphService(graph_address)

    if cmd == "serve":
        logger.info(f"GraphServer Address: {graph_address}")
        logger.info(f'SHMServer Address: {shm_address}')

        shm_server = ez.SHM.create_server()
        graph_server = ez.GRAPH.create_server()

        try:
            logger.info('Servers running...')
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
            ["python", "-m", "ezmsg.core", "serve", f"--address={graph_address}"]
        )

        while True:
            try:
                reader, writer = await ez.GRAPH.open_connection()
                await close_stream_writer(writer)
                reader, writer = await ez.SHM.open_connection()
                await close_stream_writer(writer)
                break
            except ConnectionRefusedError:
                await asyncio.sleep(0.1)

        logger.info(f"Forked ezmsg servers in PID: {popen.pid}")

    elif cmd == "graphviz":
        try:
            dag: DAG = await ez.GRAPH.dag()
        except (ConnectionRefusedError, ConnectionResetError):
            logger.info(
                f"GraphServer not running @{graph_address}, or host is refusing connections"
            )
            return

        # Let's come up with UUID node names
        node_map = {name: f'"{str(uuid4())}"' for name in dag.nodes}

        # Construct the graph
        def tree():
            return defaultdict(tree)

        graph: defaultdict = tree()

        connections = ""
        for node, conns in dag.graph.items():
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
                if type(g[leaf]) == defaultdict:
                    out += indent(
                        "\n".join(
                            [
                                f"subgraph {leaf.lower()} {{",
                                indent(f"cluster = true;", IND),
                                indent(f'label = "{leaf}";', IND),
                                f"{recurse_graph(g[leaf])}}};",
                                "",
                            ]
                        ),
                        IND,
                    )
                elif type(g[leaf]) == str:
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
