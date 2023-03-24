import os
import asyncio
import argparse
import logging
import subprocess
import typing

from collections import defaultdict
from uuid import uuid4
from textwrap import indent

from .graphserver import GraphServer
from .shmserver import SHMServer
from .netprotocol import (
    Address,
    GRAPHSERVER_ADDR_ENV,
    GRAPHSERVER_PORT_DEFAULT,
    SHMSERVER_ADDR_ENV,
    SHMSERVER_PORT_DEFAULT,
    PUBLISHER_START_PORT_ENV,
    PUBLISHER_START_PORT_DEFAULT,
)
from .dag import DAG

logger = logging.getLogger("ezmsg")

IND = "  "

def cmdline() -> None:

    parser = argparse.ArgumentParser(
        "ezmsg.core",
        description="start and stop core ezmsg server processes",
        epilog=f"""
            Change server configuration with environment variables.
            GraphServer will be hosted on ${GRAPHSERVER_ADDR_ENV} (default port: {GRAPHSERVER_PORT_DEFAULT}).  
            SHMServer will be hosted on ${SHMSERVER_ADDR_ENV} (default port: {SHMSERVER_PORT_DEFAULT}).
            Publishers will be assigned available ports starting from {PUBLISHER_START_PORT_DEFAULT}. (Change with ${PUBLISHER_START_PORT_ENV})
        """,
    )

    parser.add_argument(
        "command",
        help="command for ezmsg",
        choices=["serve", "shutdown", "start", "graphviz"],
    )

    parser.add_argument(
        "--address",
        type = str, 
        help="Address for GraphServer", 
        default=f'127.0.0.1:{GRAPHSERVER_PORT_DEFAULT}'
    )

    class Args:
        command: str
        address: str

    args = parser.parse_args(namespace=Args)

    address = Address.from_string(args.address)
    logger.info(f"GraphServer Address: {address}")

    shmserver_address_str = os.environ.get(SHMSERVER_ADDR_ENV, f'127.0.0.1:{SHMSERVER_PORT_DEFAULT}')
    logger.info(f'SHMServer Address: {shmserver_address_str} use ${SHMSERVER_ADDR_ENV} to change.')

    asyncio.run(run_command(args.command, **vars(args)))


async def run_command(cmd: str, address: Address, **kwargs) -> None:

    if cmd == "shutdown":
        try:
            await GraphServer.Connection(address).shutdown()
            logger.info(f"Shutdown GraphServer running @{address}")
        except (ConnectionRefusedError, ConnectionResetError):
            logger.info(
                f"GraphServer not running @{address}, or host is refusing connections"
            )

        try:
            await SHMServer.shutdown_server()
            logger.info("Shutdown SHMServer")
        except ConnectionRefusedError:
            logger.info("SHMServer not running, or is refusing connections")

    elif cmd == "serve":
        graph_server = None
        shm_server = await SHMServer.connect()
        try:
            graph_server = await GraphServer.connect(address)

            if graph_server is None:
                logger.info(f"GraphServer already running @ {address}")
            else:
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
            ["python", "-m", "ezmsg.core", "serve", f"--address={address}"]
        )

        while True:
            try:
                await GraphServer.open(address)
                break
            except ConnectionRefusedError:
                await asyncio.sleep(0.1)

        logger.info(f"Forked ezmsg servers.")

    elif cmd == "graphviz":
        try:
            dag: DAG = await GraphServer.Connection(address).dag()
        except (ConnectionRefusedError, ConnectionResetError):
            logger.info(
                f"GraphServer not running @{address}, or host is refusing connections"
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
