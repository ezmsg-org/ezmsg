from collections import defaultdict
from textwrap import indent
from typing import Set, DefaultDict, Optional, Tuple, List
from uuid import uuid4


GraphType = DefaultDict[str, Set[str]]
IND = "  "


def prune_graph_connections(
    graph_connections: GraphType,
) -> Tuple[Optional[GraphType], Optional[List[str]]]:
    """
    Remove nodes from the graph that are proxy_topics, i.e. nodes that are both
    source and target nodes in the connections graph.
    """
    graph_conns = graph_connections.copy()
    source_nodes = []
    for node, conns in graph_conns.items():
        if len(conns) > 0:
            source_nodes += [node]
    proxy_topics = []
    for conns in graph_conns.values():
        for conn in conns:
            if conn in source_nodes and conn not in proxy_topics:
                proxy_topics += [conn]

    # Replace Proxy Topics with actual source and downstream
    for proxy_topic in proxy_topics:
        downstreams = graph_conns.pop(proxy_topic)
        for node, conns in graph_conns.items():
            for conn in conns:
                if conn == proxy_topic:
                    new_conns = conns.copy()
                    new_conns.remove(proxy_topic)
                    new_conns |= downstreams
                    graph_conns[node] = new_conns

    return graph_conns, proxy_topics


def _pipeline_levels(
    graph_connections: GraphType, level_separator: str = "/"
) -> defaultdict:
    """
    In ezmsg, a pipeline is built with units/collections, with subcomponents
    that are either more units/collection or input/output streams. The graph of
    the connections are stored in a DAG (directed acyclic graph object), but the
    nodes themselves represent the hierarchical levels of the pipeline in their
    names which are strings of the form `top_level/sub_level/.../node_name`, where
    each level is separated by a level separator (default is `/`).
    This function computes the level of each component in the hierarchy (not just
    the connection nodes) and returns a dictionary of the level of each pipeline
    parts, where 0 is the level of the connection (leaf) nodes.
    """
    graph_levels = defaultdict(int)

    def update_level(node: str) -> None:
        """
        Update levels of a leaf node and all pipeline parent nodes.
        Note, assumes node is a leaf node of the forest.
        """
        depth = 0
        while len(node) > 0:
            graph_levels[node] = max(graph_levels[node], depth)
            node = _get_parent_node(node, level_separator)
            depth += 1

    for node, conns in graph_connections.items():
        update_level(node)
        for conn in conns:
            update_level(conn)

    return graph_levels


def _get_parent_node(node: str, level_separator: str = "/") -> str:
    return node.rsplit(level_separator, 1)[0] if level_separator in node else ""


class LeafNodeException(Exception):
    """Raised when connection nodes are not leaf nodes in the pipeline hierarchy."""


def get_compactified_graph(
    graph_connections: GraphType, compact_level: int = 0, level_separator: str = "/"
) -> GraphType:
    """
    Used for graphical visualization of ezmsg pipelines.
    Compactifies the graph connections dictionary by removing all parts that
    are at levels < compact level. Leaf nodes (streams in ezmsg) are level 0,
    so removing only them requires compact_level = 1.
    *Note: When collections contain streams at the same level as subunits or
    subcollections, removing them can make the pipeline diagram unclear, so
    they are left as is.
    Returns graph of compactified connections.
    Note: compactified graphs need no longer be DAGs.
    """

    if compact_level == 0 or len(graph_connections) == 0:
        return graph_connections

    try:
        graph_levels = _validate_connections_are_leaf_nodes(
            graph_connections, level_separator
        )
    except LeafNodeException as e:
        raise LeafNodeException(f"Cannot compactify graph: {e}") from e

    def get_node_update(node: str) -> str:
        if node not in update_node_map:
            parent_node = _get_parent_node(node, level_separator)
            if parent_node == "":
                # don't update node if it's already a forest root (i.e., no parent)
                update_node_map[node] = node
            else:
                node_level = graph_levels[node]
                if graph_levels[parent_node] == node_level + 1:
                    # node is in component with all subcomponents at same level
                    update_node_map[node] = parent_node
                else:
                    # node is in component with deeper subcomponents than node
                    graph_levels[node] += node_level + 1
                    update_node_map[node] = node
        return update_node_map[node]

    level = 0
    while level < compact_level:
        update_node_map = defaultdict(str)
        temp_connections = defaultdict(set)
        for node, conns in graph_connections.items():
            source = get_node_update(node)
            targets = [get_node_update(conn) for conn in conns]
            temp_connections[source] |= set(targets)
            temp_connections[source].discard(source)
        level += 1
        graph_connections = temp_connections

    return graph_connections


def _validate_connections_are_leaf_nodes(
    graph_connections: GraphType, level_separator: str = "/"
) -> defaultdict:
    """
    Validate that all nodes in the graph_connections dictionary are leaf nodes in the
    pipeline hierarchy that is represented in the node names. See `forest_levels` for
    more details on the hierarchy.
    """
    graph_levels = _pipeline_levels(graph_connections, level_separator)
    for leaf_node, leaf_conns in graph_connections.items():
        nodes = [leaf_node] + [conn for conn in leaf_conns]
        for node in nodes:
            if graph_levels[node] != 0:
                raise LeafNodeException(
                    f"The node '{node}' is a connection node, but not a leaf node in the pipeline hierarchy."
                )
    return graph_levels


def graph_string(
    graph_connections: GraphType,
    fmt: str,
    direction: str = "LR",
    level_separator: str = "/",
) -> str:
    """
    Returns a string representation of the graph connections for displaying with mermaid or graphviz.
    """
    if fmt not in ["mermaid", "graphviz"]:
        raise ValueError(f"Invalid format '{fmt}'. Options are 'mermaid' or 'graphviz'")

    # Let's come up with UUID node names
    node_map = {}
    for name in graph_connections.keys():
        if fmt == "mermaid":
            node_map.setdefault(name, f"{str(uuid4())}")
        else:
            node_map.setdefault(name, f'"{str(uuid4())}"')

    # Construct the graph
    def tree():
        return defaultdict(tree)

    graph: defaultdict = tree()

    connections = ""
    for node, conns in graph_connections.items():
        subgraph = graph
        path = node.split(level_separator)
        for seg in path[:-1]:
            subgraph = subgraph[seg]
        subgraph[path[-1]] = node

        for conn in sorted(conns):
            if fmt == "mermaid":
                connections += f"{node_map[node]} --> {node_map[conn]}" + "\n"
            else:  # fmt == "graphviz"
                connections += f"{node_map[node]} -> {node_map[conn]};" + "\n"

    if fmt == "graphviz":
        header = [
            "digraph EZ {",
            indent(f'rankdir="{direction}"', IND),
        ]
        footer = ["}"]

        def node_string(graph: defaultdict, node: str) -> Optional[str]:
            out = None
            if isinstance(graph[node], defaultdict):
                out = [
                    f"subgraph {node.lower()} {{",
                    indent("cluster = true;", IND),
                    indent(f'label = "{node}";', IND),
                    f"{graph_structure_string(graph[node])}}};",
                ]
            elif isinstance(graph[node], str):
                out = [f"{node_map[graph[node]]} [label={node}];"]
            return out

    else:  # fmt == mermaid
        header = [f"flowchart {direction}"]
        footer = []

        def node_string(graph: defaultdict, node: str) -> Optional[str]:
            out = None
            if isinstance(graph[node], defaultdict):
                out = [
                    f"subgraph {node.lower()} [{node}]",
                    # "direction LR",
                    f"{graph_structure_string(graph[node])}",
                    "end",
                ]
            elif isinstance(graph[node], str):
                out = [f"{node_map[graph[node]]}[{node}]"]
            return out

    def graph_structure_string(graph: defaultdict) -> str:
        out = ""
        for node in graph:
            node_string_list = node_string(graph, node)
            if node_string_list is not None:
                node_string_list += [""]  # Append a newline
                out += indent("\n".join(node_string_list), IND)
        return out[:-1]

    graph_out = "\n".join(
        header + [graph_structure_string(graph), indent(connections, IND)] + footer
    )
    return graph_out
