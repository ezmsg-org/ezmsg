from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field


class CyclicException(Exception):
    """
    Exception raised when an operation would create a cycle in the DAG.

    This exception is raised when attempting to add an edge that would
    violate the acyclic property of the directed acyclic graph.
    """

    ...


GraphType = defaultdict[str, set[str]]


@dataclass
class DAG:
    """
    Directed Acyclic Graph implementation for managing dependencies and connections.

    The DAG class provides functionality to build and maintain a directed acyclic graph,
    which is used by ezmsg to manage message flow between components while ensuring
    no circular dependencies exist.
    """

    graph: GraphType = field(default_factory=lambda: defaultdict(set), init=False)

    @property
    def nodes(self) -> set[str]:
        """
        Get all nodes in the graph.

        :return: Set of all node names in the graph
        :rtype: set[str]
        """
        return set(self.graph.keys())

    @property
    def invgraph(self) -> GraphType:
        """
        Get the inverse (reversed) graph.

        Creates a graph where all edges are reversed. This is useful for
        finding upstream dependencies.

        :return: Inverted graph representation
        :rtype: GraphType

        .. note::
           This is currently implemented inefficiently but is adequate for typical use cases.
        """
        invgraph = defaultdict(set)
        for from_node, to_nodes in self.graph.items():
            invgraph[from_node]
            for to_node in to_nodes:
                invgraph[to_node].add(from_node)
        return invgraph

    def add_edge(self, from_node: str, to_node: str) -> None:
        """
        Ensure an edge exists in the graph.

        Adds an edge from from_node to to_node. Does nothing if the edge already exists.
        If the edge would make the graph cyclic, raises CyclicException.

        :param from_node: Source node name
        :type from_node: str
        :param to_node: Destination node name
        :type to_node: str
        :raises CyclicException: If adding the edge would create a cycle
        """
        if from_node == to_node:
            raise CyclicException

        test_graph = deepcopy(self.graph)
        test_graph[from_node].add(to_node)
        test_graph[to_node]

        if from_node in _bfs(test_graph, from_node):
            raise CyclicException

        # No cycles!  Modify referenced data structure
        self.graph[from_node].add(to_node)
        self.graph[to_node]

    def remove_edge(self, from_node: str, to_node: str) -> None:
        """
        Ensure an edge is not present in the graph.

        Removes an edge from from_node to to_node. Does nothing if the edge doesn't exist.
        Automatically prunes unconnected nodes after removal.

        :param from_node: Source node name
        :type from_node: str
        :param to_node: Destination node name
        :type to_node: str
        """
        self.graph.get(from_node, set()).discard(to_node)
        self._prune()

    def downstream(self, from_node: str) -> list[str]:
        """
        Get a list of downstream nodes (including from_node).

        Performs a breadth-first search to find all nodes reachable from the given node.

        :param from_node: Starting node name
        :type from_node: str
        :return: List of downstream node names including the starting node
        :rtype: list[str]
        """
        return _bfs(self.graph, from_node) + [from_node]

    def upstream(self, from_node: str) -> list[str]:
        """
        Get a list of upstream nodes (including from_node).

        Performs a breadth-first search on the inverted graph to find all nodes
        that can reach the given node.

        :param from_node: Starting node name
        :type from_node: str
        :return: List of upstream node names including the starting node
        :rtype: list[str]
        """
        return _bfs(self.invgraph, from_node) + [from_node]

    def _prune(self) -> None:
        """Remove unconnected nodes from graph"""
        leaves = _leaves(self.graph)
        inv_leaves = _leaves(self.invgraph)
        unconnected = leaves & inv_leaves
        for node in unconnected:
            del self.graph[node]


def _leaves(graph: GraphType) -> set[str]:
    """
    Find leaf nodes in a graph.

    Leaf nodes are nodes that have no outgoing edges.

    :param graph: The graph to analyze
    :type graph: GraphType
    :return: Set of leaf node names
    :rtype: set[str]
    """
    return {f for f, t in graph.items() if len(t) == 0}


def _bfs(graph: GraphType, node: str) -> list[str]:
    """
    Breadth-first search of a graph starting from a given node.

    Traverses the graph in breadth-first order to find all reachable nodes.

    :param graph: The graph to search
    :type graph: GraphType
    :param node: Starting node for the search
    :type node: str
    :return: List of reachable node names (excluding the starting node)
    :rtype: list[str]
    """
    connected: set[str] = set()
    queue = [node]
    while queue:
        queue += [t for t in graph.get(queue.pop(0), set()) if t not in connected]
        connected.update(queue)
    return list(connected)
