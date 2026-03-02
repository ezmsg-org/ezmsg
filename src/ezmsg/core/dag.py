from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from collections.abc import Hashable


class CyclicException(Exception):
    """
    Exception raised when an operation would create a cycle in the DAG.

    This exception is raised when attempting to add an edge that would
    violate the acyclic property of the directed acyclic graph.
    """

    ...


GraphType = defaultdict[str, set[str]]
EdgeType = tuple[str, str]
OwnerType = Hashable | None


@dataclass
class DAG:
    """
    Directed Acyclic Graph implementation for managing dependencies and connections.

    The DAG class provides functionality to build and maintain a directed acyclic graph,
    which is used by ezmsg to manage message flow between components while ensuring
    no circular dependencies exist.
    """

    graph: GraphType = field(default_factory=lambda: defaultdict(set), init=False)
    edge_owners: dict[EdgeType, set[OwnerType]] = field(
        default_factory=dict, init=False
    )

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

    def add_edge(
        self, from_node: str, to_node: str, owner: OwnerType = None
    ) -> bool:
        """
        Ensure an edge exists in the graph.

        Adds an edge from from_node to to_node for the given owner.
        If this is an additional owner for an existing edge, topology does not change.
        If the edge would make the graph cyclic, raises CyclicException.

        :param from_node: Source node name
        :type from_node: str
        :param to_node: Destination node name
        :type to_node: str
        :param owner: Owner token for this edge; ``None`` is treated as persistent.
        :type owner: collections.abc.Hashable | None
        :raises CyclicException: If adding the edge would create a cycle
        :return: True if graph topology changed; False if this only added an owner.
        :rtype: bool
        """
        if from_node == to_node:
            raise CyclicException

        edge = (from_node, to_node)
        owners = self.edge_owners.setdefault(edge, set())
        if owner in owners:
            return False

        topology_changed = len(owners) == 0
        if topology_changed:
            test_graph = deepcopy(self.graph)
            test_graph[from_node].add(to_node)
            test_graph[to_node]

            if from_node in _bfs(test_graph, from_node):
                if len(owners) == 0:
                    self.edge_owners.pop(edge, None)
                raise CyclicException

            # No cycles! Modify referenced data structure
            self.graph[from_node].add(to_node)
            self.graph[to_node]

        owners.add(owner)
        return topology_changed

    def remove_edge(
        self, from_node: str, to_node: str, owner: OwnerType = None
    ) -> bool:
        """
        Ensure an edge is not present in the graph.

        Removes ownership of an edge from from_node to to_node.
        Topology only changes when the last owner is removed.
        Automatically prunes unconnected nodes after removal.

        :param from_node: Source node name
        :type from_node: str
        :param to_node: Destination node name
        :type to_node: str
        :param owner: Owner token for this edge; ``None`` targets persistent ownership.
        :type owner: collections.abc.Hashable | None
        :return: True if graph topology changed; False if owner was absent or still shared.
        :rtype: bool
        """
        edge = (from_node, to_node)
        owners = self.edge_owners.get(edge, None)
        if owners is None or owner not in owners:
            return False

        owners.remove(owner)

        topology_changed = False
        if len(owners) == 0:
            self.edge_owners.pop(edge, None)
            self.graph.get(from_node, set()).discard(to_node)
            self._prune()
            topology_changed = True

        return topology_changed

    def remove_owner(self, owner: OwnerType) -> set[EdgeType]:
        removed_edges: set[EdgeType] = set()
        for edge in list(self.edge_owners.keys()):
            if owner in self.edge_owners.get(edge, set()):
                if self.remove_edge(*edge, owner=owner):
                    removed_edges.add(edge)
        return removed_edges

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
