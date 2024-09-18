from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field

from typing import List, Set, DefaultDict


class CyclicException(Exception): ...


GraphType = DefaultDict[str, Set[str]]


@dataclass
class DAG:
    graph: GraphType = field(default_factory=lambda: defaultdict(set), init=False)

    @property
    def nodes(self) -> Set[str]:
        return set(self.graph.keys())

    @property
    def invgraph(self) -> GraphType:
        """Dumb and inefficient, but probably alright for now."""
        invgraph = defaultdict(set)
        for from_node, to_nodes in self.graph.items():
            invgraph[from_node]
            for to_node in to_nodes:
                invgraph[to_node].add(from_node)
        return invgraph

    def add_edge(self, from_node: str, to_node: str) -> None:
        """
        Ensure an edge exists in the graph.
        Does nothing if edge exists.
        If edge would make graph cyclic, raises CyclicException
        """
        if from_node == to_node:
            raise CyclicException

        test_graph = deepcopy(self.graph)
        test_graph[from_node].add(to_node)
        test_graph[to_node]

        for node in test_graph.keys():
            if node in _bfs(test_graph, node):
                raise CyclicException

        # No cycles!  Modify referenced data structure
        self.graph[from_node].add(to_node)
        self.graph[to_node]

    def remove_edge(self, from_node: str, to_node: str) -> None:
        """
        Ensure an edge is not present in the graph.
        Does nothing if edge doesn't exist
        """
        self.graph.get(from_node, set()).discard(to_node)
        self._prune()

    def downstream(self, from_node: str) -> List[str]:
        """Get a list of downstream nodes (including from_node)"""
        return _bfs(self.graph, from_node) + [from_node]

    def upstream(self, from_node: str) -> List[str]:
        """Get a list of upstream nodes (including from_node)"""
        return _bfs(self.invgraph, from_node) + [from_node]

    def _prune(self) -> None:
        """Remove unconnected nodes from graph"""
        leaves = _leaves(self.graph)
        inv_leaves = _leaves(self.invgraph)
        unconnected = leaves & inv_leaves
        for node in unconnected:
            del self.graph[node]


def _leaves(graph: GraphType) -> Set[str]:
    return set([f for f, t in graph.items() if len(t) == 0])


# Bredth First Search of a graph


def _bfs(graph: GraphType, node: str) -> List[str]:
    connected: Set[str] = set()
    queue = [node]
    while queue:
        queue += [t for t in graph.get(queue.pop(0), set()) if t not in connected]
        connected.update(queue)
    return list(connected)
