import pytest

from ezmsg.core.dag import DAG, CyclicException

simple_graph = [
    ("a", "b"),
    ("a", "c"),
    ("b", "d"),
    ("d", "e"),
    ("w", "x"),
    ("w", "y"),
    ("x", "z"),
    ("y", "z"),
]


def test_simple_dag():
    graph = DAG()

    for edge in simple_graph:
        graph.add_edge(*edge)

    assert graph.graph.keys() == graph.invgraph.keys()


def test_cycles():
    graph = DAG()

    with pytest.raises(CyclicException):
        graph.add_edge("a", "a")

    graph.add_edge("a", "b")

    with pytest.raises(CyclicException):
        graph.add_edge("b", "a")

    graph.add_edge("b", "c")

    assert graph.graph.keys() == graph.invgraph.keys()

    with pytest.raises(CyclicException):
        graph.add_edge("c", "a")

    graph.remove_edge("b", "c")
    graph.add_edge("c", "a")

    with pytest.raises(CyclicException):
        graph.add_edge("b", "c")

    graph.add_edge("c", "b")

    assert graph.graph.keys() == graph.invgraph.keys()


def test_remove():
    graph = DAG()

    assert graph.graph.keys() == graph.invgraph.keys()
    assert len(graph.nodes) == 0

    # Removing a non-existant edge should be fine
    graph.remove_edge("tx", "rx")
    assert graph.graph.keys() == graph.invgraph.keys()
    assert len(graph.nodes) == 0

    graph.add_edge("a", "b")
    graph.add_edge("b", "c")

    # Check pruning
    graph.remove_edge("a", "b")
    assert "a" not in graph.nodes
    assert graph.graph.keys() == graph.invgraph.keys()

    # Check complete pruning
    graph.remove_edge("b", "c")
    assert graph.graph.keys() == graph.invgraph.keys()
    assert len(graph.nodes) == 0

    for edge in simple_graph:
        graph.add_edge(*edge)

    graph.remove_edge("w", "x")
    assert "w" in graph.nodes
    graph.remove_edge("x", "z")
    assert "x" not in graph.nodes
    graph.remove_edge("w", "y")
    graph.remove_edge("y", "z")
    assert len(set(["w", "x", "y", "z"]) & graph.nodes) == 0


def test_bfs():
    graph = DAG()

    for edge in simple_graph:
        graph.add_edge(*edge)

    assert set(graph.downstream("b")) == set(["b", "d", "e"])
    assert set(graph.upstream("z")) == set(["w", "x", "y", "z"])
    assert set(graph.downstream("z")) == set(["z"])
    assert set(graph.upstream("a")) == set(["a"])
    assert set(graph.upstream("JERRY")) == set(["JERRY"])
    assert set(graph.downstream("JERRY")) == set(["JERRY"])
    assert "JERRY" not in graph.graph.keys()
    assert "JERRY" not in graph.invgraph.keys()

    assert graph.graph.keys() == graph.invgraph.keys()


if __name__ == "__main__":
    test_simple_dag()
    test_cycles()
    test_remove()
    test_bfs()
