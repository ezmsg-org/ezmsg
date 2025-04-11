import asyncio
import logging
import pytest

from ezmsg.core.dag import DAG
from ezmsg.core.graph_util import (
    LeafNodeException,
    _get_parent_node,
    _pipeline_levels,
    get_compactified_graph,
    graph_string,
    prune_graph_connections,
)
from ezmsg.core.graphserver import GraphService
from ezmsg.core.netprotocol import (
    Address,
    GRAPHSERVER_PORT_DEFAULT,
)
from unittest.mock import patch, AsyncMock

logger = logging.getLogger("ezmsg")

TEST_GRAPH_ADDRESS = Address("127.0.0.1", GRAPHSERVER_PORT_DEFAULT)


test_hierarchical_edges = [
    ("SIMPLE_PUB/OUTPUT", "COLLECTION/INPUT"),
    ("COLLECTION/INPUT", "COLLECTION/VERIFIER/INPUT"),
    ("COLLECTION/VERIFIER/OUTPUT_SAMPLE", "COLLECTION/OUTPUT_SAMPLE"),
    ("COLLECTION/VERIFIER/OUTPUT_SIGNAL", "COLLECTION/TERMINATE/INPUT"),
    ("COLLECTION/VERIFIER/OUTPUT_SIGNAL", "COLLECTION/LOG/INPUT"),
    ("COLLECTION/LOG/OUTPUT", "COLLECTION/OUTPUT_SIGNAL"),
    ("COLLECTION/OUTPUT_SIGNAL", "SIMPLE_SUB/INPUT"),
    ("COLLECTION/OUTPUT_SAMPLE", "SIMPLE_SUB/INPUT"),
]


def get_test_dag() -> DAG:
    """
    Returns a mock DAG for testing purposes.
    """
    dag = DAG()
    for edge in test_hierarchical_edges:
        dag.add_edge(*edge)
    return dag


expected_graph_connections = {
    "SIMPLE_PUB/OUTPUT": {"COLLECTION/INPUT"},
    "COLLECTION/INPUT": {"COLLECTION/VERIFIER/INPUT"},
    "COLLECTION/VERIFIER/OUTPUT_SAMPLE": {"COLLECTION/OUTPUT_SAMPLE"},
    "COLLECTION/VERIFIER/OUTPUT_SIGNAL": {
        "COLLECTION/LOG/INPUT",
        "COLLECTION/TERMINATE/INPUT",
    },
    "COLLECTION/LOG/OUTPUT": {"COLLECTION/OUTPUT_SIGNAL"},
    "COLLECTION/OUTPUT_SIGNAL": {"SIMPLE_SUB/INPUT"},
    "COLLECTION/OUTPUT_SAMPLE": {"SIMPLE_SUB/INPUT"},
    "COLLECTION/VERIFIER/INPUT": set(),
    "COLLECTION/LOG/INPUT": set(),
    "COLLECTION/TERMINATE/INPUT": set(),
    "SIMPLE_SUB/INPUT": set(),
}
expected_pruned_graph_connections = {
    "SIMPLE_PUB/OUTPUT": {"COLLECTION/VERIFIER/INPUT"},
    "COLLECTION/VERIFIER/OUTPUT_SAMPLE": {"SIMPLE_SUB/INPUT"},
    "COLLECTION/VERIFIER/OUTPUT_SIGNAL": {
        "COLLECTION/LOG/INPUT",
        "COLLECTION/TERMINATE/INPUT",
    },
    "COLLECTION/LOG/OUTPUT": {"SIMPLE_SUB/INPUT"},
    "COLLECTION/VERIFIER/INPUT": set(),
    "COLLECTION/LOG/INPUT": set(),
    "COLLECTION/TERMINATE/INPUT": set(),
    "SIMPLE_SUB/INPUT": set(),
}
expected_pipeline_levels = {
    "COLLECTION": 2,
    "SIMPLE_PUB": 1,
    "SIMPLE_SUB": 1,
    "COLLECTION/VERIFIER": 1,
    "COLLECTION/LOG": 1,
    "COLLECTION/TERMINATE": 1,
    "SIMPLE_PUB/OUTPUT": 0,
    "COLLECTION/INPUT": 0,
    "COLLECTION/VERIFIER/INPUT": 0,
    "COLLECTION/VERIFIER/OUTPUT_SAMPLE": 0,
    "COLLECTION/VERIFIER/OUTPUT_SIGNAL": 0,
    "COLLECTION/LOG/INPUT": 0,
    "COLLECTION/LOG/OUTPUT": 0,
    "COLLECTION/TERMINATE/INPUT": 0,
    "COLLECTION/OUTPUT_SIGNAL": 0,
    "COLLECTION/OUTPUT_SAMPLE": 0,
    "SIMPLE_SUB/INPUT": 0,
}
expected_compactified_graph_level1_no_prune = {
    "SIMPLE_PUB": {"COLLECTION/INPUT"},
    "COLLECTION/INPUT": {"COLLECTION/VERIFIER"},
    "COLLECTION/VERIFIER": {
        "COLLECTION/TERMINATE",
        "COLLECTION/OUTPUT_SAMPLE",
        "COLLECTION/LOG",
    },
    "COLLECTION/OUTPUT_SAMPLE": {"SIMPLE_SUB"},
    "COLLECTION/OUTPUT_SIGNAL": {"SIMPLE_SUB"},
    "COLLECTION/LOG": {"COLLECTION/OUTPUT_SIGNAL"},
    "COLLECTION/TERMINATE": set(),
    "SIMPLE_SUB": set(),
}
expected_compactified_graph_level1_prune = {
    "SIMPLE_PUB": {"COLLECTION/VERIFIER"},
    "COLLECTION/VERIFIER": {"COLLECTION/TERMINATE", "SIMPLE_SUB", "COLLECTION/LOG"},
    "COLLECTION/LOG": {"SIMPLE_SUB"},
    "COLLECTION/TERMINATE": set(),
    "SIMPLE_SUB": set(),
}
expected_compactified_graph_level2 = {
    "SIMPLE_PUB": {"COLLECTION"},
    "COLLECTION": {"SIMPLE_SUB"},
    "SIMPLE_SUB": set(),
}


def test_prune_graph():
    dag = get_test_dag()

    assert dict(dag.graph) == expected_graph_connections
    pruned_graph = prune_graph_connections(dag.graph)
    assert dict(pruned_graph[0]) == expected_pruned_graph_connections
    assert set(pruned_graph[1]) == {
        "COLLECTION/INPUT",
        "COLLECTION/OUTPUT_SIGNAL",
        "COLLECTION/OUTPUT_SAMPLE",
    }


def test_pipeline_levels():
    dag = get_test_dag()

    levels = _pipeline_levels(dag.graph, level_separator="/")
    assert dict(levels) == expected_pipeline_levels


def test_get_parent_node():
    # Test with a valid node and level separator
    node = "FIRST/SECOND/THIRD"
    level_separator = "/"
    expected_parent_node = "FIRST/SECOND"
    parent_node = _get_parent_node(node, level_separator)
    assert parent_node == expected_parent_node

    # Test with a node that has no parent (root node)
    node = "FIRST"
    expected_parent_node = ""
    parent_node = _get_parent_node(node, level_separator)
    assert parent_node == expected_parent_node

    # Test with an empty string as the node
    node = ""
    expected_parent_node = ""
    parent_node = _get_parent_node(node, level_separator)
    assert parent_node == expected_parent_node


def test_get_compactified_graph():
    dag = get_test_dag()

    compactified_level1_no_prune = get_compactified_graph(dag.graph, compact_level=1)
    assert (
        dict(compactified_level1_no_prune)
        == expected_compactified_graph_level1_no_prune
    )

    pruned_graph = prune_graph_connections(dag.graph)[0]
    compactified_level1_prune = get_compactified_graph(pruned_graph, compact_level=1)
    assert dict(compactified_level1_prune) == expected_compactified_graph_level1_prune

    compactified_level2 = get_compactified_graph(dag.graph, compact_level=2)
    assert dict(compactified_level2) == expected_compactified_graph_level2

    # Test compactification fails when graph of connections
    # is not between leaf nodes of pipeline
    dag.add_edge("COLLECTION/VERIFIER", "SECOND/LOG/INPUT")
    with pytest.raises(LeafNodeException):
        get_compactified_graph(dag.graph, compact_level=1)


def test_graph_string():
    """
    Test the graph_string function when given an incorrect format.
    It is tested with correct formats in the test_graph_visualization function.
    """
    graph = DAG()
    for edge in test_hierarchical_edges:
        graph.add_edge(*edge)

    with pytest.raises(ValueError):
        graph_string(graph, "invalid_format")


expected_mermaid_string = r"""flowchart LR
  subgraph simple_pub [SIMPLE_PUB]
    0[OUTPUT]
  end
  subgraph collection [COLLECTION]
    1[INPUT]
    subgraph verifier [VERIFIER]
      2[INPUT]
      3[OUTPUT_SAMPLE]
      5[OUTPUT_SIGNAL]
    end
    4[OUTPUT_SAMPLE]
    subgraph terminate [TERMINATE]
      6[INPUT]
    end
    subgraph log [LOG]
      7[INPUT]
      8[OUTPUT]
    end
    9[OUTPUT_SIGNAL]
  end
  subgraph simple_sub [SIMPLE_SUB]
    10[INPUT]
  end
  0 --> 1
  1 --> 2
  3 --> 4
  4 --> 10
  5 --> 7
  5 --> 6
  8 --> 9
  9 --> 10
"""
expected_mermaid_string_compact = r"""flowchart LR
  11[SIMPLE_PUB]
  subgraph collection [COLLECTION]
    12[VERIFIER]
    13[TERMINATE]
    14[LOG]
  end
  15[SIMPLE_SUB]
  11 --> 12
  12 --> 14
  12 --> 13
  12 --> 15
  14 --> 15
"""
expected_mermaid_string_compact_compact = r"""flowchart LR
  16[SIMPLE_PUB]
  17[COLLECTION]
  18[SIMPLE_SUB]
  16 --> 17
  17 --> 18
"""
expected_graphviz_string = r"""digraph EZ {
  rankdir="LR"
  subgraph simple_pub {
    cluster = true;
    label = "SIMPLE_PUB";
    "19" [label=OUTPUT];};
  subgraph collection {
    cluster = true;
    label = "COLLECTION";
    "20" [label=INPUT];
    subgraph verifier {
      cluster = true;
      label = "VERIFIER";
      "21" [label=INPUT];
      "22" [label=OUTPUT_SAMPLE];
      "24" [label=OUTPUT_SIGNAL];};
    "23" [label=OUTPUT_SAMPLE];
    subgraph terminate {
      cluster = true;
      label = "TERMINATE";
      "25" [label=INPUT];};
    subgraph log {
      cluster = true;
      label = "LOG";
      "26" [label=INPUT];
      "27" [label=OUTPUT];};
    "28" [label=OUTPUT_SIGNAL];};
  subgraph simple_sub {
    cluster = true;
    label = "SIMPLE_SUB";
    "29" [label=INPUT];};
  "19" -> "20";
  "20" -> "21";
  "22" -> "23";
  "23" -> "29";
  "24" -> "26";
  "24" -> "25";
  "27" -> "28";
  "28" -> "29";

}"""
expected_graphviz_string_compact = r"""digraph EZ {
  rankdir="LR"
  "30" [label=SIMPLE_PUB];
  subgraph collection {
    cluster = true;
    label = "COLLECTION";
    "31" [label=VERIFIER];
    "32" [label=TERMINATE];
    "33" [label=LOG];};
  "34" [label=SIMPLE_SUB];
  "30" -> "31";
  "31" -> "33";
  "31" -> "32";
  "31" -> "34";
  "33" -> "34";

}"""
expected_graphviz_string_compact_compact = r"""digraph EZ {
  rankdir="LR"
  "35" [label=SIMPLE_PUB];
  "36" [label=COLLECTION];
  "37" [label=SIMPLE_SUB];
  "35" -> "36";
  "36" -> "37";

}"""


@pytest.fixture
def mock_dag():
    """
    Mock the dag method of GraphService to return a test DAG.
    """
    with patch.object(
        GraphService, "dag", AsyncMock(return_value=get_test_dag())
    ) as mock_dag:
        yield mock_dag


@pytest.fixture
def mock_uuid4():
    """
    Mock function to return a reproducible UUID for testing comparison purposes.
    """
    mock_uuids = tuple(range(100))
    with patch("ezmsg.core.graph_util.uuid4", side_effect=mock_uuids) as mock_uuid:
        yield mock_uuid


@pytest.mark.asyncio
async def test_graph_visualization(mock_dag, mock_uuid4):
    """
    Test the graph visualization using the mocked DAG.
    """
    graph_service = GraphService()

    # Test mermaid format
    graph_mermaid = await graph_service.get_formatted_graph("mermaid")
    assert graph_mermaid == expected_mermaid_string

    # Test mermaid compact format
    graph_mermaid_compact = await graph_service.get_formatted_graph(
        "mermaid", compact_level=1
    )
    assert graph_mermaid_compact == expected_mermaid_string_compact

    # Test mermaid compact x2 format
    graph_mermaid_compact_compact = await graph_service.get_formatted_graph(
        "mermaid", compact_level=2
    )
    assert graph_mermaid_compact_compact == expected_mermaid_string_compact_compact

    # Test graphviz format
    graph_graphviz = await graph_service.get_formatted_graph("graphviz")
    assert graph_graphviz == expected_graphviz_string

    # Test graphviz compact format
    graph_graphviz_compact = await graph_service.get_formatted_graph(
        "graphviz", compact_level=1
    )
    assert graph_graphviz_compact == expected_graphviz_string_compact

    # Test graphviz compact x2 format
    graph_graphviz_compact_compact = await graph_service.get_formatted_graph(
        "graphviz", compact_level=2
    )
    assert graph_graphviz_compact_compact == expected_graphviz_string_compact_compact


if __name__ == "__main__":
    test_prune_graph()
    test_pipeline_levels()
    test_get_parent_node()
    test_get_compactified_graph()
    test_graph_string()

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(test_graph_visualization())
    finally:
        loop.close()
