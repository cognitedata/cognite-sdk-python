import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling.instances import EdgeList, NodeList


@pytest.fixture()
def cdf_nodes(cognite_client: CogniteClient) -> NodeList:
    nodes = cognite_client.data_modeling.instances.list(limit=-1, instance_type="node")
    assert len(nodes) > 0, "Add at least one node to CDF"
    return nodes


@pytest.fixture()
def cdf_edges(cognite_client: CogniteClient) -> EdgeList:
    edges = cognite_client.data_modeling.instances.list(limit=-1, instance_type="edge")
    assert len(edges) > 0, "Add at least one edge to CDF"
    return edges


class TestInstancesAPI:
    def test_list_nodes(self, cognite_client: CogniteClient, cdf_nodes: NodeList):
        # Act
        actual_nodes = cognite_client.data_modeling.instances.list(limit=-1)

        # Assert
        assert sorted(actual_nodes, key=lambda v: v.external_id) == sorted(cdf_nodes, key=lambda v: v.external_id)

    def test_list_edges(self, cognite_client: CogniteClient, cdf_edges: EdgeList):
        # Act
        actual_edges = cognite_client.data_modeling.instances.list(limit=-1, instance_type="edge")

        # Assert
        assert sorted(actual_edges, key=lambda v: v.external_id) == sorted(cdf_edges, key=lambda v: v.external_id)
