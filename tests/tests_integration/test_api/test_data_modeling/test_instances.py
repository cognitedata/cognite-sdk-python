import pytest

from cognite.client import CogniteClient
from cognite.client import data_modeling as models
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


@pytest.fixture()
def person_view(cognite_client: CogniteClient, integration_test_space: models.Space) -> models.View:
    return cognite_client.data_modeling.views.retrieve((integration_test_space.space, "Person", "2"))[0]


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

    def test_list_nodes_with_properties(self, cognite_client: CogniteClient, person_view: models.View):
        # Act
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1, instance_type="node", sources=person_view.as_reference()
        )

        # Assert
        assert len(person_nodes) > 0
        assert all(person.properties for person in person_nodes)

    def test_list_person_nodes_sorted_by_name(self, cognite_client: CogniteClient, person_view: models.View):
        # Act
        view_id = person_view.as_reference()
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1,
            instance_type="node",
            sources=view_id,
            sort=models.InstanceSort([view_id.space, view_id.identifier, "name"]),
        )

        # Assert
        assert (
            sorted(person_nodes, key=lambda v: v.properties[view_id.space][view_id.identifier]["name"]) == person_nodes
        )

    def test_list_person_filtering(self, cognite_client: CogniteClient, person_view: models.View):
        # Act
        view_id = person_view.as_reference()
        f = models.filters
        born_before_1950 = f.Range([view_id.space, view_id.identifier, "birthYear"], lt=1950)
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1, instance_type="node", sources=view_id, filter=born_before_1950
        )

        assert all(person.properties[view_id.space][view_id.identifier]["birthYear"] < 1950 for person in person_nodes)
