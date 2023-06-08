import pytest

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm


@pytest.fixture()
def cdf_nodes(cognite_client: CogniteClient) -> dm.NodeList:
    nodes = cognite_client.data_modeling.instances.list(limit=-1, instance_type="node")
    assert len(nodes) > 0, "Add at least one node to CDF"
    return nodes


@pytest.fixture()
def cdf_edges(cognite_client: CogniteClient) -> dm.EdgeList:
    edges = cognite_client.data_modeling.instances.list(limit=-1, instance_type="edge")
    assert len(edges) > 0, "Add at least one edge to CDF"
    return edges


@pytest.fixture()
def person_view(cognite_client: CogniteClient, integration_test_space: dm.Space) -> dm.View:
    return cognite_client.data_modeling.views.retrieve((integration_test_space.space, "Person", "2"))[0]


class TestInstancesAPI:
    def test_list_nodes(self, cognite_client: CogniteClient, cdf_nodes: dm.NodeList):
        # Act
        actual_nodes = cognite_client.data_modeling.instances.list(limit=-1)

        # Assert
        assert sorted(actual_nodes, key=lambda v: v.external_id) == sorted(cdf_nodes, key=lambda v: v.external_id)

    def test_list_edges(self, cognite_client: CogniteClient, cdf_edges: dm.EdgeList):
        # Act
        actual_edges = cognite_client.data_modeling.instances.list(limit=-1, instance_type="edge")

        # Assert
        assert sorted(actual_edges, key=lambda v: v.external_id) == sorted(cdf_edges, key=lambda v: v.external_id)

    def test_list_nodes_with_properties(self, cognite_client: CogniteClient, person_view: dm.View):
        # Act
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1, instance_type="node", sources=person_view.as_reference()
        )

        # Assert
        assert len(person_nodes) > 0
        assert all(person.properties for person in person_nodes)

    def test_list_person_nodes_sorted_by_name(self, cognite_client: CogniteClient, person_view: dm.View):
        # Act
        view_id = person_view.as_reference()
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1,
            instance_type="node",
            sources=view_id,
            sort=dm.InstanceSort([view_id.space, view_id.identifier, "name"]),
        )

        # Assert
        assert (
            sorted(person_nodes, key=lambda v: v.properties[view_id.space][view_id.identifier]["name"]) == person_nodes
        )

    def test_list_person_filtering(self, cognite_client: CogniteClient, person_view: dm.View):
        # Act
        view_id = person_view.as_reference()
        f = dm.filters
        born_before_1950 = f.Range([view_id.space, view_id.identifier, "birthYear"], lt=1950)
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1, instance_type="node", sources=view_id, filter=born_before_1950
        )

        assert all(person.properties[view_id.space][view_id.identifier]["birthYear"] < 1950 for person in person_nodes)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, person_view: dm.View):
        # Arrange
        new_node = dm.NodeApply(
            space=person_view.space,
            external_id="person:arnold_schwarzenegger",
            sources=[
                dm.NodeOrEdgeData(
                    person_view.as_reference(),
                    {
                        "birthYear": 1947,
                        "name": "Arnold Schwarzenegger",
                    },
                )
            ],
        )

        # Act
        created = cognite_client.data_modeling.instances.apply(new_node, replace=True)
        retrieved = cognite_client.data_modeling.instances.retrieve(new_node.as_id())

        # Assert
        assert created.created_time
        assert created.last_updated_time
        assert retrieved.as_id() == new_node.as_id()

        # Act
        deleted_id = cognite_client.data_modeling.instances.delete(new_node.as_id())
        retrieved_deleted = cognite_client.data_modeling.instances.retrieve(new_node.as_id())

        # Assert
        assert deleted_id[0] == new_node.as_id()
        assert retrieved_deleted is None
