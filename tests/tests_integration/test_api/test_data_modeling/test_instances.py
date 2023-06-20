from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError


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


@pytest.fixture()
def actor_view(cognite_client: CogniteClient, integration_test_space: dm.Space) -> dm.View:
    return cognite_client.data_modeling.views.retrieve((integration_test_space.space, "Actor", "2"))[0]


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
            limit=-1, instance_type="node", sources=person_view.as_id()
        )

        # Assert
        assert len(person_nodes) > 0
        assert all(person.properties for person in person_nodes)

    def test_list_person_nodes_sorted_by_name(self, cognite_client: CogniteClient, person_view: dm.View):
        view_id = person_view.as_id()
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1,
            instance_type="node",
            sources=view_id,
            sort=dm.InstanceSort(view_id.as_property_ref("name")),
        )
        assert sorted(person_nodes, key=lambda v: v.properties[view_id]["name"]) == person_nodes

    def test_list_person_filtering(self, cognite_client: CogniteClient, person_view: dm.View):
        # Act
        view_id = person_view.as_id()
        f = dm.filters
        born_before_1950 = f.Range(view_id.as_property_ref("birthYear"), lt=1950)
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1, instance_type="node", sources=view_id, filter=born_before_1950
        )

        assert all(person.properties[view_id]["birthYear"] < 1950 for person in person_nodes)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, person_view: dm.View):
        # Arrange
        new_node = dm.NodeApply(
            space=person_view.space,
            external_id="person:arnold_schwarzenegger",
            sources=[
                dm.NodeOrEdgeData(
                    person_view.as_id(),
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
        assert created.nodes[0].created_time
        assert created.nodes[0].last_updated_time
        assert retrieved.nodes[0].as_id() == new_node.as_id()

        # Act
        deleted_id = cognite_client.data_modeling.instances.delete(new_node.as_id())
        retrieved_deleted = cognite_client.data_modeling.instances.retrieve(new_node.as_id())

        # Assert
        assert deleted_id.nodes[0] == new_node.as_id()
        assert len(retrieved_deleted.nodes) == 0

    def test_apply_nodes_and_edges(self, cognite_client: CogniteClient, person_view: dm.View, actor_view: dm.View):
        # Arrange
        space = person_view.space
        person = dm.NodeApply(
            space=space,
            external_id="person:arnold_schwarzenegger",
            sources=[
                dm.NodeOrEdgeData(
                    person_view.as_id(),
                    {
                        "birthYear": 1947,
                        "name": "Arnold Schwarzenegger",
                    },
                )
            ],
        )
        actor = dm.NodeApply(
            space=space,
            external_id="actor:arnold_schwarzenegger",
            sources=[
                dm.NodeOrEdgeData(
                    actor_view.as_id(),
                    {
                        "wonOscar": False,
                        "person": {"space": space, "externalId": person.external_id},
                    },
                )
            ],
        )
        person_to_actor = dm.EdgeApply(
            space=space,
            external_id="relation:arnold_schwarzenegger:actor",
            type=dm.DirectRelationReference(space, person_view.properties["roles"].type.external_id),
            start_node=(space, person.external_id),
            end_node=dm.DirectRelationReference(space, actor.external_id),
        )
        new_nodes = [person, actor]
        new_edges = [person_to_actor]

        # Act
        created = cognite_client.data_modeling.instances.apply(new_nodes, new_edges, replace=True)

        # Assert
        assert isinstance(created, dm.InstancesApplyResult)
        assert sum(isinstance(item, dm.NodeApplyResult) for item in created.nodes) == 2
        assert sum(isinstance(item, dm.EdgeApplyResult) for item in created.edges) == 1

        # Cleanup
        cognite_client.data_modeling.instances.delete(created.nodes.as_ids())

    def test_apply_auto_create_nodes(self, cognite_client: CogniteClient, person_view: dm.View):
        # Arrange
        space = person_view.space
        person_to_actor = dm.EdgeApply(
            space=space,
            external_id="relation:sylvester_stallone:actor",
            type=dm.DirectRelationReference(space, person_view.properties["roles"].type.external_id),
            start_node=dm.DirectRelationReference(space, "person:sylvester_stallone"),
            end_node=dm.DirectRelationReference(space, "actor:sylvester_stallone"),
        )

        # Act
        created = cognite_client.data_modeling.instances.apply(
            edges=person_to_actor, auto_create_start_nodes=True, auto_create_end_nodes=True, replace=True
        ).edges[0]
        created_nodes = cognite_client.data_modeling.instances.retrieve(
            [person_to_actor.start_node.as_tuple(), person_to_actor.end_node.as_tuple()]
        ).nodes

        # Assert
        assert created.created_time
        assert created.last_updated_time
        assert len(created_nodes) == 2
        assert created_nodes[0].external_id == "person:sylvester_stallone"
        assert created_nodes[1].external_id == "actor:sylvester_stallone"

        # Cleanup
        cognite_client.data_modeling.instances.delete(created_nodes.as_ids())

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: dm.Space):
        space = integration_test_space.space
        res = cognite_client.data_modeling.instances.delete(dm.NodeId(space=space, external_id="DoesNotExists"))
        assert res.nodes == []
        assert res.edges == []

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_nodes: dm.NodeList):
        assert len(cdf_nodes) >= 2, "Please add at least two nodes to the test environment"
        # Act
        retrieved = cognite_client.data_modeling.instances.retrieve(cdf_nodes.as_ids()).nodes

        # Assert
        assert len(retrieved) == len(cdf_nodes)

    def test_retrieve_nodes_and_edges_using_id_tuples(
        self, cognite_client: CogniteClient, cdf_nodes: dm.NodeList, cdf_edges: dm.EdgeList
    ):
        assert len(cdf_nodes) >= 2, "Please add at least two nodes to the test environment"
        assert len(cdf_edges) >= 2, "Please add at least two edges to the test environment"
        # Act
        retrieved = cognite_client.data_modeling.instances.retrieve(
            nodes=[(id.space, id.external_id) for id in cdf_nodes.as_ids()],
            edges=[(id.space, id.external_id) for id in cdf_edges.as_ids()],
        )

        # Assert
        assert [node.as_id() for node in retrieved.nodes] == [node.as_id() for node in cdf_nodes]
        assert [edge.as_id() for edge in retrieved.edges] == [edge.as_id() for edge in cdf_edges]

    def test_retrieve_nodes_and_edges(
        self, cognite_client: CogniteClient, cdf_nodes: dm.NodeList, cdf_edges: dm.EdgeList
    ):
        assert len(cdf_nodes) >= 2, "Please add at least two nodes to the test environment"
        assert len(cdf_edges) >= 2, "Please add at least two edges to the test environment"
        # Act
        retrieved = cognite_client.data_modeling.instances.retrieve(nodes=cdf_nodes.as_ids(), edges=cdf_edges.as_ids())

        # Assert
        assert [node.as_id() for node in retrieved.nodes] == [node.as_id() for node in cdf_nodes]
        assert [edge.as_id() for edge in retrieved.edges] == [edge.as_id() for edge in cdf_edges]

    def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, cdf_nodes: dm.NodeList):
        assert len(cdf_nodes) >= 2, "Please add at least two nodes to the test environment"
        # Arrange
        ids = cdf_nodes.as_ids()
        ids += [dm.NodeId("myNonExistingSpace", "myImaginaryContainer")]

        # Act
        retrieved = cognite_client.data_modeling.instances.retrieve(ids)

        # Assert
        assert len(retrieved.nodes) == len(ids) - 1

    def test_retrieve_non_existent(self, cognite_client: CogniteClient):
        assert cognite_client.data_modeling.instances.retrieve(("myNonExistingSpace", "myImaginaryNode")).nodes == []

    def test_iterate_over_instances(self, cognite_client: CogniteClient):
        for nodes in cognite_client.data_modeling.instances(chunk_size=2, limit=-1):
            assert isinstance(nodes, dm.NodeList)
            assert len(nodes) <= 2

    def test_invalid_node_data(self, cognite_client: CogniteClient, person_view: dm.View):
        # Arrange
        space = person_view.space
        person = dm.NodeApply(
            space=space,
            external_id="person:arnold_schwarzenegger",
            sources=[
                dm.NodeOrEdgeData(
                    person_view.as_id(),
                    {
                        "birthYear": 1947,
                        "name": "Arnold Schwarzenegger",
                        "invalidProperty": "invalidValue",
                    },
                )
            ],
        )

        # Act
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.instances.apply(nodes=person)

        # Assert
        assert "invalidProperty" in error.value.message
