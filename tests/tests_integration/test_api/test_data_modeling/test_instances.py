from __future__ import annotations

import json
from typing import Any, cast

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    DirectRelationReference,
    Edge,
    EdgeApply,
    EdgeApplyResult,
    EdgeList,
    InstancesApplyResult,
    InstanceSort,
    Node,
    NodeApply,
    NodeApplyResult,
    NodeId,
    NodeList,
    NodeOrEdgeData,
    SingleHopConnectionDefinition,
    Space,
    View,
    ViewId,
    aggregations,
    filters,
    query,
)
from cognite.client.data_classes.data_modeling.aggregations import HistogramValue
from cognite.client.data_classes.filters import Equals
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture()
def cdf_nodes(cognite_client: CogniteClient, integration_test_space: Space) -> NodeList:
    nodes = cognite_client.data_modeling.instances.list(
        limit=-1, instance_type="node", filter=Equals(("node", "space"), integration_test_space.space)
    )
    assert len(nodes) > 0, "Add at least one node to CDF"
    return nodes


@pytest.fixture()
def cdf_edges(cognite_client: CogniteClient, integration_test_space: Space) -> EdgeList:
    edges = cognite_client.data_modeling.instances.list(
        limit=-1, instance_type="edge", filter=Equals(("edge", "space"), integration_test_space.space)
    )
    assert len(edges) > 0, "Add at least one edge to CDF"
    return edges


@pytest.fixture()
def person_view(cognite_client: CogniteClient, integration_test_space: Space) -> View:
    return cognite_client.data_modeling.views.retrieve((integration_test_space.space, "Person", "2"))[0]


@pytest.fixture()
def actor_view(cognite_client: CogniteClient, integration_test_space: Space) -> View:
    return cognite_client.data_modeling.views.retrieve((integration_test_space.space, "Actor", "2"))[0]


@pytest.fixture()
def movie_view(cognite_client: CogniteClient, integration_test_space: Space) -> View:
    return cognite_client.data_modeling.views.retrieve((integration_test_space.space, "Movie", "2"))[0]


class TestInstancesAPI:
    def test_list_nodes(self, cognite_client: CogniteClient, cdf_nodes: NodeList) -> None:
        # Act
        actual_nodes = cognite_client.data_modeling.instances.list(limit=-1)

        # Assert
        assert sorted(actual_nodes, key=lambda v: v.external_id) == sorted(cdf_nodes, key=lambda v: v.external_id)

    def test_list_edges(self, cognite_client: CogniteClient, cdf_edges: EdgeList) -> None:
        # Act
        actual_edges = cognite_client.data_modeling.instances.list(limit=-1, instance_type="edge")

        # Assert
        assert sorted(actual_edges, key=lambda v: v.external_id) == sorted(cdf_edges, key=lambda v: v.external_id)

    def test_list_nodes_with_properties(self, cognite_client: CogniteClient, person_view: View) -> None:
        # Act
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1, instance_type="node", sources=person_view.as_id()
        )

        # Assert
        assert len(person_nodes) > 0
        assert all(person.properties for person in person_nodes)

    def test_list_person_nodes_sorted_by_name(self, cognite_client: CogniteClient, person_view: View) -> None:
        view_id = person_view.as_id()
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1,
            instance_type="node",
            sources=view_id,
            sort=InstanceSort(view_id.as_property_ref("name")),
        )
        assert sorted(person_nodes, key=lambda v: v.properties[view_id]["name"]) == person_nodes

    def test_list_person_filtering(self, cognite_client: CogniteClient, person_view: View) -> None:
        # Act
        view_id = person_view.as_id()
        born_before_1950 = filters.Range(view_id.as_property_ref("birthYear"), lt=1950)
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1, instance_type="node", sources=view_id, filter=born_before_1950
        )

        assert all(cast(int, person.properties[view_id]["birthYear"]) < 1950 for person in person_nodes)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, person_view: View) -> None:
        # Arrange
        new_node = NodeApply(
            space=person_view.space,
            external_id="person:arnold_schwarzenegger",
            sources=[
                NodeOrEdgeData(
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

    def test_apply_nodes_and_edges(self, cognite_client: CogniteClient, person_view: View, actor_view: View) -> None:
        # Arrange
        space = person_view.space
        person = NodeApply(
            space=space,
            external_id="person:arnold_schwarzenegger",
            sources=[
                NodeOrEdgeData(
                    person_view.as_id(),
                    {
                        "birthYear": 1947,
                        "name": "Arnold Schwarzenegger",
                    },
                )
            ],
        )
        actor = NodeApply(
            space=space,
            external_id="actor:arnold_schwarzenegger",
            sources=[
                NodeOrEdgeData(
                    actor_view.as_id(),
                    {
                        "wonOscar": False,
                        "person": {"space": space, "externalId": person.external_id},
                    },
                )
            ],
        )
        person_to_actor = EdgeApply(
            space=space,
            external_id="relation:arnold_schwarzenegger:actor",
            type=DirectRelationReference(
                space, cast(SingleHopConnectionDefinition, person_view.properties["roles"]).type.external_id
            ),
            start_node=(space, person.external_id),
            end_node=DirectRelationReference(space, actor.external_id),
        )
        new_nodes = [person, actor]
        new_edges = [person_to_actor]

        # Act
        created = cognite_client.data_modeling.instances.apply(new_nodes, new_edges, replace=True)

        # Assert
        assert isinstance(created, InstancesApplyResult)
        assert sum(isinstance(item, NodeApplyResult) for item in created.nodes) == 2
        assert sum(isinstance(item, EdgeApplyResult) for item in created.edges) == 1

        # Cleanup
        cognite_client.data_modeling.instances.delete(created.nodes.as_ids())

    def test_apply_auto_create_nodes(self, cognite_client: CogniteClient, person_view: View) -> None:
        # Arrange
        space = person_view.space
        person_to_actor = EdgeApply(
            space=space,
            external_id="relation:sylvester_stallone:actor",
            type=DirectRelationReference(
                space, cast(SingleHopConnectionDefinition, person_view.properties["roles"]).type.external_id
            ),
            start_node=DirectRelationReference(space, "person:sylvester_stallone"),
            end_node=DirectRelationReference(space, "actor:sylvester_stallone"),
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

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        space = integration_test_space.space
        res = cognite_client.data_modeling.instances.delete(NodeId(space=space, external_id="DoesNotExists"))
        assert res.nodes == []
        assert res.edges == []

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_nodes: NodeList) -> None:
        assert len(cdf_nodes) >= 2, "Please add at least two nodes to the test environment"
        # Act
        retrieved = cognite_client.data_modeling.instances.retrieve(cdf_nodes.as_ids()).nodes

        # Assert
        assert retrieved == cdf_nodes

    def test_retrieve_nodes_and_edges_using_id_tuples(
        self, cognite_client: CogniteClient, cdf_nodes: NodeList, cdf_edges: EdgeList
    ) -> None:
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
        self, cognite_client: CogniteClient, cdf_nodes: NodeList, cdf_edges: EdgeList
    ) -> None:
        assert len(cdf_nodes) >= 2, "Please add at least two nodes to the test environment"
        assert len(cdf_edges) >= 2, "Please add at least two edges to the test environment"
        # Act
        retrieved = cognite_client.data_modeling.instances.retrieve(nodes=cdf_nodes.as_ids(), edges=cdf_edges.as_ids())

        # Assert
        assert [node.as_id() for node in retrieved.nodes] == [node.as_id() for node in cdf_nodes]
        assert [edge.as_id() for edge in retrieved.edges] == [edge.as_id() for edge in cdf_edges]

    def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, cdf_nodes: NodeList) -> None:
        assert len(cdf_nodes) >= 2, "Please add at least two nodes to the test environment"
        # Arrange
        ids_without_missing = cdf_nodes.as_ids()
        ids_with_missing = [*ids_without_missing, NodeId("myNonExistingSpace", "myImaginaryContainer")]

        # Act
        retrieved = cognite_client.data_modeling.instances.retrieve(ids_with_missing)

        # Assert
        assert retrieved.nodes.as_ids() == ids_without_missing

    def test_retrieve_non_existent(self, cognite_client: CogniteClient) -> None:
        assert cognite_client.data_modeling.instances.retrieve(("myNonExistingSpace", "myImaginaryNode")).nodes == []

    def test_iterate_over_instances(self, cognite_client: CogniteClient) -> None:
        for nodes in cognite_client.data_modeling.instances(chunk_size=2, limit=-1):
            assert isinstance(nodes, NodeList)
            assert len(nodes) <= 2

    def test_apply_invalid_node_data(self, cognite_client: CogniteClient, person_view: View) -> None:
        # Arrange
        space = person_view.space
        person = NodeApply(
            space=space,
            external_id="person:arnold_schwarzenegger",
            sources=[
                NodeOrEdgeData(
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
        assert error.value.code == 400
        assert "invalidProperty" in error.value.message

    def test_apply_failed_and_successful_task(
        self, cognite_client: CogniteClient, person_view: View, monkeypatch: Any
    ) -> None:
        # Arrange
        space = person_view.space
        valid_person = NodeApply(
            space=space,
            external_id="person:arnold_schwarzenegger",
            sources=[
                NodeOrEdgeData(
                    person_view.as_id(),
                    {
                        "birthYear": 1947,
                        "name": "Arnold Schwarzenegger",
                    },
                ),
            ],
        )
        invalid_person = NodeApply(
            space=space,
            external_id="person:sylvester_stallone",
            sources=[
                NodeOrEdgeData(
                    person_view.as_id(),
                    {
                        "birthYear": 1946,
                        "name": "Sylvester Stallone",
                        "invalidProperty": "invalidValue",
                    },
                ),
            ],
        )
        monkeypatch.setattr(cognite_client.data_modeling.instances, "_CREATE_LIMIT", 1)

        try:
            # Act
            with pytest.raises(CogniteAPIError) as error:
                cognite_client.data_modeling.instances.apply(nodes=[valid_person, invalid_person])

            # Assert
            assert "invalidProperty" in error.value.message
            assert error.value.code == 400
            assert len(error.value.successful) == 1
            assert len(error.value.failed) == 1
        finally:
            # Cleanup
            cognite_client.data_modeling.instances.delete(valid_person.as_id())

    def test_search_node_data(self, cognite_client: CogniteClient, person_view: View) -> None:
        # Act
        search_result = cognite_client.data_modeling.instances.search(
            person_view.as_id(), query="Quentin", properties=["name"]
        )

        # Assert
        assert len(search_result) == 1
        assert search_result[0].external_id == "person:quentin_tarantino"

    def test_search_node_data_with_invalid_property(self, cognite_client: CogniteClient, person_view: View) -> None:
        # Act
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.instances.search(
                person_view.as_id(), query="Quentin", properties=["invalidProperty"]
            )

        # Assert
        assert "Unknown property" in error.value.message

    def test_search_node_data_with_filtering(self, cognite_client: CogniteClient, person_view: View) -> None:
        # Act
        view_id = person_view.as_id()
        f = filters
        born_after_2000 = f.Range([view_id.space, view_id.as_source_identifier(), "birthYear"], gt=2000)

        # Act
        search_result = cognite_client.data_modeling.instances.search(
            view_id, query="Quentin", properties=["name"], filter=born_after_2000
        )

        # Assert
        assert len(search_result) == 0

    def test_aggregate_histogram_across_nodes(self, cognite_client: CogniteClient, person_view: View) -> None:
        view_id = person_view.as_id()
        birth_by_decade = aggregations.Histogram("birthYear", interval=10.0)

        histogram = cognite_client.data_modeling.instances.histogram(view_id, birth_by_decade)
        assert isinstance(histogram, HistogramValue)

        histogram_seq = cognite_client.data_modeling.instances.histogram(view_id, [birth_by_decade])
        assert len(histogram_seq) == 1 and isinstance(histogram_seq[0], HistogramValue)

    def test_aggregate_with_grouping(self, cognite_client: CogniteClient, movie_view: View) -> None:
        # Arrange
        view_id = movie_view.as_id()
        avg_agg = aggregations.Avg("runTimeMinutes")
        max_agg = aggregations.Max("runTimeMinutes")

        # Act
        counts = cognite_client.data_modeling.instances.aggregate(
            view_id,
            aggregates=[avg_agg, max_agg],
            group_by=["releaseYear"],
        )

        # Assert
        assert len(counts)

    def test_aggregate_count_persons(self, cognite_client: CogniteClient, person_view: View) -> None:
        # Arrange
        view_id = person_view.as_id()
        count_agg = aggregations.Count("externalId")

        # Act
        counts = cognite_client.data_modeling.instances.aggregate(
            view_id,
            aggregates=count_agg,
            instance_type="node",
            limit=10,
        )

        # Assert
        assert len(counts) == 1
        assert counts[0].aggregates[0].value > 0, "Add at least one person to the view to run this test"

    def test_aggregate_invalid_view_id(self, cognite_client: CogniteClient) -> None:
        # Arrange
        view_id = ViewId("myNonExistingSpace", "myNonExistingView", "myNonExistingVersion")
        count_agg = aggregations.Count("externalId")

        # Act
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.instances.aggregate(
                view_id,
                aggregates=count_agg,
                instance_type="node",
                limit=10,
            )

        # Assert
        assert error.value.code == 400
        assert "View not found" in error.value.message

    def test_dump_json_serialize_load_node(self, cdf_nodes: NodeList) -> None:
        # Arrange
        node = cdf_nodes.get(external_id="movie:pulp_fiction")
        assert node is not None, "Pulp fiction movie not found, please recreate it"

        # Act
        node_dumped = node.dump(camel_case=True)
        node_json = json.dumps(node_dumped)
        node_loaded = Node.load(node_json)

        # Assert
        assert node == node_loaded

    def test_dump_json_serialize_load_edge(self, cdf_edges: EdgeList) -> None:
        # Arrange
        edge = cdf_edges.get(external_id="relation:quentin_tarantino:director")
        assert edge is not None, "Relation between Quentin Tarantino person and director not found, please recreate it"

        # Act
        edge_dumped = edge.dump(camel_case=True)
        edge_json = json.dumps(edge_dumped)
        edge_loaded = Edge.load(edge_json)

        # Assert
        assert edge == edge_loaded

    def test_query_oscar_winning_actors_before_2000(
        self, cognite_client: CogniteClient, movie_view: View, actor_view: View
    ) -> None:
        # Arrange
        # Create a query that finds all actors that won Oscars in a movies released before 2000 sorted by external id.
        movie_id = movie_view.as_id()
        actor_id = actor_view.as_id()
        f = filters
        q = query
        movies_before_2000 = q.NodeResultSetExpression(filter=f.Range(movie_id.as_property_ref("releaseYear"), lt=2000))
        actors_in_movie = q.EdgeResultSetExpression(
            from_="movies", filter=f.Equals(["edge", "type"], {"space": movie_view.space, "externalId": "Movie.actors"})
        )
        actor = q.NodeResultSetExpression(
            from_="actors_in_movie", filter=f.Equals(actor_id.as_property_ref("wonOscar"), True)
        )

        my_query = q.Query(
            {
                "movies": movies_before_2000,
                "actors_in_movie": actors_in_movie,
                "actors": actor,
            },
            select={
                "movies": q.Select(
                    [q.SourceSelector(movie_id, ["title", "releaseYear"])],
                    sort=[InstanceSort(movie_id.as_property_ref("title"))],
                ),
                "actors": q.Select(
                    [q.SourceSelector(actor_id, ["wonOscar"])], sort=[InstanceSort(["node", "externalId"])]
                ),
            },
        )

        # Act
        result = cognite_client.data_modeling.instances.query(my_query)

        # Assert
        assert len(result["movies"]) > 0, "Add at least one movie withe release year before 2000"
        assert all(
            cast(int, movie.properties.get(movie_id, {}).get("releaseYear")) < 2000 for movie in result["movies"]
        )
        assert result["movies"] == sorted(result["movies"], key=lambda x: x.properties.get(movie_id, {}).get("title"))
        assert len(result["actors"]) > 0, "Add at leas one actor that acted in the movies released before 2000"
        assert all(actor.properties.get(actor_id, {}).get("wonOscar") for actor in result["actors"])
        assert result["actors"] == sorted(result["actors"], key=lambda x: x.external_id)

    def test_sync_movies_released_in_1994(self, cognite_client: CogniteClient, movie_view: View) -> None:
        # Arrange
        movie_id = movie_view.as_id()
        q = query
        f = filters
        movies_released_1994 = q.NodeResultSetExpression(filter=f.Equals(movie_id.as_property_ref("releaseYear"), 1994))
        my_query = q.Query(
            with_={"movies": movies_released_1994},
            select={"movies": q.Select([q.SourceSelector(movie_id, ["title", "releaseYear"])])},
        )

        # Act
        result = cognite_client.data_modeling.instances.sync(my_query)
        assert len(result["movies"]) > 0, "Add at least one movie released in 1994"

        new_1994_movie = NodeApply(
            space=movie_view.space,
            external_id="movie:forrest_gump",
            sources=[
                NodeOrEdgeData(
                    source=movie_id,
                    properties={
                        "title": "Forrest Gump",
                        "releaseYear": 1994,
                        "runTimeMinutes": 142,
                    },
                )
            ],
        )

        try:
            cognite_client.data_modeling.instances.apply(nodes=new_1994_movie)
            my_query.cursors = result.cursors
            new_result = cognite_client.data_modeling.instances.sync(my_query)

            # Assert
            assert len(new_result["movies"]) == 1, "Only the new movie should be returned"
            assert new_result["movies"][0].external_id == new_1994_movie.external_id
        finally:
            cognite_client.data_modeling.instances.delete(new_1994_movie.as_id())
