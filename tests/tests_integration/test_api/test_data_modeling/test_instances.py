from __future__ import annotations

import json
import time
from typing import Any, ClassVar, cast

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.aggregations import HistogramValue
from cognite.client.data_classes.data_modeling import (
    DataModel,
    DirectRelationReference,
    Edge,
    EdgeApply,
    EdgeApplyResult,
    EdgeList,
    InstancesApplyResult,
    InstancesDeleteResult,
    InstanceSort,
    InstancesResult,
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
    ViewList,
    aggregations,
    filters,
    query,
)
from cognite.client.data_classes.data_modeling.query import (
    NodeResultSetExpression,
    Query,
    QueryResult,
    Select,
    SourceSelector,
)
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture(scope="session")
def movie_nodes(populated_movie: InstancesResult) -> NodeList:
    return populated_movie.nodes


@pytest.fixture(scope="session")
def movie_edges(populated_movie: InstancesResult) -> EdgeList:
    return populated_movie.edges


@pytest.fixture(scope="session")
def movie_views(movie_model: DataModel[View]) -> ViewList:
    return ViewList(movie_model.views)


@pytest.fixture()
def person_view(movie_views: ViewList) -> View:
    return cast(View, movie_views.get(external_id="Person"))


@pytest.fixture()
def actor_view(movie_views: ViewList) -> View:
    return cast(View, movie_views.get(external_id="Actor"))


@pytest.fixture()
def movie_view(movie_views: ViewList) -> View:
    return cast(View, movie_views.get(external_id="Movie"))


class TestInstancesAPI:
    def test_list_nodes(self, cognite_client: CogniteClient, movie_nodes: NodeList) -> None:
        # Act
        listed_nodes = cognite_client.data_modeling.instances.list(limit=-1, instance_type="node")

        # Assert
        movie_node_ids = set(movie_nodes.as_ids())
        assert movie_node_ids
        assert movie_node_ids <= set(listed_nodes.as_ids())

    def test_list_edges(self, cognite_client: CogniteClient, movie_edges: EdgeList) -> None:
        # Act
        listed_edges = cognite_client.data_modeling.instances.list(limit=-1, instance_type="edge")

        # Assert
        assert set(movie_edges.as_ids()) <= set(listed_edges.as_ids())

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

    @pytest.mark.parametrize("space_exists", (True, False))
    def test_list_space_filtering(
        self, cognite_client: CogniteClient, integration_test_space: Space, space_exists: bool
    ) -> None:
        space = integration_test_space.space
        if not space_exists:
            space += "doesntexist" * 2
        nodes = cognite_client.data_modeling.instances.list(space=space, instance_type="node", limit=None)
        if space_exists:
            assert len(nodes) > 0
        else:
            assert len(nodes) == 0
        assert all(node.space == space for node in nodes)

    def test_list_spaces_filtering(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        space = integration_test_space.space
        nodes = cognite_client.data_modeling.instances.list(
            space=["foo-doesnt-exists", space, "bar-doesnt-exists"], instance_type="node", limit=None
        )
        assert len(nodes) > 0
        assert all(node.space == space for node in nodes)

    def test_list_space_and_person_filtering(
        self, cognite_client: CogniteClient, person_view: View, integration_test_space: Space
    ) -> None:
        space = integration_test_space.space
        view_id = person_view.as_id()
        born_before_1950 = filters.Range(person_view.as_property_ref("birthYear"), lt=1950)
        person_nodes = cognite_client.data_modeling.instances.list(
            instance_type="node", sources=view_id, space=space, filter=born_before_1950, limit=None
        )
        assert len(person_nodes) > 0
        assert all(node.space == space for node in person_nodes)
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

        created: InstancesApplyResult | None = None
        deleted_result: InstancesDeleteResult | None = None

        try:
            # Act
            created = cognite_client.data_modeling.instances.apply(new_node, replace=True)
            retrieved = cognite_client.data_modeling.instances.retrieve(new_node.as_id())

            # Assert
            assert len(created.nodes) == 1
            assert created.nodes[0].created_time
            assert created.nodes[0].last_updated_time
            assert retrieved.nodes[0].as_id() == new_node.as_id()

            # Act
            deleted_result = cognite_client.data_modeling.instances.delete(new_node.as_id())
            retrieved_deleted = cognite_client.data_modeling.instances.retrieve(new_node.as_id())

            # Assert
            assert len(deleted_result.nodes) == 1
            assert deleted_result.nodes[0] == new_node.as_id()
            assert len(retrieved_deleted.nodes) == 0
        finally:
            # Cleanup
            if created is not None and created.nodes and deleted_result is None:
                cognite_client.data_modeling.instances.delete(nodes=created.nodes.as_ids())

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
        created: InstancesApplyResult | None = None
        try:
            # Act
            created = cognite_client.data_modeling.instances.apply(new_nodes, new_edges, replace=True)

            # Assert
            assert isinstance(created, InstancesApplyResult)
            assert sum(isinstance(item, NodeApplyResult) for item in created.nodes) == 2
            assert sum(isinstance(item, EdgeApplyResult) for item in created.edges) == 1
        finally:
            # Cleanup
            if created is not None:
                cognite_client.data_modeling.instances.delete(created.nodes.as_ids(), edges=created.edges.as_ids())

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
        node_pair = [person_to_actor.start_node.as_tuple(), person_to_actor.end_node.as_tuple()]
        created_edges: InstancesApplyResult | None = None
        try:
            # Act
            created_edges = cognite_client.data_modeling.instances.apply(
                edges=person_to_actor, auto_create_start_nodes=True, auto_create_end_nodes=True, replace=True
            )
            created_nodes = cognite_client.data_modeling.instances.retrieve(node_pair)

            # Assert
            assert len(created_edges.edges) == 1
            assert created_edges.edges[0].created_time
            assert created_edges.edges[0].last_updated_time
            assert len(created_nodes.nodes) == 2
            assert created_nodes.nodes[0].external_id == "person:sylvester_stallone"
            assert created_nodes.nodes[1].external_id == "actor:sylvester_stallone"
        finally:
            # Cleanup
            if created_edges is not None:
                cognite_client.data_modeling.instances.delete(nodes=node_pair, edges=created_edges.edges.as_ids())

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        space = integration_test_space.space
        res = cognite_client.data_modeling.instances.delete(NodeId(space=space, external_id="DoesNotExists"))
        assert res.nodes == []
        assert res.edges == []

    def test_retrieve_multiple(self, cognite_client: CogniteClient, movie_nodes: NodeList) -> None:
        # Act
        retrieved = cognite_client.data_modeling.instances.retrieve(movie_nodes.as_ids())

        # Assert
        assert len(retrieved.nodes) == len(movie_nodes)

    def test_retrieve_nodes_and_edges_using_id_tuples(
        self, cognite_client: CogniteClient, movie_nodes: NodeList, movie_edges: EdgeList
    ) -> None:
        # Act
        retrieved = cognite_client.data_modeling.instances.retrieve(
            nodes=[(id.space, id.external_id) for id in movie_nodes.as_ids()],
            edges=[(id.space, id.external_id) for id in movie_edges.as_ids()],
        )

        # Assert
        assert set(retrieved.nodes.as_ids()) == set(movie_nodes.as_ids())
        assert set(retrieved.edges.as_ids()) == set(movie_edges.as_ids())

    def test_retrieve_nodes_and_edges(
        self, cognite_client: CogniteClient, movie_nodes: NodeList, movie_edges: EdgeList
    ) -> None:
        # Act
        retrieved = cognite_client.data_modeling.instances.retrieve(
            nodes=movie_nodes.as_ids(), edges=movie_edges.as_ids()
        )

        # Assert
        assert set(retrieved.nodes.as_ids()) == set(movie_nodes.as_ids())
        assert set(retrieved.edges.as_ids()) == set(movie_edges.as_ids())

    @pytest.mark.xfail  # TODO: Unknown ids should not raise
    def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, movie_nodes: NodeList) -> None:
        # Arrange
        ids_without_missing = movie_nodes.as_ids()
        ids_with_missing = [*ids_without_missing, NodeId("myNonExistingSpace", "myImaginaryContainer")]

        # Act
        retrieved = cognite_client.data_modeling.instances.retrieve(ids_with_missing)

        # Assert
        assert retrieved.nodes.as_ids() == ids_without_missing

    @pytest.mark.xfail  # TODO: Unknown ids should not raise
    def test_retrieve_non_existent(self, cognite_client: CogniteClient) -> None:
        assert cognite_client.data_modeling.instances.retrieve(("myNonExistingSpace", "myImaginaryNode")).nodes == []

    def test_iterate_over_instances(self, cognite_client: CogniteClient) -> None:
        iterator = cognite_client.data_modeling.instances(chunk_size=2)
        first_iter = next(iterator)
        assert isinstance(first_iter, NodeList)
        assert len(first_iter) <= 2

        second_iter = next(iterator)
        assert isinstance(second_iter, NodeList)
        assert len(second_iter) <= 2

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
        result = cognite_client.data_modeling.instances.aggregate(
            view_id,
            aggregates=[avg_agg, max_agg],
            group_by="releaseYear",
        )

        # Assert
        assert len(result)

    def test_aggregate_multiple(self, cognite_client: CogniteClient, movie_view: View) -> None:
        # Arrange
        view_id = movie_view.as_id()
        avg_agg = aggregations.Avg("runTimeMinutes")
        max_agg = aggregations.Max("runTimeMinutes")

        # Act
        result = cognite_client.data_modeling.instances.aggregate(
            view_id,
            aggregates=[avg_agg, max_agg],
        )

        # Assert
        assert len(result) == 2
        assert result[0].property == "runTimeMinutes"
        assert result[1].property == "runTimeMinutes"
        max_value = next((item for item in result if isinstance(item, aggregations.MaxValue)), None)
        avg_value = next((item for item in result if isinstance(item, aggregations.AvgValue)), None)
        assert max_value is not None
        assert avg_value is not None
        assert max_value.value > avg_value.value

    def test_aggregate_count_persons(self, cognite_client: CogniteClient, person_view: View) -> None:
        # Arrange
        view_id = person_view.as_id()
        count_agg = aggregations.Count("name")

        # Act
        count = cognite_client.data_modeling.instances.aggregate(
            view_id,
            aggregates=count_agg,
            instance_type="node",
            limit=10,
        )

        # Assert
        assert count.property == "name"
        assert count.value > 0, "Add at least one person to the view to run this test"

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
        assert "One or more views do not exist: " in error.value.message
        assert view_id.as_source_identifier() in error.value.message

    def test_dump_json_serialize_load_node(self, movie_nodes: NodeList) -> None:
        # Arrange
        node = movie_nodes.get(external_id="movie:pulp_fiction")
        assert node is not None, "Pulp fiction movie not found, please recreate it"

        # Act
        node_dumped = node.dump(camel_case=True)
        node_json = json.dumps(node_dumped)
        node_loaded = Node.load(node_json)

        # Assert
        assert node == node_loaded

    def test_dump_json_serialize_load_edge(self, movie_edges: EdgeList) -> None:
        # Arrange
        edge = movie_edges.get(external_id="person:quentin_tarantino:director:quentin_tarantino")
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
        assert len(result["movies"]) > 0, "Add at least one movie with release year before 2000"
        assert all(
            cast(int, movie.properties.get(movie_id, {}).get("releaseYear")) < 2000 for movie in result["movies"]
        )
        assert result["movies"] == sorted(result["movies"], key=lambda x: x.properties.get(movie_id, {}).get("title"))
        assert len(result["actors"]) > 0, "Add at least one actor that acted in the movies released before 2000"
        assert all(actor.properties.get(actor_id, {}).get("wonOscar") for actor in result["actors"])
        assert result["actors"] == sorted(result["actors"], key=lambda x: x.external_id)


class TestInstancesSync:
    def test_sync_movies_released_in_1994(self, cognite_client: CogniteClient, movie_view: View) -> None:
        movie_id = movie_view.as_id()
        movies_released_1994 = NodeResultSetExpression(
            filter=filters.Equals(movie_id.as_property_ref("releaseYear"), 1994)
        )
        my_query = Query(
            with_={"movies": movies_released_1994},
            select={"movies": Select([SourceSelector(movie_id, ["title", "releaseYear"])])},
        )

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

    def test_subscribe_to_movies_released_in_1994(self, cognite_client: CogniteClient, movie_view: View) -> None:
        # Arrange
        movie_id = movie_view.as_id()
        movies_released_1994 = NodeResultSetExpression(
            filter=filters.Equals(movie_id.as_property_ref("releaseYear"), 1994)
        )
        my_query = Query(
            with_={"movies": movies_released_1994},
            select={"movies": Select([SourceSelector(movie_id, [".*"])])},
        )

        class State:
            invocation_count: ClassVar[int] = 0

        def callback(result: QueryResult) -> None:
            State.invocation_count += 1

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

        updated_1994_movie = NodeApply(
            space=movie_view.space,
            external_id="movie:forrest_gump",
            sources=[
                NodeOrEdgeData(
                    source=movie_id,
                    properties={
                        "runTimeMinutes": 200,
                    },
                )
            ],
        )

        def wait_for_invocation_count_to_have_value(value: int) -> None:
            max_wait_seconds = 5
            start_time = time.time()
            while State.invocation_count != value and (abs(time.time() - start_time) > max_wait_seconds):
                time.sleep(0.1)

        context = cognite_client.data_modeling.instances.subscribe(my_query, callback, poll_delay_seconds=2)

        try:
            cognite_client.data_modeling.instances.apply(nodes=new_1994_movie)
            wait_for_invocation_count_to_have_value(1)

            cognite_client.data_modeling.instances.apply(nodes=updated_1994_movie)
            wait_for_invocation_count_to_have_value(2)
        finally:
            cognite_client.data_modeling.instances.delete(new_1994_movie.as_id())

        assert context.is_alive()
        context.cancel()
        # May take some time for the thread to actually die, so we check in a loop for a little while
        for i in range(10):
            if context.is_alive():
                time.sleep(1)
            else:
                return
        else:
            assert not context.is_alive()
