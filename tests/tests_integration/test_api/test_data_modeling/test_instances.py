from __future__ import annotations

import json
import math
import time
from datetime import date, datetime, timezone
from typing import Any, ClassVar, cast

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.aggregations import HistogramValue
from cognite.client.data_classes.data_modeling import (
    ContainerApply,
    ContainerProperty,
    DataModel,
    DirectRelationReference,
    Edge,
    EdgeApply,
    EdgeApplyResult,
    EdgeList,
    Float64,
    InstancesApplyResult,
    InstancesDeleteResult,
    InstanceSort,
    InstancesResult,
    MappedPropertyApply,
    Node,
    NodeApply,
    NodeApplyResult,
    NodeId,
    NodeList,
    NodeOrEdgeData,
    SingleHopConnectionDefinition,
    Space,
    View,
    ViewApply,
    ViewId,
    ViewList,
    aggregations,
    filters,
    query,
)
from cognite.client.data_classes.data_modeling.data_types import UnitReference
from cognite.client.data_classes.data_modeling.instances import TargetUnit
from cognite.client.data_classes.data_modeling.query import (
    NodeResultSetExpression,
    Query,
    QueryResult,
    Select,
    SourceSelector,
)
from cognite.client.data_classes.data_modeling.typed_instances import TypedNodeWrite
from cognite.client.data_classes.filters import Prefix
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string


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


@pytest.fixture(scope="session")
def unit_view(cognite_client: CogniteClient, integration_test_space: Space) -> View:
    container = ContainerApply(
        space=integration_test_space.space,
        external_id="integration_test_unit_container",
        properties={"pressure": ContainerProperty(type=Float64(unit=UnitReference("pressure:bar")))},
    )
    view = ViewApply(
        space=integration_test_space.space,
        external_id="integration_test_unit_view",
        version="v1",
        properties={
            "pressure": MappedPropertyApply(container=container.as_id(), container_property_identifier="pressure")
        },
    )
    _ = cognite_client.data_modeling.containers.apply(container)
    return cognite_client.data_modeling.views.apply(view)


@pytest.fixture(scope="session")
def node_with_1_1_pressure_in_bar(
    cognite_client: CogniteClient, unit_view: View, integration_test_space: Space
) -> NodeApply:
    node = NodeApply(
        space=integration_test_space.space,
        external_id="pressure_1.1_bar_node",
        sources=[
            NodeOrEdgeData(
                unit_view.as_id(),
                {"pressure": 1.1},
            )
        ],
    )
    _ = cognite_client.data_modeling.instances.apply(node)
    return node


class PrimitiveNullable(TypedNodeWrite):
    def __init__(
        self,
        space: str,
        external_id: str,
        text: str | None = None,
        boolean: bool | None = None,
        float32: float | None = None,
        float64: float | None = None,
        int32: int | None = None,
        int64: int | None = None,
        timestamp: datetime | None = None,
        date: date | None = None,
        json: dict | None = None,
        direct: DirectRelationReference | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space=space, external_id=external_id, existing_version=existing_version, type=type)
        self.text = text
        self.boolean = boolean
        self.float32 = float32
        self.float64 = float64
        self.int32 = int32
        self.int64 = int64
        self.timestamp = timestamp
        self.date = date
        self.json = json
        self.direct = direct

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("IntegrationTestSpace", "PrimitiveNullable", "1")


class PrimitiveListed(TypedNodeWrite):
    def __init__(
        self,
        space: str,
        external_id: str,
        text: list[str] | None = None,
        boolean: list[bool] | None = None,
        float32: list[float] | None = None,
        float64: list[float] | None = None,
        int32: list[int] | None = None,
        int64: list[int] | None = None,
        timestamp: list[datetime] | None = None,
        date: list[date] | None = None,
        json: list[dict] | None = None,
        direct: list[DirectRelationReference] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        super().__init__(space=space, external_id=external_id, existing_version=existing_version, type=type)
        self.text = text
        self.boolean = boolean
        self.float32 = float32
        self.float64 = float64
        self.int32 = int32
        self.int64 = int64
        self.timestamp = timestamp
        self.date = date
        self.json = json
        self.direct = direct

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("IntegrationTestSpace", "PrimitiveListed", "1")


class TestInstancesAPI:
    def test_list_nodes(self, cognite_client: CogniteClient, movie_nodes: NodeList) -> None:
        listed_nodes = cognite_client.data_modeling.instances.list(limit=-1, instance_type="node")

        movie_node_ids = set(movie_nodes.as_ids())
        assert movie_node_ids
        assert movie_node_ids <= set(listed_nodes.as_ids())

    def test_list_edges(self, cognite_client: CogniteClient, movie_edges: EdgeList) -> None:
        listed_edges = cognite_client.data_modeling.instances.list(limit=-1, instance_type="edge")
        assert set(movie_edges.as_ids()) <= set(listed_edges.as_ids())

    def test_list_nodes_with_properties(self, cognite_client: CogniteClient, person_view: View) -> None:
        person_nodes = cognite_client.data_modeling.instances.list(
            limit=-1, instance_type="node", sources=person_view.as_id()
        )
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
        new_node = NodeApply(
            space=person_view.space,
            external_id="person:arnold_schwarzenegger" + random_string(5),
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
            created = cognite_client.data_modeling.instances.apply(new_node, replace=True)
            retrieved = cognite_client.data_modeling.instances.retrieve(new_node.as_id())

            assert len(created.nodes) == 1
            assert created.nodes[0].created_time
            assert created.nodes[0].last_updated_time
            assert len(retrieved.nodes)
            assert retrieved.nodes[0].as_id() == new_node.as_id()

            deleted_result = cognite_client.data_modeling.instances.delete(new_node.as_id())
            retrieved_deleted = cognite_client.data_modeling.instances.retrieve(new_node.as_id())

            assert len(deleted_result.nodes) == 1
            assert deleted_result.nodes[0] == new_node.as_id()
            assert len(retrieved_deleted.nodes) == 0
        finally:
            if created is not None and created.nodes and deleted_result is None:
                cognite_client.data_modeling.instances.delete(nodes=created.nodes.as_ids())

    def test_apply_nodes_and_edges(self, cognite_client: CogniteClient, person_view: View, actor_view: View) -> None:
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
            created = cognite_client.data_modeling.instances.apply(new_nodes, new_edges, replace=True)

            assert isinstance(created, InstancesApplyResult)
            assert sum(isinstance(item, NodeApplyResult) for item in created.nodes) == 2
            assert sum(isinstance(item, EdgeApplyResult) for item in created.edges) == 1
        finally:
            if created is not None:
                cognite_client.data_modeling.instances.delete(created.nodes.as_ids(), edges=created.edges.as_ids())

    def test_apply_auto_create_nodes(self, cognite_client: CogniteClient, person_view: View) -> None:
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
            created_edges = cognite_client.data_modeling.instances.apply(
                edges=person_to_actor, auto_create_start_nodes=True, auto_create_end_nodes=True, replace=True
            )
            created_nodes = cognite_client.data_modeling.instances.retrieve(node_pair)

            assert len(created_edges.edges) == 1
            assert created_edges.edges[0].created_time
            assert created_edges.edges[0].last_updated_time
            assert len(created_nodes.nodes) == 2
            assert created_nodes.nodes[0].external_id == "person:sylvester_stallone"
            assert created_nodes.nodes[1].external_id == "actor:sylvester_stallone"
        finally:
            if created_edges is not None:
                cognite_client.data_modeling.instances.delete(nodes=node_pair, edges=created_edges.edges.as_ids())

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        space = integration_test_space.space
        res = cognite_client.data_modeling.instances.delete(NodeId(space=space, external_id="DoesNotExists"))
        assert res.nodes == []
        assert res.edges == []

    def test_retrieve_multiple(self, cognite_client: CogniteClient, movie_nodes: NodeList) -> None:
        retrieved = cognite_client.data_modeling.instances.retrieve(movie_nodes.as_ids())
        assert len(retrieved.nodes) == len(movie_nodes)

    def test_retrieve_nodes_and_edges_using_id_tuples(
        self, cognite_client: CogniteClient, movie_nodes: NodeList, movie_edges: EdgeList
    ) -> None:
        retrieved = cognite_client.data_modeling.instances.retrieve(
            nodes=[(id.space, id.external_id) for id in movie_nodes.as_ids()],
            edges=[(id.space, id.external_id) for id in movie_edges.as_ids()],
        )
        assert set(retrieved.nodes.as_ids()) == set(movie_nodes.as_ids())
        assert set(retrieved.edges.as_ids()) == set(movie_edges.as_ids())

    def test_retrieve_nodes_and_edges(
        self, cognite_client: CogniteClient, movie_nodes: NodeList, movie_edges: EdgeList
    ) -> None:
        retrieved = cognite_client.data_modeling.instances.retrieve(
            nodes=movie_nodes.as_ids(), edges=movie_edges.as_ids()
        )
        assert set(retrieved.nodes.as_ids()) == set(movie_nodes.as_ids())
        assert set(retrieved.edges.as_ids()) == set(movie_edges.as_ids())

    def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, movie_nodes: NodeList) -> None:
        ids_without_missing = movie_nodes.as_ids()
        ids_with_missing = [*ids_without_missing, NodeId("myNonExistingSpace", "myImaginaryContainer")]

        retrieved = cognite_client.data_modeling.instances.retrieve(ids_with_missing)
        assert retrieved.nodes.as_ids() == ids_without_missing

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
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.instances.apply(nodes=person)

        assert error.value.code == 400
        assert "invalidProperty" in error.value.message

    def test_apply_failed_and_successful_task(
        self, cognite_client: CogniteClient, person_view: View, monkeypatch: Any
    ) -> None:
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
            with pytest.raises(CogniteAPIError) as error:
                cognite_client.data_modeling.instances.apply(nodes=[valid_person, invalid_person])

            assert "invalidProperty" in error.value.message
            assert error.value.code == 400
            assert len(error.value.successful) == 1
            assert len(error.value.failed) == 1
        finally:
            cognite_client.data_modeling.instances.delete(valid_person.as_id())

    def test_search_node_data(self, cognite_client: CogniteClient, person_view: View) -> None:
        search_result = cognite_client.data_modeling.instances.search(
            person_view.as_id(), query="Quentin", properties=["name"]
        )
        assert len(search_result) == 1
        assert search_result[0].external_id == "person:quentin_tarantino"

    def test_search_node_data_with_invalid_property(self, cognite_client: CogniteClient, person_view: View) -> None:
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.instances.search(
                person_view.as_id(), query="Quentin", properties=["invalidProperty"]
            )
        assert "Unknown property" in error.value.message

    def test_search_node_data_with_filtering(self, cognite_client: CogniteClient, person_view: View) -> None:
        view_id = person_view.as_id()
        f = filters
        born_after_2000 = f.Range([view_id.space, view_id.as_source_identifier(), "birthYear"], gt=2000)

        search_result = cognite_client.data_modeling.instances.search(
            view_id, query="Quentin", properties=["name"], filter=born_after_2000
        )
        assert len(search_result) == 0

    def test_search_with_sort(self, cognite_client: CogniteClient, person_view: View) -> None:
        search = lambda direction: cognite_client.data_modeling.instances.search(
            person_view.as_id(),
            query="",
            filter=Prefix(["node", "externalId"], "person:j"),
            sort=InstanceSort(["node", "externalId"], direction=direction),
        )
        expected_result_asc = ["person:jamie_foxx", "person:joel_coen", "person:john_travolta"]
        assert [node.external_id for node in search("ascending")] == expected_result_asc
        assert [node.external_id for node in search("descending")] == list(reversed(expected_result_asc))

    def test_aggregate_histogram_across_nodes(self, cognite_client: CogniteClient, person_view: View) -> None:
        view_id = person_view.as_id()
        birth_by_decade = aggregations.Histogram("birthYear", interval=10.0)

        histogram = cognite_client.data_modeling.instances.histogram(view_id, birth_by_decade)
        assert isinstance(histogram, HistogramValue)

        histogram_seq = cognite_client.data_modeling.instances.histogram(view_id, [birth_by_decade])
        assert len(histogram_seq) == 1 and isinstance(histogram_seq[0], HistogramValue)

    def test_aggregate_with_grouping(self, cognite_client: CogniteClient, movie_view: View) -> None:
        view_id = movie_view.as_id()
        avg_agg = aggregations.Avg("runTimeMinutes")
        max_agg = aggregations.Max("runTimeMinutes")

        result = cognite_client.data_modeling.instances.aggregate(
            view_id,
            aggregates=[avg_agg, max_agg],
            group_by="releaseYear",
        )
        assert len(result)

    def test_aggregate_multiple(self, cognite_client: CogniteClient, movie_view: View) -> None:
        view_id = movie_view.as_id()
        avg_agg = aggregations.Avg("runTimeMinutes")
        max_agg = aggregations.Max("runTimeMinutes")

        result = cognite_client.data_modeling.instances.aggregate(
            view_id,
            aggregates=[avg_agg, max_agg],
        )
        assert len(result) == 2
        assert result[0].property == "runTimeMinutes"
        assert result[1].property == "runTimeMinutes"
        max_value = next((item for item in result if isinstance(item, aggregations.MaxValue)), None)
        avg_value = next((item for item in result if isinstance(item, aggregations.AvgValue)), None)
        assert max_value is not None
        assert avg_value is not None
        assert max_value.value > avg_value.value

    def test_aggregate_count_persons(self, cognite_client: CogniteClient, person_view: View) -> None:
        view_id = person_view.as_id()
        count_agg = aggregations.Count("name")

        count = cognite_client.data_modeling.instances.aggregate(
            view_id,
            aggregates=count_agg,
            instance_type="node",
            limit=10,
        )
        assert count.property == "name"
        assert count.value > 0, "Add at least one person to the view to run this test"

    def test_aggregate_invalid_view_id(self, cognite_client: CogniteClient) -> None:
        view_id = ViewId("myNonExistingSpace", "myNonExistingView", "myNonExistingVersion")
        count_agg = aggregations.Count("externalId")

        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.instances.aggregate(
                view_id,
                aggregates=count_agg,
                instance_type="node",
                limit=10,
            )
        assert error.value.code == 400
        assert "One or more views do not exist: " in error.value.message
        assert view_id.as_source_identifier() in error.value.message

    def test_dump_json_serialize_load_node(self, movie_nodes: NodeList) -> None:
        node = movie_nodes.get(external_id="movie:pulp_fiction")
        assert node is not None, "Pulp fiction movie not found, please recreate it"

        node_dumped = node.dump(camel_case=True)
        node_json = json.dumps(node_dumped)
        node_loaded = Node.load(node_json)

        assert node == node_loaded

    def test_dump_json_serialize_load_edge(self, movie_edges: EdgeList) -> None:
        edge = movie_edges.get(external_id="person:quentin_tarantino:director:quentin_tarantino")
        assert edge is not None, "Relation between Quentin Tarantino person and director not found, please recreate it"

        edge_dumped = edge.dump(camel_case=True)
        edge_json = json.dumps(edge_dumped)
        edge_loaded = Edge.load(edge_json)

        assert edge == edge_loaded

    def test_query_oscar_winning_actors_before_2000(
        self, cognite_client: CogniteClient, movie_view: View, actor_view: View
    ) -> None:
        # Create a query that finds all actors that won Oscars in a movies released before 2000 sorted by external id.
        movie_id = movie_view.as_id()
        actor_id = actor_view.as_id()
        f, q = filters, query
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
        result = cognite_client.data_modeling.instances.query(my_query)

        assert len(result["movies"]) > 0, "Add at least one movie with release year before 2000"
        assert all(
            cast(int, movie.properties.get(movie_id, {}).get("releaseYear")) < 2000 for movie in result["movies"]
        )
        assert result["movies"] == sorted(result["movies"], key=lambda x: x.properties.get(movie_id, {}).get("title"))
        assert len(result["actors"]) > 0, "Add at least one actor that acted in the movies released before 2000"
        assert all(actor.properties.get(actor_id, {}).get("wonOscar") for actor in result["actors"])
        assert result["actors"] == sorted(result["actors"], key=lambda x: x.external_id)

    def test_retrieve_in_units(
        self, cognite_client: CogniteClient, node_with_1_1_pressure_in_bar: NodeApply, unit_view: View
    ) -> None:
        node = node_with_1_1_pressure_in_bar
        source = SourceSelector(unit_view.as_id(), target_units=[TargetUnit("pressure", UnitReference("pressure:pa"))])

        retrieved = cognite_client.data_modeling.instances.retrieve(node.as_id(), sources=[source])
        assert retrieved.nodes
        assert math.isclose(retrieved.nodes[0]["pressure"], 1.1 * 1e5)

    def test_list_in_units(
        self, cognite_client: CogniteClient, node_with_1_1_pressure_in_bar: NodeApply, unit_view: View
    ) -> None:
        source = SourceSelector(unit_view.as_id(), target_units=[TargetUnit("pressure", UnitReference("pressure:pa"))])
        is_node = filters.Equals(["node", "externalId"], node_with_1_1_pressure_in_bar.external_id)
        listed = cognite_client.data_modeling.instances.list(instance_type="node", filter=is_node, sources=[source])

        assert listed
        assert len(listed) == 1
        assert math.isclose(listed[0]["pressure"], 1.1 * 1e5)

    def test_search_in_units(
        self, cognite_client: CogniteClient, node_with_1_1_pressure_in_bar: NodeApply, unit_view: View
    ) -> None:
        target_units = [TargetUnit("pressure", UnitReference("pressure:pa"))]
        is_node = filters.Equals(["node", "externalId"], node_with_1_1_pressure_in_bar.external_id)

        searched = cognite_client.data_modeling.instances.search(
            view=unit_view.as_id(), query="", filter=is_node, target_units=target_units
        )

        assert searched
        assert len(searched) == 1
        assert math.isclose(searched[0]["pressure"], 1.1 * 1e5)

    def test_aggregate_in_units(
        self, cognite_client: CogniteClient, node_with_1_1_pressure_in_bar: NodeApply, unit_view: View
    ) -> None:
        target_units = [TargetUnit("pressure", UnitReference("pressure:pa"))]
        is_node = filters.Equals(["node", "externalId"], node_with_1_1_pressure_in_bar.external_id)

        aggregated = cognite_client.data_modeling.instances.aggregate(
            view=unit_view.as_id(),
            aggregates=[aggregations.Avg("pressure")],
            target_units=target_units,
            filter=is_node,
        )

        assert aggregated
        assert len(aggregated) == 1
        assert math.isclose(aggregated[0].value, 1.1 * 1e5)

    def test_query_in_units(
        self, cognite_client: CogniteClient, node_with_1_1_pressure_in_bar: NodeApply, unit_view: View
    ) -> None:
        is_node = filters.Equals(["node", "externalId"], node_with_1_1_pressure_in_bar.external_id)
        target_units = [TargetUnit("pressure", UnitReference("pressure:pa"))]
        query = Query(
            with_={"nodes": NodeResultSetExpression(filter=is_node, limit=1)},
            select={"nodes": Select([SourceSelector(unit_view.as_id(), ["pressure"], target_units)])},
        )
        queried = cognite_client.data_modeling.instances.query(query)

        assert queried
        assert len(queried["nodes"]) == 1
        assert math.isclose(queried["nodes"][0]["pressure"], 1.1 * 1e5)

    @pytest.mark.usefixtures("primitive_nullable_view")
    def test_write_typed_node(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        space = integration_test_space.space
        external_id = "node_test_write_read_custom_properties"
        primitive = PrimitiveNullable(
            space=space,
            external_id=external_id,
            text="text",
            boolean=True,
            float32=1.1,
            float64=1.1,
            int32=1,
            int64=1,
            # Replacing microseconds with 0 to avoid comparison issues in the asserting below
            # as server only stores milliseconds
            timestamp=datetime.now(timezone.utc).replace(microsecond=0),
            date=date.today(),
            json={"key": "value", "nested": {"key": "value"}},
            direct=DirectRelationReference(space, external_id),
        )
        try:
            created = cognite_client.data_modeling.instances.apply(primitive)
            assert len(created.nodes) == 1
            assert created.nodes[0].external_id == external_id
        finally:
            cognite_client.data_modeling.instances.delete(primitive.as_id())

    @pytest.mark.usefixtures("primitive_nullable_listed_view")
    def test_write_typed_node_listed_properties(
        self, cognite_client: CogniteClient, integration_test_space: Space
    ) -> None:
        space = integration_test_space.space
        external_id = "node_test_write_read_custom_properties_listable"
        primitive_listed = PrimitiveListed(
            space=space,
            external_id=external_id,
            text=["text"],
            boolean=[True],
            float32=[1.1],
            float64=[1.1],
            int32=[1],
            int64=[1],
            # Replacing microseconds with 0 to avoid comparison issues in the asserting below
            timestamp=[datetime.now(timezone.utc).replace(microsecond=0)],
            date=[date.today()],
            json=[{"key": "value", "nested": {"key": "value"}}],
            direct=[DirectRelationReference(space, external_id)],
        )
        try:
            created = cognite_client.data_modeling.instances.apply(primitive_listed)
            assert len(created.nodes) == 1
            assert created.nodes[0].external_id == external_id
        finally:
            cognite_client.data_modeling.instances.delete(primitive_listed.as_id())


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

            assert len(new_result["movies"]) == 1, "Only the new movie should be returned"
            assert new_result["movies"][0].external_id == new_1994_movie.external_id
        finally:
            cognite_client.data_modeling.instances.delete(new_1994_movie.as_id())

    def test_subscribe_to_movies_released_in_1994(self, cognite_client: CogniteClient, movie_view: View) -> None:
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
