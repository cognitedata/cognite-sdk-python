from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import pandas as pd
import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    DataModel,
    DataModelApply,
    EdgeApply,
    NodeApply,
    NodeOrEdgeData,
    SingleHopConnectionDefinition,
    Space,
    SpaceApply,
    View,
    ViewList,
)
from cognite.client.data_classes.data_modeling.ids import DataModelId, ViewId
from cognite.client.data_classes.data_modeling.instances import EdgeApplyList, InstancesResult, NodeApplyList

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture(scope="session")
def integration_test_space(cognite_client: CogniteClient) -> Space:
    space_id = "IntegrationTestSpace"
    space = cognite_client.data_modeling.spaces.retrieve(space_id)
    if space is not None:
        return space
    new_space = SpaceApply(space_id, name="Integration Test Space", description="The space used for integration tests.")
    return cognite_client.data_modeling.spaces.apply(new_space)


@pytest.fixture(scope="session")
def movie_model(cognite_client: CogniteClient, integration_test_space: Space) -> DataModel[View]:
    movie_id = DataModelId(space=integration_test_space.space, external_id="Movie")
    models = cognite_client.data_modeling.data_models.retrieve(movie_id, inline_views=True)
    if models:
        return models.latest_version()
    movie_id = DataModelId(space=integration_test_space.space, external_id=movie_id.external_id, version="1")
    graphql = (RESOURCES / "movie_model.graphql").read_text()
    created = cognite_client.data_modeling.graphql.apply_dml(
        id=movie_id, dml=graphql, name="Movie Model", description="The Movie Model used in Integration Tests"
    )
    models = cognite_client.data_modeling.data_models.retrieve(created.as_id(), inline_views=True)
    assert models, "The Movie Model was not created"
    return models.latest_version()


@pytest.fixture(scope="session")
def empty_model(cognite_client: CogniteClient, integration_test_space: Space) -> DataModel[ViewId]:
    new_data_model = DataModelApply(
        space=integration_test_space.space,
        external_id="EmptyDataModel",
        version="v1",
        description="Integration test an empty data model",
        name="An Empty Data Model",
    )
    model = cognite_client.data_modeling.data_models.retrieve(new_data_model.as_id())
    if model:
        return model.latest_version()
    return cognite_client.data_modeling.data_models.apply(new_data_model)


@pytest.fixture(scope="session")
def populated_movie(cognite_client: CogniteClient, movie_model: DataModel[View]) -> InstancesResult:
    views = ViewList(movie_model.views)
    nodes = _read_nodes(views)
    edges = _read_edges(views)
    result = cognite_client.data_modeling.instances.retrieve(nodes.as_ids(), edges.as_ids())
    if len(result.nodes) == len(nodes) and len(result.edges) == len(edges):
        return result

    created = cognite_client.data_modeling.instances.apply(nodes, edges)
    result = cognite_client.data_modeling.instances.retrieve(created.nodes.as_ids(), created.edges.as_ids())
    return result


@dataclass
class ID:
    """
    Identifier used to create an external id based on
    columns in a dataframe. The ID will be "[prefix]:[column1]:[column2]:..."
    """

    prefix: str
    columns: list[str]

    def create(self, row: pd.Series) -> str:
        return (":".join([self.prefix] + [str(row[c]) for c in self.columns])).replace(" ", "_").lower()


@dataclass
class DirectID(ID):
    field_name: str


@dataclass
class NodeSource:
    view_id: str
    csv_path: Path
    id: ID
    properties: list[str]
    direct_relations: list[DirectID] = field(default_factory=list)


NODE_SOURCES = [
    NodeSource("Movie", RESOURCES / "movies.csv", ID("movie", ["title"]), ["title", "releaseYear", "runTimeMinutes"]),
    NodeSource("Person", RESOURCES / "persons.csv", ID("person", ["name"]), ["name", "birthYear"]),
    NodeSource(
        "Actor",
        RESOURCES / "actors.csv",
        ID("actor", ["personName"]),
        ["wonOscar"],
        [DirectID("person", ["personName"], "person")],
    ),
    NodeSource(
        "Director",
        RESOURCES / "directors.csv",
        ID("director", ["personName"]),
        ["wonOscar"],
        [DirectID("person", ["personName"], "person")],
    ),
]


@dataclass
class EdgeSource:
    view_id: str
    field_name: str
    start: ID
    end: ID
    csv_path: Path


EDGE_SOURCES = [
    EdgeSource(
        "Person",
        "roles",
        ID("person", ["personName"]),
        ID("actor", ["personName"]),
        RESOURCES / "relation_actors_movies.csv",
    ),
    EdgeSource(
        "Person",
        "roles",
        ID("person", ["personName"]),
        ID("director", ["personName"]),
        RESOURCES / "relation_directors_movies.csv",
    ),
    EdgeSource(
        "Movie", "actors", ID("movie", ["movie"]), ID("actor", ["personName"]), RESOURCES / "relation_actors_movies.csv"
    ),
    EdgeSource(
        "Movie",
        "directors",
        ID("movie", ["movie"]),
        ID("director", ["personName"]),
        RESOURCES / "relation_directors_movies.csv",
    ),
    EdgeSource(
        "Actor", "movies", ID("actor", ["personName"]), ID("movie", ["movie"]), RESOURCES / "relation_actors_movies.csv"
    ),
    EdgeSource(
        "Director",
        "movies",
        ID("director", ["personName"]),
        ID("movie", ["movie"]),
        RESOURCES / "relation_directors_movies.csv",
    ),
]


def _read_nodes(views: ViewList) -> NodeApplyList:
    nodes: list[NodeApply] = []
    for source in NODE_SOURCES:
        df = pd.read_csv(source.csv_path).infer_objects()
        view = cast(View, views.get(external_id=source.view_id))
        for _, row in df.iterrows():
            external_id = source.id.create(row)

            node = NodeApply(
                space=view.space,
                external_id=external_id,
                sources=[
                    NodeOrEdgeData(
                        source=view.as_id(),
                        properties={
                            **row[source.properties].to_dict(),
                            **{
                                direct.field_name: {"space": view.space, "externalId": direct.create(row)}
                                for direct in source.direct_relations
                            },
                        },
                    )
                ],
            )
            nodes.append(node)
    return NodeApplyList(nodes)


def _read_edges(views: ViewList) -> EdgeApplyList:
    edges: list[EdgeApply] = []
    for source in EDGE_SOURCES:
        df = pd.read_csv(source.csv_path).infer_objects()
        df = df[list(set(source.start.columns + source.end.columns))].drop_duplicates()
        view = cast(View, views.get(external_id=source.view_id))
        for _, row in df.iterrows():
            start_ext_id = source.start.create(row)
            end_ext_id = source.end.create(row)
            type_ext_id = cast(SingleHopConnectionDefinition, view.properties[source.field_name]).type.external_id
            edge = EdgeApply(
                space=view.space,
                external_id=f"{start_ext_id}:{end_ext_id}",
                type=(view.space, type_ext_id),
                start_node=(view.space, start_ext_id),
                end_node=(view.space, end_ext_id),
            )
            edges.append(edge)
    return EdgeApplyList(edges)
