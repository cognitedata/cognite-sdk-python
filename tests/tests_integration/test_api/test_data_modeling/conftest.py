from pathlib import Path

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import DataModel, DataModelApply, Space, SpaceApply, View
from cognite.client.data_classes.data_modeling.ids import DataModelId, ViewId

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
