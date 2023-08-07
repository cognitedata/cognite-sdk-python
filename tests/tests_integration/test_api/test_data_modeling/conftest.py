import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Space, SpaceApply


@pytest.fixture(scope="session")
def integration_test_space(cognite_client: CogniteClient) -> Space:
    space_id = "IntegrationTestSpace"
    space = cognite_client.data_modeling.spaces.retrieve(space_id)
    if space is not None:
        return space
    new_space = SpaceApply(space_id, name="Integration Test Space", description="The space used for integration tests.")
    return cognite_client.data_modeling.spaces.apply(new_space)
