import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Space


@pytest.fixture(scope="session")
def integration_test_space(cognite_client: CogniteClient) -> Space:
    space = cognite_client.data_modeling.spaces.retrieve("IntegrationTestsImmutable")
    assert space is not None, "could not find space IntegrationTestsImmutable"
    return space
