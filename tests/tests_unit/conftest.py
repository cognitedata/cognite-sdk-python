import pytest

from cognite.client import ClientConfig, CogniteClient
from cognite.client.beta import CogniteClient as CogniteBetaClient
from cognite.client.credentials import APIKey


@pytest.fixture(scope="module")
def client_config() -> ClientConfig:
    return ClientConfig(client_name="any", project="dummy", credentials=APIKey("bla"))


@pytest.fixture(scope="module")
def cognite_client(client_config: ClientConfig) -> CogniteClient:
    yield CogniteClient(client_config)


@pytest.fixture(scope="module")
def cognite_beta_client(client_config: ClientConfig) -> CogniteBetaClient:
    yield CogniteBetaClient(client_config)
