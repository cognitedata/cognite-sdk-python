import pytest

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import APIKey


@pytest.fixture(scope="module")
def cognite_client():
    cnf = ClientConfig(client_name="any", project="dummy", credentials=APIKey("bla"))
    yield CogniteClient(cnf)
