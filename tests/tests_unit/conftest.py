import pytest

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import Token


@pytest.fixture(scope="class")
def cognite_client():
    cnf = ClientConfig(client_name="any", project="dummy", credentials=Token("bla"))
    yield CogniteClient(cnf)
