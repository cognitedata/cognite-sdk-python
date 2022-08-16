import pytest

from cognite.client import ClientConfig, CogniteClient


@pytest.fixture(scope="module")
def cognite_client():
    cnf = ClientConfig(client_name="any", project="dummy", api_key="bla")
    yield CogniteClient(cnf)
