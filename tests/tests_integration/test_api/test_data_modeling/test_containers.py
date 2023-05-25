import pytest

from cognite.client import CogniteClient


@pytest.fixture()
def cdf_containers(cognite_client: CogniteClient):
    containers = cognite_client.data_modeling.containers.list(limit=-1)
    assert len(containers), "There must be at least one container in CDF"
    return containers


class TestContainersAPI:
    def test_list(self, cognite_client: CogniteClient, cdf_containers):
        actual_containers = cognite_client.data_modeling.containers.list(limit=-1)
        assert actual_containers == cdf_containers
