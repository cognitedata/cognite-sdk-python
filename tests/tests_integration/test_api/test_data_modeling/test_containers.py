import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ContainerList


@pytest.fixture()
def cdf_containers(cognite_client: CogniteClient):
    containers = cognite_client.data_modeling.containers.list(limit=-1)
    assert len(containers), "There must be at least one container in CDF"
    return containers


class TestContainersAPI:
    def test_list(self, cognite_client: CogniteClient, cdf_containers: ContainerList, integration_test_space):
        # Arrange
        expected_containers = ContainerList([c for c in cdf_containers if c.space == integration_test_space.space])

        # Act
        actual_containers = cognite_client.data_modeling.containers.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert sorted(actual_containers, key=lambda c: c.external_id) == sorted(
            expected_containers, key=lambda c: c.external_id
        )
        assert all(c.space == integration_test_space.space for c in actual_containers)
