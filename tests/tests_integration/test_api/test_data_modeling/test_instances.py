import pytest

import cognite.client.data_classes.data_modeling as models
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling.instances import InstanceList


@pytest.fixture()
def cdf_nodes(cognite_client: CogniteClient):
    nodes = cognite_client.data_modeling.instances.list()
    assert len(nodes) > 0, "Add at least one node to CDF"
    return nodes


class TestInstancesAPI:
    def test_list(self, cognite_client: CogniteClient, cdf_nodes: InstanceList, integration_test_space: models.Space):
        # Arrange
        expected_nodes = models.ViewList([n for n in cdf_nodes if n.space == integration_test_space.space])

        # Act
        actual_nodes = cognite_client.data_modeling.instances.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert sorted(actual_nodes, key=lambda v: v.external_id) == sorted(expected_nodes, key=lambda v: v.external_id)
        assert all(v.space == integration_test_space.space for v in actual_nodes)
