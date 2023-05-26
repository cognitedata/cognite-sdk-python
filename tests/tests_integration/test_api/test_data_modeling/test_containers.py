import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    Container,
    ContainerList,
    ContainerPropertyIdentifier,
    PrimitiveProperty,
    Space,
    TextProperty,
)
from cognite.client.data_classes.data_modeling.ids import DataModelingId


@pytest.fixture()
def cdf_containers(cognite_client: CogniteClient):
    containers = cognite_client.data_modeling.containers.list(limit=-1)
    assert len(containers), "There must be at least one container in CDF"
    return containers


class TestContainersAPI:
    def test_list(self, cognite_client: CogniteClient, cdf_containers: ContainerList, integration_test_space: Space):
        # Arrange
        expected_containers = ContainerList([c for c in cdf_containers if c.space == integration_test_space.space])

        # Act
        actual_containers = cognite_client.data_modeling.containers.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert sorted(actual_containers, key=lambda c: c.external_id) == sorted(
            expected_containers, key=lambda c: c.external_id
        )
        assert all(c.space == integration_test_space.space for c in actual_containers)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, integration_test_space: Space):
        # Arrange
        new_container = Container(
            space=integration_test_space.space,
            external_id="IntegrationTestContainer",
            description="Integration test, should not persist",
            name="Create and delete container",
            used_for="node",
            properties={
                "name": ContainerPropertyIdentifier(
                    type=TextProperty(list=False),
                ),
                "year": ContainerPropertyIdentifier(type=PrimitiveProperty("int32", list=False)),
            },
        )

        # Act
        created = cognite_client.data_modeling.containers.apply(new_container)
        retrieved = cognite_client.data_modeling.containers.retrieve((new_container.space, new_container.external_id))

        # Assert
        assert created.created_time
        assert created.last_updated_time
        assert created.external_id == new_container.external_id
        assert retrieved.external_id == new_container.external_id

        # Act
        deleted_id = cognite_client.data_modeling.containers.delete((new_container.space, new_container.external_id))
        retrieved_deleted = cognite_client.data_modeling.containers.retrieve(
            (new_container.space, new_container.external_id)
        )

        # Assert
        assert deleted_id[0] == DataModelingId.from_tuple((new_container.space, new_container.external_id))
        assert retrieved_deleted is None
