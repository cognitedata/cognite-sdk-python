import pytest

import cognite.client.data_classes.data_modeling as dm
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture()
def cdf_containers(cognite_client: CogniteClient):
    containers = cognite_client.data_modeling.containers.list(limit=-1)
    assert len(containers), "There must be at least one container in CDF"
    return containers


class TestContainersAPI:
    def test_list(
        self, cognite_client: CogniteClient, cdf_containers: dm.ContainerList, integration_test_space: dm.Space
    ):
        # Arrange
        expected_containers = dm.ContainerList([c for c in cdf_containers if c.space == integration_test_space.space])

        # Act
        actual_containers = cognite_client.data_modeling.containers.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert sorted(actual_containers, key=lambda c: c.external_id) == sorted(
            expected_containers, key=lambda c: c.external_id
        )
        assert all(c.space == integration_test_space.space for c in actual_containers)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, integration_test_space: dm.Space):
        # Arrange
        new_container = dm.ContainerApply(
            space=integration_test_space.space,
            external_id="IntegrationTestContainer",
            properties={
                "name": dm.ContainerProperty(
                    type=dm.Text(),
                ),
                "year": dm.ContainerProperty(type=dm.Int32()),
            },
            description="Integration test, should not persist",
            name="Create and delete container",
            used_for="node",
        )

        # Act
        created = cognite_client.data_modeling.containers.apply(new_container)
        retrieved = cognite_client.data_modeling.containers.retrieve((new_container.space, new_container.external_id))

        # Assert
        assert created.created_time
        assert created.last_updated_time
        assert retrieved.as_apply().dump() == new_container.dump()

        # Act
        deleted_id = cognite_client.data_modeling.containers.delete((new_container.space, new_container.external_id))
        retrieved_deleted = cognite_client.data_modeling.containers.retrieve(
            (new_container.space, new_container.external_id)
        )

        # Assert
        assert deleted_id[0] == dm.ContainerId(new_container.space, new_container.external_id)
        assert retrieved_deleted is None

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: dm.Space):
        space = integration_test_space.space
        assert (
            cognite_client.data_modeling.containers.delete(dm.ContainerId(space=space, external_id="DoesNotExists"))
            == []
        )

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_containers: dm.ContainerList):
        assert len(cdf_containers) >= 2, "Please add at least two containers to the test environment"
        # Arrange
        ids = [dm.ContainerId(c.space, c.external_id) for c in cdf_containers]

        # Act
        retrieved = cognite_client.data_modeling.containers.retrieve(ids)

        # Assert
        assert [container.as_id() for container in retrieved] == ids

    def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, cdf_containers: dm.ContainerList):
        assert len(cdf_containers) >= 2, "Please add at least two containers to the test environment"
        # Arrange
        ids_without_missing = [dm.ContainerId(c.space, c.external_id) for c in cdf_containers]
        ids_with_missing = [*ids_without_missing, dm.ContainerId("myNonExistingSpace", "myImaginaryContainer")]

        # Act
        retrieved = cognite_client.data_modeling.containers.retrieve(ids_with_missing)

        # Assert
        assert [container.as_id() for container in retrieved] == ids_without_missing

    def test_retrieve_non_existent(self, cognite_client: CogniteClient):
        assert cognite_client.data_modeling.containers.retrieve(("myNonExistingSpace", "myImaginaryContainer")) is None

    def test_iterate_over_containers(self, cognite_client: CogniteClient):
        for containers in cognite_client.data_modeling.containers(chunk_size=2, limit=-1):
            assert isinstance(containers, dm.ContainerList)

    def test_apply_invalid_container(self, cognite_client: CogniteClient):
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.containers.apply(
                dm.ContainerApply(
                    space="nonExistingSpace",
                    external_id="myContainer",
                    properties={"name": dm.ContainerProperty(type=dm.Text())},
                    used_for="node",
                )
            )

        # Assert
        assert error.value.code == 400
        assert "One or more spaces do not exist" in error.value.message

    def test_apply_failed_and_successful_task(
        self, cognite_client: CogniteClient, integration_test_space: dm.Space, monkeypatch
    ):
        # Arrange
        valid_container = dm.ContainerApply(
            space=integration_test_space.space,
            external_id="IntegrationTestContainer",
            properties={
                "name": dm.ContainerProperty(
                    type=dm.Text(),
                ),
            },
            used_for="node",
        )
        invalid_container = dm.ContainerApply(
            space="nonExistingSpace",
            external_id="myContainer",
            properties={"name": dm.ContainerProperty(type=dm.Text())},
            used_for="node",
        )
        monkeypatch.setattr(cognite_client.data_modeling.containers, "_CREATE_LIMIT", 1)

        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.containers.apply([valid_container, invalid_container])

        # Assert
        assert "One or more spaces do not exist" in error.value.message
        assert error.value.code == 400
        assert len(error.value.successful) == 1
        assert len(error.value.failed) == 1

        # Cleanup
        cognite_client.data_modeling.instances.delete(valid_container.as_id())
