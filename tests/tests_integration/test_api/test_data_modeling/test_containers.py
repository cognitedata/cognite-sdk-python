import json
from typing import Any

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    Container,
    ContainerApply,
    ContainerId,
    ContainerList,
    ContainerProperty,
    Int32,
    Space,
    Text,
)
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture()
def cdf_containers(cognite_client: CogniteClient) -> ContainerList:
    containers = cognite_client.data_modeling.containers.list(limit=-1)
    assert len(containers), "There must be at least one container in CDF"
    return containers


class TestContainersAPI:
    def test_list(
        self, cognite_client: CogniteClient, cdf_containers: ContainerList, integration_test_space: Space
    ) -> None:
        # Arrange
        expected_containers = ContainerList([c for c in cdf_containers if c.space == integration_test_space.space])

        # Act
        actual_containers = cognite_client.data_modeling.containers.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert sorted(actual_containers, key=lambda c: c.external_id) == sorted(
            expected_containers, key=lambda c: c.external_id
        )
        assert all(c.space == integration_test_space.space for c in actual_containers)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        # Arrange
        new_container = ContainerApply(
            space=integration_test_space.space,
            external_id="IntegrationTestContainer",
            properties={
                "name": ContainerProperty(
                    type=Text(),
                ),
                "year": ContainerProperty(type=Int32()),
            },
            description="Integration test, should not persist",
            name="Create and delete container",
            used_for="node",
        )

        # Act
        created = cognite_client.data_modeling.containers.apply(new_container)
        retrieved = cognite_client.data_modeling.containers.retrieve((new_container.space, new_container.external_id))

        # Assert
        assert retrieved is not None
        assert created.created_time
        assert created.last_updated_time
        assert retrieved.as_apply().dump() == new_container.dump()

        # Act
        deleted_id = cognite_client.data_modeling.containers.delete((new_container.space, new_container.external_id))
        retrieved_deleted = cognite_client.data_modeling.containers.retrieve(
            (new_container.space, new_container.external_id)
        )

        # Assert
        assert deleted_id[0] == ContainerId(new_container.space, new_container.external_id)
        assert retrieved_deleted is None

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        space = integration_test_space.space
        assert (
            cognite_client.data_modeling.containers.delete(ContainerId(space=space, external_id="DoesNotExists")) == []
        )

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_containers: ContainerList) -> None:
        assert len(cdf_containers) >= 2, "Please add at least two containers to the test environment"
        # Arrange
        ids = [ContainerId(c.space, c.external_id) for c in cdf_containers]

        # Act
        retrieved = cognite_client.data_modeling.containers.retrieve(ids)

        # Assert
        assert [container.as_id() for container in retrieved] == ids

    def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, cdf_containers: ContainerList) -> None:
        assert len(cdf_containers) >= 2, "Please add at least two containers to the test environment"
        # Arrange
        ids_without_missing = [ContainerId(c.space, c.external_id) for c in cdf_containers]
        ids_with_missing = [*ids_without_missing, ContainerId("myNonExistingSpace", "myImaginaryContainer")]

        # Act
        retrieved = cognite_client.data_modeling.containers.retrieve(ids_with_missing)

        # Assert
        assert [container.as_id() for container in retrieved] == ids_without_missing

    def test_retrieve_non_existent(self, cognite_client: CogniteClient) -> None:
        assert cognite_client.data_modeling.containers.retrieve(("myNonExistingSpace", "myImaginaryContainer")) is None

    def test_iterate_over_containers(self, cognite_client: CogniteClient) -> None:
        for containers in cognite_client.data_modeling.containers(chunk_size=2, limit=-1):
            assert isinstance(containers, ContainerList)

    def test_apply_invalid_container(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.containers.apply(
                ContainerApply(
                    space="nonExistingSpace",
                    external_id="myContainer",
                    properties={"name": ContainerProperty(type=Text())},
                    used_for="node",
                )
            )

        # Assert
        assert error.value.code == 400
        assert "One or more spaces do not exist" in error.value.message

    def test_apply_failed_and_successful_task(
        self, cognite_client: CogniteClient, integration_test_space: Space, monkeypatch: Any
    ) -> None:
        # Arrange
        valid_container = ContainerApply(
            space=integration_test_space.space,
            external_id="IntegrationTestContainer",
            properties={
                "name": ContainerProperty(
                    type=Text(),
                ),
            },
            used_for="node",
        )
        invalid_container = ContainerApply(
            space="nonExistingSpace",
            external_id="myContainer",
            properties={"name": ContainerProperty(type=Text())},
            used_for="node",
        )
        monkeypatch.setattr(cognite_client.data_modeling.containers, "_CREATE_LIMIT", 1)

        try:
            # Act
            with pytest.raises(CogniteAPIError) as error:
                cognite_client.data_modeling.containers.apply([valid_container, invalid_container])

            # Assert
            assert "One or more spaces do not exist" in error.value.message
            assert error.value.code == 400
            assert len(error.value.successful) == 1
            assert len(error.value.failed) == 1

        finally:
            # Cleanup
            cognite_client.data_modeling.containers.delete(valid_container.as_id())

    def test_dump_json_serialize_load(self, cdf_containers: ContainerList) -> None:
        # Arrange
        container = cdf_containers.get(external_id="Movie")
        assert container is not None, "Movie container is missing from test environment"

        # Act
        container_dump = container.dump(camel_case=True)
        container_json = json.dumps(container_dump)
        container_loaded = Container.load(container_json)

        # Assert
        assert container == container_loaded
