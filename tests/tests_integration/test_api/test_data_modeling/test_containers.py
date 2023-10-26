from __future__ import annotations

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
    DataModel,
    Int32,
    MappedProperty,
    Space,
    Text,
    View,
)
from cognite.client.data_classes.data_modeling.containers import BTreeIndex, UniquenessConstraint
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture(scope="session")
def movie_containers(cognite_client: CogniteClient, movie_model: DataModel[View]) -> ContainerList:
    container_ids = {
        prop.container
        for v in movie_model.views
        for name, prop in v.properties.items()
        if isinstance(prop, MappedProperty)
    }
    containers = cognite_client.data_modeling.containers.retrieve(list(container_ids))
    assert len(containers) == len(container_ids), "The movie model is missing containers"
    return containers


class TestContainersAPI:
    def test_list(
        self, cognite_client: CogniteClient, movie_containers: ContainerList, integration_test_space: Space
    ) -> None:
        # Arrange
        expected_containers = ContainerList([c for c in movie_containers if c.space == integration_test_space.space])
        expected_ids = set(expected_containers.as_ids())

        # Act
        actual_containers = cognite_client.data_modeling.containers.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert expected_ids, "The movie model is missing containers"
        assert expected_ids <= set(actual_containers.as_ids())
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
            constraints={"uniqueName": UniquenessConstraint(properties=["name"])},
            indexes={"nameIdx": BTreeIndex(properties=["name"])},
        )
        created: Container | None = None
        deleted_ids: list[ContainerId] = []
        # Act
        try:
            created = cognite_client.data_modeling.containers.apply(new_container)
            retrieved = cognite_client.data_modeling.containers.retrieve(new_container.as_id())

            # Assert
            assert retrieved is not None
            assert created.created_time
            assert created.last_updated_time
            assert retrieved.as_apply().dump() == new_container.dump()

            # Act
            deleted_indexes = cognite_client.data_modeling.containers.delete_indexes(
                [(new_container.as_id(), "nameIdx")]
            )
            assert deleted_indexes == [(new_container.as_id(), "nameIdx")]
            deleted_constraints = cognite_client.data_modeling.containers.delete_constraints(
                [(new_container.as_id(), "uniqueName")]
            )
            assert deleted_constraints == [(new_container.as_id(), "uniqueName")]
            deleted_ids = cognite_client.data_modeling.containers.delete(new_container.as_id())
            retrieved_deleted = cognite_client.data_modeling.containers.retrieve(new_container.as_id())

            # Assert
            assert len(deleted_ids) == 1
            assert deleted_ids[0] == new_container.as_id()
            assert retrieved_deleted is None
        finally:
            # Cleanup
            if created and not deleted_ids:
                cognite_client.data_modeling.containers.delete(created.as_id())

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        space = integration_test_space.space
        assert (
            cognite_client.data_modeling.containers.delete(ContainerId(space=space, external_id="DoesNotExists")) == []
        )

    def test_retrieve_multiple(self, cognite_client: CogniteClient, movie_containers: ContainerList) -> None:
        # Arrange
        ids = movie_containers.as_ids()

        # Act
        retrieved = cognite_client.data_modeling.containers.retrieve(ids)

        # Assert
        assert set(retrieved.as_ids()) == set(ids)

    def test_retrieve_multiple_with_missing(
        self, cognite_client: CogniteClient, movie_containers: ContainerList
    ) -> None:
        # Arrange
        ids_without_missing = movie_containers.as_ids()
        ids_with_missing = [*ids_without_missing, ContainerId("myNonExistingSpace", "myImaginaryContainer")]

        # Act
        retrieved = cognite_client.data_modeling.containers.retrieve(ids_with_missing)

        # Assert
        assert set(retrieved.as_ids()) == set(ids_without_missing)

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

    def test_dump_json_serialize_load(self, movie_containers: ContainerList) -> None:
        # Arrange
        container = movie_containers.get(external_id="Movie")
        assert container is not None, "Movie container is missing from test environment"

        # Act
        container_dump = container.dump(camel_case=True)
        container_json = json.dumps(container_dump)
        container_loaded = Container.load(container_json)

        # Assert
        assert container == container_loaded

    def test_load_and_create_only_required_fields(
        self, cognite_client: CogniteClient, integration_test_space: Space
    ) -> None:
        external_id = "test_load_and_create_only_required_fields"
        data = {
            "externalId": external_id,
            "space": integration_test_space.space,
            "properties": {"name": {"type": {"type": "text"}}},
            "indexes": {"nameIdx": {"properties": ["name"], "indexType": "btree"}},
        }

        container = ContainerApply.load(data)

        try:
            created = cognite_client.data_modeling.containers.apply(container)

            assert created.external_id == external_id
        finally:
            cognite_client.data_modeling.containers.delete(container.as_id())
