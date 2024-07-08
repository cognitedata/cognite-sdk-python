from __future__ import annotations

import json

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    Container,
    ContainerApply,
    ContainerId,
    ContainerList,
    ContainerProperty,
    DataModel,
    Float64,
    MappedProperty,
    Space,
    Text,
    View,
)
from cognite.client.data_classes.data_modeling.data_types import UnitReference
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


@pytest.fixture(scope="session")
def unit_pressure_container(cognite_client: CogniteClient, integration_test_space: Space) -> Container:
    unit_container = ContainerApply(
        space=integration_test_space.space,
        external_id="test_container_with_unit",
        properties={"pressure": ContainerProperty(type=Float64(unit=UnitReference(external_id="pressure:bar")))},
        used_for="node",
    )

    return cognite_client.data_modeling.containers.apply(unit_container)


class TestContainersAPI:
    """Dev. note: We do not do the "create+delete pattern" in these tests as it has a bunch of
    undesireable effects on the system (e.g. elasticsearch mapping explosion).
    """

    def test_list(
        self, cognite_client: CogniteClient, movie_containers: ContainerList, integration_test_space: Space
    ) -> None:
        expected_containers = ContainerList([c for c in movie_containers if c.space == integration_test_space.space])
        expected_ids = set(expected_containers.as_ids())
        assert expected_ids, "The movie model is missing containers"

        actual_containers = cognite_client.data_modeling.containers.list(space=integration_test_space.space, limit=-1)
        assert expected_ids <= set(actual_containers.as_ids())
        assert all(c.space == integration_test_space.space for c in actual_containers)

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        space = integration_test_space.space
        res = cognite_client.data_modeling.containers.delete(ContainerId(space=space, external_id="DoesNotExists"))
        assert res == []

    def test_retrieve_multiple(self, cognite_client: CogniteClient, movie_containers: ContainerList) -> None:
        ids = movie_containers.as_ids()
        retrieved = cognite_client.data_modeling.containers.retrieve(ids)
        assert set(retrieved.as_ids()) == set(ids)

    def test_retrieve_multiple_with_missing(
        self, cognite_client: CogniteClient, movie_containers: ContainerList
    ) -> None:
        ids_without_missing = movie_containers.as_ids()
        ids_with_missing = [*ids_without_missing, ContainerId("myNonExistingSpace", "myImaginaryContainer")]

        retrieved = cognite_client.data_modeling.containers.retrieve(ids_with_missing)
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
        assert error.value.code == 400
        assert "One or more spaces do not exist" in error.value.message

    def test_dump_json_serialize_load(self, movie_containers: ContainerList) -> None:
        container = movie_containers.get(external_id="Movie")
        assert container is not None, "Movie container is missing from test environment"

        container_dump = container.dump(camel_case=True)
        container_json = json.dumps(container_dump)
        container_loaded = Container.load(container_json)
        assert container == container_loaded

    def test_retrieve_container_with_unit_property(
        self, cognite_client: CogniteClient, unit_pressure_container: Container
    ) -> None:
        retrieved = cognite_client.data_modeling.containers.retrieve(unit_pressure_container.as_id())

        assert retrieved is not None
        pressure_type = retrieved.properties["pressure"].type
        assert isinstance(pressure_type, Float64)
        assert pressure_type.unit is not None
        assert pressure_type.unit.external_id == "pressure:bar"
