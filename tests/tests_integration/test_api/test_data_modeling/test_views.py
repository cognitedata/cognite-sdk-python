import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    ContainerList,
    Space,
)


@pytest.fixture()
def cdf_views(cognite_client: CogniteClient):
    views = cognite_client.data_modeling.views.list(limit=-1)
    assert len(views), "There must be at least one view in CDF"
    return views


class TestViewsAPI:
    def test_list(self, cognite_client: CogniteClient, cdf_views: ContainerList, integration_test_space: Space):
        # Arrange
        expected_views = ContainerList([v for v in cdf_views if v.space == integration_test_space.space])

        # Act
        actual_views = cognite_client.data_modeling.views.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert sorted(actual_views, key=lambda v: v.external_id) == sorted(expected_views, key=lambda v: v.external_id)
        assert all(v.space == integration_test_space.space for v in actual_views)

    # def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, integration_test_space: Space):
    #     # Arrange
    #     new_container = Container(
    #         space=integration_test_space.space,
    #         external_id="IntegrationTestContainer",
    #         description="Integration test, should not persist",
    #         name="Create and delete container",
    #         used_for="node",
    #         properties={
    #             "name": ContainerPropertyIdentifier(
    #                 type=TextProperty(list=False),
    #             ),
    #             "year": ContainerPropertyIdentifier(type=PrimitiveProperty("int32", list=False)),
    #         },
    #     )
    #
    #     # Act
    #     created = cognite_client.data_modeling.containers.apply(new_container)
    #     retrieved = cognite_client.data_modeling.containers.retrieve((new_container.space, new_container.external_id))
    #
    #     # Assert
    #     assert created.created_time
    #     assert created.last_updated_time
    #     assert created.external_id == new_container.external_id
    #     assert retrieved.external_id == new_container.external_id
    #
    #     # Act
    #     deleted_id = cognite_client.data_modeling.containers.delete((new_container.space, new_container.external_id))
    #     retrieved_deleted = cognite_client.data_modeling.containers.retrieve(
    #         (new_container.space, new_container.external_id)
    #     )
    #
    #     # Assert
    #     assert deleted_id[0] == DataModelingId.from_tuple((new_container.space, new_container.external_id))
    #     assert retrieved_deleted is None
    #
    # def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: Space):
    #     space = integration_test_space.space
    #     assert (
    #         cognite_client.data_modeling.containers.delete(DataModelingId(space=space, external_id="DoesNotExists"))
    #         == []
    #     )
    #
    # def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_containers: ContainerList):
    #     assert len(cdf_containers) >= 2, "Please add at least two containers to the test environment"
    #     # Arrange
    #     ids = [DataModelingId(c.space, c.external_id) for c in cdf_containers]
    #
    #     # Act
    #     retrieved = cognite_client.data_modeling.containers.retrieve(ids)
    #
    #     # Assert
    #     assert len(retrieved) == len(ids)
    #
    # def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, cdf_containers: ContainerList):
    #     assert len(cdf_containers) >= 2, "Please add at least two containers to the test environment"
    #     # Arrange
    #     ids = [DataModelingId(c.space, c.external_id) for c in cdf_containers]
    #     ids += [DataModelingId("myNonExistingSpace", "myImaginaryContainer")]
    #
    #     # Act
    #     retrieved = cognite_client.data_modeling.containers.retrieve(ids)
    #
    #     # Assert
    #     assert len(retrieved) == len(ids) - 1
    #
    # def test_retrieve_non_existent(self, cognite_client: CogniteClient):
    #     assert cognite_client.data_modeling.containers.retrieve(("myNonExistingSpace", "myImaginaryContainer")) is None
    #
    # def test_iterate_over_containers(self, cognite_client: CogniteClient):
    #     for containers in cognite_client.data_modeling.containers(chunk_size=2, limit=-1):
    #         assert isinstance(containers, ContainerList)
