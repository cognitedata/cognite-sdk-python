import pytest

import cognite.client.data_classes.data_modeling as models
from cognite.client import CogniteClient


@pytest.fixture()
def cdf_views(cognite_client: CogniteClient):
    views = cognite_client.data_modeling.views.list(limit=-1)
    assert len(views), "There must be at least one view in CDF"
    return views


class TestViewsAPI:
    def test_list(
        self, cognite_client: CogniteClient, cdf_views: models.ViewList, integration_test_space: models.Space
    ):
        # Arrange
        expected_views = models.ViewList([v for v in cdf_views if v.space == integration_test_space.space])

        # Act
        actual_views = cognite_client.data_modeling.views.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert sorted(actual_views, key=lambda v: v.external_id) == sorted(expected_views, key=lambda v: v.external_id)
        assert all(v.space == integration_test_space.space for v in actual_views)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, integration_test_space: models.Space):
        # Arrange
        new_view = models.ViewApply(
            space=integration_test_space.space,
            external_id="IntegrationTestView",
            description="Integration test, should not persist",
            version="v1",
            name="Create and delete view",
            properties={
                "name": models.MappedApplyPropertyDefinition(
                    container=models.ContainerReference(
                        space=integration_test_space.space,
                        external_id="Person",
                    ),
                    container_property_identifier="name",
                    name="fullName",
                ),
            },
        )
        new_id = models.VersionedDataModelingId(new_view.space, new_view.external_id, new_view.version)

        # Act
        created = cognite_client.data_modeling.views.apply(new_view)
        retrieved = cognite_client.data_modeling.views.retrieve(new_id)

        # Assert
        assert created.created_time
        assert created.last_updated_time
        assert created.as_apply().dump() == new_view.dump()
        assert retrieved.dump() == created.dump()

        # Act
        deleted_id = cognite_client.data_modeling.views.delete(new_id)
        retrieved_deleted = cognite_client.data_modeling.views.retrieve(new_id)

        # Assert
        assert deleted_id[0] == new_id
        assert retrieved_deleted is None

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: models.Space):
        space = integration_test_space.space
        assert (
            cognite_client.data_modeling.views.delete(
                models.VersionedDataModelingId(space=space, external_id="DoesNotExists", version="v0")
            )
            == []
        )

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_views: models.ViewList):
        assert len(cdf_views) >= 2, "Please add at least two views to the test environment"
        # Arrange
        ids = [models.VersionedDataModelingId(v.space, v.external_id, v.version) for v in cdf_views]

        # Act
        retrieved = cognite_client.data_modeling.views.retrieve(ids)

        # Assert
        assert len(retrieved) == len(ids)

    def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, cdf_views: models.ViewList):
        assert len(cdf_views) >= 2, "Please add at least two views to the test environment"
        # Arrange
        ids = [models.VersionedDataModelingId(v.space, v.external_id, v.version) for v in cdf_views]
        ids += [models.VersionedDataModelingId("myNonExistingSpace", "myImaginaryView", "v0")]

        # Act
        retrieved = cognite_client.data_modeling.views.retrieve(ids)

        # Assert
        assert len(retrieved) == len(ids) - 1

    def test_retrieve_non_existent(self, cognite_client: CogniteClient):
        assert cognite_client.data_modeling.views.retrieve(("myNonExistingSpace", "myImaginaryView", "v0")) is None

    def test_iterate(self, cognite_client: CogniteClient, integration_test_space: models.Space):
        for containers in cognite_client.data_modeling.views(
            chunk_size=2, limit=-1, space=integration_test_space.space
        ):
            assert isinstance(containers, models.ViewList)
