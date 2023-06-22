import pytest

import cognite.client.data_classes.data_modeling as dm
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture()
def cdf_views(cognite_client: CogniteClient):
    views = cognite_client.data_modeling.views.list(limit=-1)
    assert len(views), "There must be at least one view in CDF"
    return views


class TestViewsAPI:
    def test_list(self, cognite_client: CogniteClient, cdf_views: dm.ViewList, integration_test_space: dm.Space):
        # Arrange
        expected_views = dm.ViewList([v for v in cdf_views if v.space == integration_test_space.space])

        # Act
        actual_views = cognite_client.data_modeling.views.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert sorted(actual_views, key=lambda v: v.external_id) == sorted(expected_views, key=lambda v: v.external_id)
        assert all(v.space == integration_test_space.space for v in actual_views)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, integration_test_space: dm.Space):
        # Arrange
        new_view = dm.ViewApply(
            space=integration_test_space.space,
            external_id="IntegrationTestView",
            version="v1",
            description="Integration test, should not persist",
            name="Create and delete view",
            properties={
                "name": dm.MappedApplyPropertyDefinition(
                    container=dm.ContainerId(
                        space=integration_test_space.space,
                        external_id="Person",
                    ),
                    container_property_identifier="name",
                    name="fullName",
                ),
            },
        )
        new_id = dm.ViewId(new_view.space, new_view.external_id, new_view.version)

        # Act
        created = cognite_client.data_modeling.views.apply(new_view)
        retrieved = cognite_client.data_modeling.views.retrieve(new_id)[0]

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
        assert not retrieved_deleted

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: dm.Space):
        space = integration_test_space.space
        assert (
            cognite_client.data_modeling.views.delete(
                dm.ViewId(space=space, external_id="DoesNotExists", version="v0")
            )
            == []
        )

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_views: dm.ViewList):
        assert len(cdf_views) >= 2, "Please add at least two views to the test environment"
        # Arrange
        ids = [dm.ViewId(v.space, v.external_id, v.version) for v in cdf_views]

        # Act
        retrieved = cognite_client.data_modeling.views.retrieve(ids)

        # Assert
        assert [view.as_id() for view in retrieved] == ids

    def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, cdf_views: dm.ViewList):
        assert len(cdf_views) >= 2, "Please add at least two views to the test environment"
        # Arrange
        ids_without_missing = [v.as_id() for v in cdf_views]
        ids_with_missing = [*ids_without_missing, dm.ViewId("myNonExistingSpace", "myImaginaryView", "v0")]

        # Act
        retrieved = cognite_client.data_modeling.views.retrieve(ids_with_missing)

        # Assert
        assert [view.as_id() for view in retrieved] == ids_without_missing

    def test_retrieve_non_existent(self, cognite_client: CogniteClient):
        assert not cognite_client.data_modeling.views.retrieve(("myNonExistingSpace", "myImaginaryView", "v0"))

    def test_iterate(self, cognite_client: CogniteClient, integration_test_space: dm.Space):
        for containers in cognite_client.data_modeling.views(
            chunk_size=2, limit=-1, space=integration_test_space.space
        ):
            assert isinstance(containers, dm.ViewList)

    def test_apply_invalid_view(self, cognite_client: CogniteClient, integration_test_space: dm.Space):
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.views.apply(
                dm.ViewApply(
                    space="nonExistingSpace",
                    external_id="myView",
                    version="v1",
                    properties={
                        "name": dm.MappedApplyPropertyDefinition(
                            container=dm.ContainerId(
                                space=integration_test_space.space,
                                external_id="Person",
                            ),
                            container_property_identifier="name",
                            name="fullName",
                        ),
                    },
                )
            )

        # Assert
        assert error.value.code == 400
        assert "One or more spaces do not exist" in error.value.message

    def test_apply_failed_and_successful_task(
        self, cognite_client: CogniteClient, integration_test_space: dm.Space, monkeypatch
    ):
        # Arrange
        valid_view = dm.ViewApply(
            space=integration_test_space.space,
            external_id="myView",
            version="v1",
            properties={
                "name": dm.MappedApplyPropertyDefinition(
                    container=dm.ContainerId(
                        space=integration_test_space.space,
                        external_id="Person",
                    ),
                    container_property_identifier="name",
                    name="fullName",
                ),
            },
        )
        invalid_view = dm.ViewApply(
            space="nonExistingSpace",
            external_id="myView",
            version="v1",
            properties={
                "name": dm.MappedApplyPropertyDefinition(
                    container=dm.ContainerId(
                        space=integration_test_space.space,
                        external_id="Person",
                    ),
                    container_property_identifier="name",
                    name="fullName",
                ),
            },
        )
        monkeypatch.setattr(cognite_client.data_modeling.views, "_CREATE_LIMIT", 1)

        try:
            # Act
            with pytest.raises(CogniteAPIError) as error:
                cognite_client.data_modeling.views.apply([valid_view, invalid_view])

            # Assert
            assert "One or more spaces do not exist" in error.value.message
            assert error.value.code == 400
            assert len(error.value.successful) == 1
            assert len(error.value.failed) == 1
        finally:
            # Cleanup
            cognite_client.data_modeling.views.delete(valid_view.as_id())
