import pytest

import cognite.client.data_classes.data_modeling as models
from cognite.client import CogniteClient


@pytest.fixture(scope="function")
def cdf_data_models(cognite_client: CogniteClient):
    data_models = cognite_client.data_modeling.data_models.list(limit=-1)
    assert len(data_models) > 0, "Please create at least one data model in CDF."
    return data_models


class TestDataModelsAPI:
    def test_list(
        self, cognite_client: CogniteClient, cdf_data_models: models.DataModelList, integration_test_space: models.Space
    ):
        # Arrange
        expected_data_models = models.DataModelList(
            [m for m in cdf_data_models if m.space == integration_test_space.space]
        )

        # Act
        actual_data_models = cognite_client.data_modeling.data_models.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert sorted(actual_data_models, key=lambda m: m.external_id) == sorted(
            expected_data_models, key=lambda m: m.external_id
        )
        assert all(v.space == integration_test_space.space for v in actual_data_models)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, integration_test_space: models.Space):
        # Arrange
        new_view = models.View(
            space=integration_test_space.space,
            external_id="IntegrationTestViewDataModel",
            description="Integration test, should not persist",
            version="v1",
            name="View of create and delete data model",
            used_for="node",
            properties={
                "name": models.ViewCorePropertyDefinition(
                    container=models.ContainerReference(
                        space=integration_test_space.space,
                        external_id="Person",
                    ),
                    container_property_identifier="name",
                    name="fullName",
                ),
            },
        )
        new_data_model = models.DataModel(
            space=integration_test_space.space,
            external_id="IntegrationTestDataModel",
            description="Integration test, should not persist",
            version="v1",
            name="Create and delete data model with view",
            views=[new_view],
        )
        new_view_id = models.VersionedDataModelingId.from_tuple(
            (new_view.space, new_view.external_id, new_view.version)
        )
        new_id = models.VersionedDataModelingId.from_tuple(
            (new_data_model.space, new_data_model.external_id, new_data_model.version)
        )

        # Act
        created = cognite_client.data_modeling.data_models.apply(new_data_model)
        retrieved = cognite_client.data_modeling.data_models.retrieve(new_id)
        retrieved_view = cognite_client.data_modeling.views.retrieve(new_view_id)

        # Assert
        assert created.created_time
        assert created.last_updated_time
        assert created.external_id == new_data_model.external_id
        assert retrieved.external_id == new_data_model.external_id
        assert retrieved_view.created_time
        assert retrieved_view.last_updated_time
        assert retrieved_view.external_id == new_view.external_id

        # Act
        deleted_data_model_id = cognite_client.data_modeling.data_models.delete(new_id)
        deleted_view_id = cognite_client.data_modeling.views.delete(new_view_id)
        retrieved_deleted = cognite_client.data_modeling.data_models.retrieve(new_id)

        # Assert
        assert deleted_data_model_id[0] == new_id
        assert deleted_view_id[0] == new_view_id
        assert retrieved_deleted is None

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: models.Space):
        space = integration_test_space.space
        assert (
            cognite_client.data_modeling.data_models.delete(
                models.VersionedDataModelingId(space=space, external_id="DoesNotExists", version="v0")
            )
            == []
        )

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_data_models: models.DataModelList):
        assert len(cdf_data_models) >= 2, "Please add at least two data models to the test environment"
        # Arrange
        ids = [models.VersionedDataModelingId(v.space, v.external_id, v.version) for v in cdf_data_models]

        # Act
        retrieved = cognite_client.data_modeling.data_models.retrieve(ids)

        # Assert
        assert len(retrieved) == len(ids)

    def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, cdf_data_models: models.DataModelList):
        assert len(cdf_data_models) >= 2, "Please add at least two data models to the test environment"
        # Arrange
        ids = [models.VersionedDataModelingId(v.space, v.external_id, v.version) for v in cdf_data_models]
        ids += [models.VersionedDataModelingId("myNonExistingSpace", "myImaginaryDataModel", "v0")]

        # Act
        retrieved = cognite_client.data_modeling.data_models.retrieve(ids)

        # Assert
        assert len(retrieved) == len(ids) - 1

    def test_retrieve_non_existent(self, cognite_client: CogniteClient):
        assert (
            cognite_client.data_modeling.data_models.retrieve(("myNonExistingSpace", "myImaginaryDataModel", "v0"))
            is None
        )

    def test_iterate(self, cognite_client: CogniteClient, integration_test_space: models.Space):
        for containers in cognite_client.data_modeling.data_models(chunk_size=2, limit=-1):
            assert isinstance(containers, models.DataModelList)

    def test_list_expand_inline_views(self, cognite_client: CogniteClient, integration_test_space: models.Space):
        # Act
        data_models = cognite_client.data_modeling.data_models.list(
            space=integration_test_space.space, limit=-1, inline_views=True
        )

        # Assert
        assert all(isinstance(v, models.View) for m in data_models for v in m.views)

    @pytest.mark.skip("Backend not implemented.")
    def test_filter(self, cognite_client: CogniteClient, cdf_data_models: models.DataModelList):
        # Arrange
        data_model = cdf_data_models[0]

        data_models = cognite_client.data_modeling.data_models.filter(
            {"equals": models.dsl.EqualObject(["name"], data_model.name)}
        )

        assert data_models[0] == data_model
