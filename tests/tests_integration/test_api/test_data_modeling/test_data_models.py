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
        new_view = models.ViewApply(
            space=integration_test_space.space,
            external_id="IntegrationTestViewDataModel",
            version="v1",
            description="Integration test, should not persist",
            name="View of create and delete data model",
            properties={
                "name": models.MappedApplyPropertyDefinition(
                    container=models.ContainerId(
                        space=integration_test_space.space,
                        external_id="Person",
                    ),
                    container_property_identifier="name",
                    name="fullName",
                ),
            },
        )
        new_data_model = models.DataModelApply(
            space=integration_test_space.space,
            external_id="IntegrationTestDataModel",
            version="v1",
            description="Integration test, should not persist",
            name="Create and delete data model with view",
            views=[new_view],
        )
        new_view_id = models.ViewId(new_view.space, new_view.external_id, new_view.version)
        new_id = models.DataModelId(new_data_model.space, new_data_model.external_id, new_data_model.version)

        # Act
        created = cognite_client.data_modeling.data_models.apply(new_data_model)
        retrieved = cognite_client.data_modeling.data_models.retrieve(new_id)[0]
        cognite_client.data_modeling.views.retrieve(new_view_id)

        # Assert
        assert created.created_time
        assert created.last_updated_time
        new_data_model_dump = new_data_model.dump()
        new_data_model_dump["views"] = [v.as_id().dump() for v in new_data_model.views]
        assert created.as_apply().dump() == new_data_model_dump
        assert created.dump() == retrieved.dump()

        # Act
        deleted_data_model_id = cognite_client.data_modeling.data_models.delete(new_id)
        deleted_view_id = cognite_client.data_modeling.views.delete(new_view_id)
        retrieved_deleted = cognite_client.data_modeling.data_models.retrieve(new_id)

        # Assert
        assert deleted_data_model_id[0] == new_id
        assert deleted_view_id[0] == new_view_id
        assert not retrieved_deleted

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: models.Space):
        space = integration_test_space.space
        assert (
            cognite_client.data_modeling.data_models.delete(
                models.DataModelId(space=space, external_id="DoesNotExists", version="v0")
            )
            == []
        )

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_data_models: models.DataModelList):
        assert len(cdf_data_models) >= 2, "Please add at least two data models to the test environment"
        # Arrange
        ids = [models.DataModelId(v.space, v.external_id, v.version) for v in cdf_data_models]

        # Act
        retrieved = cognite_client.data_modeling.data_models.retrieve(ids)

        # Assert
        assert [dm.as_id() for dm in retrieved] == ids

    def test_retrieve_multiple_with_missing(self, cognite_client: CogniteClient, cdf_data_models: models.DataModelList):
        assert len(cdf_data_models) >= 2, "Please add at least two data models to the test environment"
        # Arrange
        ids_without_missing = [models.DataModelId(v.space, v.external_id, v.version) for v in cdf_data_models]
        ids_with_missing = [
            *ids_without_missing,
            models.DataModelId("myNonExistingSpace", "myImaginaryDataModel", "v0"),
        ]

        # Act
        retrieved = cognite_client.data_modeling.data_models.retrieve(ids_with_missing)

        # Assert
        assert [data_model.as_id() for data_model in retrieved] == ids_without_missing

    def test_retrieve_non_existent(self, cognite_client: CogniteClient):
        assert not cognite_client.data_modeling.data_models.retrieve(
            ("myNonExistingSpace", "myImaginaryDataModel", "v0")
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
