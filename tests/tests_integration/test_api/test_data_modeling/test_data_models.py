import json
from typing import Any

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    ContainerId,
    DataModel,
    DataModelApply,
    DataModelId,
    DataModelList,
    MappedPropertyApply,
    Space,
    View,
    ViewApply,
    ViewId,
)
from cognite.client.exceptions import CogniteAPIError


@pytest.fixture(scope="function")
def cdf_data_models(cognite_client: CogniteClient) -> DataModelList[ViewId]:
    data_models = cognite_client.data_modeling.data_models.list(limit=-1)
    assert len(data_models) > 0, "Please create at least one data model in CDF."
    return data_models


@pytest.fixture(scope="function")
def movie_model(cdf_data_models: DataModelList[ViewId]) -> DataModel:
    movie_model = cdf_data_models.get(external_id="Movie")
    assert movie_model is not None, "Please create a data model with external_id 'Movie' in CDF."
    return movie_model


class TestDataModelsAPI:
    def test_list(
        self, cognite_client: CogniteClient, cdf_data_models: DataModelList[ViewId], integration_test_space: Space
    ) -> None:
        # Arrange
        expected_data_models = DataModelList[ViewId](
            [m for m in cdf_data_models if m.space == integration_test_space.space]
        )

        # Act
        actual_data_models = cognite_client.data_modeling.data_models.list(space=integration_test_space.space, limit=-1)

        # Assert
        assert sorted(actual_data_models, key=lambda m: m.external_id) == sorted(
            expected_data_models, key=lambda m: m.external_id
        )
        assert all(v.space == integration_test_space.space for v in actual_data_models)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        # Arrange
        new_view = ViewApply(
            space=integration_test_space.space,
            external_id="IntegrationTestViewDataModel",
            version="v1",
            description="Integration test, should not persist",
            name="View of create and delete data model",
            properties={
                "name": MappedPropertyApply(
                    container=ContainerId(
                        space=integration_test_space.space,
                        external_id="Person",
                    ),
                    container_property_identifier="name",
                    name="fullName",
                ),
            },
        )
        new_data_model = DataModelApply(
            space=integration_test_space.space,
            external_id="IntegrationTestDataModel",
            version="v1",
            description="Integration test, should not persist",
            name="Create and delete data model with view",
            views=[new_view],
        )
        new_view_id = ViewId(new_view.space, new_view.external_id, new_view.version)
        new_id = DataModelId(new_data_model.space, new_data_model.external_id, new_data_model.version)

        # Act
        created = cognite_client.data_modeling.data_models.apply(new_data_model)
        retrieved = cognite_client.data_modeling.data_models.retrieve(new_id)[0]
        cognite_client.data_modeling.views.retrieve(new_view_id)

        # Assert
        assert created.created_time
        assert created.last_updated_time
        new_data_model_dump = new_data_model.dump()
        new_data_model_dump["views"] = [
            (v.as_id() if isinstance(v, ViewApply) else v).dump() for v in (new_data_model.views or [])
        ]
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

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        space = integration_test_space.space
        assert (
            cognite_client.data_modeling.data_models.delete(
                DataModelId(space=space, external_id="DoesNotExists", version="v0")
            )
            == []
        )

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_data_models: DataModelList[ViewId]) -> None:
        assert len(cdf_data_models) >= 2, "Please add at least two data models to the test environment"
        # Arrange
        ids = [DataModelId(v.space, v.external_id, v.version) for v in cdf_data_models]

        # Act
        retrieved = cognite_client.data_modeling.data_models.retrieve(ids)

        # Assert
        assert [dm.as_id() for dm in retrieved] == ids

    def test_retrieve_with_inline_views(self, cognite_client: CogniteClient, movie_model: DataModel) -> None:
        # Act
        retrieved = cognite_client.data_modeling.data_models.retrieve(movie_model.as_id(), inline_views=True)

        # Assert
        assert len(retrieved) == 1
        assert all(isinstance(v, View) for v in retrieved[0].views)

    def test_retrieve_without_inline_views(self, cognite_client: CogniteClient, movie_model: DataModel) -> None:
        # Act
        retrieved = cognite_client.data_modeling.data_models.retrieve(movie_model.as_id(), inline_views=False)

        # Assert
        assert len(retrieved) == 1
        assert all(isinstance(v, ViewId) for v in retrieved[0].views)

    def test_retrieve_multiple_with_missing(
        self, cognite_client: CogniteClient, cdf_data_models: DataModelList[ViewId]
    ) -> None:
        assert len(cdf_data_models) >= 2, "Please add at least two data models to the test environment"
        # Arrange
        ids_without_missing = [DataModelId(v.space, v.external_id, v.version) for v in cdf_data_models]
        ids_with_missing = [
            *ids_without_missing,
            DataModelId("myNonExistingSpace", "myImaginaryDataModel", "v0"),
        ]

        # Act
        retrieved = cognite_client.data_modeling.data_models.retrieve(ids_with_missing)

        # Assert
        assert [data_model.as_id() for data_model in retrieved] == ids_without_missing

    def test_retrieve_non_existent(self, cognite_client: CogniteClient) -> None:
        assert not cognite_client.data_modeling.data_models.retrieve(
            ("myNonExistingSpace", "myImaginaryDataModel", "v0")
        )

    def test_iterate(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        for containers in cognite_client.data_modeling.data_models(chunk_size=2, limit=-1):
            assert isinstance(containers, DataModelList)

    def test_list_expand_inline_views(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        # Act
        data_models = cognite_client.data_modeling.data_models.list(
            space=integration_test_space.space, limit=-1, inline_views=True
        )

        # Assert
        for m in data_models:
            assert m.views is not None
            assert all([isinstance(v, View) for v in m.views])

    def test_apply_invalid_data_model(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.data_models.apply(
                DataModelApply(
                    space="nonExistingSpace",
                    external_id="IntegrationTestDataModel",
                    version="v1",
                )
            )

        # Assert
        assert error.value.code == 400
        assert "One or more spaces do not exist" in error.value.message

    def test_apply_failed_and_successful_task(
        self, cognite_client: CogniteClient, integration_test_space: Space, monkeypatch: Any
    ) -> None:
        # Arrange
        valid_data_model = DataModelApply(
            space=integration_test_space.space,
            external_id="IntegrationTestDataModel1",
            version="v1",
        )
        invalid_data_model = DataModelApply(
            space="nonExistingSpace",
            external_id="IntegrationTestDataModel2",
            version="v1",
        )
        monkeypatch.setattr(cognite_client.data_modeling.data_models, "_CREATE_LIMIT", 1)

        try:
            # Act
            with pytest.raises(CogniteAPIError) as error:
                cognite_client.data_modeling.data_models.apply([valid_data_model, invalid_data_model])

            # Assert
            assert "One or more spaces do not exist" in error.value.message
            assert error.value.code == 400
            assert len(error.value.successful) == 1
            assert len(error.value.failed) == 1

        finally:
            # Cleanup
            cognite_client.data_modeling.data_models.delete(valid_data_model.as_id())

    def test_dump_json_serialize_load(self, cognite_client: CogniteClient, movie_model: DataModel) -> None:
        # Arrange
        models = cognite_client.data_modeling.data_models.retrieve(movie_model.as_id(), inline_views=True)
        assert len(models) == 1, "Please the movie data model to the test environment"
        model = models[0]

        # Act
        model_dumped = model.dump(camel_case=True)
        model_json = json.dumps(model_dumped)
        model_loaded = DataModel.load(model_json)

        # Assert
        assert model == model_loaded
