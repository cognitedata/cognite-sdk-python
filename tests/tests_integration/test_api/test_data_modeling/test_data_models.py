from __future__ import annotations

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
from cognite.client.utils._text import random_string


@pytest.fixture(scope="session")
def cdf_data_models(
    cognite_client: CogniteClient,
    integration_test_space: Space,
    movie_model: DataModel[View],
    empty_model: DataModel[ViewId],
) -> DataModelList[ViewId]:
    # The movie model and empty fixture are used to ensure that
    # there are at least two data models in the test environment.
    data_models = cognite_client.data_modeling.data_models.list(limit=-1)
    assert len(data_models) >= 2, "Please add at least two data models to the test environment"
    return data_models


class TestDataModelsAPI:
    def test_list(
        self, cognite_client: CogniteClient, cdf_data_models: DataModelList[ViewId], integration_test_space: Space
    ) -> None:
        expected_data_models = DataModelList[ViewId](
            [m for m in cdf_data_models if m.space == integration_test_space.space]
        )
        expected_ids = set(expected_data_models.as_ids())

        actual_data_models = cognite_client.data_modeling.data_models.list(space=integration_test_space.space, limit=-1)

        assert expected_ids, "The test environment is missing data models"
        assert expected_ids <= set(actual_data_models.as_ids())
        assert all(v.space == integration_test_space.space for v in actual_data_models)

    def test_apply_retrieve_and_delete(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        new_view = ViewApply(
            space=integration_test_space.space,
            external_id="IntegrationTestViewDataModel" + random_string(5),
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
        created: DataModel | None = None
        created_view: View | None = None
        deleted_models: list[DataModelId] = []
        deleted_view_ids: list[ViewId] = []
        try:
            created = cognite_client.data_modeling.data_models.apply(new_data_model)
            retrieved_models = cognite_client.data_modeling.data_models.retrieve(new_data_model.as_id())
            retrieved = retrieved_models.latest_version()

            assert created.created_time
            assert created.last_updated_time
            created_dump = created.as_apply().dump()
            new_model_dump = new_data_model.dump()
            # The returned data model with created only has ViewId and not the full View.
            created_dump.pop("views")
            new_model_dump.pop("views")
            assert created_dump == new_model_dump
            assert created.dump() == retrieved.dump()

            deleted_models = cognite_client.data_modeling.data_models.delete(new_data_model.as_id())
            deleted_view_ids = cognite_client.data_modeling.views.delete(new_view.as_id())
            retrieved_deleted = cognite_client.data_modeling.data_models.retrieve(new_data_model.as_id())

            assert len(deleted_models) == 1
            assert deleted_models[0] == new_data_model.as_id()
            assert len(deleted_view_ids) == 1
            assert deleted_view_ids[0] == new_view.as_id()
            assert not retrieved_deleted
        finally:
            if created and not deleted_models:
                cognite_client.data_modeling.data_models.delete(created.as_id())
            if created_view and not deleted_view_ids:
                cognite_client.data_modeling.views.delete(created_view.as_id())

    def test_delete_non_existent(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        space = integration_test_space.space
        res = cognite_client.data_modeling.data_models.delete(
            DataModelId(space=space, external_id="DoesNotExists", version="v0")
        )
        assert res == []

    def test_retrieve_multiple(self, cognite_client: CogniteClient, cdf_data_models: DataModelList[ViewId]) -> None:
        ids = cdf_data_models.as_ids()
        retrieved = cognite_client.data_modeling.data_models.retrieve(ids)
        assert retrieved.as_ids() == ids

    def test_retrieve_with_inline_views(self, cognite_client: CogniteClient, movie_model: DataModel) -> None:
        retrieved = cognite_client.data_modeling.data_models.retrieve(movie_model.as_id(), inline_views=True)

        assert len(retrieved) == 1
        assert all(isinstance(v, View) for v in retrieved[0].views)

    def test_retrieve_without_inline_views(self, cognite_client: CogniteClient, movie_model: DataModel) -> None:
        retrieved = cognite_client.data_modeling.data_models.retrieve(movie_model.as_id(), inline_views=False)

        assert len(retrieved) == 1
        assert all(isinstance(v, ViewId) for v in retrieved[0].views)

    def test_retrieve_multiple_with_missing(
        self, cognite_client: CogniteClient, cdf_data_models: DataModelList[ViewId]
    ) -> None:
        ids_without_missing = cdf_data_models.as_ids()
        ids_with_missing = [
            *ids_without_missing,
            DataModelId("myNonExistingSpace", "myImaginaryDataModel", "v0"),
        ]
        retrieved = cognite_client.data_modeling.data_models.retrieve(ids_with_missing)
        assert retrieved.as_ids() == ids_without_missing

    def test_retrieve_non_existent(self, cognite_client: CogniteClient) -> None:
        assert not cognite_client.data_modeling.data_models.retrieve(
            ("myNonExistingSpace", "myImaginaryDataModel", "v0")
        )

    def test_iterate(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        for containers in cognite_client.data_modeling.data_models(chunk_size=2, limit=-1):
            assert isinstance(containers, DataModelList)

    def test_list_expand_inline_views(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        data_models = cognite_client.data_modeling.data_models.list(
            space=integration_test_space.space, limit=10, inline_views=True
        )
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
        assert error.value.code == 400
        assert "One or more spaces do not exist" in error.value.message

    def test_apply_failed_and_successful_task(
        self, cognite_client: CogniteClient, integration_test_space: Space, monkeypatch: Any
    ) -> None:
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
            with pytest.raises(CogniteAPIError) as error:
                cognite_client.data_modeling.data_models.apply([valid_data_model, invalid_data_model])

            assert "One or more spaces do not exist" in error.value.message
            assert error.value.code == 400
            assert len(error.value.successful) == 1
            assert len(error.value.failed) == 1
        finally:
            cognite_client.data_modeling.data_models.delete(valid_data_model.as_id())

    def test_dump_json_serialize_load(self, cognite_client: CogniteClient, movie_model: DataModel) -> None:
        model_dumped = movie_model.dump(camel_case=True)
        model_json = json.dumps(model_dumped)
        model_loaded: DataModel = DataModel.load(model_json)

        assert model_loaded.dump() == movie_model.dump()
