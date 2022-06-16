import os

import pytest

from cognite.client.data_classes import TransformationDestination


class TestTransformationSchemaAPI:
    def test_assets(self, cognite_client):
        asset_columns = cognite_client.transformations.schema.retrieve(destination=TransformationDestination.assets())
        assert len(asset_columns) > 0
        assert len([col for col in asset_columns if col.name == "id"]) > 0
        assert [col for col in asset_columns if col.name == "id"][0].type.type == "long"
        assert [col for col in asset_columns if col.name == "metadata"][0].type.type == "map"
        assert [col for col in asset_columns if col.name == "metadata"][0].type.key_type == "string"

    def test_assets_delete(self, cognite_client):
        asset_columns = cognite_client.transformations.schema.retrieve(
            destination=TransformationDestination.assets(), conflict_mode="delete"
        )
        assert len(asset_columns) == 1
        assert asset_columns[0].type.type == "long"
        assert asset_columns[0].sql_type == "BIGINT"
        assert asset_columns[0].name == "id"
        assert asset_columns[0].nullable == True

    def test_raw(self, cognite_client):
        asset_columns = cognite_client.transformations.schema.retrieve(destination=TransformationDestination.raw())
        assert len(asset_columns) > 0
        assert len([col for col in asset_columns if col.name == "key"]) > 0

    def test_asset_hierarchy(self, cognite_client):
        asset_columns = cognite_client.transformations.schema.retrieve(
            destination=TransformationDestination.asset_hierarchy()
        )
        assert len(asset_columns) > 0
        assert len([col for col in asset_columns if col.name == "externalId"]) > 0

    @pytest.mark.skip
    def test_alpha_data_model_schema(self, cognite_client):
        project_name = os.environ["COGNITE_PROJECT"]
        dm_name = "python-sdk-test-dm"
        cognite_client.post(
            f"/api/v1/projects/{project_name}/datamodelstorage/definitions/apply",
            json={
                "items": [
                    {
                        "externalId": dm_name,
                        "properties": {
                            "test": {"type": "text", "nullable": True},
                            "test2": {"type": "int64", "nullable": True},
                        },
                    }
                ]
            },
            params={},
            headers={"cdf-version": "alpha"},
        )
        model_cols = cognite_client.transformations.schema._alpha_retrieve_data_model_schema(dm_name)
        assert len(model_cols) == 3
        assert [col for col in model_cols if col.name == "externalId"][0].type.type == "string"
        assert [col for col in model_cols if col.name == "test"][0].type.type == "string"
        assert [col for col in model_cols if col.name == "test2"][0].type.type == "long"

        cognite_client.post(
            f"/api/v1/projects/{project_name}/datamodelstorage/definitions/delete",
            json={"items": [{"externalId": dm_name}]},
            params={},
            headers={"cdf-version": "alpha"},
        )
