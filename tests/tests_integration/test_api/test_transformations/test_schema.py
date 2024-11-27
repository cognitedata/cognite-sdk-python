from cognite.client.data_classes import TransformationDestination


class TestTransformationSchemaAPI:
    def test_assets(self, cognite_client):
        asset_columns = cognite_client.transformations.schema.retrieve(destination=TransformationDestination.assets())
        assert len(asset_columns) > 0
        assert len([col for col in asset_columns if col.name == "id"]) > 0
        assert next(col for col in asset_columns if col.name == "id").type.type == "long"
        assert next(col for col in asset_columns if col.name == "metadata").type.type == "map"
        assert next(col for col in asset_columns if col.name == "metadata").type.key_type == "string"

    def test_assets_delete(self, cognite_client):
        asset_columns = cognite_client.transformations.schema.retrieve(
            destination=TransformationDestination.assets(), conflict_mode="delete"
        )
        assert len(asset_columns) == 1
        assert asset_columns[0].type.type == "long"
        assert asset_columns[0].sql_type == "BIGINT"
        assert asset_columns[0].name == "id"
        # assert asset_columns[0].nullable is True # TODO: revert when schema is fixed, @silvavelosa

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
