from cognite.client.data_classes import TransformationDestination


class TestTransformationSchemaAPI:
    def test_assets(self, cognite_client):
        asset_columns = cognite_client.transformations.schema.retrieve(destination=TransformationDestination.assets())
        assert len(asset_columns) > 0
        assert len([col for col in asset_columns if col.name == "id"]) > 0
        assert [col for col in asset_columns if col.name == "id"][0].type.type == "long"
        assert [col for col in asset_columns if col.name == "metadata"][0].type.type == "map"
        assert [col for col in asset_columns if col.name == "metadata"][0].type.key_type == "string"

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
