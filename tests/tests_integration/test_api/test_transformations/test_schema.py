from cognite.client.data_classes import TransformationDestination
from cognite.client.data_classes.transformations.common import (
    DataModelInfo,
    ViewInfo,
)


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
        assert asset_columns[0].nullable is True

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

    def test_nodes_with_view(self, cognite_client):
        nodes_columns = cognite_client.transformations.schema.retrieve_instances(
            destination=TransformationDestination.nodes(
                view=ViewInfo(
                    space="test-space", external_id="testInstanceViewExternalId", version="testInstanceViewVersion"
                ),
                instance_space="test-space",
            ),
            conflict_mode="upsert",
            instance_type="nodes",
        )
        assert len(nodes_columns) > 0
        assert len([col for col in nodes_columns if col.name == "space"]) > 0
        assert [col for col in nodes_columns if col.name == "space"][0].type.type == "string"
        assert [col for col in nodes_columns if col.name == "externalId"][0].type.type == "string"

    def test_edges_with_view(self, cognite_client):
        edges_columns = cognite_client.transformations.schema.retrieve_instances(
            destination=TransformationDestination.edges(
                view=ViewInfo(
                    space="test-space", external_id="testInstanceViewExternalId", version="testInstanceViewVersion"
                ),
                instance_space="test-space",
            ),
            conflict_mode="upsert",
            instance_type="edges",
        )
        assert len(edges_columns) > 0
        assert len([col for col in edges_columns if col.name == "space"]) > 0
        assert [col for col in edges_columns if col.name == "space"][0].type.type == "string"
        assert [col for col in edges_columns if col.name == "externalId"][0].type.type == "string"

    def test_instance_type_data_model(self, cognite_client):
        type_columns = cognite_client.transformations.schema.retrieve_instances(
            destination=TransformationDestination.instances(
                data_model=DataModelInfo(
                    space="authorBook",
                    external_id="author_book",
                    version="2",
                    destination_type="AuthorBook_relation",
                    destination_relationship_from_type=None,
                ),
                instance_space="test-instanceSpace",
            ),
            conflict_mode="upsert",
            instance_type="nodes",
        )
        assert len(type_columns) > 0
        assert len([col for col in type_columns if col.name == "space"]) > 0
        assert [col for col in type_columns if col.name == "space"][0].type.type == "string"
        assert [col for col in type_columns if col.name == "externalId"][0].type.type == "string"

    def test_instance_relationship_data_model(self, cognite_client):
        type_columns = cognite_client.transformations.schema.retrieve_instances(
            destination=TransformationDestination.instances(
                data_model=DataModelInfo(
                    space="authorBook",
                    external_id="author_book",
                    version="2",
                    destination_type="AuthorBook_relation",
                    destination_relationship_from_type="author_book",
                ),
                instance_space="test-instanceSpace",
            ),
            conflict_mode="upsert",
            instance_type="edges",
        )
        assert len(type_columns) > 0
        assert len([col for col in type_columns if col.name == "space"]) > 0
        assert [col for col in type_columns if col.name == "space"][0].type.type == "string"
        assert [col for col in type_columns if col.name == "externalId"][0].type.type == "string"
