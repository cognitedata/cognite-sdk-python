from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import TransformationDestination
from cognite.client.data_classes.transformations.schema import TransformationSchemaMapType


class TestTransformationSchemaAPI:
    def test_assets(self, cognite_client: CogniteClient) -> None:
        asset_columns = cognite_client.transformations.schema.retrieve(destination=TransformationDestination.assets())
        assert len(asset_columns) > 0
        assert len([col for col in asset_columns if col.name == "id"]) > 0

        id_type = next(col for col in asset_columns if col.name == "id").type
        assert id_type.type == "long"

        metadata_type = next(col for col in asset_columns if col.name == "metadata").type
        assert isinstance(metadata_type, TransformationSchemaMapType)
        assert metadata_type.type == "map"
        assert metadata_type.key_type == "string"

    @pytest.mark.xfail(reason="Nullable changed to False in a recent refactor, fix/revert underway")
    def test_assets_delete(self, cognite_client: CogniteClient) -> None:
        asset_columns = cognite_client.transformations.schema.retrieve(
            destination=TransformationDestination.assets(), conflict_mode="delete"
        )
        assert len(asset_columns) == 1
        assert asset_columns[0].type.type == "long"
        assert asset_columns[0].sql_type == "BIGINT"
        assert asset_columns[0].name == "id"
        # assert asset_columns[0].nullable is True # TODO: revert when schema is fixed, @silvavelosa

    def test_raw(self, cognite_client: CogniteClient) -> None:
        asset_columns = cognite_client.transformations.schema.retrieve(destination=TransformationDestination.raw())
        assert len(asset_columns) > 0
        assert len([col for col in asset_columns if col.name == "key"]) > 0

    def test_asset_hierarchy(self, cognite_client: CogniteClient) -> None:
        asset_columns = cognite_client.transformations.schema.retrieve(
            destination=TransformationDestination.asset_hierarchy()
        )
        assert len(asset_columns) > 0
        assert len([col for col in asset_columns if col.name == "externalId"]) > 0
