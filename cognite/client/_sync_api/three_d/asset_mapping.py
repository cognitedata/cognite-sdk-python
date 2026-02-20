"""
===============================================================================
873c2427dd138d36132ca89077d1ed66
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import (
    BoundingBox3D,
    ThreeDAssetMapping,
    ThreeDAssetMappingList,
    ThreeDAssetMappingWrite,
)
from cognite.client.utils._async_helpers import run_sync


class Sync3DAssetMappingAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def list(
        self,
        model_id: int,
        revision_id: int,
        node_id: int | None = None,
        asset_id: int | None = None,
        intersects_bounding_box: BoundingBox3D | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ThreeDAssetMappingList:
        """
        `List 3D node asset mappings. <https://developer.cognite.com/api#tag/3D-Asset-Mapping/operation/get3DMappings>`_

        Args:
            model_id: Id of the model.
            revision_id: Id of the revision.
            node_id: List only asset mappings associated with this node.
            asset_id: List only asset mappings associated with this asset.
            intersects_bounding_box: If given, only return asset mappings for assets whose bounding box intersects with the given bounding box.
            limit: Maximum number of asset mappings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            The list of asset mappings.

        Example:

            List 3d node asset mappings:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.three_d.asset_mappings.list(model_id=1, revision_id=1)

            List 3d node asset mappings for assets whose bounding box intersects with a given bounding box:

                >>> from cognite.client.data_classes import BoundingBox3D
                >>> bbox = BoundingBox3D(min=[0.0, 0.0, 0.0], max=[1.0, 1.0, 1.0])
                >>> res = client.three_d.asset_mappings.list(
                ...     model_id=1, revision_id=1, intersects_bounding_box=bbox)
        """
        return run_sync(
            self.__async_client.three_d.asset_mappings.list(
                model_id=model_id,
                revision_id=revision_id,
                node_id=node_id,
                asset_id=asset_id,
                intersects_bounding_box=intersects_bounding_box,
                limit=limit,
            )
        )

    @overload
    def create(
        self, model_id: int, revision_id: int, asset_mapping: ThreeDAssetMapping | ThreeDAssetMappingWrite
    ) -> ThreeDAssetMapping: ...

    @overload
    def create(
        self,
        model_id: int,
        revision_id: int,
        asset_mapping: Sequence[ThreeDAssetMapping] | Sequence[ThreeDAssetMappingWrite],
    ) -> ThreeDAssetMappingList: ...

    def create(
        self,
        model_id: int,
        revision_id: int,
        asset_mapping: ThreeDAssetMapping
        | ThreeDAssetMappingWrite
        | Sequence[ThreeDAssetMapping]
        | Sequence[ThreeDAssetMappingWrite],
    ) -> ThreeDAssetMapping | ThreeDAssetMappingList:
        """
        `Create 3d node asset mappings. <https://developer.cognite.com/api#tag/3D-Asset-Mapping/operation/create3DMappings>`_

        Args:
            model_id: Id of the model.
            revision_id: Id of the revision.
            asset_mapping: The asset mapping(s) to create.

        Returns:
            The created asset mapping(s).

        Example:

            Create new 3d node asset mapping:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ThreeDAssetMappingWrite
                >>> my_mapping = ThreeDAssetMappingWrite(node_id=1, asset_id=1)
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.three_d.asset_mappings.create(model_id=1, revision_id=1, asset_mapping=my_mapping)
        """
        return run_sync(
            self.__async_client.three_d.asset_mappings.create(
                model_id=model_id, revision_id=revision_id, asset_mapping=asset_mapping
            )
        )

    def delete(
        self, model_id: int, revision_id: int, asset_mapping: ThreeDAssetMapping | Sequence[ThreeDAssetMapping]
    ) -> None:
        """
        `Delete 3d node asset mappings. <https://developer.cognite.com/api#tag/3D-Asset-Mapping/operation/delete3DMappings>`_

        Args:
            model_id: Id of the model.
            revision_id: Id of the revision.
            asset_mapping: The asset mapping(s) to delete.

        Example:

            Delete 3d node asset mapping:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> mapping_to_delete = client.three_d.asset_mappings.list(model_id=1, revision_id=1)[0]
                >>> res = client.three_d.asset_mappings.delete(model_id=1, revision_id=1, asset_mapping=mapping_to_delete)
        """
        return run_sync(
            self.__async_client.three_d.asset_mappings.delete(
                model_id=model_id, revision_id=revision_id, asset_mapping=asset_mapping
            )
        )
