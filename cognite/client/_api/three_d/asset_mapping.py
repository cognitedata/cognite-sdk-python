from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    BoundingBox3D,
    ThreeDAssetMapping,
    ThreeDAssetMappingList,
    ThreeDAssetMappingWrite,
)
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import interpolate_and_url_encode, split_into_chunks, unpack_items_in_payload
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._validation import assert_type


class ThreeDAssetMappingAPI(APIClient):
    _RESOURCE_PATH = "/3d/models/{}/revisions/{}/mappings"

    def list(
        self,
        model_id: int,
        revision_id: int,
        node_id: int | None = None,
        asset_id: int | None = None,
        intersects_bounding_box: BoundingBox3D | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ThreeDAssetMappingList:
        """`List 3D node asset mappings. <https://developer.cognite.com/api#tag/3D-Asset-Mapping/operation/get3DMappings>`_

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            node_id (int | None): List only asset mappings associated with this node.
            asset_id (int | None): List only asset mappings associated with this asset.
            intersects_bounding_box (BoundingBox3D | None): If given, only return asset mappings for assets whose bounding box intersects with the given bounding box.
            limit (int | None): Maximum number of asset mappings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ThreeDAssetMappingList: The list of asset mappings.

        Example:

            List 3d node asset mappings:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.asset_mappings.list(model_id=1, revision_id=1)

            List 3d node asset mappings for assets whose bounding box intersects with a given bounding box:

                >>> from cognite.client.data_classes import BoundingBox3D
                >>> bbox = BoundingBox3D(min=[0.0, 0.0, 0.0], max=[1.0, 1.0, 1.0])
                >>> res = client.three_d.asset_mappings.list(
                ...     model_id=1, revision_id=1, intersects_bounding_box=bbox)
        """
        path = interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        flt: dict[str, str | int | None] = {"nodeId": node_id, "assetId": asset_id}
        if intersects_bounding_box:
            flt["intersectsBoundingBox"] = _json.dumps(intersects_bounding_box)
        return self._list(
            list_cls=ThreeDAssetMappingList,
            resource_cls=ThreeDAssetMapping,
            resource_path=path,
            method="GET",
            filter=flt,
            limit=limit,
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
        """`Create 3d node asset mappings. <https://developer.cognite.com/api#tag/3D-Asset-Mapping/operation/create3DMappings>`_

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            asset_mapping (ThreeDAssetMapping | ThreeDAssetMappingWrite | Sequence[ThreeDAssetMapping] | Sequence[ThreeDAssetMappingWrite]): The asset mapping(s) to create.

        Returns:
            ThreeDAssetMapping | ThreeDAssetMappingList: The created asset mapping(s).

        Example:

            Create new 3d node asset mapping::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ThreeDAssetMappingWrite
                >>> my_mapping = ThreeDAssetMappingWrite(node_id=1, asset_id=1)
                >>> client = CogniteClient()
                >>> res = client.three_d.asset_mappings.create(model_id=1, revision_id=1, asset_mapping=my_mapping)
        """
        path = interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        return self._create_multiple(
            list_cls=ThreeDAssetMappingList,
            resource_cls=ThreeDAssetMapping,
            resource_path=path,
            items=asset_mapping,
            input_resource_cls=ThreeDAssetMappingWrite,
        )

    def delete(
        self, model_id: int, revision_id: int, asset_mapping: ThreeDAssetMapping | Sequence[ThreeDAssetMapping]
    ) -> None:
        """`Delete 3d node asset mappings. <https://developer.cognite.com/api#tag/3D-Asset-Mapping/operation/delete3DMappings>`_

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            asset_mapping (ThreeDAssetMapping | Sequence[ThreeDAssetMapping]): The asset mapping(s) to delete.

        Example:

            Delete 3d node asset mapping::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> mapping_to_delete = client.three_d.asset_mappings.list(model_id=1, revision_id=1)[0]
                >>> res = client.three_d.asset_mappings.delete(model_id=1, revision_id=1, asset_mapping=mapping_to_delete)
        """
        path = interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        assert_type(asset_mapping, "asset_mapping", [Sequence, ThreeDAssetMapping])
        if isinstance(asset_mapping, ThreeDAssetMapping):
            asset_mapping = [asset_mapping]
        chunks = split_into_chunks(
            [ThreeDAssetMapping(a.node_id, a.asset_id).dump(camel_case=True) for a in asset_mapping], self._DELETE_LIMIT
        )
        tasks = [{"url_path": path + "/delete", "json": {"items": chunk}} for chunk in chunks]
        summary = execute_tasks(self._post, tasks, self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload, task_list_element_unwrap_fn=lambda el: ThreeDAssetMapping._load(el)
        )
