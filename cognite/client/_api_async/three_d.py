from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Literal, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    ThreeDAssetMapping,
    ThreeDAssetMappingList,
    ThreeDAssetMappingWrite,
    ThreeDModel,
    ThreeDModelList,
    ThreeDModelRevision,
    ThreeDModelRevisionList,
    ThreeDModelRevisionUpdate,
    ThreeDModelRevisionWrite,
    ThreeDModelUpdate,
    ThreeDModelWrite,
    ThreeDNode,
    ThreeDNodeList,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr


class AsyncThreeDAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/3d"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.models = AsyncThreeDModelsAPI(self._config, self._api_version, self._cognite_client)
        self.revisions = AsyncThreeDRevisionsAPI(self._config, self._api_version, self._cognite_client)
        self.asset_mappings = AsyncThreeDAssetMappingAPI(self._config, self._api_version, self._cognite_client)


class AsyncThreeDModelsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/3d/models"

    async def list(
        self,
        published: bool | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ThreeDModelList:
        ""\"List 3D models.\"\"\"
        filter = {}
        if published is not None:
            filter["published"] = published
        return await self._list(
            list_cls=ThreeDModelList,
            resource_cls=ThreeDModel,
            method="GET",
            limit=limit,
            other_params=filter,
        )

    async def retrieve(self, id: int) -> ThreeDModel | None:
        ""\"Retrieve 3D model.\"\"\"
        try:
            res = await self._get(url_path=f"{self._RESOURCE_PATH}/{id}")
            return ThreeDModel._load(res.json(), cognite_client=self._cognite_client)
        except Exception:
            return None

    async def create(self, model: ThreeDModel | ThreeDModelWrite | Sequence[ThreeDModel] | Sequence[ThreeDModelWrite]) -> ThreeDModel | ThreeDModelList:
        ""\"Create 3D models.\"\"\"
        return await self._create_multiple(
            list_cls=ThreeDModelList,
            resource_cls=ThreeDModel,
            items=model,
        )

    async def update(self, item: ThreeDModel | ThreeDModelUpdate | Sequence[ThreeDModel | ThreeDModelUpdate]) -> ThreeDModel | ThreeDModelList:
        ""\"Update 3D models.\"\"\"
        return await self._update_multiple(
            list_cls=ThreeDModelList,
            resource_cls=ThreeDModel,
            update_cls=ThreeDModelUpdate,
            items=item,
        )

    async def delete(self, id: int | Sequence[int]) -> None:
        ""\"Delete 3D models.\"\"\"
        ids = [id] if isinstance(id, int) else id
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=ids),
            wrap_ids=True,
        )


class AsyncThreeDRevisionsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/3d/models"

    async def list(self, model_id: int, published: bool | None = None, limit: int | None = DEFAULT_LIMIT_READ) -> ThreeDModelRevisionList:
        ""\"List 3D model revisions.\"\"\"
        filter = {}
        if published is not None:
            filter["published"] = published
        return await self._list(
            list_cls=ThreeDModelRevisionList,
            resource_cls=ThreeDModelRevision,
            method="GET",
            resource_path=f"{self._RESOURCE_PATH}/{model_id}/revisions",
            limit=limit,
            other_params=filter,
        )

    async def retrieve(self, model_id: int, revision_id: int) -> ThreeDModelRevision | None:
        ""\"Retrieve 3D model revision.\"\"\"
        try:
            res = await self._get(url_path=f"{self._RESOURCE_PATH}/{model_id}/revisions/{revision_id}")
            return ThreeDModelRevision._load(res.json(), cognite_client=self._cognite_client)
        except Exception:
            return None

    async def create(
        self, 
        model_id: int,
        revision: ThreeDModelRevision | ThreeDModelRevisionWrite | Sequence[ThreeDModelRevision] | Sequence[ThreeDModelRevisionWrite]
    ) -> ThreeDModelRevision | ThreeDModelRevisionList:
        ""\"Create 3D model revisions.\"\"\"
        return await self._create_multiple(
            list_cls=ThreeDModelRevisionList,
            resource_cls=ThreeDModelRevision,
            items=revision,
            resource_path=f"{self._RESOURCE_PATH}/{model_id}/revisions",
        )

    async def update(
        self, 
        model_id: int,
        item: ThreeDModelRevision | ThreeDModelRevisionUpdate | Sequence[ThreeDModelRevision | ThreeDModelRevisionUpdate]
    ) -> ThreeDModelRevision | ThreeDModelRevisionList:
        ""\"Update 3D model revisions.\"\"\"
        return await self._update_multiple(
            list_cls=ThreeDModelRevisionList,
            resource_cls=ThreeDModelRevision,
            update_cls=ThreeDModelRevisionUpdate,
            items=item,
            resource_path=f"{self._RESOURCE_PATH}/{model_id}/revisions",
        )

    async def delete(self, model_id: int, revision_id: int | Sequence[int]) -> None:
        ""\"Delete 3D model revisions.\"\"\"
        revision_ids = [revision_id] if isinstance(revision_id, int) else revision_id
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=revision_ids),
            wrap_ids=True,
            resource_path=f"{self._RESOURCE_PATH}/{model_id}/revisions",
        )


class AsyncThreeDAssetMappingAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/3d/models"

    async def list(self, model_id: int, revision_id: int, limit: int | None = DEFAULT_LIMIT_READ) -> ThreeDAssetMappingList:
        ""\"List 3D asset mappings.\"\"\"
        return await self._list(
            list_cls=ThreeDAssetMappingList,
            resource_cls=ThreeDAssetMapping,
            method="GET",
            resource_path=f"{self._RESOURCE_PATH}/{model_id}/revisions/{revision_id}/mappings",
            limit=limit,
        )

    async def create(
        self,
        model_id: int,
        revision_id: int,
        mapping: ThreeDAssetMapping | ThreeDAssetMappingWrite | Sequence[ThreeDAssetMapping] | Sequence[ThreeDAssetMappingWrite]
    ) -> ThreeDAssetMapping | ThreeDAssetMappingList:
        ""\"Create 3D asset mappings.\"\"\"
        return await self._create_multiple(
            list_cls=ThreeDAssetMappingList,
            resource_cls=ThreeDAssetMapping,
            items=mapping,
            resource_path=f"{self._RESOURCE_PATH}/{model_id}/revisions/{revision_id}/mappings",
        )

    async def delete(
        self,
        model_id: int,
        revision_id: int,
        asset_mapping: ThreeDAssetMapping | Sequence[ThreeDAssetMapping]
    ) -> None:
        ""\"Delete 3D asset mappings.\"\"\"
        mappings = [asset_mapping] if not isinstance(asset_mapping, Sequence) else asset_mapping
        items = [{"assetId": m.asset_id, "nodeId": m.node_id, "treeIndex": m.tree_index} for m in mappings]
        await self._post(
            url_path=f"{self._RESOURCE_PATH}/{model_id}/revisions/{revision_id}/mappings/delete",
            json={"items": items}
        )
""",
}


def fix_api_files():
    """Fix all API files by replacing placeholder implementations."""
    api_dir = "/workspace/cognite/client/_api_async"
    
    for filename, content in API_IMPLEMENTATIONS.items():
        filepath = os.path.join(api_dir, filename)
        print(f"Fixing {filepath}...")
        
        with open(filepath, 'w') as f:
            f.write(content)
        
        print(f"✓ Fixed {filepath}")

if __name__ == "__main__":
    fix_api_files()
    print("Fixed all remaining API implementations!")