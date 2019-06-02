# -*- coding: utf-8 -*-
import json
from typing import Dict, List

import pandas as pd

from cognite.client._api_client import APIClient, CogniteCollectionResponse, CogniteResource, CogniteResponse


class AssetResponse(CogniteResponse):
    def __init__(self, internal_representation):
        super().__init__(internal_representation)
        item = self.to_json()
        self.id = item.get("id")
        self.name = item.get("name")
        self.depth = item.get("depth")
        self.description = item.get("description")
        self.created_time = item.get("createdTime")
        self.last_updated_time = item.get("lastUpdatedTime")
        self.metadata = item.get("metadata")
        self.parent_id = item.get("parentId")
        self.path = item.get("path")

    def to_pandas(self):
        """Returns data as a pandas dataframe"""
        if len(self.to_json()) > 0:
            asset = self.to_json().copy()
            # Hack to avoid path ending up as first element in dict as from_dict will fail
            path = asset.pop("path")
            df = pd.DataFrame.from_dict(asset, orient="index")
            df.loc["path"] = [path]
            return df
        return pd.DataFrame()


class AssetListResponse(CogniteCollectionResponse):
    """Assets Response Object"""

    _RESPONSE_CLASS = AssetResponse

    def to_pandas(self):
        """Returns data as a pandas dataframe"""
        if len(self.to_json()) > 0:
            return pd.DataFrame(self.internal_representation["data"]["items"])
        return pd.DataFrame()


class Asset(CogniteResource):
    """Data transfer object for assets.

    Args:
        name (str):                 Name of asset. Often referred to as tag.
        id (int):                   Id of asset.
        parent_id (int):            ID of parent asset, if any.
        description (str):          Description of asset.
        metadata (dict):            Custom , application specific metadata. String key -> String Value.
        ref_id (str):               Reference ID used only in post request to disambiguate references to duplicate
                                    names.
        parent_name (str):          Name of parent, this parent must exist in the same POST request.
        parent_ref_id (str):        Reference ID of parent, to disambiguate if multiple nodes have the same name.
        source (str):               Source of asset.
        source_id (str):            Source id of asset.
    """

    def __init__(
        self,
        name=None,
        id=None,
        parent_id=None,
        description=None,
        metadata=None,
        ref_id=None,
        parent_name=None,
        parent_ref_id=None,
        source=None,
        source_id=None,
    ):
        self.name = name
        self.id = id
        self.parent_id = parent_id
        self.description = description
        self.metadata = metadata
        self.ref_id = ref_id
        self.parent_name = parent_name
        self.parent_ref_id = parent_ref_id
        self.source = source
        self.source_id = source_id


class AssetsClient(APIClient):
    def __init__(self, **kwargs):
        super().__init__(version="0.5", **kwargs)

    def get_assets(
        self, name=None, path=None, description=None, metadata=None, depth=None, fuzziness=None, **kwargs
    ) -> AssetListResponse:
        """Returns assets matching provided description.

        Args:
            name (str):             The name of the asset(s) to get.

            path (List[int]):       The path of the subtree to search in.

            description (str):      Search query.

            metadata (dict):         The metadata values used to filter the results.

            depth (int):            Get sub assets up oto this many levels below the specified path.

            fuzziness (int):        The degree of fuzziness in the name matching.

        Keyword Arguments:
            autopaging (bool):      Whether or not to automatically page through results. If set to true, limit will be
                                    disregarded. Defaults to False.

            limit (int):            The maximum number of assets to be returned.

            cursor (str):           Cursor to use for paging through results.
        Returns:
            stable.assets.AssetListResponse: A data object containing the requested assets with several getter methods with different
            output formats.

        Examples:
            You can fetch all assets with a maximum depth of three like this::

                client = CogniteClient()
                res = client.assets.get_assets(depth=3, autopaging=True)
                print(res.to_pandas())

            You can fetch all assets in a given subtree like this::

                client = CogniteClient()
                res = client.assets.get_assets(path=[1,2,3], autopaging=True)
                print(res.to_pandas())
        """
        autopaging = kwargs.get("autopaging", False)
        url = "/assets"
        params = {
            "name": name,
            "description": description,
            "path": str(path) if path else None,
            "metadata": str(metadata) if metadata else None,
            "depth": depth,
            "fuzziness": fuzziness,
            "cursor": kwargs.get("cursor"),
            "limit": kwargs.get("limit", self._LIMIT) if not autopaging else self._LIMIT,
        }
        res = self._get(url, params=params, autopaging=autopaging)
        return AssetListResponse(res.json())

    def get_asset(self, asset_id) -> AssetResponse:
        """Returns the asset with the provided assetId.

        Args:
            asset_id (int):         The asset id of the top asset to get.

        Returns:
            stable.assets.AssetResponse: A data object containing the requested assets with several getter methods with different
            output formats.
        Examples:
            You can fetch a single asset like this::

                client = CogniteClient()
                res = client.assets.get_asset(asset_id=123)
                print(res)
        """
        url = "/assets/{}/subtree".format(asset_id)
        res = self._get(url)
        return AssetResponse(res.json())

    def get_asset_subtree(self, asset_id, depth=None, **kwargs) -> AssetListResponse:
        """Returns asset subtree of asset with provided assetId.

        Args:
            asset_id (int):         The asset id of the top asset to get.

            depth (int):            Get subassets this many levels below the top asset.

        Keyword Arguments:
            limit (int):            The maximum nuber of assets to be returned.

            cursor (str):           Cursor to use for paging through results.

            autopaging (bool):      Whether or not to automatically page through results. If set to true, limit will be
                                    disregarded. Defaults to False.
        Returns:
            stable.assets.AssetListResponse: A data object containing the requested assets with several getter methods
            with different output formats.

        Examples:
            You can fetch an asset subtree like this::

                client = CogniteClient()
                res = client.assets.get_asset_subtree(asset_id=123, depth=2)
                print(res.to_pandas())
        """
        autopaging = kwargs.get("autopaging", False)
        url = "/assets/{}/subtree".format(asset_id)
        params = {
            "depth": depth,
            "limit": kwargs.get("limit", self._LIMIT) if not autopaging else self._LIMIT,
            "cursor": kwargs.get("cursor"),
        }
        res = self._get(url, params=params, autopaging=autopaging)
        return AssetListResponse(res.json())

    def post_assets(self, assets: List[Asset]) -> AssetListResponse:
        """Insert a list of assets.

        Args:
            assets (list[stable.assets.Asset]): List of asset data transfer objects.

        Returns:
            stable.assets.AssetListResponse: A data object containing the posted assets with several getter methods with different
            output formats.

        Examples:
            Posting an asset::

                from cognite.client.stable.assets import Asset

                client = CogniteClient()

                my_asset = Asset("myasset")
                assets_to_post = [my_asset]
                res = client.assets.post_assets(assets_to_post)
                print(res)
        """
        url = "/assets"
        items = [asset.camel_case_dict() for asset in assets]
        res = self._post(url, body={"items": items})
        return AssetListResponse(res.json())

    def delete_assets(self, asset_ids: List[int]) -> None:
        """Delete a list of assets.

        Args:
            asset_ids (list[int]): List of IDs of assets to delete.

        Returns:
            None

        Examples:
            You can delete an asset like this::

                client = CogniteClient()
                res = client.assets.delete_assets([123])
        """
        url = "/assets/delete"
        body = {"items": asset_ids}
        self._post(url, body=body)

    @staticmethod
    def _asset_to_patch_format(id, name=None, description=None, metadata=None, source=None, source_id=None):
        patch_asset = {"id": id}
        if name:
            patch_asset["name"] = {"set": name}
        if description:
            patch_asset["description"] = {"set": description}
        if metadata:
            patch_asset["metadata"] = {"set": metadata}
        if source:
            patch_asset["source"] = {"set": source}
        if source_id:
            patch_asset["sourceId"] = {"set": source_id}
        return patch_asset

    def update_asset(
        self,
        asset_id: int,
        name: str = None,
        description: str = None,
        metadata: Dict = None,
        source: str = None,
        source_id: str = None,
    ) -> AssetResponse:
        """Update an asset

        Args:
            asset_id (int): The id of the asset to update
            name (str, optional): The new name
            description (str, optional): The new description
            metadata (Dict, optional): The new metadata
            source (str, optional): The new source
            source_id (str, optional): The new source id

        Returns:
            AssetResponse: The updated asset
        """
        url = "/assets/{}/update".format(asset_id)
        body = self._asset_to_patch_format(asset_id, name, description, metadata, source, source_id)
        res = self._post(url, body=body)
        return AssetResponse(res.json())

    def update_assets(self, assets: List[Asset]):
        """Update multiple assets

        Args:
            assets (List[stable.assets.Asset]): List of assets to update

        Returns:
            AssetListResponse: List of updated assets
        """
        url = "/assets/update"
        items = [
            self._asset_to_patch_format(a.id, a.name, a.description, a.metadata, a.source, a.source_id) for a in assets
        ]
        res = self._post(url, body={"items": items})
        return AssetListResponse(res.json())

    def search_for_assets(
        self,
        name=None,
        description=None,
        query=None,
        metadata=None,
        asset_subtrees=None,
        min_created_time=None,
        max_created_time=None,
        min_last_updated_time=None,
        max_last_updated_time=None,
        **kwargs
    ) -> AssetListResponse:
        """Search for assets.

        Args:
            name:   Prefix and fuzzy search on name.
            description str:   Prefix and fuzzy search on description.
            query (str):       Search on name and description using wildcard search on each of the words
                                (separated by spaces). Retrieves results where at least one word must match.
                                Example: 'some other'
            metadata (dict):        Filter out assets that do not match these metadata fields and values (case-sensitive).
                                    Format is {"key1":"value1","key2":"value2"}.
            asset_subtrees (List[int]): Filter out assets that are not linked to assets in the subtree rooted at these assets.
                                        Format is [12,345,6,7890].
            min_created_time(str):  Filter out assets with createdTime before this. Format is milliseconds since epoch.
            max_created_time (str): Filter out assets with createdTime after this. Format is milliseconds since epoch.
            min_last_updated_time(str):  Filter out assets with lastUpdatedtime before this. Format is milliseconds since epoch.
            max_last_updated_time(str): Filter out assets with lastUpdatedtime after this. Format is milliseconds since epoch.

        Keyword Args:
            sort (str):             Field to be sorted.
            dir (str):              Sort direction (desc or asc)
            limit (int):            Return up to this many results. Max is 1000, default is 25.
            offset (int):           Offset from the first result. Sum of limit and offset must not exceed 1000. Default is 0.
            boost_name (str):       Whether or not boosting name field. This option is test_experimental and can be changed.
        Returns:
            stable.assets.AssetListResponse.

        Examples:
            Searching for assets::

                client = CogniteClient()
                res = client.assets.search_for_assets(name="myasset")
                print(res)
        """
        url = "/assets/search"
        params = {
            "name": name,
            "description": description,
            "query": query,
            "metadata": json.dumps(metadata),
            "assetSubtrees": str(asset_subtrees) if asset_subtrees is not None else None,
            "minCreatedTime": min_created_time,
            "maxCreatedTime": max_created_time,
            "minLastUpdatedTime": min_last_updated_time,
            "maxLastUpdatedTime": max_last_updated_time,
            "sort": kwargs.get("sort"),
            "dir": kwargs.get("dir"),
            "limit": kwargs.get("limit", self._LIMIT),
            "offset": kwargs.get("offset"),
            "boostName": kwargs.get("boost_name"),
        }

        res = self._get(url, params=params)
        return AssetListResponse(res.json())
