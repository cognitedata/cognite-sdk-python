# -*- coding: utf-8 -*-
"""Assets Module.

This module mirrors the Assets API.

https://doc.cognitedata.com/0.5/#Cognite-API-Assets
"""
import json
from typing import List

import cognite._constants as constants
import cognite._utils as utils
import cognite.config as config
from cognite.v05.dto import Asset, AssetListResponse, AssetResponse


def get_assets(name=None, path=None, description=None, metadata=None, depth=None, fuzziness=None, **kwargs):
    """Returns assets matching provided description.

    Args:
        name (str):             The name of the asset(s) to get.

        path (str):             The path of the subtree to search in.

        description (str):      Search query.

        metadata (dict):         The metadata values used to filter the results.

        depth (int):            Get sub assets up oto this many levels below the specified path.

        fuzziness (int):        The degree of fuzziness in the name matching.

    Keyword Arguments:
        autopaging (bool):      Whether or not to automatically page through results. If set to true, limit will be
                                disregarded. Defaults to False.

        limit (int):            The maximum number of assets to be returned.

        cursor (str):           Cursor to use for paging through results.

        api_key (str):          Your api-key.

        project (str):          Project name.
    Returns:
        v05.dto.AssetListResponse: A data object containing the requested assets with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.5/projects/{}/assets".format(project)
    params = {
        "name": name,
        "description": description,
        "path": path,
        "metadata": str(metadata) if metadata else None,
        "depth": depth,
        "fuzziness": fuzziness,
        "cursor": kwargs.get("cursor"),
        "limit": kwargs.get("limit", constants.LIMIT) if not kwargs.get("autopaging") else constants.LIMIT,
    }
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, params=params, headers=headers, cookies=config.get_cookies())
    assets = []
    assets.extend(res.json()["data"]["items"])
    next_cursor = res.json()["data"].get("nextCursor")

    while next_cursor and kwargs.get("autopaging"):
        params["cursor"] = next_cursor
        res = utils.get_request(url=url, headers=headers, params=params, cookies=config.get_cookies())
        assets.extend(res.json()["data"]["items"])
        next_cursor = res.json()["data"].get("nextCursor")

    return AssetListResponse(
        {
            "data": {
                "nextCursor": next_cursor,
                "previousCursor": res.json()["data"].get("previousCursor"),
                "items": assets,
            }
        }
    )


def get_asset(asset_id, **kwargs):
    """Returns the asset with the provided assetId.

    Args:
        asset_id (int):         The asset id of the top asset to get.

    Keyword Arguments:
        api_key (str):          Your api-key.

        project (str):          Project name.
    Returns:
        v05.dto.AssetResponse: A data object containing the requested assets with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.5/projects/{}/assets/{}/subtree".format(project, asset_id)
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return AssetResponse(res.json())


def get_asset_subtree(asset_id, depth=None, **kwargs):
    """Returns asset subtree of asset with provided assetId.

    Args:
        asset_id (int):         The asset id of the top asset to get.

        depth (int):            Get subassets this many levels below the top asset.

    Keyword Arguments:
        limit (int):            The maximum nuber of assets to be returned.

        cursor (str):           Cursor to use for paging through results.

        api_key (str):          Your api-key.

        project (str):          Project name.
    Returns:
        v05.dto.AssetListResponse: A data object containing the requested assets with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.5/projects/{}/assets/{}/subtree".format(project, asset_id)
    params = {"depth": depth, "limit": kwargs.get("limit", constants.LIMIT), "cursor": kwargs.get("cursor")}
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, params=params, headers=headers, cookies=config.get_cookies())
    return AssetListResponse(res.json())


def post_assets(assets: List[Asset], **kwargs):
    """Insert a list of assets.

    Args:
        assets (list[v05.dto.Asset]): List of asset data transfer objects.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        v05.dto.AssetListResponse: A data object containing the posted assets with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.5/projects/{}/assets".format(project)
    body = {"items": [asset.__dict__ for asset in assets]}
    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}
    res = utils.post_request(url, body=body, headers=headers, cookies=config.get_cookies())
    return AssetListResponse(res.json())


def delete_assets(asset_ids: List[int], **kwargs):
    """Delete a list of assets.

    Args:
        asset_ids (list[int]): List of IDs of assets to delete.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        An empty response.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.5/projects/{}/assets/delete".format(project)
    body = {"items": asset_ids}
    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}
    res = utils.post_request(url, body=body, headers=headers, cookies=config.get_cookies())
    return res.json()


def search_for_assets(
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
):
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
            api_key (str):          Your api-key.
            project (str):          Project name.
            sort (str):             Field to be sorted.
            dir (str):              Sort direction (desc or asc)
            limit (int):            Return up to this many results. Max is 1000, default is 25.
            offset (int):           Offset from the first result. Sum of limit and offset must not exceed 1000. Default is 0.
            boost_name (str):       Whether or not boosting name field. This option is experimental and can be changed.
        Returns:
            v05.dto.EventListResponse.
        """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.5/projects/{}/assets/search".format(project)
    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}
    params = {
        "name": name,
        "description": description,
        "query": query,
        "metadata": json.dumps(metadata),
        "assetSubtrees": asset_subtrees,
        "minCreatedTime": min_created_time,
        "maxCreatedTime": max_created_time,
        "minLastUpdatedTime": min_last_updated_time,
        "maxLastUpdatedTime": max_last_updated_time,
        "sort": kwargs.get("sort"),
        "dir": kwargs.get("dir"),
        "limit": kwargs.get("limit", 1000),
        "offset": kwargs.get("offset"),
        "boostName": kwargs.get("boost_name"),
    }

    res = utils.get_request(url, headers=headers, params=params)
    return AssetListResponse(res.json())
