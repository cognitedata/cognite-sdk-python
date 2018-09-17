# -*- coding: utf-8 -*-
"""Assets Module.

This module mirrors the Assets API.

https://doc.cognitedata.com/0.4/#Cognite-API-Assets
"""
from typing import List

import cognite._constants as constants
import cognite._utils as utils
import cognite.config as config
from cognite.v04.dto import AssetResponse, Asset


# Author: TK
def get_assets(name=None, path=None, description=None, metadata=None, depth=None, fuzziness=None, **kwargs):
    """Returns assets matching provided description.

    Args:
        name (str):             The name of the asset(s) to get.

        path (str):             The path of the subtree to search in.

        description (str):      Search query.

        metadata (str):         The metadata values used to filter the results.

        depth (int):            Get sub assets up oto this many levels below the specified path.

        fuzziness (int):        The degree of fuzziness in the name matching.

    Keyword Arguments:
        limit (int):            The maximum number of assets to be returned.

        cursor (str):           Cursor to use for paging through results.

        api_key (str):          Your api-key.

        project (str):          Project name.
    Returns:
        v04.dto.AssetResponse: A data object containing the requested assets with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.4/projects/{}/assets".format(project)
    params = {
        "name": name,
        "description": description,
        "path": path,
        "metadata": metadata,
        "depth": depth,
        "fuzziness": fuzziness,
        "cursor": kwargs.get("cursor"),
        "limit": kwargs.get("limit", constants.LIMIT),
    }
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, params=params, headers=headers, cookies=config.get_cookies())
    return AssetResponse(res.json())


# Author: TK
def get_asset_subtree(asset_id="", depth=None, **kwargs):
    """Returns assets with provided assetId.

    Args:
        asset_id (str):         The asset id of the top asset to get.

        depth (int):            Get subassets this many levels below the top asset.

    Keyword Arguments:
        limit (int):            The maximum nuber of assets to be returned.

        cursor (str):           Cursor to use for paging through results.

        api_key (str):          Your api-key.

        project (str):          Project name.
    Returns:
        v04.dto.AssetResponse: A data object containing the requested assets with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.4/projects/{}/assets/{}".format(project, asset_id)
    params = {"depth": depth, "limit": kwargs.get("limit", constants.LIMIT), "cursor": kwargs.get("cursor")}
    headers = {"api-key": api_key, "accept": "application/json"}
    res = utils.get_request(url, params=params, headers=headers, cookies=config.get_cookies())
    return AssetResponse(res.json())


def post_assets(assets: List[Asset], **kwargs):
    """Insert a list of assets.

    Args:
        assets (list[v04.dto.Asset]): List of asset data transfer objects.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        v04.dto.AssetResponse: A data object containing the posted assets with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.4/projects/{}/assets".format(project)
    body = {"items": [asset.__dict__ for asset in assets]}
    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}
    res = utils.post_request(url, body=body, headers=headers, cookies=config.get_cookies())
    return AssetResponse(res.json())


def delete_assets(asset_ids: List[int], **kwargs):
    """Delete a list of assets.

    Args:
        asset_ids (list[v04.dto.Asset]): List of IDs of assets to delete.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        An empty response.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.4/projects/{}/assets/delete".format(project)
    body = {"items": asset_ids}
    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}
    res = utils.post_request(url, body=body, headers=headers, cookies=config.get_cookies())
    return res.json()
