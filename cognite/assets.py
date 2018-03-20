# -*- coding: utf-8 -*-
"""Assets Module.

This module mirrors the Assets API.
"""
import cognite._constants as _constants
import cognite._utils as _utils
import cognite.config as config
from cognite.data_objects import AssetSearchObject


# Author: TK
def search_assets(description, **kwargs):
    '''Returns assets matching provided description.

    Args:
        description (str):      Search query.

    Keyword Arguments:
        api_key (str):          Your api-key.

        project (str):          Project name.
    Returns:
        AssetSearchObject
    '''
    api_key, project = config.get_config_variables(kwargs.get('api_key'), kwargs.get('project'))
    url = config.get_base_url() + '/projects/{}/assets'.format(project)
    params = {
        'description': description,
    }
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    res = _utils.get_request(url, params=params, headers=headers, cookies=config.get_cookies())

    return AssetSearchObject(res.json())


# Author: TK
def get_assets(tag_id=None, depth=None, limit=_constants.LIMIT, **kwargs):
    '''Returns assets with provided assetId.

    Args:
        tag_id (str):         The tag ID of the top asset to get.

        depth (int):            Get subassets this many levels below the top asset.

    Keyword Arguments:
        api_key (str):          Your api-key.

        project (str):          Project name.
    Returns:
        AssetSearchObject
    '''
    api_key, project = config.get_config_variables(kwargs.get('api_key'), kwargs.get('project'))
    url = config.get_base_url() + '/projects/{}/assets/{}'.format(project, tag_id)
    params = {
        'depth': depth,
        'limit': limit
    }
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    res = _utils.get_request(url, params=params, headers=headers, cookies=config.get_cookies())
    return AssetSearchObject(res.json())
