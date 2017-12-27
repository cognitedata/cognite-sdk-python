import cognite.config as config
import cognite._constants as _constants
import cognite._utils as _utils
import io
import pandas as pd
from cognite._data_objects import AssetSearchObject


# Author: TK
def searchAssets(description, api_key=None, project=None):
    api_key, project = config._get_config_variables(api_key, project)
    url = _constants._BASE_URL + '/projects/{}/assets'.format(project)
    params = {
        'description': description,
    }
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    r = _utils._get_request(url, params=params, headers=headers)

    return AssetSearchObject(r.json())


# Author: TK
def getAssets(tagId=None, depth=None, limit=None, api_key=None, project=None):
    api_key, project = config._get_config_variables(api_key, project)
    url = _constants._BASE_URL + '/projects/{}/assets/{}'.format(project, tagId)
    params = {
        'depth': depth,
    }
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    r = _utils._get_request(url, params=params, headers=headers)
    return AssetSearchObject(r.json())