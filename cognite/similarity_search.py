import cognite._constants as _constants
import cognite._utils as _utils
from cognite.config import _get_config_variables
from cognite._data_objects import SimilaritySearchObject

def search(input_tags, query_tags, input_interval, query_interval, modes, limit=10, api_key=None, project=None):
    api_key, project = _get_config_variables(api_key, project)
    url = _constants._BASE_URL + '/projects/{}/similaritysearch'.format(project)
    body = {
        'inputTags': input_tags,
        'queryTags': query_tags,
        'inputInterval': {'start': input_interval[0], 'end': input_interval[1]} if type(input_interval) == tuple else input_interval,
        'queryInterval': {'start': query_interval[0], 'end': query_interval[1]} if type(query_interval) == tuple else query_interval,
        'modes': modes,
        'limit': limit
    }
    headers = {
        'api-key': api_key,
        'content-type': 'application/json',
        'accept': 'application/json'
    }
    r = _utils._post_request(url=url, body=body, headers=headers)
    return SimilaritySearchObject(r.json())