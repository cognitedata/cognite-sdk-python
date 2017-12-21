import cognite.config as config
import json
import requests

from cognite._data_objects import TagMatchingObject

def tag_matching(tagIds, fuzzy_threshold=0, platform=None, api_key=None, project=None):
    api_key, project = config._get_config_variables(api_key, project)
    url = 'https://api.cognitedata.com/api/0.3/projects/{}/tagmatching'.format(project)
    body = {
        'tagIds': tagIds,
        'metadata': {
            'fuzzyThreshold': fuzzy_threshold,
            'platform': platform
        }
    }
    headers = {
        'api-key': api_key,
        'content-type': '*/*',
        'accept': 'application/json'
    }
    r = requests.post(url=url, data=json.dumps(body), headers=headers)
    if r.status_code != 200:
        raise Exception(r.json()['error'])
    return TagMatchingObject(r.json())
