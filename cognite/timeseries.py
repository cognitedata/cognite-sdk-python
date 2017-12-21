import cognite.config as config
import io
import json
import pandas as pd
import requests

from cognite._data_objects import DatapointsObject, LatestDatapointObject
from cognite._utils import _granularity_to_ms, _ProgressIndicator

def get_datapoints(tagId, aggregates=None, granularity=None, start=None, end=None, limit=config._LIMIT, api_key=None,
                   project=None):
    api_key, project = config._get_config_variables(api_key, project)
    tagId = tagId.replace('/', '%2F')
    url = 'https://api.cognitedata.com/api/0.3/projects/{}/timeseries/data/{}'.format(project, tagId)
    params = {
        'aggregates': aggregates,
        'granularity': granularity,
        'limit': limit,
        'start': start,
        'end': end,
    }
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    r = requests.get(url, params=params, headers=headers)
    if r.status_code != 200:
        raise Exception(r.json()['error'])
    return DatapointsObject(r.json())

def get_latest(tagId, api_key=None, project=None):
    api_key, project = config._get_config_variables(api_key, project)
    tagId = tagId.replace('/', '%2F')
    url = 'https://api.cognitedata.com/api/0.3/projects/{}/timeseries/latest/{}'.format(project, tagId)
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise Exception(r.json()['error'])
    return LatestDatapointObject(r.json())

def get_multi_tag_datapoints(tagIds, aggregates=None, granularity=None, start=None, end=None, limit=config._LIMIT, api_key=None,
                             project=None):
    api_key, project = config._get_config_variables(api_key, project)
    url = 'https://api.cognitedata.com/api/0.3/projects/{}/timeseries/dataquery'.format(project)
    body = {
        'items': [{'tagId': '{}'.format(tagId)} if type(tagId) == str else {'tagId': '{}'.format(tagId['tagId']), 'aggregates': tagId['aggregates']} for tagId in tagIds],
        'aggregates': aggregates,
        'granularity': granularity,
        'start': start,
        'end': end,
        'limit': limit
    }
    headers = {
        'api-key': api_key,
        'content-type': 'application/json',
        'accept': 'application/json'
    }
    r = requests.post(url=url, data=json.dumps(body), headers=headers)
    if r.status_code != 200:
        raise Exception(r.json()['error'])
    return [DatapointsObject({'data': {'items': [dp]}}) for dp in r.json()['data']['items']]

def get_datapoints_frame(tagIds, aggregates, granularity, start=None, end=None, api_key=None, project=None):
    api_key, project = config._get_config_variables(api_key, project)
    url = 'https://api.cognitedata.com/api/0.3/projects/{}/timeseries/dataframe'.format(project)
    body = {
        'items': [{'tagId': '{}'.format(tagId)} if type(tagId) == str else {'tagId': '{}'.format(tagId['tagId']), 'aggregates': tagId['aggregates']} for tagId in tagIds],
        'aggregates': aggregates,
        'granularity': granularity,
        'start': start,
        'end': end,
        'limit': config._LIMIT
    }
    headers = {
        'api-key': api_key,
        'content-type': 'application/json',
        'accept': 'text/csv'
    }
    dataframes = []
    p = _ProgressIndicator(tagIds, start, end)
    while len(dataframes) == 0 or dataframes[-1].shape[0] == config._LIMIT:
        r = requests.post(url=url, data=json.dumps(body), headers=headers)
        if r.status_code != 200:
            raise Exception(r.json()['error'])
        dataframes.append(pd.read_csv(io.StringIO(r.content.decode(r.encoding if r.encoding else r.apparent_encoding))))
        latest_timestamp = int(dataframes[-1].iloc[-1, 0])
        p._update_progress(latest_timestamp)
        body['start'] = latest_timestamp + _granularity_to_ms(granularity)
    return pd.concat(dataframes).reset_index(drop=True)
