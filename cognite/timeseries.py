import cognite.config as config
from cognite._helpers import _granularity_to_ms
import io
import json
import pandas as pd
import requests

from cognite._data_objects import DatapointsObject, LatestDatapointObject

def get_datapoints(tagId, aggregates=None, granularity=None, start=None, end=None, limit=10000, api_key=None,
                   project=None):
    api_key, project = config.get_config_variables(api_key, project)
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
        return r.text()
    return DatapointsObject(r.json())

def get_latest(tagId, api_key=None, project=None):
    api_key, project = config.get_config_variables(api_key, project)
    tagId = tagId.replace('/', '%2F')
    url = 'https://api.cognitedata.com/api/0.3/projects/{}/timeseries/latest/{}'.format(project, tagId)
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        return r.text()
    return LatestDatapointObject(r.json())

def get_multi_tag_datapoints(tagIds, aggregates=None, granularity=None, start=None, end=None, limit=10000, api_key=None,
                             project=None):
    api_key, project = config.get_config_variables(api_key, project)
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
        return r.text()
    return [DatapointsObject({'data': {'items': [dp]}}) for dp in r.json()['data']['items']]

def get_datapoints_frame(tagIds, aggregates, granularity, start=None, end=None, api_key=None,
                         project=None):
    api_key, project = config.get_config_variables(api_key, project)
    limit = 10000
    url = 'https://api.cognitedata.com/api/0.3/projects/{}/timeseries/dataframe'.format(project)
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
        'accept': 'text/csv'
    }
    r = requests.post(url=url, data=json.dumps(body), headers=headers)
    if r.status_code != 200:
        print(r.text())
        return None
    dataframes = [pd.read_csv(io.StringIO(r.content.decode(r.encoding if r.encoding else r.apparent_encoding)))]
    while dataframes[-1].shape[0] == limit:
        start = int(dataframes[-1].iloc[-1,0]) + _granularity_to_ms(granularity)
        body['start'] = start
        r = requests.post(url=url, data=json.dumps(body), headers=headers)
        if r.status_code != 200:
            return r.text()
        dataframes.append(pd.read_csv(io.StringIO(r.content.decode(r.encoding if r.encoding else r.apparent_encoding))))
    return pd.concat(dataframes).reset_index(drop=True)
