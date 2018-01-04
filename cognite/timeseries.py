import cognite.config as config
import cognite._constants as _constants
import cognite._utils as _utils
import io
import pandas as pd

from cognite._data_objects import DatapointsObject, LatestDatapointObject

def get_datapoints(tagId, aggregates=None, granularity=None, start=None, end=None, limit=_constants._LIMIT,
                   api_key=None, project=None):
    api_key, project = config._get_config_variables(api_key, project)
    tagId = tagId.replace('/', '%2F')
    url = _constants._BASE_URL + '/projects/{}/timeseries/data/{}'.format(project, tagId)
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
    r = _utils._get_request(url, params=params, headers=headers)
    return DatapointsObject(r.json())

def get_latest(tagId, api_key=None, project=None):
    api_key, project = config._get_config_variables(api_key, project)
    tagId = tagId.replace('/', '%2F')
    url = _constants._BASE_URL + '/projects/{}/timeseries/latest/{}'.format(project, tagId)
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    r = _utils._get_request(url, headers=headers)
    return LatestDatapointObject(r.json())

def get_multi_tag_datapoints(tagIds, aggregates=None, granularity=None, start=None, end=None, limit=_constants._LIMIT,
                             api_key=None, project=None):
    api_key, project = config._get_config_variables(api_key, project)
    url = _constants._BASE_URL + '/projects/{}/timeseries/dataquery'.format(project)
    body = {
        'items': [{'tagId': '{}'.format(tagId)}
                  if type(tagId) == str
                  else {'tagId': '{}'.format(tagId['tagId']), 'aggregates': tagId['aggregates']} for tagId in tagIds],
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
    r = _utils._post_request(url=url, body=body, headers=headers)
    return [DatapointsObject({'data': {'items': [dp]}}) for dp in r.json()['data']['items']]

def get_datapoints_frame(tagIds, aggregates, granularity, start=None, end=None, api_key=None, project=None):
    api_key, project = config._get_config_variables(api_key, project)
    url = _constants._BASE_URL + '/projects/{}/timeseries/dataframe'.format(project)
    body = {
        'items': [{'tagId': '{}'.format(tagId)}
                  if type(tagId) == str
                  else {'tagId': '{}'.format(tagId['tagId']), 'aggregates': tagId['aggregates']} for tagId in tagIds],
        'aggregates': aggregates,
        'granularity': granularity,
        'start': start,
        'end': end,
        'limit': _constants._LIMIT
    }
    headers = {
        'api-key': api_key,
        'content-type': 'application/json',
        'accept': 'text/csv'
    }
    dataframes = []
    p = _utils._ProgressIndicator(tagIds, start, end)
    while len(dataframes) == 0 or dataframes[-1].shape[0] == _constants._LIMIT:
        r = _utils._post_request(url=url, body=body, headers=headers)
        dataframes.append(pd.read_csv(io.StringIO(r.content.decode(r.encoding if r.encoding else r.apparent_encoding))))
        latest_timestamp = int(dataframes[-1].iloc[-1, 0])
        p._update_progress(latest_timestamp)
        body['start'] = latest_timestamp + _utils._granularity_to_ms(granularity)
    return pd.concat(dataframes).reset_index(drop=True)
