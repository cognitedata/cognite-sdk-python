import cognite.config as config
import cognite._constants as _constants
import cognite._utils as _utils
import io
import pandas as pd

from cognite._data_objects import DatapointsObject, LatestDatapointObject

def get_datapoints(tagId, aggregates=None, granularity=None, start=None, end=None, limit=_constants._LIMIT,
                   api_key=None, project=None):
    '''Returns a DatapointsObject containing a list of datapoints for the given query.

    Args:
        tagId (str):            The tagId to retrieve data for.

        aggregates (list):      The list of aggregate functions you wish to apply to the data. Valid aggregate functions
                                are: 'average/avg, max, min, count, sum, interpolation/int, stepinterpolation/step'.

        granularity (str):      The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
                                second/s', or a multiple of these indicated by a number as a prefix.

                                    Example: '12hour'.

        start (str):            Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.

                                    Example: '2d-ago' will get everything that is up to 2 days old. Can also send time in
                                    ms since epoch.

        end (str):              Get datapoints up to this time. Same format as for start.

        limit (int):            Return up to this number of datapoints.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        DatapointsObject: The data can be retrieved from this object with the following methods:
            to_json(): Returns the data in Json format.
            to_pandas(): Returns the data as a pandas dataframe.
            to_ndarray(): Returns the data as a numpy array.
    '''
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
    '''Returns a LatestDatapointObject containing a list of datapoints for the given query.

    Args:
        tagId (str):            The tagId to retrieve data for.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        DatapointsObject: The data can be retrieved from this object with the following methods:
            to_json(): Returns the data in Json format.
            to_pandas(): Returns the data as a pandas dataframe.
            to_ndarray(): Returns the data as a numpy array.
    '''
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
    '''Returns a list of DatapointsObjects each of which contains a list of datapoints for the given tagId.

    Args:
        tagIds (list):          The list of tagIds to retrieve data for.

        aggregates (list):      The list of aggregate functions you wish to apply to the data. Valid aggregate functions
                                are: 'average/avg, max, min, count, sum, interpolation/int, stepinterpolation/step'.

        granularity (str):      The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
                                second/s', or a multiple of these indicated by a number as a prefix.

                                    Example: '12h'.

        start (str):            Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.

                                    Example: '2d-ago' will get everything that is up to 2 days old. Can also send time in
                                    ms since epoch.

        end (str):              Get datapoints up to this time. Same format as for start.

        limit (int):            Return up to this number of datapoints.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        list of DatapointsObjects: The data can be retrieved from these object with the following methods:
            to_json(): Returns the data in Json format.
            to_pandas(): Returns the data as a pandas dataframe.
            to_ndarray(): Returns the data as a numpy array.
    '''
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
    '''Returns a pandas dataframe of datapoints for the given tagIds all on the same timestamps.

    Args:
        tagIds (list):          The list of tagIds to retrieve data for. Each tagId can be either a string containing
                                the tagId or a dictionary containing the tagId and a list of specific aggregate
                                functions.

                                    Example option 1: ['<tagId1>', '<tagId2>'].

                                    Example option 2: [{'tagId': '<tagId1>', 'aggregates': ['avg', 'max']},
                                                    {'tagId': '<tagId2>', 'aggregates': ['step']}]

        aggregates (list):      The list of aggregate functions you wish to apply to the data for which you have not
                                specified an aggregate function. Valid aggregate functions are: 'average/avg, max, min,
                                count, sum, interpolation/int, stepinterpolation/step'.

        granularity (str):      The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
                                second/s', or a multiple of these indicated by a number as a prefix.

                                    Example: '12hour'.

        start (str):            Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.

                                    Example: '2d-ago' will get everything that is up to 2 days old. Can also send time in
                                    ms since epoch.

        end (str):              Get datapoints up to this time. Same format as for start.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        pandas dataframe: A pandas dataframe containing the datapoints for the given tagIds. The datapoints for all the
                        tagIds will all be on the same timestamps.
    '''
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
