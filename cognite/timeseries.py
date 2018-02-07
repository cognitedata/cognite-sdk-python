# -*- coding: utf-8 -*-
"""Timeseries Module

This module mirrors the Timeseries API. It allows you to fetch data from the api and output it in various formats.
"""
import io
import pandas as pd
import cognite.config as config
import cognite._constants as _constants
import cognite._utils as _utils

from cognite.data_objects import DatapointsObject, LatestDatapointObject

def get_datapoints(tag_id, aggregates=None, granularity=None, start=None, end=None, api_key=None, project=None):
    '''Returns a DatapointsObject containing a list of datapoints for the given query.

    This method will automate paging for the user and return all data for the given time period.

    Args:
        tag_id (str):           The tag_id to retrieve data for.

        aggregates (list):      The list of aggregate functions you wish to apply to the data. Valid aggregate functions
                                are: 'average/avg, max, min, count, sum, interpolation/int, stepinterpolation/step'.

        granularity (str):      The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
                                second/s', or a multiple of these indicated by a number as a prefix e.g. '12hour'.

        start (Union[str, int]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
                                    E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
                                    epoch.

        end (Union[str, int]):      Get datapoints up to this time. Same format as for start.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        DatapointsObject: A data object containing the requested data with several getter methods with different
        output formats.
    '''
    api_key, project = config.get_config_variables(api_key, project)
    tag_id = tag_id.replace('/', '%2F')
    url = config.get_base_url() + '/projects/{}/timeseries/data/{}'.format(project, tag_id)
    params = {
        'aggregates': aggregates,
        'granularity': granularity,
        'limit': _constants.LIMIT,
        'start': start,
        'end': end,
    }
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    prog_ind = _utils.ProgressIndicator([tag_id], start, end, api_key, project)
    datapoints = []
    while not datapoints or len(datapoints[-1]) == _constants.LIMIT:
        res = _utils.get_request(url, params=params, headers=headers).json()['data']['items'][0]['datapoints']
        if not res and not datapoints:
            prog_ind.terminate()
            return DatapointsObject({'data': {'items': [{'tagId': tag_id, 'datapoints': []}]}})
        datapoints.append(res)
        latest_timestamp = int(datapoints[-1][-1]['timestamp'])
        prog_ind.update_progress(latest_timestamp)
        params['start'] = latest_timestamp + (_utils.granularity_to_ms(granularity) if granularity else 0)
    prog_ind.terminate()
    dps = []
    [ dps.extend(el) for el in datapoints ]
    return DatapointsObject({'data': {'items': [{'tagId': tag_id, 'datapoints': dps}]}})

def get_latest(tag_id, api_key=None, project=None):
    '''Returns a LatestDatapointObject containing the latest datapoint for the given tag_id.

    Args:
        tag_id (str):           The tag_id to retrieve data for.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        DatapointsObject: A data object containing the requested data with several getter methods with different
        output formats.
    '''
    api_key, project = config.get_config_variables(api_key, project)
    tag_id = tag_id.replace('/', '%2F')
    url = config.get_base_url() + '/projects/{}/timeseries/latest/{}'.format(project, tag_id)
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    res = _utils.get_request(url, headers=headers)
    return LatestDatapointObject(res.json())

def get_multi_tag_datapoints(tag_ids, aggregates=None, granularity=None, start=None, end=None, limit=_constants.LIMIT,
                             api_key=None, project=None):
    '''Returns a list of DatapointsObjects each of which contains a list of datapoints for the given tag_id.

    Args:
        tag_ids (list):                 The list of tag_ids to retrieve data for.

        aggregates (list, optional):    The list of aggregate functions you wish to apply to the data. Valid aggregate
                                        functions are: 'average/avg, max, min, count, sum, interpolation/int,
                                        stepinterpolation/step'.

        granularity (str):              The granularity of the aggregate values. Valid entries are : 'day/d, hour/h,
                                        minute/m, second/s', or a multiple of these indicated by a number as a prefix
                                        e.g. '12hour'.

        start (Union[str, int]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
                                    E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
                                    epoch.

        end (Union[str, int]):      Get datapoints up to this time. Same format as for start.

        limit (int):                    Return up to this number of datapoints.

        api_key (str):                  Your api-key.

        project (str):                  Project name.

    Returns:
        list(DatapointsObject): A list of data objects containing the requested data with several getter methods
        with different output formats.
    '''
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/timeseries/dataquery'.format(project)
    body = {
        'items': [{'tagId': '{}'.format(tag_id)}
                  if isinstance(tag_id, str)
                  else {'tagId': '{}'.format(tag_id['tagId']), 'aggregates': tag_id['aggregates']} for tag_id in tag_ids],
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
    res = _utils.post_request(url=url, body=body, headers=headers)
    return [DatapointsObject({'data': {'items': [dp]}}) for dp in res.json()['data']['items']]

def get_datapoints_frame(tag_ids, aggregates, granularity, start=None, end=None, api_key=None, project=None):
    '''Returns a pandas dataframe of datapoints for the given tag_ids all on the same timestamps.

    This method will automate paging for the user and return all data for the given time period.

    Args:
        tag_ids (list):     The list of tag_ids to retrieve data for. Each tag_id can be either a string containing the
                            tag_id or a dictionary containing the tag_id and a list of specific aggregate functions.

        aggregates (list):  The list of aggregate functions you wish to apply to the data for which you have not
                            specified an aggregate function. Valid aggregate functions are: 'average/avg, max, min,
                            count, sum, interpolation/int, stepinterpolation/step'.

        granularity (str):  The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
                            second/s', or a multiple of these indicated by a number as a prefix e.g. '12hour'.

        start (Union[str, int]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
                                    E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
                                    epoch.

        end (Union[str, int]):      Get datapoints up to this time. Same format as for start.

        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        pandas.DataFrame: A pandas dataframe containing the datapoints for the given tag_ids. The datapoints for all the
        tag_ids will all be on the same timestamps.

    Note:
        The ``tag_ids`` parameter can take a list of strings and/or dicts on the following formats::

            Using strings:
                ['<tag_id1>', '<tag_id2>']

            Using dicts:
                [{'tagId': '<tag_id1>', 'aggregates': ['<aggfunc1>', '<aggfunc2>']},
                {'tagId': '<tag_id2>', 'aggregates': []}]

            Using both:
                ['<tagid1>', {'tagId': '<tag_id2>', 'aggregates': ['<aggfunc1>', '<aggfunc2>']}]
    '''
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/timeseries/dataframe'.format(project)
    num_aggregates = 0
    num_agg_per_tag = len(aggregates)
    for tag_id in tag_ids:
        if isinstance(tag_id, str):
            num_aggregates += num_agg_per_tag
        else:
            num_aggregates += len(tag_id['aggregates'])
    per_tag_limit = int(_constants.LIMIT / num_aggregates)
    body = {
        'items': [{'tagId': '{}'.format(tag_id)}
                  if isinstance(tag_id, str)
                  else {'tagId': '{}'.format(tag_id['tagId']), 'aggregates': tag_id['aggregates']} for tag_id in tag_ids],
        'aggregates': aggregates,
        'granularity': granularity,
        'start': start,
        'end': end,
        'limit': per_tag_limit
    }
    headers = {
        'api-key': api_key,
        'content-type': 'application/json',
        'accept': 'text/csv'
    }
    dataframes = []
    prog_ind = _utils.ProgressIndicator(tag_ids, start, end, api_key, project)
    while not dataframes or dataframes[-1].shape[0] == per_tag_limit:
        res = _utils.post_request(url=url, body=body, headers=headers)
        dataframes.append(
            pd.read_csv(io.StringIO(res.content.decode(res.encoding if res.encoding else res.apparent_encoding))))
        if dataframes[-1].empty and len(dataframes) == 1:
            prog_ind.terminate()
            return pd.DataFrame()
        latest_timestamp = int(dataframes[-1].iloc[-1, 0])
        prog_ind.update_progress(latest_timestamp)
        body['start'] = latest_timestamp + _utils.granularity_to_ms(granularity)
    prog_ind.terminate()
    return pd.concat(dataframes).reset_index(drop=True)
