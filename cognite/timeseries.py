# -*- coding: utf-8 -*-
"""Timeseries Module

This module mirrors the Timeseries API. It allows you to fetch data from the api and output it in various formats.
"""
import io
import os
import warnings
from datetime import datetime
from functools import partial
from multiprocessing import Pool
from typing import List

import pandas as pd

import cognite._constants as _constants
import cognite._utils as _utils
import cognite.config as config
from cognite._protobuf_descriptors import _api_timeseries_data_v1_pb2
from cognite.data_objects import DatapointsObject, LatestDatapointObject, TimeseriesObject, TimeSeriesDTO, DatapointDTO


def get_timeseries(prefix=None, description=None, include_metadata=False, asset_id=None, path=None, **kwargs):
    '''Returns a TimeseriesObject containing the requested timeseries.

    This method will automate paging for the user and return info about all files for the given query. If limit is
    specified the method will not page and return that many results.

    Args:
        prefix (str):           List timeseries with this prefix in the name.

        description (str):      Filter timeseries taht contains this string in its description.

        include_metadata (bool):    Decide if teh metadata field should be returned or not. Defaults to False.

        asset_id (int):        Get timeseries related to this asset.

        path (str):             Get timeseries under this asset path branch.

    Keyword Arguments:
        limit (int):            Number of results to return.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        TimeseriesObject: A data object containing the requested timeseries with several getter methods with different
        output formats.
    '''
    api_key, project = config.get_config_variables(kwargs.get('api_key'), kwargs.get('project'))
    url = config.get_base_url() + '/projects/{}/timeseries'.format(project)
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    params = {
        'q': prefix,
        'description': description,
        'includeMetadata': include_metadata,
        'assetId': asset_id,
        'path': path,
        'limit': kwargs.get('limit', 10000)
    }
    timeseries = []

    res = _utils.get_request(url=url, headers=headers, params=params, cookies=config.get_cookies())
    timeseries.extend(res.json()['data']['items'])
    next_cursor = res.json()['data'].get('nextCursor', None)
    limit = kwargs.get('limit', float('inf'))

    while next_cursor is not None and len(timeseries) < limit:
        params['cursor'] = next_cursor
        res = _utils.get_request(url=url, headers=headers, params=params, cookies=config.get_cookies())
        timeseries.extend(res.json()['data']['items'])
        next_cursor = res.json()['data'].get('nextCursor', None)

    return TimeseriesObject(timeseries)


def get_datapoints(tag_id, aggregates=None, granularity=None, start=None, end=None, **kwargs):
    '''Returns a DatapointsObject containing a list of datapoints for the given query.

    This method will automate paging for the user and return all data for the given time period.

    Args:
        tag_id (str):           The tag_id to retrieve data for.

        aggregates (list):      The list of aggregate functions you wish to apply to the data. Valid aggregate functions
                                are: 'average/avg, max, min, count, sum, interpolation/int, stepinterpolation/step'.

        granularity (str):      The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
                                second/s', or a multiple of these indicated by a number as a prefix e.g. '12hour'.

        start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
                                    E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
                                    epoch or a datetime object which will be converted to ms since epoch UTC.

        end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.

    Keyword Arguments:
        protobuf (bool):        Download the data using the binary protobuf format. Only applicable when getting raw data.
                                Defaults to True.

        processes (int):        Number of download processes to run in parallell. Defaults to number returned by cpu_count().

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        DatapointsObject: A data object containing the requested data with several getter methods with different
        output formats.
    '''
    api_key, project = config.get_config_variables(kwargs.get('api_key'), kwargs.get('project'))
    start, end = _utils.interval_to_ms(start, end)

    diff = end - start
    num_of_processes = kwargs.get('processes', os.cpu_count())

    granularity_ms = 1
    if granularity:
        granularity_ms = _utils.granularity_to_ms(granularity)

    # Ensure that number of steps is not greater than the number data points that will be returned
    steps = min(num_of_processes, max(1, int(diff / granularity_ms)))
    # Make step size a multiple of the granularity requested in order to ensure evenly spaced results
    step_size = _utils.round_to_nearest(int(diff / steps), base=granularity_ms)
    # Create list of where each of the parallelized intervals will begin
    step_starts = [start + (i * step_size) for i in range(steps)]
    args = [{'start': start, 'end': start + step_size, 'display_progress': False} for start in step_starts]

    partial_get_dps = partial(
        _get_datapoints_helper_wrapper,
        tag_id=tag_id,
        aggregates=aggregates,
        granularity=granularity,
        protobuf=True,
        api_key=api_key,
        project=project
    )

    display_progress = len(args) <= 2
    if display_progress:
        args[-1]['display_progress'] = True
    else:
        prog_ind = _utils.ProgressIndicator([tag_id], start, end, display_progress=False)

    p = Pool(steps)

    datapoints = p.map(partial_get_dps, args)
    concat_dps = []
    [concat_dps.extend(el) for el in datapoints]

    if not display_progress:
        prog_ind.terminate()

    return DatapointsObject({'data': {'items': [{'tagId': tag_id, 'datapoints': concat_dps}]}})


def _get_datapoints_helper_wrapper(args, tag_id, aggregates, granularity, protobuf, api_key, project):
    return _get_datapoints_helper(
        tag_id,
        aggregates,
        granularity,
        args['start'],
        args['end'],
        protobuf=protobuf,
        api_key=api_key,
        project=project,
        display_progress=args['display_progress']
    )


def _get_datapoints_helper(tag_id, aggregates=None, granularity=None, start=None, end=None, **kwargs):
    '''Returns a list of datapoints for the given query.

    This method will automate paging for the given time period.

    Args:
        tag_id (str):           The tag_id to retrieve data for.

        aggregates (list):      The list of aggregate functions you wish to apply to the data. Valid aggregate functions
                                are: 'average/avg, max, min, count, sum, interpolation/int, stepinterpolation/step'.

        granularity (str):      The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
                                second/s', or a multiple of these indicated by a number as a prefix e.g. '12hour'.

        start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
                                    E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
                                    epoch or a datetime object which will be converted to ms since epoch UTC.

        end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.

    Keyword Arguments:
        protobuf (bool):        Download the data using the binary protobuf format. Only applicable when getting raw data.
                                Defaults to True.

        display_progress (bool):   Whether or not to display progress indicator. Defaults to True.

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        list of datapoints: A list containing datapoint dicts.
    '''
    api_key, project = kwargs.get('api_key'), kwargs.get('project')
    tag_id = tag_id.replace('/', '%2F')
    url = config.get_base_url() + '/projects/{}/timeseries/data/{}'.format(project, tag_id)

    use_protobuf = kwargs.get('protobuf', True) and aggregates is None
    limit = _constants.LIMIT if aggregates is None else _constants.LIMIT_AGG

    params = {
        'aggregates': aggregates,
        'granularity': granularity,
        'limit': limit,
        'start': start,
        'end': end,
    }

    headers = {
        'api-key': api_key,
        'accept': 'application/protobuf' if use_protobuf else 'application/json'
    }
    display_progress = kwargs.get('display_progress', True)
    prog_ind = _utils.ProgressIndicator([tag_id], start, end, api_key, project, display_progress=display_progress)
    datapoints = []
    while not datapoints or len(datapoints[-1]) == limit:
        res = _utils.get_request(url, params=params, headers=headers)
        if use_protobuf:
            ts_data = _api_timeseries_data_v1_pb2.TimeseriesData()
            ts_data.ParseFromString(res.content)
            res = [{'timestamp': p.timestamp, 'value': p.value} for p in ts_data.numericData.points]
        else:
            res = res.json()['data']['items'][0]['datapoints']

        if not res:
            warning = "An interval with no data has been requested ({}, {})." \
                .format(params['start'], params['end'])
            warnings.warn(warning)
            break

        datapoints.append(res)
        latest_timestamp = int(datapoints[-1][-1]['timestamp'])
        prog_ind.update_progress(latest_timestamp)
        params['start'] = latest_timestamp + (_utils.granularity_to_ms(granularity) if granularity else 1)
    if display_progress:
        prog_ind.terminate()
    dps = []
    [dps.extend(el) for el in datapoints]
    return dps


def get_latest(tag_id, **kwargs):
    '''Returns a LatestDatapointObject containing the latest datapoint for the given tag_id.

    Args:
        tag_id (str):           The tag_id to retrieve data for.

    Keyword Arguments:
        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        DatapointsObject: A data object containing the requested data with several getter methods with different
        output formats.
    '''
    api_key, project = config.get_config_variables(kwargs.get('api_key'), kwargs.get('project'))
    tag_id = tag_id.replace('/', '%2F')
    url = config.get_base_url() + '/projects/{}/timeseries/latest/{}'.format(project, tag_id)
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    res = _utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return LatestDatapointObject(res.json())


def get_multi_tag_datapoints(tag_ids, aggregates=None, granularity=None, start=None, end=None, limit=None, **kwargs):
    '''Returns a list of DatapointsObjects each of which contains a list of datapoints for the given tag_id.

    Args:
        tag_ids (list):                 The list of tag_ids to retrieve data for.

        aggregates (list, optional):    The list of aggregate functions you wish to apply to the data. Valid aggregate
                                        functions are: 'average/avg, max, min, count, sum, interpolation/int,
                                        stepinterpolation/step'.

        granularity (str):              The granularity of the aggregate values. Valid entries are : 'day/d, hour/h,
                                        minute/m, second/s', or a multiple of these indicated by a number as a prefix
                                        e.g. '12hour'.

        start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
                                    E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
                                    epoch or a datetime object which will be converted to ms since epoch UTC.

        end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.

        limit (int):                    Return up to this number of datapoints.

    Keyword Arguments:
        api_key (str):                  Your api-key.

        project (str):                  Project name.

    Returns:
        list(DatapointsObject): A list of data objects containing the requested data with several getter methods
        with different output formats.
    '''
    api_key, project = config.get_config_variables(kwargs.get('api_key'), kwargs.get('project'))
    url = config.get_base_url() + '/projects/{}/timeseries/dataquery'.format(project)

    if isinstance(start, datetime):
        start = _utils.datetime_to_ms(start)
    if isinstance(end, datetime):
        end = _utils.datetime_to_ms(end)

    if limit is None:
        limit = _constants.LIMIT if aggregates is not None else _constants.LIMIT_AGG

    body = {
        'items': [{'tagId': '{}'.format(tag_id)}
                  if isinstance(tag_id, str)
                  else {'tagId': '{}'.format(tag_id['tagId']), 'aggregates': tag_id.get('aggregates', [])} for tag_id in
                  tag_ids],
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
    res = _utils.post_request(url=url, body=body, headers=headers, cookies=config.get_cookies())
    return [DatapointsObject({'data': {'items': [dp]}}) for dp in res.json()['data']['items']]


def get_datapoints_frame(tag_ids, aggregates, granularity, start=None, end=None, **kwargs):
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

        start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
                                    E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
                                    epoch or a datetime object which will be converted to ms since epoch UTC.

        end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.

    Keyword Arguments:
        api_key (str): Your api-key.

        project (str): Project name.

        processes (int):    Number of download processes to run in parallell. Defaults to number returned by cpu_count().

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
    api_key, project = config.get_config_variables(kwargs.get('api_key'), kwargs.get('project'))

    start, end = _utils.interval_to_ms(start, end)

    diff = end - start
    num_of_processes = kwargs.get('processes', os.cpu_count())

    granularity_ms = 1
    if granularity:
        granularity_ms = _utils.granularity_to_ms(granularity)

    # Ensure that number of steps is not greater than the number data points that will be returned
    steps = min(num_of_processes, max(1, int(diff / granularity_ms)))
    # Make step size a multiple of the granularity requested in order to ensure evenly spaced results
    step_size = _utils.round_to_nearest(int(diff / steps), base=granularity_ms)
    # Create list of where each of the parallelized intervals will begin
    step_starts = [start + (i * step_size) for i in range(steps)]
    args = [{'start': start, 'end': start + step_size, 'display_progress': False} for start in step_starts]

    partial_get_dpsf = partial(
        _get_datapoints_frame_helper_wrapper,
        tag_ids=tag_ids,
        aggregates=aggregates,
        granularity=granularity,
        api_key=api_key,
        project=project
    )

    display_progress = len(args) <= 2
    if display_progress:
        args[-1]['display_progress'] = True
    else:
        prog_ind = _utils.ProgressIndicator(tag_ids, start, end, display_progress=False)
    p = Pool(steps)

    dataframes = p.map(partial_get_dpsf, args)
    df = pd.concat(dataframes).drop_duplicates(subset='timestamp').reset_index(drop=True)

    if not display_progress:
        prog_ind.terminate()

    return df


def _get_datapoints_frame_helper_wrapper(args, tag_ids, aggregates, granularity, api_key, project):
    return _get_datapoints_frame_helper(
        tag_ids,
        aggregates,
        granularity,
        args['start'],
        args['end'],
        api_key=api_key,
        project=project,
        display_progress=args['display_progress']
    )


def _get_datapoints_frame_helper(tag_ids, aggregates, granularity, start=None, end=None, **kwargs):
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

        start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
                                    E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
                                    epoch or a datetime object which will be converted to ms since epoch UTC.

        end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.

    Keyword Arguments:
        api_key (str): Your api-key.

        project (str): Project name.

        display_progress (bool): Whether or not to display progress indicator.

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
    api_key, project = kwargs.get('api_key'), kwargs.get('project')
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
                  else {'tagId': '{}'.format(tag_id['tagId']), 'aggregates': tag_id.get('aggregates', [])} for tag_id in
                  tag_ids],
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
    display_progress = kwargs.get('display_progress', True)
    prog_ind = _utils.ProgressIndicator(tag_ids, start, end, api_key, project,
                                        display_progress=display_progress)
    while not dataframes or dataframes[-1].shape[0] == per_tag_limit:
        res = _utils.post_request(url=url, body=body, headers=headers, cookies=config.get_cookies())
        dataframes.append(
            pd.read_csv(io.StringIO(res.content.decode(res.encoding if res.encoding else res.apparent_encoding))))
        if dataframes[-1].empty:
            warning = "An interval with no data has been requested ({}, {})." \
                .format(body['start'], body['end'])
            warnings.warn(warning)
            break
        latest_timestamp = int(dataframes[-1].iloc[-1, 0])
        prog_ind.update_progress(latest_timestamp)
        body['start'] = latest_timestamp + _utils.granularity_to_ms(granularity)
    if display_progress:
        prog_ind.terminate()
    return pd.concat(dataframes).reset_index(drop=True)


def post_time_series(time_series: List[TimeSeriesDTO], **kwargs):
    '''Create a new time series.

    Args:
        timeseries (list[TimeSeriesDTO]):   List of time series data transfer objects to create.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.
    Returns:
        An empty response.
    '''

    api_key, project = config.get_config_variables(kwargs.get('api_key'), kwargs.get('project'))
    url = config.get_base_url() + '/projects/{}/timeseries'.format(project)

    body = {
        'items': [ts.__dict__ for ts in time_series]
    }

    headers = {
        'api-key': api_key,
        'content-type': 'application/json',
        'accept': 'application/json'
    }

    res = _utils.post_request(url, body=body, headers=headers)
    return res.json()


def update_time_series(time_series: List[TimeSeriesDTO], **kwargs):
    '''Update an existing time series.

    For each field that can be updated, a null value indicates that nothing should be done.

    Args:
        timeseries (list[TimeSeriesDTO]):   List of time series data transfer objects to create.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        An empty response.
    '''

    api_key, project = config.get_config_variables(kwargs.get('api_key'), kwargs.get('project'))
    url = config.get_base_url() + '/projects/{}/timeseries'.format(project)

    body = {
        'items': [ts.__dict__ for ts in time_series]
    }

    headers = {
        'api-key': api_key,
        'content-type': 'application/json',
        'accept': 'application/json'
    }

    res = _utils.put_request(url, body=body, headers=headers)
    return res.json()


def post_datapoints(tag_id, datapoints: List[DatapointDTO], **kwargs):
    '''Insert a list of datapoints.

    Args:
        tag_id (str):       ID of timeseries to insert to.

        datapoints (list[DatapointDTO): List of datapoint data transfer objects to insert.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        An empty response.
    '''
    api_key, project = config.get_config_variables(kwargs.get('api_key'), kwargs.get('project'))
    tag_id = tag_id.replace('/', '%2F')
    url = config.get_base_url() + '/projects/{}/timeseries/data/{}'.format(project, tag_id)

    body = {
        'items': [dp.__dict__ for dp in datapoints]
    }

    headers = {
        'api-key': api_key,
        'content-type': 'application/json',
        'accept': 'application/json'
    }

    res = _utils.post_request(url, body=body, headers=headers)
    return res.json()
