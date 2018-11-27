# -*- coding: utf-8 -*-
"""Timeseries Module

This module mirrors the Timeseries API. It allows you to fetch data from the api and output it in various formats.

https://doc.cognitedata.com/0.4/#Cognite-API-Time-series
"""
import io
import os
import warnings
from concurrent.futures import ThreadPoolExecutor as Pool
from functools import partial
from typing import List
from urllib.parse import quote

import pandas as pd

import cognite._constants as _constants
import cognite._utils as _utils
import cognite.config as config
from cognite.auxiliary._protobuf_descriptors import _api_timeseries_data_v1_pb2
from cognite.v04.dto import (
    Datapoint,
    DatapointsResponse,
    DatapointsResponseIterator,
    LatestDatapointResponse,
    TimeSeries,
    TimeSeriesResponse,
    TimeseriesWithDatapoints,
)


def get_datapoints(tag_id, aggregates=None, granularity=None, start=None, end=None, **kwargs):
    """Returns a DatapointsObject containing a list of datapoints for the given query.

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
        v04.dto.DatapointsResponse: A data object containing the requested data with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    start, end = _utils.interval_to_ms(start, end)

    diff = end - start
    num_of_processes = kwargs.get("processes", _constants.NUM_OF_WORKERS)

    granularity_ms = 1
    if granularity:
        granularity_ms = _utils.granularity_to_ms(granularity)

    # Ensure that number of steps is not greater than the number data points that will be returned
    steps = min(num_of_processes, max(1, int(diff / granularity_ms)))
    # Make step size a multiple of the granularity requested in order to ensure evenly spaced results
    step_size = _utils.round_to_nearest(int(diff / steps), base=granularity_ms)
    # Create list of where each of the parallelized intervals will begin
    step_starts = [start + (i * step_size) for i in range(steps)]
    args = [{"start": start, "end": start + step_size} for start in step_starts]

    partial_get_dps = partial(
        _get_datapoints_helper_wrapper,
        tag_id=tag_id,
        aggregates=aggregates,
        granularity=granularity,
        protobuf=kwargs.get("protobuf", True),
        api_key=api_key,
        project=project,
    )

    if steps == 1:
        dps = _get_datapoints_helper(
            tag_id,
            aggregates,
            granularity,
            start,
            end,
            protobuf=kwargs.get("protobuf", True),
            api_key=api_key,
            project=project,
        )
        return DatapointsResponse({"data": {"items": [{"tagId": tag_id, "datapoints": dps}]}})

    with Pool(steps) as p:
        datapoints = p.map(partial_get_dps, args)

    concat_dps = []
    [concat_dps.extend(el) for el in datapoints]

    return DatapointsResponse({"data": {"items": [{"tagId": tag_id, "datapoints": concat_dps}]}})


def _get_datapoints_helper_wrapper(args, tag_id, aggregates, granularity, protobuf, api_key, project):
    return _get_datapoints_helper(
        tag_id, aggregates, granularity, args["start"], args["end"], protobuf=protobuf, api_key=api_key, project=project
    )


def _get_datapoints_helper(tag_id, aggregates=None, granularity=None, start=None, end=None, **kwargs):
    """Returns a list of datapoints for the given query.

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

        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        list of datapoints: A list containing datapoint dicts.
    """
    api_key, project = kwargs.get("api_key"), kwargs.get("project")
    url = config.get_base_url() + "/api/0.4/projects/{}/timeseries/data/{}".format(project, quote(tag_id, safe=""))

    use_protobuf = kwargs.get("protobuf", True) and aggregates is None
    limit = _constants.LIMIT if aggregates is None else _constants.LIMIT_AGG

    params = {"aggregates": aggregates, "granularity": granularity, "limit": limit, "start": start, "end": end}

    headers = {"api-key": api_key, "accept": "application/protobuf" if use_protobuf else "application/json"}
    datapoints = []
    while (not datapoints or len(datapoints[-1]) == limit) and params["end"] > params["start"]:
        res = _utils.get_request(url, params=params, headers=headers)
        if use_protobuf:
            ts_data = _api_timeseries_data_v1_pb2.TimeseriesData()
            ts_data.ParseFromString(res.content)
            res = [{"timestamp": p.timestamp, "value": p.value} for p in ts_data.numericData.points]
        else:
            res = res.json()["data"]["items"][0]["datapoints"]

        if not res:
            warning = "An interval with no data has been requested ({}, {}).".format(params["start"], params["end"])
            warnings.warn(warning)
            break

        datapoints.append(res)
        latest_timestamp = int(datapoints[-1][-1]["timestamp"])
        params["start"] = latest_timestamp + (_utils.granularity_to_ms(granularity) if granularity else 1)
    dps = []
    [dps.extend(el) for el in datapoints]
    return dps


def _split_TimeseriesWithDatapoints_if_over_limit(
    timeseries_with_datapoints: TimeseriesWithDatapoints, limit: int
) -> List[TimeseriesWithDatapoints]:
    """Takes a TimeseriesWithDatapoints and splits it into multiple so that each has a max number of datapoints equal
    to the limit given.

    Args:
        timeseries_with_datapoints (TimeseriesWithDatapoints): The timeseries with data to potentially split up.

    Returns:
        A list of TimeSeriesWithDatapoints where each has a maximum number of datapoints equal to the limit given.
    """
    timeseries_with_datapoints_list = []
    if len(timeseries_with_datapoints.datapoints) > limit:
        i = 0
        while i < len(timeseries_with_datapoints.datapoints):
            timeseries_with_datapoints_list.append(
                TimeseriesWithDatapoints(
                    tagId=timeseries_with_datapoints.tagId,
                    datapoints=timeseries_with_datapoints.datapoints[i : i + limit],
                )
            )
            i += limit
    else:
        timeseries_with_datapoints_list.append(timeseries_with_datapoints)

    return timeseries_with_datapoints_list


def post_multi_tag_datapoints(timeseries_with_datapoints: List[TimeseriesWithDatapoints], **kwargs):
    """Insert data into multiple timeseries.

    Args:
        timeseries_with_datapoints (List[v04.dto.TimeseriesWithDatapoints]): The timeseries with data to insert.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        An empty response.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.4/projects/{}/timeseries/data".format(project)

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}

    ul_dps_limit = 100000

    # Make sure we only work with TimeseriesWithDatapoints objects that has a max number of datapoints
    timeseries_with_datapoints_limited = []
    for entry in timeseries_with_datapoints:
        timeseries_with_datapoints_limited.extend(_split_TimeseriesWithDatapoints_if_over_limit(entry, ul_dps_limit))

    # Group these TimeseriesWithDatapoints if possible so that we upload as much as possible in each call to the API
    timeseries_to_upload_binned = _utils.first_fit(
        list_items=timeseries_with_datapoints_limited, max_size=ul_dps_limit, get_count=lambda x: len(x.datapoints)
    )

    for bin in timeseries_to_upload_binned:
        body = {
            "items": [
                {"tagId": ts_with_data.tagId, "datapoints": [dp.__dict__ for dp in ts_with_data.datapoints]}
                for ts_with_data in bin
            ]
        }
        res = _utils.post_request(url, body=body, headers=headers)

    return res.json()


def post_datapoints(tag_id, datapoints: List[Datapoint], **kwargs):
    """Insert a list of datapoints.

    Args:
        tag_id (str):       ID of timeseries to insert to.

        datapoints (list[v04.dto.Datapoint): List of datapoint data transfer objects to insert.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        An empty response.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.4/projects/{}/timeseries/data/{}".format(project, quote(tag_id, safe=""))

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}

    ul_dps_limit = 100000
    i = 0
    while i < len(datapoints):
        body = {"items": [dp.__dict__ for dp in datapoints[i : i + ul_dps_limit]]}
        res = _utils.post_request(url, body=body, headers=headers)
        i += ul_dps_limit
    return res.json()


def get_latest(tag_id, **kwargs):
    """Returns a LatestDatapointObject containing the latest datapoint for the given tag_id.

    Args:
        tag_id (str):           The tag_id to retrieve data for.

    Keyword Arguments:
        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        v04.dto.LatestDatapointsResponse: A data object containing the requested data with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.4/projects/{}/timeseries/latest/{}".format(project, quote(tag_id, safe=""))
    headers = {"api-key": api_key, "accept": "application/json"}
    res = _utils.get_request(url, headers=headers, cookies=config.get_cookies())
    return LatestDatapointResponse(res.json())


def get_multi_tag_datapoints(datapoints_queries, aggregates=None, granularity=None, start=None, end=None, **kwargs):
    """Returns a list of DatapointsObjects each of which contains a list of datapoints for the given timeseries.

    This method will automate paging for the user and return all data for the given time period(s).

    Args:
        datapoints_queries (list[v04.dto.DatapointsQuery]): The list of DatapointsQuery objects specifying which
                                                                    timeseries to retrieve data for.

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

    Keyword Arguments:
        api_key (str):                  Your api-key.

        project (str):                  Project name.

    Returns:
        list(v04.dto.DatapointsResponse): A list of data objects containing the requested data with several getter methods
        with different output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.4/projects/{}/timeseries/dataquery".format(project)
    start, end = _utils.interval_to_ms(start, end)

    num_of_dpqs_with_agg = 0
    num_of_dpqs_raw = 0
    for dpq in datapoints_queries:
        if (dpq.aggregateFunctions is None and aggregates is None) or dpq.aggregateFunctions == "":
            num_of_dpqs_raw += 1
        else:
            num_of_dpqs_with_agg += 1

    items = []
    for dpq in datapoints_queries:
        if dpq.aggregateFunctions is None and aggregates is None:
            dpq.limit = int(_constants.LIMIT / num_of_dpqs_raw)
        else:
            dpq.limit = int(_constants.LIMIT_AGG / num_of_dpqs_with_agg)
        items.append(dpq.__dict__)
    body = {
        "items": items,
        "aggregateFunctions": ",".join(aggregates) if aggregates is not None else None,
        "granularity": granularity,
        "start": start,
        "end": end,
    }
    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}
    datapoints_responses = []
    has_incomplete_requests = True
    while has_incomplete_requests:
        res = _utils.post_request(url=url, body=body, headers=headers, cookies=config.get_cookies()).json()["data"][
            "items"
        ]
        datapoints_responses.append(res)
        has_incomplete_requests = False
        for i, dpr in enumerate(res):
            dpq = datapoints_queries[i]
            if len(dpr["datapoints"]) == dpq.limit:
                has_incomplete_requests = True
                latest_timestamp = dpr["datapoints"][-1]["timestamp"]
                ts_granularity = granularity if dpq.granularity is None else dpq.granularity
                next_start = latest_timestamp + (_utils.granularity_to_ms(ts_granularity) if ts_granularity else 1)
            else:
                next_start = end - 1
                if datapoints_queries[i].end:
                    next_start = datapoints_queries[i].end - 1

            datapoints_queries[i].start = next_start

    results = [{"data": {"items": [{"tagId": dpq.tagId, "datapoints": []}]}} for dpq in datapoints_queries]
    for res in datapoints_responses:
        for i, ts in enumerate(res):
            results[i]["data"]["items"][0]["datapoints"].extend(ts["datapoints"])
    return DatapointsResponseIterator([DatapointsResponse(result) for result in results])


def get_datapoints_frame(tag_ids, aggregates, granularity, start=None, end=None, **kwargs):
    """Returns a pandas dataframe of datapoints for the given tag_ids all on the same timestamps.

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
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))

    start, end = _utils.interval_to_ms(start, end)

    diff = end - start
    num_of_processes = kwargs.get("processes", _constants.NUM_OF_WORKERS)

    granularity_ms = 1
    if granularity:
        granularity_ms = _utils.granularity_to_ms(granularity)

    # Ensure that number of steps is not greater than the number data points that will be returned
    steps = min(num_of_processes, max(1, int(diff / granularity_ms)))
    # Make step size a multiple of the granularity requested in order to ensure evenly spaced results
    step_size = _utils.round_to_nearest(int(diff / steps), base=granularity_ms)
    # Create list of where each of the parallelized intervals will begin
    step_starts = [start + (i * step_size) for i in range(steps)]
    args = [{"start": start, "end": start + step_size} for start in step_starts]

    partial_get_dpsf = partial(
        _get_datapoints_frame_helper_wrapper,
        tag_ids=tag_ids,
        aggregates=aggregates,
        granularity=granularity,
        api_key=api_key,
        project=project,
    )

    if steps == 1:
        return _get_datapoints_frame_helper(
            tag_ids, aggregates, granularity, start, end, api_key=api_key, project=project
        )

    with Pool(steps) as p:
        dataframes = p.map(partial_get_dpsf, args)

    df = pd.concat(dataframes).drop_duplicates(subset="timestamp").reset_index(drop=True)

    return df


def _get_datapoints_frame_helper_wrapper(args, tag_ids, aggregates, granularity, api_key, project):
    return _get_datapoints_frame_helper(
        tag_ids, aggregates, granularity, args["start"], args["end"], api_key=api_key, project=project
    )


def _get_datapoints_frame_helper(tag_ids, aggregates, granularity, start=None, end=None, **kwargs):
    """Returns a pandas dataframe of datapoints for the given tag_ids all on the same timestamps.

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
    """
    api_key, project = kwargs.get("api_key"), kwargs.get("project")
    url = config.get_base_url() + "/api/0.4/projects/{}/timeseries/dataframe".format(project)

    num_aggregates = 0
    for tag in tag_ids:
        if isinstance(tag, str) or tag.get("aggregates") is None:
            num_aggregates += len(aggregates)
        else:
            num_aggregates += len(tag["aggregates"])
    per_tag_limit = int(_constants.LIMIT / num_aggregates)
    body = {
        "items": [
            {"tagId": "{}".format(tag_id)}
            if isinstance(tag_id, str)
            else {"tagId": "{}".format(tag_id["tagId"]), "aggregates": tag_id.get("aggregates", [])}
            for tag_id in tag_ids
        ],
        "aggregates": aggregates,
        "granularity": granularity,
        "start": start,
        "end": end,
        "limit": per_tag_limit,
    }
    headers = {"api-key": api_key, "content-type": "application/json", "accept": "text/csv"}
    dataframes = []
    while (not dataframes or dataframes[-1].shape[0] == per_tag_limit) and body["end"] > body["start"]:
        res = _utils.post_request(url=url, body=body, headers=headers, cookies=config.get_cookies())
        dataframes.append(
            pd.read_csv(io.StringIO(res.content.decode(res.encoding if res.encoding else res.apparent_encoding)))
        )
        if dataframes[-1].empty:
            warning = "An interval with no data has been requested ({}, {}).".format(body["start"], body["end"])
            warnings.warn(warning)
            break
        latest_timestamp = int(dataframes[-1].iloc[-1, 0])
        body["start"] = latest_timestamp + _utils.granularity_to_ms(granularity)
    return pd.concat(dataframes).reset_index(drop=True)


def get_timeseries(prefix=None, description=None, include_metadata=False, asset_id=None, path=None, **kwargs):
    """Returns a TimeseriesObject containing the requested timeseries.

    Args:
        prefix (str):           List timeseries with this prefix in the name.

        description (str):      Filter timeseries taht contains this string in its description.

        include_metadata (bool):    Decide if the metadata field should be returned or not. Defaults to False.

        asset_id (int):        Get timeseries related to this asset.

        path (str):             Get timeseries under this asset path branch.

    Keyword Arguments:
        limit (int):            Number of results to return.

        api_key (str):          Your api-key.

        project (str):          Project name.

        autopaging (bool):      Whether or not to automatically page through results. If set to true, limit will be
                                disregarded. Defaults to False.

    Returns:
        v04.dto.TimeSeriesResponse: A data object containing the requested timeseries with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.4/projects/{}/timeseries".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    params = {
        "q": prefix,
        "description": description,
        "includeMetadata": include_metadata,
        "assetId": asset_id,
        "path": path,
        "limit": kwargs.get("limit", 10000) if not kwargs.get("autopaging") else 10000,
    }

    timeseries = []
    res = _utils.get_request(url=url, headers=headers, params=params, cookies=config.get_cookies())
    timeseries.extend(res.json()["data"]["items"])
    next_cursor = res.json()["data"].get("nextCursor")

    while next_cursor and kwargs.get("autopaging"):
        params["cursor"] = next_cursor
        res = _utils.get_request(url=url, headers=headers, params=params, cookies=config.get_cookies())
        timeseries.extend(res.json()["data"]["items"])
        next_cursor = res.json()["data"].get("nextCursor")

    return TimeSeriesResponse(
        {
            "data": {
                "nextCursor": next_cursor,
                "previousCursor": res.json()["data"].get("previousCursor"),
                "items": timeseries,
            }
        }
    )


def post_time_series(time_series: List[TimeSeries], **kwargs):
    """Create a new time series.

    Args:
        timeseries (list[v04.dto.TimeSeries]):   List of time series data transfer objects to create.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.
    Returns:
        An empty response.
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.4/projects/{}/timeseries".format(project)

    body = {"items": [ts.__dict__ for ts in time_series]}

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}

    res = _utils.post_request(url, body=body, headers=headers)
    return res.json()


def update_time_series(time_series: List[TimeSeries], **kwargs):
    """Update an existing time series.

    For each field that can be updated, a null value indicates that nothing should be done.

    Args:
        timeseries (list[v04.dto.TimeSeries]):   List of time series data transfer objects to update.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        An empty response.
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.4/projects/{}/timeseries".format(project)

    body = {"items": [ts.__dict__ for ts in time_series]}

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}

    res = _utils.put_request(url, body=body, headers=headers)
    return res.json()
