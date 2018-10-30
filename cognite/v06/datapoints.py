# -*- coding: utf-8 -*-
"""Datapoints Module

This module mirrors the Datapoints API. It allows you to fetch data from the api and output it in various formats.

https://doc.cognitedata.com/0.6/#Cognite-API-Datapoints
"""
from concurrent.futures import ThreadPoolExecutor as Pool
from functools import partial

import cognite._constants as _constants
import cognite._utils as _utils
import cognite.config as config
from cognite.v05.dto import DatapointsResponse


def get_datapoints(id, start, end=None, aggregates=None, granularity=None, **kwargs):
    """Returns a DatapointsObject containing a list of datapoints for the given query.

    This method will automate paging for the user and return all data for the given time period.

    Args:
        id (int):             The unique id of the timeseries to retrieve data for.

        start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
                                    E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
                                    epoch or a datetime object which will be converted to ms since epoch UTC.

        end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.

        aggregates (list):      The list of aggregate functions you wish to apply to the data. Valid aggregate functions
                                are: 'average/avg, max, min, count, sum, interpolation/int, stepinterpolation/step'.

        granularity (str):      The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
                                second/s', or a multiple of these indicated by a number as a prefix e.g. '12hour'.

    Keyword Arguments:
        include_outside_points (bool):      No description

        processes (int):        Number of download processes to run in parallell. Defaults to number returned by cpu_count().

        api_key (str):          Your api-key.

        project (str):          Project name.

        limit (str):            Max number of datapoints to return. If limit is specified, this method will not automate
                                paging and will return a maximum of 100,000 dps.

    Returns:
        v05.dto.DatapointsResponse: A data object containing the requested data with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    start, end = _utils.interval_to_ms(start, end)

    if kwargs.get("limit"):
        return _get_datapoints_user_defined_limit(
            id,
            aggregates,
            granularity,
            start,
            end,
            limit=kwargs.get("limit"),
            include_outside_points=kwargs.get("include_outside_points", False),
            api_key=api_key,
            project=project,
        )

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
        id=id,
        aggregates=aggregates,
        granularity=granularity,
        include_outside_points=kwargs.get("include_outside_points", False),
        api_key=api_key,
        project=project,
    )

    with Pool(steps) as p:
        datapoints = p.map(partial_get_dps, args)

    concat_dps = []
    [concat_dps.extend(el) for el in datapoints]

    return DatapointsResponse({"data": {"items": [{"id": id, "datapoints": concat_dps}]}})


def _get_datapoints_helper_wrapper(args, id, aggregates, granularity, include_outside_points, api_key, project):
    return _get_datapoints_helper(
        id,
        aggregates,
        granularity,
        args["start"],
        args["end"],
        api_key=api_key,
        project=project,
        include_outside_points=include_outside_points,
    )


def _get_datapoints_helper(id, aggregates=None, granularity=None, start=None, end=None, **kwargs):
    """Returns a list of datapoints for the given query.

    This method will automate paging for the given time period.

    Args:
        id (int):       The unique id of the timeseries to retrieve data for.

        aggregates (list):      The list of aggregate functions you wish to apply to the data. Valid aggregate functions
                                are: 'average/avg, max, min, count, sum, interpolation/int, stepinterpolation/step'.

        granularity (str):      The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
                                second/s', or a multiple of these indicated by a number as a prefix e.g. '12hour'.

        start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
                                    E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
                                    epoch or a datetime object which will be converted to ms since epoch UTC.

        end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.

    Keyword Arguments:
        include_outside_points (bool):  No description.

        api_key (str):          Your api-key. Obligatory in this helper method.

        project (str):          Project name. Obligatory in this helper method.

    Returns:
        list of datapoints: A list containing datapoint dicts.
    """
    api_key, project = kwargs.get("api_key"), kwargs.get("project")
    url = config.get_base_url() + "/api/0.6/projects/{}/timeseries/{}/data".format(project, id)

    limit = _constants.LIMIT if aggregates is None else _constants.LIMIT_AGG

    params = {
        "aggregates": aggregates,
        "granularity": granularity,
        "limit": limit,
        "start": start,
        "end": end,
        "includeOutsidePoints": kwargs.get("include_outside_points", False),
    }

    headers = {"api-key": api_key, "accept": "application/json"}
    datapoints = []
    while (not datapoints or len(datapoints[-1]) == limit) and params["end"] > params["start"]:
        res = _utils.get_request(url, params=params, headers=headers)
        res = res.json()["data"]["items"][0]["datapoints"]

        if not res:
            break

        datapoints.append(res)
        latest_timestamp = int(datapoints[-1][-1]["timestamp"])
        params["start"] = latest_timestamp + (_utils.granularity_to_ms(granularity) if granularity else 1)
    dps = []
    [dps.extend(el) for el in datapoints]
    return dps


def _get_datapoints_user_defined_limit(id, aggregates, granularity, start, end, limit, **kwargs):
    """Returns a DatapointsResponse object with the requested data.

    No paging or parallelizing is done.

    Args:
        id (int):       The unique id of the timeseries to retrieve data for.

        aggregates (list):      The list of aggregate functions you wish to apply to the data. Valid aggregate functions
                                are: 'average/avg, max, min, count, sum, interpolation/int, stepinterpolation/step'.

        granularity (str):      The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
                                second/s', or a multiple of these indicated by a number as a prefix e.g. '12hour'.

        start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
                                    E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
                                    epoch or a datetime object which will be converted to ms since epoch UTC.

        end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.

        limit (str):            Max number of datapoints to return. Max is 100,000.

    Keyword Arguments:
        include_outside_points (bool):  No description.

        api_key (str):          Your api-key. Obligatory in this helper method.

        project (str):          Project name. Obligatory in this helper method.
    Returns:
        v05.dto.DatapointsResponse: A data object containing the requested data with several getter methods with different
        output formats.
    """
    api_key, project = kwargs.get("api_key"), kwargs.get("project")
    url = config.get_base_url() + "/api/0.6/projects/{}/timeseries/{}/data".format(project, id)

    params = {
        "aggregates": aggregates,
        "granularity": granularity,
        "limit": limit,
        "start": start,
        "end": end,
        "includeOutsidePoints": kwargs.get("include_outside_points", False),
    }

    headers = {"api-key": api_key, "accept": "application/json"}
    res = _utils.get_request(url, params=params, headers=headers)
    res = res.json()["data"]["items"][0]["datapoints"]

    return DatapointsResponse({"data": {"items": [{"id": id, "datapoints": res}]}})
