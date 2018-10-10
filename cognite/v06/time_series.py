# -*- coding: utf-8 -*-
"""Timeseries Module

This module mirrors the Timeseries API. It allows you to fetch data from the api and output it in various formats.

https://doc.cognitedata.com/0.6/#Cognite-API-Time-series
"""

from cognite import _utils, config
from cognite.v05.dto import TimeSeriesResponse


def get_time_series_by_id(id, include_metadata=False, **kwargs):
    """Returns a TimeseriesResponse object containing the requested timeseries.

    Args:
        id (int):           ID of timeseries to look up

        include_metadata (bool):    Decide if the metadata field should be returned or not. Defaults to False.

    Keyword Arguments:
        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        v05.dto.TimeSeriesResponse: A data object containing the requested timeseries.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/timeseries/{}".format(project, id)
    headers = {"api-key": api_key, "accept": "application/json"}
    params = {"includeMetadata": include_metadata}

    res = _utils.get_request(url=url, headers=headers, params=params, cookies=config.get_cookies())
    return TimeSeriesResponse(res.json())


def get_multiple_time_series_by_id(ids, include_metadata=False, **kwargs):
    """Returns a TimeseriesResponse object containing the requested timeseries.

    Args:
        ids (List[int]):           IDs of timeseries to look up

    Keyword Arguments:
        api_key (str):          Your api-key.

        project (str):          Project name.

    Returns:
        v05.dto.TimeSeriesResponse: A data object containing the requested timeseries with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/timeseries/byids".format(project)
    headers = {"api-key": api_key, "accept": "application/json", "content-type": "application/json"}
    body = {"items": ids}
    params = {"includeMetadata": include_metadata}
    res = _utils.post_request(url=url, body=body, params=params, headers=headers, cookies=config.get_cookies())
    return TimeSeriesResponse(res.json())
