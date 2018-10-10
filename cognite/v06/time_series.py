# -*- coding: utf-8 -*-
"""Timeseries Module

This module mirrors the Timeseries API. It allows you to fetch data from the api and output it in various formats.

https://doc.cognitedata.com/0.6/#Cognite-API-Time-series
"""
import time
from typing import List
from urllib.parse import quote

import cognite._utils as _utils
import cognite.config as config
from cognite.v05.dto import TimeSeries, TimeSeriesResponse


def get_time_series(name=None, description=None, include_metadata=False, asset_id=None, path=None, **kwargs):
    """Returns a TimeseriesObject containing the requested timeseries.

    Args:
        name (str):           List timeseries with this name.

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
        v05.dto.TimeSeriesResponse: A data object containing the requested timeseries with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/timeseries".format(project)
    headers = {"api-key": api_key, "accept": "application/json"}
    params = {
        "name": name,
        "description": description,
        "includeMetadata": include_metadata,
        "assetId": asset_id,
        "path": path,
        "limit": kwargs.get("limit", 10000) if not kwargs.get("autopaging") else 10000,
    }

    time_series = []
    res = _utils.get_request(url=url, headers=headers, params=params, cookies=config.get_cookies())
    time_series.extend(res.json()["data"]["items"])
    next_cursor = res.json()["data"].get("nextCursor")

    while next_cursor and kwargs.get("autopaging"):
        params["cursor"] = next_cursor
        res = _utils.get_request(url=url, headers=headers, params=params, cookies=config.get_cookies())
        time_series.extend(res.json()["data"]["items"])
        next_cursor = res.json()["data"].get("nextCursor")

    return TimeSeriesResponse(
        {
            "data": {
                "nextCursor": next_cursor,
                "previousCursor": res.json()["data"].get("previousCursor"),
                "items": time_series,
            }
        }
    )


def post_time_series(time_series: List[TimeSeries], **kwargs):
    """Create a new time series.

    Args:
        time_series (list[v05.dto.TimeSeries]):   List of time series data transfer objects to create.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.
    Returns:
        v05.dto.TimeSeriesResponse: A data object containting the posted time series.
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/timeseries".format(project)

    body = {"items": [ts.__dict__ for ts in time_series]}

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}

    res = _utils.post_request(url, body=body, headers=headers)
    return TimeSeriesResponse(res.json())


def update_time_series(time_series: List[TimeSeries], **kwargs):
    """Update an existing time series.

    For each field that can be updated, a null value indicates that nothing should be done.

    Args:
        time_series (list[v05.dto.TimeSeries]):   List of time series data transfer objects to update.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        An empty response.
    """

    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/timeseries".format(project)

    body = {"items": [ts.__dict__ for ts in time_series]}

    headers = {"api-key": api_key, "content-type": "application/json", "accept": "application/json"}

    res = _utils.put_request(url, body=body, headers=headers)
    return res.json()


def delete_time_series(name, **kwargs):
    """Delete a timeseries.

    Args:
        name (str):   Name of timeseries to delete.

    Keyword Args:
        api_key (str): Your api-key.

        project (str): Project name.

    Returns:
        An empty response.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/timeseries/{}".format(project, quote(name, safe=""))

    headers = {"api-key": api_key, "accept": "application/json"}

    res = _utils.delete_request(url, headers=headers)
    return res.json()


def live_data_generator(name, update_frequency=1, **kwargs):
    """Generator function which continously polls latest datapoint of a timeseries and yields new datapoints.

    Args:
        name (str): Name of timeseries to get latest datapoints for.

        update_frequency (float): Frequency to pull for data in seconds.

    Keyword Args:

        api_key (str): Your api-key.

        project (str): Project name.

    Yields:
        dict: Dictionary containing timestamp and value of latest datapoint.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    last_timestamp = get_latest(name, api_key=api_key, project=project).to_json()["timestamp"]
    while True:
        latest = get_latest(name, api_key=api_key, project=project).to_json()
        if last_timestamp == latest["timestamp"]:
            time.sleep(update_frequency)
        else:
            yield latest
        last_timestamp = latest["timestamp"]
