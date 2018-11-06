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


def search_for_time_series(
    name=None,
    description=None,
    query=None,
    unit=None,
    is_string=None,
    is_step=None,
    metadata=None,
    asset_ids=None,
    asset_subtrees=None,
    min_created_time=None,
    max_created_time=None,
    min_last_updated_time=None,
    max_last_updated_time=None,
    **kwargs
):
    """Returns a TimeSeriesResponse object containing the search results.

    Args:
        name (str): Prefix and fuzzy search on name.
        description (str):  Prefix and fuzzy search on description.
        query (str):    Search on name and description using wildcard search on each of the words (separated by spaces).
                        Retrieves results where at least on word must match. Example: "some other"
        unit (str): Filter on unit (case-sensitive)
        is_string (bool): Filter on whether the ts is a string ts or not.
        is_step (bool): Filter on whether the ts is a step ts or not.
        metadata (Dict):    Filter out time series that do not match these metadata fields and values (case-sensitive).
                            Format is {"key1": "val1", "key2", "val2"}
        asset_ids (List): Filter out time series that are not linked to any of these assets. Format is [12,345,6,7890].
        asset_subtrees (List):  Filter out time series that are not linked to assets in the subtree rooted at these assets.
                                Format is [12,345,6,7890].
        min_created_time (int):   Filter out time series with createdTime before this. Format is milliseconds since epoch.
        max_created_time (int):   Filter out time series with createdTime after this. Format is milliseconds since epoch.
        min_last_updated_time (int): Filter out time series with lastUpdatedTime before this. Format is milliseconds since epoch.
        max_last_updated_time (int): Filter out time series with lastUpdatedTime after this. Format is milliseconds since epoch.

    Keyword Arguments:
        api_key (str):  Your api-key.
        project (str):  Project name.
        sort (str):     "createdTime" or "lastUpdatedTime". Field to be sorted.
                        If not specified, results are sorted by relevance score.
        dir (str):      "asc" or "desc". Only applicable if sort is specified. Default 'desc'.
        limit (int):    Return up to this many results. Maximum is 1000. Default is 25.
        offset (int):   Offset from the first result. Sum of limit and offset must not exceed 1000. Default is 0.
        boost_name (bool): Whether or not boosting name field. This option is experimental and can be changed.

    Returns:
        v05.dto.TimeSeriesResponse: A data object containing the requested timeseries with several getter methods with different
        output formats.
    """
    api_key, project = config.get_config_variables(kwargs.get("api_key"), kwargs.get("project"))
    url = config.get_base_url() + "/api/0.6/projects/{}/timeseries/search".format(project)
    headers = {"api-key": api_key, "accept": "application/json", "content-type": "application/json"}
    params = {
        "name": name,
        "description": description,
        "query": query,
        "unit": unit,
        "isString": is_string,
        "isStep": is_step,
        "metadata": str(metadata) if metadata is not None else None,
        "assetIds": str(asset_ids) if asset_ids is not None else None,
        "assetSubtrees": str(asset_subtrees) if asset_subtrees is not None else None,
        "minCreatedTime": min_created_time,
        "maxCreatedTime": max_created_time,
        "minLastUpdatedTime": min_last_updated_time,
        "maxLastUpdatedTime": max_last_updated_time,
        "sort": kwargs.get("sort"),
        "dir": kwargs.get("dir"),
        "limit": kwargs.get("limit"),
        "offset": kwargs.get("offset"),
        "boostName": kwargs.get("boost_name"),
    }
    res = _utils.get_request(url, params=params, headers=headers)
    return TimeSeriesResponse(res.json())
