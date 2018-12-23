# -*- coding: utf-8 -*-
"""Time Series

This module mirrors the Timeseries API. It allows you to fetch data from the api and output it in various formats.
"""
from copy import deepcopy
from typing import Dict, List
from urllib.parse import quote

import pandas as pd

from cognite.client._api_client import APIClient, CogniteResponse


class TimeSeriesResponse(CogniteResponse):
    """Time series Response Object"""

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]["items"]

    def to_pandas(self):
        """Returns data as a pandas dataframe"""
        items = deepcopy(self.internal_representation["data"]["items"])
        if items and items[0].get("metadata") is None:
            return pd.DataFrame(items)
        for d in items:
            if d.get("metadata"):
                d.update(d.pop("metadata"))
        return pd.DataFrame(items)


class TimeSeries:
    """Data Transfer Object for a time series.

    Args:
        name (str):       Unique name of time series.
        is_string (bool):    Whether the time series is string valued or not.
        metadata (dict):    Metadata.
        unit (str):         Physical unit of the time series.
        asset_id (int):     Asset that this time series belongs to.
        description (str):  Description of the time series.
        security_categories (list(int)): Security categories required in order to access this time series.
        is_step (bool):        Whether or not the time series is a step series.

    """

    def __init__(
        self,
        name,
        is_string=False,
        metadata=None,
        unit=None,
        asset_id=None,
        description=None,
        security_categories=None,
        is_step=None,
    ):
        self.name = name
        self.isString = is_string
        self.metadata = metadata
        self.unit = unit
        self.assetId = asset_id
        self.description = description
        self.securityCategories = security_categories
        self.isStep = is_step


class TimeSeriesClient(APIClient):
    def get_timeseries(
        self, prefix=None, description=None, include_metadata=False, asset_id=None, path=None, **kwargs
    ) -> TimeSeriesResponse:
        """Returns a TimeseriesObject containing the requested timeseries.

        Args:
            prefix (str):           List timeseries with this prefix in the name.

            description (str):      Filter timeseries taht contains this string in its description.

            include_metadata (bool):    Decide if the metadata field should be returned or not. Defaults to False.

            asset_id (int):        Get timeseries related to this asset.

            path (str):             Get timeseries under this asset path branch.

        Keyword Arguments:
            limit (int):            Number of results to return.

            autopaging (bool):      Whether or not to automatically page through results. If set to true, limit will be
                                    disregarded. Defaults to False.

        Returns:
            stable.dto.TimeSeriesResponse: A data object containing the requested timeseries with several getter methods with different
            output formats.
        """
        url = "/timeseries"
        params = {
            "q": prefix,
            "description": description,
            "includeMetadata": include_metadata,
            "assetId": asset_id,
            "path": path,
            "limit": kwargs.get("limit", 10000) if not kwargs.get("autopaging") else 10000,
        }

        time_series = []
        res = self._get(url=url, params=params)
        time_series.extend(res.json()["data"]["items"])
        next_cursor = res.json()["data"].get("nextCursor")

        while next_cursor and kwargs.get("autopaging"):
            params["cursor"] = next_cursor
            res = self._get(url=url, params=params)
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

    def post_time_series(self, time_series: List[TimeSeries]) -> Dict:
        """Create a new time series.

        Args:
            time_series (list[stable.dto.TimeSeries]):   List of time series data transfer objects to create.

        Returns:
            An empty response.
        """

        url = "/timeseries"

        body = {"items": [ts.__dict__ for ts in time_series]}

        res = self._post(url, body=body)
        return res.json()

    def update_time_series(self, time_series: List[TimeSeries]) -> Dict:
        """Update an existing time series.

        For each field that can be updated, a null value indicates that nothing should be done.

        Args:
            time_series (list[stable.dto.TimeSeries]):   List of time series data transfer objects to update.

        Returns:
            An empty response.
        """

        url = "/timeseries"

        body = {"items": [ts.__dict__ for ts in time_series]}

        res = self._put(url, body=body)
        return res.json()

    def delete_time_series(self, name) -> Dict:
        """Delete a timeseries.

        Args:
            name (str):   Name of timeseries to delete.

        Returns:
            An empty response.
        """
        url = "/timeseries/{}".format(quote(name, safe=""))

        res = self._delete(url)
        return res.json()
