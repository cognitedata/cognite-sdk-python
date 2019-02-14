# -*- coding: utf-8 -*-
from copy import copy
from typing import List

import pandas as pd

from cognite.client._api_client import APIClient, CogniteResponse


class TimeSeriesResponse(CogniteResponse):
    """Time series Response Object"""

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]["items"]

    def to_pandas(self, include_metadata: bool = False):
        """Returns data as a pandas dataframe

        Args:
            include_metadata (bool): Whether or not to include metadata fields in the resulting dataframe
        """
        items = copy(self.internal_representation["data"]["items"])
        if items and items[0].get("metadata") is None:
            return pd.DataFrame(items)
        for d in items:
            if d.get("metadata"):
                metadata = d.pop("metadata")
                if include_metadata:
                    d.update(metadata)
        return pd.DataFrame(items)


class TimeSeriesClient(APIClient):
    def __init__(self, **kwargs):
        super().__init__(version="0.6", **kwargs)

    def delete_time_series_by_id(self, ids: List[int]) -> None:
        """Delete multiple time series by id.

        Args:
            ids (List[int]):   IDs of time series to delete.

        Returns:
            None

        Examples:
            Delete a single time series by id::

                client = CogniteClient()

                client.time_series.delete_time_series_by_id(ids=[my_ts_id])
        """
        url = "/timeseries/delete"
        body = {"items": ids}
        self._post(url, body=body)

    def get_time_series_by_id(self, id: int) -> TimeSeriesResponse:
        """Returns a TimeseriesResponse object containing the requested timeseries.

        Args:
            id (int):           ID of timeseries to look up

        Returns:
            client.experimental.time_series.TimeSeriesResponse: A data object containing the requested timeseries.
        """
        url = "/timeseries/{}".format(id)
        params = {}
        res = self._get(url=url, params=params)
        return TimeSeriesResponse(res.json())

    def get_multiple_time_series_by_id(self, ids: List[int]) -> TimeSeriesResponse:
        """Returns a TimeseriesResponse object containing the requested timeseries.

        Args:
            ids (List[int]):           IDs of timeseries to look up

        Returns:
            client.experimental.time_series.TimeSeriesResponse: A data object containing the requested timeseries with several
            getter methods with different output formats.
        """
        url = "/timeseries/byids"
        body = {"items": ids}
        params = {}
        res = self._post(url=url, body=body, params=params)
        return TimeSeriesResponse(res.json())

    def search_for_time_series(
        self,
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
    ) -> TimeSeriesResponse:
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
            sort (str):     "createdTime" or "lastUpdatedTime". Field to be sorted.
                            If not specified, results are sorted by relevance score.
            dir (str):      "asc" or "desc". Only applicable if sort is specified. Default 'desc'.
            limit (int):    Return up to this many results. Maximum is 1000. Default is 25.
            offset (int):   Offset from the first result. Sum of limit and offset must not exceed 1000. Default is 0.
            boost_name (bool): Whether or not boosting name field. This option is test_experimental and can be changed.

        Returns:
            client.experimental.time_series.TimeSeriesResponse: A data object containing the requested timeseries with several getter methods with different
            output formats.
        """
        url = "/timeseries/search"
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
            "limit": kwargs.get("limit", self._LIMIT),
            "offset": kwargs.get("offset"),
            "boostName": kwargs.get("boost_name"),
        }
        res = self._get(url, params=params)
        return TimeSeriesResponse(res.json())
