# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor as Pool
from functools import partial
from typing import List

import pandas as pd

from cognite.client import _utils
from cognite.client._api_client import APIClient, CogniteResponse


class DatapointsResponse(CogniteResponse):
    """Datapoints Response Object."""

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]["items"][0]

    def to_pandas(self):
        """Returns data as a pandas dataframe"""
        return pd.DataFrame(self.internal_representation["data"]["items"][0]["datapoints"])


class DatapointsClient(APIClient):
    def __init__(self, **kwargs):
        super().__init__(version="0.6", **kwargs)
        self._LIMIT_AGG = 10000
        self._LIMIT = 100000

    def get_datapoints(self, id, start, end=None, aggregates=None, granularity=None, **kwargs) -> DatapointsResponse:
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

            workers (int):        Number of download processes to run in parallell. Defaults to number returned by cpu_count().

            limit (str):            Max number of datapoints to return. If limit is specified, this method will not automate
                                    paging and will return a maximum of 100,000 dps.

        Returns:
            client.test_experimental.datapoints.DatapointsResponse: A data object containing the requested data with several getter methods with different
            output formats.
        """
        start, end = _utils.interval_to_ms(start, end)

        if kwargs.get("limit"):
            return self._get_datapoints_user_defined_limit(
                id,
                aggregates,
                granularity,
                start,
                end,
                limit=kwargs.get("limit"),
                include_outside_points=kwargs.get("include_outside_points", False),
            )

        num_of_workers = kwargs.get("processes", self._num_of_workers)
        if kwargs.get("include_outside_points") is True:
            num_of_workers = 1

        windows = _utils.get_datapoints_windows(start, end, granularity, num_of_workers)

        partial_get_dps = partial(
            self._get_datapoints_helper_wrapper,
            id=id,
            aggregates=aggregates,
            granularity=granularity,
            include_outside_points=kwargs.get("include_outside_points", False),
        )

        with Pool(len(windows)) as p:
            datapoints = p.map(partial_get_dps, windows)

        concat_dps = []
        [concat_dps.extend(el) for el in datapoints]

        return DatapointsResponse({"data": {"items": [{"id": id, "datapoints": concat_dps}]}})

    def _get_datapoints_helper_wrapper(self, args, id, aggregates, granularity, include_outside_points):
        return self._get_datapoints_helper(
            id, aggregates, granularity, args["start"], args["end"], include_outside_points=include_outside_points
        )

    def _get_datapoints_helper(self, id, aggregates=None, granularity=None, start=None, end=None, **kwargs):
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

        Returns:
            list of datapoints: A list containing datapoint dicts.
        """
        url = "/timeseries/{}/data".format(id)

        limit = self._LIMIT if aggregates is None else self._LIMIT_AGG

        params = {
            "aggregates": aggregates,
            "granularity": granularity,
            "limit": limit,
            "start": start,
            "end": end,
            "includeOutsidePoints": kwargs.get("include_outside_points", False),
        }

        datapoints = []
        while (not datapoints or len(datapoints[-1]) == limit) and params["end"] > params["start"]:
            res = self._get(url, params=params)
            res = res.json()["data"]["items"][0]["datapoints"]

            if not res:
                break

            datapoints.append(res)
            latest_timestamp = int(datapoints[-1][-1]["timestamp"])
            params["start"] = latest_timestamp + (_utils.granularity_to_ms(granularity) if granularity else 1)
        dps = []
        [dps.extend(el) for el in datapoints]
        return dps

    def _get_datapoints_user_defined_limit(
        self, id: int, aggregates: List, granularity: str, start, end, limit, **kwargs
    ) -> DatapointsResponse:
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

        Returns:
            client.test_experimental.datapoints.DatapointsResponse: A data object containing the requested data with several getter methods with different
            output formats.
        """
        url = "/timeseries/{}/data".format(id)

        params = {
            "aggregates": aggregates,
            "granularity": granularity,
            "limit": limit,
            "start": start,
            "end": end,
            "includeOutsidePoints": kwargs.get("include_outside_points", False),
        }
        res = self._get(url, params=params)
        res = res.json()["data"]["items"][0]["datapoints"]
        return DatapointsResponse({"data": {"items": [{"id": id, "datapoints": res}]}})
