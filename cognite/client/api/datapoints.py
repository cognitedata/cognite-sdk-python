# -*- coding: utf-8 -*-
from collections import namedtuple
from datetime import datetime
from typing import *
from typing import List

from cognite.client._auxiliary._protobuf_descriptors import _api_timeseries_data_v2_pb2
from cognite.client._utils import utils
from cognite.client._utils.api_client import APIClient
from cognite.client._utils.resource_base import CogniteResource, CogniteResourceList


# GenClass: DatapointsGetDatapoint
class Datapoints(CogniteResource):
    """No description.

    Args:
        id (int): Id of the timeseries the datapoints belong to
        external_id (str): External id of the timeseries the datapoints belong to (Only if id is not set)
        datapoints (List[Dict[str, Any]]): The list of datapoints
    """

    def __init__(self, id: int = None, external_id: str = None, datapoints: List[Dict[str, Any]] = None):
        self.id = id
        self.external_id = external_id
        self.datapoints = datapoints

    # GenStop
    def __len__(self):
        return len(self.datapoints)

    def __getitem__(self, item):
        return self.datapoints[item]

    def __iter__(self):
        for dp in self.datapoints:
            yield dp


class DatapointsList(CogniteResourceList):
    _RESOURCE = Datapoints


# GenClass: DatapointsQuery
class DatapointsQuery(CogniteResource):
    """Parameters describing a query for datapoints.

    Args:
        id (int): Id of the timeseries to query
        external_id (str): External id of the timeseries to query (Only if id is not set)
        start (str): Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since epoch.
        end (str): Get datapoints up to this time. The format is the same as for start.
        limit (int): Return up to this number of datapoints.
        aggregates (List[str]): The aggregates to be returned.  Use default if null. An empty string must be sent to get raw data if the default is a set of aggregates.
        granularity (str): The granularity size and granularity of the aggregates.
        include_outside_points (bool): Whether to include the last datapoint before the requested time period,and the first one after the requested period. This can be useful for interpolating data. Not available for aggregates.
        aliases (List[Dict[str, Any]]): No description.
    """

    def __init__(
        self,
        id: int = None,
        external_id: str = None,
        start: str = None,
        end: str = None,
        limit: int = None,
        aggregates: List[str] = None,
        granularity: str = None,
        include_outside_points: bool = None,
        aliases: List[Dict[str, Any]] = None,
    ):
        self.id = id
        self.external_id = external_id
        self.start = start
        self.end = end
        self.limit = limit
        self.aggregates = aggregates
        self.granularity = granularity
        self.include_outside_points = include_outside_points
        self.aliases = aliases

    # GenStop


_DPWindow = namedtuple("Window", ["start", "end"])


class DatapointsAPI(APIClient):
    _LIMIT_AGG = 10000
    _LIMIT = 100000
    _RESOURCE_PATH = "/timeseries/data"

    def get(
        self,
        start: Union[int, str, datetime],
        end: Union[int, str, datetime],
        id: Union[int, List[int]] = None,
        external_id: Union[str, List[str]] = None,
        aggregates: List[str] = None,
        granularity: str = None,
        include_outside_points: bool = None,
        limit: int = None,
    ) -> Union[Datapoints, List[Datapoints]]:
        start = utils.timestamp_to_ms(start)
        end = utils.timestamp_to_ms(end)
        all_ids, is_single_id = self._process_ids(id, external_id, wrap_ids=True)

        tasks = []
        for id_object in all_ids:
            id = id_object.get("id")
            external_id = id_object.get("externalId")
            max_workers_per_ts = max(self._max_workers // len(all_ids), 1)
            if include_outside_points:
                max_workers_per_ts = 1
            tasks.append(
                (
                    start,
                    end,
                    id,
                    external_id,
                    aggregates,
                    granularity,
                    include_outside_points,
                    limit,
                    max_workers_per_ts,
                )
            )

        results = utils.execute_tasks_concurrently(
            self._get_datapoints_concurrently, tasks, max_workers=min(self._max_workers, len(all_ids))
        )
        for dps_res in results:
            if limit:
                dps_res["datapoints"] = dps_res["datapoints"][:limit]

        dps_list = DatapointsList._load(results)

        if is_single_id:
            return dps_list[0]
        return dps_list

    def _get_datapoints_concurrently(
        self,
        start: int,
        end: int,
        id: int,
        external_id: str,
        aggregates: List[str],
        granularity: str,
        include_outside_points: bool,
        limit: int,
        max_workers: int,
    ) -> Dict[str, Any]:
        datapoints = []
        windows = self._get_windows(start, end, granularity=granularity, max_windows=max_workers)
        tasks = [
            (w.start, w.end, id, external_id, aggregates, granularity, include_outside_points, limit) for w in windows
        ]
        results = utils.execute_tasks_concurrently(self._get_datapoints, tasks, max_workers=max_workers)
        for res in results:
            returned_id = res["id"]
            returned_external_id = res["externalId"]
            datapoints.extend(res["datapoints"])
        return {"id": returned_id, "externalId": returned_external_id, "datapoints": datapoints}

    def _get_datapoints(
        self,
        start: int,
        end: int,
        id: int,
        external_id: str,
        aggregates: List[str],
        granularity: str,
        include_outside_points: bool,
        limit: int,
    ) -> Dict[str, Any]:
        per_request_limit = self._LIMIT_AGG if aggregates else self._LIMIT
        utils.assert_only_one_of_id_or_external_id(id, external_id)
        if id:
            items = [{"id": id}]
        elif external_id:
            items = [{"externalId": external_id}]
        payload = {
            "items": items,
            "start": start,
            "end": end,
            "aggregates": aggregates,
            "granularity": granularity,
            "includeOutsidePoints": include_outside_points,
            "limit": per_request_limit,
        }
        res = {"datapoints": []}
        datapoints = []
        while (
            (not datapoints or len(res["datapoints"]) == per_request_limit)
            and payload["end"] > payload["start"]
            and len(datapoints) < (limit or float("inf"))
        ):
            res = self._post(self._RESOURCE_PATH + "/get", json=payload).json()["data"]["items"][0]
            datapoints.extend(res["datapoints"])
            latest_timestamp = int(datapoints[-1]["timestamp"])
            payload["start"] = latest_timestamp + (utils.granularity_to_ms(granularity) if granularity else 1)
        return {"id": res["id"], "externalId": res.get("externalId"), "datapoints": datapoints}

    @staticmethod
    def _get_windows(start: int, end: int, granularity: str, max_windows: int):
        diff = end - start
        granularity_ms = utils.granularity_to_ms(granularity) if granularity else 1

        # Ensure that number of steps is not greater than the number data points that will be returned
        steps = min(max_windows, max(1, int(diff / granularity_ms)))

        step_size = diff // steps
        windows = []
        next_start = start
        next_end = next_start + step_size
        while (not windows or windows[-1].end < end) and next_start < next_end:
            windows.append(_DPWindow(start=next_start, end=next_end))
            next_start += step_size + granularity_ms
            next_end = next_start + step_size
            if next_end > end:
                next_end = end
        return windows

    def get_latest(self):
        pass

    def get_dataframe(self):
        pass

    def insert(self):
        pass

    def delete(self):
        pass

    # def get_datapoints(self, name, start, end=None, aggregates=None, granularity=None, **kwargs) -> DatapointsResponse:
    #     """Returns a DatapointsObject containing a list of datapoints for the given query.
    #
    #     This method will automate paging for the user and return all data for the given time period.
    #
    #     Args:
    #         name (str):             The name of the timeseries to retrieve data for.
    #
    #         start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
    #                                     E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
    #                                     epoch or a datetime object which will be converted to ms since epoch UTC.
    #
    #         end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.
    #
    #         aggregates (list):      The list of aggregate functions you wish to apply to the data. Valid aggregate functions
    #                                 are: 'average/avg, max, min, count, sum, interpolation/int, stepinterpolation/step'.
    #
    #         granularity (str):      The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
    #                                 second/s', or a multiple of these indicated by a number as a prefix e.g. '12hour'.
    #
    #     Keyword Arguments:
    #         workers (int):    Number of download workers to run in parallell. Defaults to 10.
    #
    #         include_outside_points (bool):      No description
    #
    #         protobuf (bool):        Download the data using the binary protobuf format. Only applicable when getting raw data.
    #                                 Defaults to True.
    #
    #         limit (str):            Max number of datapoints to return. If limit is specified, this method will not automate
    #                                 paging and will return a maximum of 100,000 dps.
    #
    #     Returns:
    #         stable.datapoints.DatapointsResponse: A data object containing the requested data with several getter methods with different
    #         output formats.
    #
    #     Examples:
    #         Getting the last 3 days of raw datapoints for a given time series::
    #
    #             client = CogniteClient()
    #             res = client.datapoints.get_datapoints(name="my_ts", start="3d-ago")
    #             print(res.to_pandas())
    #     """
    #     start, end = utils.interval_to_ms(start, end)
    #
    #     if aggregates:
    #         aggregates = ",".join(aggregates)
    #
    #     if kwargs.get("limit"):
    #         return self._get_datapoints_user_defined_limit(
    #             name,
    #             aggregates,
    #             granularity,
    #             start,
    #             end,
    #             limit=kwargs.get("limit"),
    #             protobuf=kwargs.get("protobuf"),
    #             include_outside_points=kwargs.get("include_outside_points", False),
    #         )
    #
    #     num_of_workers = kwargs.get("workers", self._num_of_workers)
    #     if kwargs.get("include_outside_points") is True:
    #         num_of_workers = 1
    #
    #     windows = utils.get_datapoints_windows(start, end, granularity, num_of_workers)
    #     tasks = [
    #         (
    #             name,
    #             aggregates,
    #             granularity,
    #             window["start"],
    #             window["end"],
    #             kwargs.get("protobuf", True),
    #             kwargs.get("include_outside_points", False),
    #         )
    #         for window in windows
    #     ]
    #     datapoints = utils.execute_tasks_concurrently(self._get_datapoints_helper, tasks, num_of_workers)
    #
    #     concat_dps = []
    #     [concat_dps.extend(el) for el in datapoints]
    #
    #     return DatapointsResponse({"data": {"items": [{"name": name, "datapoints": concat_dps}]}})
    #
    # def _get_datapoints_helper(
    #     self,
    #     name,
    #     aggregates=None,
    #     granularity=None,
    #     start=None,
    #     end=None,
    #     include_outside_points: bool = False,
    #     protobuf: bool = True,
    # ):
    #     url = "/timeseries/data/{}".format(quote(name, safe=""))
    #
    #     use_protobuf = protobuf and aggregates is None
    #     limit = self._LIMIT if aggregates is None else self._LIMIT_AGG
    #
    #     params = {
    #         "aggregates": aggregates,
    #         "granularity": granularity,
    #         "limit": limit,
    #         "start": start,
    #         "end": end,
    #         "includeOutsidePoints": include_outside_points,
    #     }
    #
    #     headers = {"accept": "application/protobuf"} if use_protobuf else {}
    #     datapoints = []
    #     while (not datapoints or len(datapoints[-1]) == limit) and params["end"] > params["start"]:
    #         res = self._get(url, params=params, headers=headers)
    #         if use_protobuf:
    #             ts_data = _api_timeseries_data_v2_pb2.TimeseriesData()
    #             ts_data.ParseFromString(res.content)
    #             res = [{"timestamp": p.timestamp, "value": p.value} for p in ts_data.numericData.points]
    #         else:
    #             res = res.json()["data"]["items"][0]["datapoints"]
    #
    #         if not res:
    #             break
    #
    #         datapoints.append(res)
    #         latest_timestamp = int(datapoints[-1][-1]["timestamp"])
    #         params["start"] = latest_timestamp + (utils.granularity_to_ms(granularity) if granularity else 1)
    #     dps = []
    #     [dps.extend(el) for el in datapoints]
    #     return dps
    #
    # def _get_datapoints_user_defined_limit(
    #     self, name, aggregates, granularity, start, end, limit, include_outside_points, protobuf
    # ):
    #     url = "/timeseries/data/{}".format(quote(name, safe=""))
    #
    #     use_protobuf = protobuf and aggregates is None
    #
    #     params = {
    #         "aggregates": aggregates,
    #         "granularity": granularity,
    #         "limit": limit,
    #         "start": start,
    #         "end": end,
    #         "includeOutsidePoints": include_outside_points,
    #     }
    #     headers = {"accept": "application/protobuf"} if use_protobuf else {}
    #     res = self._get(url, params=params, headers=headers)
    #     if use_protobuf:
    #         ts_data = _api_timeseries_data_v2_pb2.TimeseriesData()
    #         ts_data.ParseFromString(res.content)
    #         res = [{"timestamp": p.timestamp, "value": p.value} for p in ts_data.numericData.points]
    #     else:
    #         res = res.json()["data"]["items"][0]["datapoints"]
    #
    #     return DatapointsResponse({"data": {"items": [{"name": name, "datapoints": res}]}})
    #
    # def _split_TimeseriesWithDatapoints_if_over_limit(
    #     self, timeseries_with_datapoints: TimeseriesWithDatapoints, limit: int
    # ) -> List[TimeseriesWithDatapoints]:
    #     """Takes a TimeseriesWithDatapoints and splits it into multiple so that each has a max number of datapoints equal
    #     to the limit given.
    #
    #     Args:
    #         timeseries_with_datapoints (stable.datapoints.TimeseriesWithDatapoints): The timeseries with data to potentially split up.
    #
    #     Returns:
    #         A list of stable.datapoints.TimeSeriesWithDatapoints where each has a maximum number of datapoints equal to the limit given.
    #     """
    #     timeseries_with_datapoints_list = []
    #     if len(timeseries_with_datapoints.datapoints) > limit:
    #         i = 0
    #         while i < len(timeseries_with_datapoints.datapoints):
    #             timeseries_with_datapoints_list.append(
    #                 TimeseriesWithDatapoints(
    #                     name=timeseries_with_datapoints.name,
    #                     datapoints=timeseries_with_datapoints.datapoints[i : i + limit],
    #                 )
    #             )
    #             i += limit
    #     else:
    #         timeseries_with_datapoints_list.append(timeseries_with_datapoints)
    #
    #     return timeseries_with_datapoints_list
    #
    # def post_multi_time_series_datapoints(self, timeseries_with_datapoints: List[TimeseriesWithDatapoints]) -> None:
    #     """Insert data into multiple timeseries.
    #
    #     Args:
    #         timeseries_with_datapoints (List[stable.datapoints.TimeseriesWithDatapoints]): The timeseries with data to insert.
    #
    #     Returns:
    #         None
    #
    #     Examples:
    #         Posting some dummy datapoints to multiple time series. This example assumes that the time series have
    #         already been created::
    #
    #             from cognite.client.stable.datapoints import TimeseriesWithDatapoints, Datapoint
    #
    #             start = 1514761200000
    #             my_dummy_data_1 = [Datapoint(timestamp=ms, value=i) for i, ms in range(start, start+100)]
    #             ts_with_datapoints_1 = TimeSeriesWithDatapoints(name="ts1", datapoints=my_dummy_data_1)
    #
    #             start = 1503331800000
    #             my_dummy_data_2 = [Datapoint(timestamp=ms, value=i) for i, ms in range(start, start+100)]
    #             ts_with_datapoints_2 = TimeSeriesWithDatapoints(name="ts2", datapoints=my_dummy_data_2)
    #
    #             my_dummy_data = [ts_with_datapoints_1, ts_with_datapoints_2]
    #
    #             client = CogniteClient()
    #             res = client.datapoints.post_multi_time_series_datapoints(my_dummy_data)
    #     """
    #     url = "/timeseries/data"
    #
    #     ul_dps_limit = 100000
    #
    #     # Make sure we only work with TimeseriesWithDatapoints objects that has a max number of datapoints
    #     timeseries_with_datapoints_limited = []
    #     for entry in timeseries_with_datapoints:
    #         timeseries_with_datapoints_limited.extend(
    #             self._split_TimeseriesWithDatapoints_if_over_limit(entry, ul_dps_limit)
    #         )
    #
    #     # Group these TimeseriesWithDatapoints if possible so that we upload as much as possible in each call to the API
    #     timeseries_to_upload_binned = utils.first_fit(
    #         list_items=timeseries_with_datapoints_limited, max_size=ul_dps_limit, get_count=lambda x: len(x.datapoints)
    #     )
    #
    #     for bin in timeseries_to_upload_binned:
    #         body = {
    #             "items": [
    #                 {"name": ts_with_data.name, "datapoints": [dp.__dict__ for dp in ts_with_data.datapoints]}
    #                 for ts_with_data in bin
    #             ]
    #         }
    #         self._post(url, json=body)
    #
    # def post_datapoints(self, name, datapoints: List[Datapoint]) -> None:
    #     """Insert a list of datapoints.
    #
    #     Args:
    #         name (str):       Name of timeseries to insert to.
    #
    #         datapoints (List[stable.datapoints.Datapoint]): List of datapoint data transfer objects to insert.
    #
    #     Returns:
    #         None
    #
    #     Examples:
    #         Posting some dummy datapoints::
    #
    #             from cognite.client.stable.datapoints import Datapoint
    #
    #             client = CogniteClient()
    #
    #             start = 1514761200000
    #             my_dummy_data = [Datapoint(timestamp=start+off, value=off) for off in range(100)]
    #             client.datapoints.post_datapoints(ts_name, my_dummy_data)
    #     """
    #     url = "/timeseries/data/{}".format(quote(name, safe=""))
    #
    #     ul_dps_limit = 100000
    #     i = 0
    #     while i < len(datapoints):
    #         body = {"items": [dp.__dict__ for dp in datapoints[i : i + ul_dps_limit]]}
    #         self._post(url, json=body)
    #         i += ul_dps_limit
    #
    # def get_latest(self, name, before=None) -> LatestDatapointResponse:
    #     """Returns a LatestDatapointObject containing the latest datapoint for the given timeseries.
    #
    #     Args:
    #         name (str):       The name of the timeseries to retrieve data for.
    #
    #     Returns:
    #         stable.datapoints.LatestDatapointsResponse: A data object containing the requested data with several getter methods with different
    #         output formats.
    #
    #     Examples:
    #         Get the latest datapoint from a time series before time x::
    #
    #             client = CogniteClient()
    #             x = 1514761200000
    #             client.datapoints.get_latest(name="my_ts", before=x)
    #
    #     """
    #     url = "/timeseries/latest/{}".format(quote(name, safe=""))
    #     params = {"before": before}
    #     res = self._get(url, params=params)
    #     return LatestDatapointResponse(res.json())
    #
    # def get_multi_time_series_datapoints(
    #     self, datapoints_queries: List[DatapointsQuery], start, end=None, aggregates=None, granularity=None, **kwargs
    # ) -> DatapointsResponseIterator:
    #     """Returns a list of DatapointsObjects each of which contains a list of datapoints for the given timeseries.
    #
    #     This method will automate paging for the user and return all data for the given time period(s).
    #
    #     Args:
    #         datapoints_queries (list[stable.datapoints.DatapointsQuery]): The list of DatapointsQuery objects specifying which
    #                                                                     timeseries to retrieve data for.
    #
    #         start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
    #                                     E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
    #                                     epoch or a datetime object which will be converted to ms since epoch UTC.
    #
    #         end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.
    #
    #         aggregates (list, optional):    The list of aggregate functions you wish to apply to the data. Valid aggregate
    #                                         functions are: 'average/avg, max, min, count, sum, interpolation/int,
    #                                         stepinterpolation/step'.
    #
    #         granularity (str):              The granularity of the aggregate values. Valid entries are : 'day/d, hour/h,
    #                                         minute/m, second/s', or a multiple of these indicated by a number as a prefix
    #                                         e.g. '12hour'.
    #
    #     Keyword Arguments:
    #         include_outside_points (bool):  No description.
    #
    #     Returns:
    #         stable.datapoints.DatapointsResponseIterator: An iterator which iterates over stable.datapoints.DatapointsResponse objects.
    #     """
    #     url = "/timeseries/dataquery"
    #     start, end = utils.interval_to_ms(start, end)
    #
    #     datapoints_queries = [copy(dpq) for dpq in datapoints_queries]
    #     num_of_dpqs_with_agg = 0
    #     num_of_dpqs_raw = 0
    #     for dpq in datapoints_queries:
    #         if (dpq.aggregates is None and aggregates is None) or dpq.aggregates == "":
    #             num_of_dpqs_raw += 1
    #         else:
    #             num_of_dpqs_with_agg += 1
    #
    #     items = []
    #     for dpq in datapoints_queries:
    #         if dpq.aggregates is None and aggregates is None:
    #             dpq.limit = int(self._LIMIT / num_of_dpqs_raw)
    #         else:
    #             dpq.limit = int(self._LIMIT_AGG / num_of_dpqs_with_agg)
    #         items.append(dpq.__dict__)
    #     body = {
    #         "items": items,
    #         "aggregates": ",".join(aggregates) if aggregates is not None else None,
    #         "granularity": granularity,
    #         "start": start,
    #         "includeOutsidePoints": kwargs.get("include_outside_points", False),
    #         "end": end,
    #     }
    #     datapoints_responses = []
    #     has_incomplete_requests = True
    #     while has_incomplete_requests:
    #         res = self._post(url_path=url, json=body).json()["data"]["items"]
    #         datapoints_responses.append(res)
    #         has_incomplete_requests = False
    #         for i, dpr in enumerate(res):
    #             dpq = datapoints_queries[i]
    #             if len(dpr["datapoints"]) == dpq.limit:
    #                 has_incomplete_requests = True
    #                 latest_timestamp = dpr["datapoints"][-1]["timestamp"]
    #                 ts_granularity = granularity if dpq.granularity is None else dpq.granularity
    #                 next_start = latest_timestamp + (utils.granularity_to_ms(ts_granularity) if ts_granularity else 1)
    #             else:
    #                 next_start = end - 1
    #                 if datapoints_queries[i].end:
    #                     next_start = datapoints_queries[i].end - 1
    #             datapoints_queries[i].start = next_start
    #
    #     results = [{"data": {"items": [{"name": dpq.name, "datapoints": []}]}} for dpq in datapoints_queries]
    #     for res in datapoints_responses:
    #         for i, ts in enumerate(res):
    #             results[i]["data"]["items"][0]["datapoints"].extend(ts["datapoints"])
    #     return DatapointsResponseIterator([DatapointsResponse(result) for result in results])
    #
    # def get_datapoints_frame(self, time_series, aggregates, granularity, start, end=None, **kwargs) -> pd.DataFrame:
    #     """Returns a pandas dataframe of datapoints for the given timeseries all on the same timestamps.
    #
    #     This method will automate paging for the user and return all data for the given time period.
    #
    #     Args:
    #         time_series (list):  The list of timeseries names to retrieve data for. Each timeseries can be either a string
    #                             containing the timeseries or a dictionary containing the names of thetimeseries and a
    #                             list of specific aggregate functions.
    #
    #         aggregates (list):  The list of aggregate functions you wish to apply to the data for which you have not
    #                             specified an aggregate function. Valid aggregate functions are: 'average/avg, max, min,
    #                             count, sum, interpolation/int, stepinterpolation/step'.
    #
    #         granularity (str):  The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
    #                             second/s', or a multiple of these indicated by a number as a prefix e.g. '12hour'.
    #
    #         start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
    #                                     E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
    #                                     epoch or a datetime object which will be converted to ms since epoch UTC.
    #
    #         end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.
    #
    #     Keyword Arguments:
    #         limit (str): Max number of rows to return. If limit is specified, this method will not automate
    #                         paging and will return a maximum of 100,000 rows.
    #
    #         workers (int):    Number of download workers to run in parallell. Defaults to 10.
    #
    #     Returns:
    #         pandas.DataFrame: A pandas dataframe containing the datapoints for the given timeseries. The datapoints for all the
    #         timeseries will all be on the same timestamps.
    #
    #     Examples:
    #         Get a dataframe of aggregated time series data::
    #
    #             client = CogniteClient()
    #
    #             res = client.datapoints.get_datapoints_frame(time_series=["ts1", "ts2"],
    #                             aggregates=["avg"], granularity="30s", start="1w-ago")
    #
    #             print(res)
    #
    #         The ``timeseries`` parameter can take a list of strings and/or dicts on the following formats.
    #         This is useful for specifying aggregate functions on a per time series level::
    #
    #             Using strings:
    #                 ['<timeseries1>', '<timeseries2>']
    #
    #             Using dicts:
    #                 [{'name': '<timeseries1>', 'aggregates': ['<aggfunc1>', '<aggfunc2>']},
    #                 {'name': '<timeseries2>', 'aggregates': []}]
    #
    #             Using both:
    #                 ['<timeseries1>', {'name': '<timeseries2>', 'aggregates': ['<aggfunc1>', '<aggfunc2>']}]
    #     """
    #     if not isinstance(time_series, list):
    #         raise ValueError("time_series should be a list")
    #     start, end = utils.interval_to_ms(start, end)
    #
    #     if kwargs.get("limit"):
    #         return self._get_datapoints_frame_user_defined_limit(
    #             time_series, aggregates, granularity, start, end, limit=kwargs.get("limit")
    #         )
    #
    #     num_of_workers = kwargs.get("workers") or self._num_of_workers
    #
    #     windows = utils.get_datapoints_windows(start, end, granularity, num_of_workers)
    #
    #     partial_get_dpsf = partial(
    #         self._get_datapoints_frame_helper_wrapper,
    #         time_series=time_series,
    #         aggregates=aggregates,
    #         granularity=granularity,
    #     )
    #
    #     with Pool(len(windows)) as p:
    #         dataframes = p.map(partial_get_dpsf, windows)
    #
    #     df = pd.concat(dataframes).drop_duplicates(subset="timestamp").reset_index(drop=True)
    #
    #     return df
    #
    # def _get_datapoints_frame_helper_wrapper(self, args, time_series, aggregates, granularity):
    #     return self._get_datapoints_frame_helper(time_series, aggregates, granularity, args["start"], args["end"])
    #
    # def _get_datapoints_frame_helper(self, time_series, aggregates, granularity, start=None, end=None):
    #     """Returns a pandas dataframe of datapoints for the given timeseries all on the same timestamps.
    #
    #     This method will automate paging for the user and return all data for the given time period.
    #
    #     Args:
    #         time_series (list):     The list of timeseries names to retrieve data for. Each timeseries can be either a string containing the
    #                             ts name or a dictionary containing the ts name and a list of specific aggregate functions.
    #
    #         aggregates (list):  The list of aggregate functions you wish to apply to the data for which you have not
    #                             specified an aggregate function. Valid aggregate functions are: 'average/avg, max, min,
    #                             count, sum, interpolation/int, stepinterpolation/step'.
    #
    #         granularity (str):  The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
    #                             second/s', or a multiple of these indicated by a number as a prefix e.g. '12hour'.
    #
    #         start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
    #                                     E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
    #                                     epoch or a datetime object which will be converted to ms since epoch UTC.
    #
    #         end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.
    #
    #     Returns:
    #         pandas.DataFrame: A pandas dataframe containing the datapoints for the given timeseries. The datapoints for all the
    #         timeseries will all be on the same timestamps.
    #
    #     Note:
    #         The ``timeseries`` parameter can take a list of strings and/or dicts on the following formats::
    #
    #             Using strings:
    #                 ['<timeseries1>', '<timeseries2>']
    #
    #             Using dicts:
    #                 [{'name': '<timeseries1>', 'aggregates': ['<aggfunc1>', '<aggfunc2>']},
    #                 {'name': '<timeseries2>', 'aggregates': []}]
    #
    #             Using both:
    #                 ['<timeseries1>', {'name': '<timeseries2>', 'aggregates': ['<aggfunc1>', '<aggfunc2>']}]
    #     """
    #     url = "/timeseries/dataframe"
    #     num_aggregates = 0
    #     for ts in time_series:
    #         if isinstance(ts, str) or ts.get("aggregates") is None:
    #             num_aggregates += len(aggregates)
    #         else:
    #             num_aggregates += len(ts["aggregates"])
    #
    #     per_tag_limit = int(self._LIMIT / num_aggregates)
    #
    #     body = {
    #         "items": [
    #             {"name": "{}".format(ts)}
    #             if isinstance(ts, str)
    #             else {"name": "{}".format(ts["name"]), "aggregates": ts.get("aggregates", [])}
    #             for ts in time_series
    #         ],
    #         "aggregates": aggregates,
    #         "granularity": granularity,
    #         "start": start,
    #         "end": end,
    #         "limit": per_tag_limit,
    #     }
    #     headers = {"accept": "text/csv"}
    #     dataframes = []
    #     while (not dataframes or dataframes[-1].shape[0] == per_tag_limit) and body["end"] > body["start"]:
    #         res = self._post(url_path=url, json=body, headers=headers)
    #         dataframes.append(
    #             pd.read_csv(io.StringIO(res.content.decode(res.encoding if res.encoding else res.apparent_encoding)))
    #         )
    #         if dataframes[-1].empty:
    #             break
    #         latest_timestamp = int(dataframes[-1].iloc[-1, 0])
    #         body["start"] = latest_timestamp + utils.granularity_to_ms(granularity)
    #     return pd.concat(dataframes).reset_index(drop=True)
    #
    # def _get_datapoints_frame_user_defined_limit(self, time_series, aggregates, granularity, start, end, limit):
    #     """Returns a DatapointsResponse object with the requested data.
    #
    #     No paging or parallelizing is done.
    #
    #     Args:
    #         time_series (List[str]):       The list of timeseries names to retrieve data for. Each timeseries can be either a string containing the
    #                             ts name or a dictionary containing the ts name and a list of specific aggregate functions.
    #
    #         aggregates (list):      The list of aggregate functions you wish to apply to the data. Valid aggregate functions
    #                                 are: 'average/avg, max, min, count, sum, interpolation/int, stepinterpolation/step'.
    #
    #         granularity (str):      The granularity of the aggregate values. Valid entries are : 'day/d, hour/h, minute/m,
    #                                 second/s', or a multiple of these indicated by a number as a prefix e.g. '12hour'.
    #
    #         start (Union[str, int, datetime]):    Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s.
    #                                     E.g. '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since
    #                                     epoch or a datetime object which will be converted to ms since epoch UTC.
    #
    #         end (Union[str, int, datetime]):      Get datapoints up to this time. Same format as for start.
    #
    #         limit (int):            Max number of rows to retrieve. Max is 100,000.
    #
    #     Returns:
    #         stable.datapoints.DatapointsResponse: A data object containing the requested data with several getter methods with different
    #         output formats.
    #     """
    #     url = "/timeseries/dataframe"
    #     body = {
    #         "items": [
    #             {"name": "{}".format(ts)}
    #             if isinstance(ts, str)
    #             else {"name": "{}".format(ts["name"]), "aggregates": ts.get("aggregates", [])}
    #             for ts in time_series
    #         ],
    #         "aggregates": aggregates,
    #         "granularity": granularity,
    #         "start": start,
    #         "end": end,
    #         "limit": limit,
    #     }
    #
    #     headers = {"accept": "text/csv"}
    #     res = self._post(url_path=url, json=body, headers=headers)
    #     df = pd.read_csv(io.StringIO(res.content.decode(res.encoding if res.encoding else res.apparent_encoding)))
    #
    #     return df
    #
    # def post_datapoints_frame(self, dataframe) -> None:
    #     """Write a dataframe.
    #     The dataframe must have a 'timestamp' column with timestamps in milliseconds since epoch.
    #     The names of the remaining columns specify the names of the time series to which column contents will be written.
    #     Said time series must already exist.
    #
    #     Args:
    #         dataframe (pandas.DataFrame):  Pandas DataFrame Object containing the time series.
    #
    #     Returns:
    #         None
    #
    #     Examples:
    #         Post a dataframe with white noise::
    #
    #             client = CogniteClient()
    #             ts_name = 'NOISE'
    #
    #             start = datetime(2018, 1, 1)
    #             # The scaling by 1000 is important: timestamp() returns seconds
    #             x = [(start + timedelta(days=d)).timestamp() * 1000 for d in range(100)]
    #             y = np.random.normal(0, 1, 100)
    #
    #             # The time column must be called precisely 'timestamp'
    #             df = pd.DataFrame({'timestamp': x, ts_name: y})
    #
    #             client.datapoints.post_datapoints_frame(df)
    #     """
    #
    #     try:
    #         timestamp = dataframe.timestamp
    #         names = dataframe.drop(["timestamp"], axis=1).columns
    #     except:
    #         raise ValueError("DataFrame not on a correct format")
    #
    #     for name in names:
    #         data_points = [Datapoint(int(timestamp.iloc[i]), dataframe[name].iloc[i]) for i in range(0, len(dataframe))]
    #         self.post_datapoints(name, data_points)
    #
    # def live_data_generator(self, name, update_frequency=1):
    #     """Generator function which continously polls latest datapoint of a timeseries and yields new datapoints.
    #
    #     Args:
    #         name (str): Name of timeseries to get latest datapoints for.
    #
    #         update_frequency (float): Frequency to pull for data in seconds.
    #
    #     Yields:
    #         dict: Dictionary containing timestamp and value of latest datapoint.
    #     """
    #     last_timestamp = self.get_latest(name).to_json()["timestamp"]
    #     while True:
    #         latest = self.get_latest(name).to_json()
    #         if last_timestamp == latest["timestamp"]:
    #             time.sleep(update_frequency)
    #         else:
    #             yield latest
    #         last_timestamp = latest["timestamp"]
