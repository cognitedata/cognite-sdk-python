# -*- coding: utf-8 -*-
import json
from collections import namedtuple
from datetime import datetime
from typing import *
from typing import List

from cognite.client._utils import utils
from cognite.client._utils.api_client import APIClient
from cognite.client._utils.resource_base import CogniteResource, CogniteResourceList


class Datapoint(CogniteResource):
    """An object representing a datapoint.

    Args:
        timestamp (Union[int, float]): The data timestamp in milliseconds since the epoch (Jan 1, 1970).
        value (Union[str, int, float]): The data value. Can be String or numeric depending on the metric
        average (float): The integral average value in the aggregate period
        max (float): The maximum value in the aggregate period
        min (float): The minimum value in the aggregate period
        count (int): The number of datapoints in the aggregate period
        sum (float): The sum of the datapoints in the aggregate period
        interpolation (float): The interpolated value of the series in the beginning of the aggregate
        step_interpolation (float): The last value before or at the beginning of the aggregate.
        continuous_variance (float): The variance of the interpolated underlying function.
        discrete_variance (float): The variance of the datapoint values.
        total_variation (float): The total variation of the interpolated underlying function.
    """

    def __init__(
        self,
        timestamp: Union[int, float] = None,
        value: Union[str, int, float] = None,
        average: float = None,
        max: float = None,
        min: float = None,
        count: int = None,
        sum: float = None,
        interpolation: float = None,
        step_interpolation: float = None,
        continuous_variance: float = None,
        discrete_variance: float = None,
        total_variation: float = None,
    ):
        self.timestamp = timestamp
        self.value = value
        self.average = average
        self.max = max
        self.min = min
        self.count = count
        self.sum = sum
        self.interpolation = interpolation
        self.step_interpolation = step_interpolation
        self.continuous_variance = continuous_variance
        self.discrete_variance = discrete_variance
        self.total_variation = total_variation


class Datapoints:
    """An object representing a list of datapoints.

    Args:
        id (int): Id of the timeseries the datapoints belong to
        external_id (str): External id of the timeseries the datapoints belong to (Only if id is not set)
        timestamp (List[Union[int, float]]): The data timestamp in milliseconds since the epoch (Jan 1, 1970).
        value (List[Union[int, str, float]]): The data value. Can be String or numeric depending on the metric
        average (List[float]): The integral average value in the aggregate period
        max (List[float]): The maximum value in the aggregate period
        min (List[float]): The minimum value in the aggregate period
        count (List[int]): The number of datapoints in the aggregate period
        sum (List[float]): The sum of the datapoints in the aggregate period
        interpolation (List[float]): The interpolated value of the series in the beginning of the aggregate
        step_interpolation (List[float]): The last value before or at the beginning of the aggregate.
        continuous_variance (List[float]): The variance of the interpolated underlying function.
        discrete_variance (List[float]): The variance of the datapoint values.
        total_variation (List[float]): The total variation of the interpolated underlying function.
    """

    def __init__(
        self,
        id: int = None,
        external_id: str = None,
        timestamp: List[Union[int, float]] = None,
        value: List[Union[int, str, float]] = None,
        average: List[float] = None,
        max: List[float] = None,
        min: List[float] = None,
        count: List[int] = None,
        sum: List[float] = None,
        interpolation: List[float] = None,
        step_interpolation: List[float] = None,
        continuous_variance: List[float] = None,
        discrete_variance: List[float] = None,
        total_variation: List[float] = None,
    ):
        self.id = id
        self.external_id = external_id
        self.timestamp = timestamp
        self.value = value
        self.average = average
        self.max = max
        self.min = min
        self.count = count
        self.sum = sum
        self.interpolation = interpolation
        self.step_interpolation = step_interpolation
        self.continuous_variance = continuous_variance
        self.discrete_variance = discrete_variance
        self.total_variation = total_variation

        self.__datapoint_objects = None

    def __str__(self):
        return json.dumps(self.dump(), indent=4)

    def __repr__(self):
        return self.__str__()

    def __len__(self) -> int:
        return len(self.timestamp)

    def __getitem__(self, item) -> Union[Datapoint, List[Datapoint]]:
        return self.__get_datapoint_objects()[item]

    def __iter__(self) -> Datapoint:
        for dp in self.__get_datapoint_objects():
            yield dp

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        dumped = {
            "id": self.id,
            "external_id": self.external_id,
            "datapoints": [dp.dump(camel_case=camel_case) for dp in self.__get_datapoint_objects()],
        }
        if camel_case:
            dumped = {utils.to_camel_case(key): value for key, value in dumped.items()}
        return {key: value for key, value in dumped.items() if value is not None}

    @classmethod
    def _load(cls, dps_object):
        instance = cls()
        instance.id = dps_object["id"]
        instance.external_id = dps_object["externalId"]
        for dp in dps_object["datapoints"]:
            for key, value in dp.items():
                snake_key = utils.to_snake_case(key)
                current_attr = getattr(instance, snake_key) or []
                current_attr.append(value)
                setattr(instance, snake_key, current_attr)
        return instance

    def _get_operative_attr_names(self) -> List[str]:
        attrs = []
        for attr in self.__dict__.copy():
            if attr not in ["id", "external_id", "_Datapoints__datapoint_objects"] and getattr(self, attr) is not None:
                attrs.append(attr)
        return attrs

    def __get_datapoint_objects(self) -> List[Datapoint]:
        if self.__datapoint_objects is None:
            self.__datapoint_objects = []
            for i in range(len(self)):
                dp_args = {}
                for attr in self._get_operative_attr_names():
                    dp_args[attr] = getattr(self, attr)[i]
                self.__datapoint_objects.append(Datapoint(**dp_args))
        return self.__datapoint_objects

    def _truncate(self, limit: int):
        truncated_datapoints = Datapoints(id=self.id, external_id=self.external_id)
        for attr in self._get_operative_attr_names():
            setattr(truncated_datapoints, attr, getattr(self, attr)[:limit])
        return truncated_datapoints


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
    ) -> Union[Datapoints, DatapointsList]:
        start = utils.timestamp_to_ms(start)
        end = utils.timestamp_to_ms(end)
        all_ids, is_single_id = self._process_ids(id, external_id, wrap_ids=True)

        max_workers_per_ts = max(self._max_workers // len(all_ids), 1)
        if include_outside_points:
            max_workers_per_ts = 1

        tasks = []
        for id_object in all_ids:
            tasks.append(
                (start, end, id_object, aggregates, granularity, include_outside_points, limit, max_workers_per_ts)
            )

        results = utils.execute_tasks_concurrently(
            self._get_datapoints_concurrently, tasks, max_workers=min(self._max_workers, len(all_ids))
        )

        if limit:
            for i, dps_res in enumerate(results):
                results[i] = dps_res._truncate(limit=limit)

        dps_list = DatapointsList(results)

        if is_single_id:
            return dps_list[0]
        return dps_list

    def _get_datapoints_concurrently(
        self,
        start: int,
        end: int,
        identifier: Dict[str, Union[str, int]],
        aggregates: List[str],
        granularity: str,
        include_outside_points: bool,
        limit: int,
        max_workers: int,
    ) -> Datapoints:
        windows = self._get_windows(start, end, granularity=granularity, max_windows=max_workers)
        tasks = [(w.start, w.end, identifier, aggregates, granularity, include_outside_points, limit) for w in windows]
        dps_objects = utils.execute_tasks_concurrently(self._get_datapoints_with_paging, tasks, max_workers=max_workers)
        return self._concatenate_datapoints(*dps_objects)

    def _get_datapoints_with_paging(
        self,
        start: int,
        end: int,
        identifier: Dict[str, Union[str, int]],
        aggregates: List[str],
        granularity: str,
        include_outside_points: bool,
        limit: int,
    ) -> Datapoints:
        per_request_limit = self._LIMIT_AGG if aggregates else self._LIMIT
        next_start = start
        last_received_datapoints = Datapoints(timestamp=[])
        datapoints = Datapoints(timestamp=[])
        while (
            (len(datapoints) == 0 or len(last_received_datapoints) == per_request_limit)
            and end > next_start
            and len(datapoints) < (limit or float("inf"))
        ):
            last_received_datapoints = self._get_datapoints(
                next_start, end, identifier, aggregates, granularity, include_outside_points
            )
            latest_timestamp = int(last_received_datapoints.timestamp[-1])
            next_start = latest_timestamp + (utils.granularity_to_ms(granularity) if granularity else 1)
            datapoints.id = last_received_datapoints.id
            datapoints.external_id = last_received_datapoints.external_id
            datapoints = self._concatenate_datapoints(datapoints, last_received_datapoints)
        return datapoints

    def _get_datapoints(
        self,
        start: int,
        end: int,
        identifier: Dict[str, Union[str, int]],
        aggregates: List[str],
        granularity: str,
        include_outside_points: bool,
    ) -> Datapoints:
        payload = {
            "items": [identifier],
            "start": start,
            "end": end,
            "aggregates": aggregates,
            "granularity": granularity,
            "includeOutsidePoints": include_outside_points,
            "limit": self._LIMIT_AGG if aggregates else self._LIMIT,
        }
        res = self._post(self._RESOURCE_PATH + "/get", json=payload).json()["data"]["items"][0]
        return Datapoints._load(res)

    @staticmethod
    def _concatenate_datapoints(*dps_objects: Datapoints) -> Datapoints:
        # assert 1 == len(set([dps.id for dps in dps_objects]))
        # assert 1 == len(set([dps.external_id for dps in dps_objects]))
        concat_dps_object = Datapoints(id=dps_objects[0].id, external_id=dps_objects[0].external_id)
        for dps in dps_objects:
            for attr in dps._get_operative_attr_names():
                current = getattr(concat_dps_object, attr) or []
                current.extend(getattr(dps, attr))
                setattr(concat_dps_object, attr, current)
        return concat_dps_object

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

    def get_latest(
        self,
        id: Union[int, List[int]] = None,
        external_id: Union[str, List[str]] = None,
        before: Union[int, str, datetime] = None,
    ):
        before = utils.timestamp_to_ms(before) if before else None
        all_ids, is_single_id = self._process_ids(id, external_id, wrap_ids=True)
        if before:
            for id in all_ids:
                id.update({"before": before})

        res = self._post(url_path=self._RESOURCE_PATH + "/latest", json={"items": all_ids}).json()["data"]["items"]
        if is_single_id:
            return Datapoints._load(res[0])
        return DatapointsList._load(res)

    def get_dataframe(self):
        pass

    def insert(self):
        pass

    def delete(self):
        pass
