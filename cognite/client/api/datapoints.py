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
        timestamp (List[Union[int, float]]): The data timestamps in milliseconds since the epoch (Jan 1, 1970).
        value (List[Union[int, str, float]]): The data values. Can be String or numeric depending on the metric
        average (List[float]): The integral average values in the aggregate period
        max (List[float]): The maximum values in the aggregate period
        min (List[float]): The minimum values in the aggregate period
        count (List[int]): The number of datapoints in the aggregate periods
        sum (List[float]): The sum of the datapoints in the aggregate periods
        interpolation (List[float]): The interpolated values of the series in the beginning of the aggregates
        step_interpolation (List[float]): The last values before or at the beginning of the aggregates.
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
        self.timestamp = timestamp or []
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

    def _get_operative_attrs(self) -> Generator[Tuple[str, Any], None, None]:
        for attr, value in self.__dict__.copy().items():
            if attr not in ["id", "external_id", "_Datapoints__datapoint_objects"] and value is not None:
                yield attr, value

    def __get_datapoint_objects(self) -> List[Datapoint]:
        if self.__datapoint_objects is None:
            self.__datapoint_objects = []
            for i in range(len(self)):
                dp_args = {}
                for attr, value in self._get_operative_attrs():
                    dp_args[attr] = getattr(self, attr)[i]
                self.__datapoint_objects.append(Datapoint(**dp_args))
        return self.__datapoint_objects

    def _truncate(self, limit: int):
        truncated_datapoints = Datapoints(id=self.id, external_id=self.external_id)
        for attr, value in self._get_operative_attrs():
            setattr(truncated_datapoints, attr, value[:limit])
        return truncated_datapoints


class DatapointsList(CogniteResourceList):
    _RESOURCE = Datapoints


class DatapointsQuery(CogniteResource):
    """Parameters describing a query for datapoints.

    Args:
        start (Union[str, int, datetime]): Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since epoch.
        end (Union[str, int, datetime]): Get datapoints up to this time. The format is the same as for start.
        id (int): Id of the timeseries to query
        external_id (str): External id of the timeseries to query (Only if id is not set)
        limit (int): Return up to this number of datapoints.
        aggregates (List[str]): The aggregates to be returned.  Use default if null. An empty string must be sent to get raw data if the default is a set of aggregates.
        granularity (str): The granularity size and granularity of the aggregates.
        include_outside_points (bool): Whether to include the last datapoint before the requested time period,and the first one after the requested period. This can be useful for interpolating data. Not available for aggregates.
    """

    def __init__(
        self,
        start: Union[str, int, datetime],
        end: Union[str, int, datetime],
        id: int = None,
        external_id: str = None,
        limit: int = None,
        aggregates: List[str] = None,
        granularity: str = None,
        include_outside_points: bool = None,
    ):
        self.id = id
        self.external_id = external_id
        self.start = start
        self.end = end
        self.limit = limit
        self.aggregates = aggregates
        self.granularity = granularity
        self.include_outside_points = include_outside_points


_DPWindow = namedtuple("Window", ["start", "end"])


class DatapointsAPI(APIClient):
    _LIMIT_AGG = 10000
    _LIMIT = 100000
    _RESOURCE_PATH = "/timeseries/data"

    def get(
        self,
        start: Union[int, str, datetime],
        end: Union[int, str, datetime],
        id: Union[int, List[int], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]] = None,
        external_id: Union[
            int, List[int], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]
        ] = None,
        aggregates: List[str] = None,
        granularity: str = None,
        include_outside_points: bool = None,
        limit: int = None,
    ) -> Union[Datapoints, DatapointsList]:
        start = utils.timestamp_to_ms(start)
        end = utils.timestamp_to_ms(end)
        ts_items, is_single_id = self._process_ts_identifiers(id, external_id)

        max_workers_per_ts = max(self._max_workers // len(ts_items), 1)
        if include_outside_points:
            max_workers_per_ts = 1

        tasks = []
        for ts_item in ts_items:
            tasks.append(
                (start, end, ts_item, aggregates, granularity, include_outside_points, limit, max_workers_per_ts)
            )

        results = utils.execute_tasks_concurrently(
            self._get_datapoints_concurrently, tasks, max_workers=min(self._max_workers, len(ts_items))
        )

        if limit:
            for i, dps_res in enumerate(results):
                results[i] = dps_res._truncate(limit=limit)

        dps_list = DatapointsList(results)

        if is_single_id:
            return dps_list[0]
        return dps_list

    def get_latest(
        self,
        id: Union[int, List[int]] = None,
        external_id: Union[str, List[str]] = None,
        before: Union[int, str, datetime] = None,
    ) -> Union[Datapoints, DatapointsList]:
        before = utils.timestamp_to_ms(before) if before else None
        all_ids, is_single_id = self._process_ids(id, external_id, wrap_ids=True)
        if before:
            for id in all_ids:
                id.update({"before": before})

        res = self._post(url_path=self._RESOURCE_PATH + "/latest", json={"items": all_ids}).json()["data"]["items"]
        if is_single_id:
            return Datapoints._load(res[0])
        return DatapointsList._load(res)

    def query(self, query: Union[DatapointsQuery, List[DatapointsQuery]]) -> Union[Datapoints, DatapointsList]:
        is_single_query = False
        if isinstance(query, DatapointsQuery):
            is_single_query = True
            query = [query]

        tasks = []
        for q in query:
            tasks.append((q.start, q.end, q.id, q.external_id, q.aggregates, q.granularity, q.include_outside_points))
        results = utils.execute_tasks_concurrently(self.get, tasks, max_workers=self._max_workers)
        if is_single_query:
            return results[0]
        return DatapointsList(results)

    def insert(
        self,
        datapoints: Union[
            List[Dict[Union[int, float, datetime], Union[int, float, str]]],
            List[Tuple[Union[int, float, datetime], Union[int, float, str]]],
        ],
        id: int = None,
        external_id: str = None,
    ):
        utils.assert_exactly_one_of_id_or_external_id(id, external_id)
        datapoints = self._validate_and_format_datapoints(datapoints)
        utils.assert_timestamp_not_in_jan_in_1970(datapoints[0]["timestamp"])
        if id:
            post_dps_object = {"id": id}
        else:
            post_dps_object = {"externalId": external_id}
        post_dps_object.update({"datapoints": datapoints})
        self._insert_datapoints_concurrently([post_dps_object])

    def insert_multiple(self, datapoints: List[Dict[str, Union[str, int, List]]]):
        valid_dps_objects = []
        for dps_object in datapoints:
            for key in dps_object:
                if key not in ("id", "externalId", "datapoints"):
                    raise AssertionError(
                        "Invalid key '{}' in datapoints. Must contain 'datapoints', and 'id' or 'externalId".format(key)
                    )
            valid_dps_object = dps_object.copy()
            valid_dps_object["datapoints"] = self._validate_and_format_datapoints(dps_object["datapoints"])
            valid_dps_objects.append(valid_dps_object)
        self._insert_datapoints_concurrently(valid_dps_objects)

    def delete_range(self):
        pass

    def delete_ranges(self):
        pass

    def get_dataframe(self):
        pass

    def insert_dataframe(self):
        pass

    def _get_datapoints_concurrently(
        self,
        start: int,
        end: int,
        ts_item: Dict[str, Any],
        aggregates: List[str],
        granularity: str,
        include_outside_points: bool,
        limit: int,
        max_workers: int,
    ) -> Datapoints:
        windows = self._get_windows(start, end, granularity=granularity, max_windows=max_workers)
        tasks = [(w.start, w.end, ts_item, aggregates, granularity, include_outside_points, limit) for w in windows]
        dps_objects = utils.execute_tasks_concurrently(self._get_datapoints_with_paging, tasks, max_workers=max_workers)
        return self._concatenate_datapoints(*dps_objects)

    def _get_datapoints_with_paging(
        self,
        start: int,
        end: int,
        ts_item: Dict[str, Any],
        aggregates: List[str],
        granularity: str,
        include_outside_points: bool,
        limit: int,
    ) -> Datapoints:
        per_request_limit = self._LIMIT_AGG if aggregates else self._LIMIT
        next_start = start
        datapoints = Datapoints(timestamp=[])
        concatenated_datapoints = Datapoints(timestamp=[])
        while (
            (len(concatenated_datapoints) == 0 or len(datapoints) == per_request_limit)
            and end > next_start
            and len(concatenated_datapoints) < (limit or float("inf"))
        ):
            datapoints = self._get_datapoints(next_start, end, ts_item, aggregates, granularity, include_outside_points)
            if len(datapoints) == 0:
                break
            latest_timestamp = int(datapoints.timestamp[-1])
            next_start = latest_timestamp + (utils.granularity_to_ms(granularity) if granularity else 1)
            concatenated_datapoints.id = datapoints.id
            concatenated_datapoints.external_id = datapoints.external_id
            concatenated_datapoints = self._concatenate_datapoints(concatenated_datapoints, datapoints)
        return concatenated_datapoints

    def _get_datapoints(
        self,
        start: int,
        end: int,
        ts_item: Dict[str, Any],
        aggregates: List[str],
        granularity: str,
        include_outside_points: bool,
    ) -> Datapoints:
        payload = {
            "items": [ts_item],
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
        assert 1 == len(set([dps.id for dps in dps_objects]))
        assert 1 == len(set([dps.external_id for dps in dps_objects]))
        concat_dps_object = Datapoints(id=dps_objects[0].id, external_id=dps_objects[0].external_id)
        for dps in dps_objects:
            for attr, value in dps._get_operative_attrs():
                current = getattr(concat_dps_object, attr) or []
                current.extend(value)
                setattr(concat_dps_object, attr, current)
        return concat_dps_object

    @staticmethod
    def _get_windows(start: int, end: int, granularity: str, max_windows: int) -> List[_DPWindow]:
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

    @staticmethod
    def _process_ts_identifiers(ids, external_ids) -> Tuple[List[Dict], bool]:
        is_list = False
        items = []

        if isinstance(ids, List):
            is_list = True
            for item in ids:
                items.append(DatapointsAPI._process_single_ts_item(item, False))
        elif ids is None:
            pass
        else:
            items.append(DatapointsAPI._process_single_ts_item(ids, False))

        if isinstance(external_ids, List):
            is_list = True
            for item in external_ids:
                items.append(DatapointsAPI._process_single_ts_item(item, True))
        elif external_ids is None:
            pass
        else:
            items.append(DatapointsAPI._process_single_ts_item(external_ids, True))

        return items, not is_list and len(items) == 1

    @staticmethod
    def _process_single_ts_item(item, external: bool):
        item_type = "externalId" if external else "id"
        id_type = str if external else int
        if isinstance(item, id_type):
            return {item_type: item}
        elif isinstance(item, Dict):
            for key in item:
                if not key in [item_type, "aggregates"]:
                    raise ValueError("Unknown key '{}' in {} dict argument".format(key, item_type))
            if not item_type in item:
                raise ValueError(
                    "When passing a dict to the {} argument, '{}' must be specified.".format(item_type, item_type)
                )
            return item
        raise TypeError("Invalid type '{}' for argument '{}'".format(type(item), item_type))

    def _insert_datapoints_concurrently(self, post_dps_objects: List[Dict[str, Any]]):
        tasks = []
        for dps_object in post_dps_objects:
            for i in range(0, len(dps_object["datapoints"]), self._LIMIT):
                dps_object_chunk = dps_object.copy()
                dps_object_chunk["datapoints"] = dps_object["datapoints"][i : i + self._LIMIT]
                tasks.append((dps_object_chunk,))
        utils.execute_tasks_concurrently(self._insert_datapoints, tasks, max_workers=self._max_workers)

    def _insert_datapoints(self, post_dps_objects: List[Dict[str, Any]]):
        self._post(url_path=self._RESOURCE_PATH, json={"items": post_dps_objects})

    def _validate_and_format_datapoints(
        self,
        datapoints: Union[
            List[Dict[Union[int, float, datetime], Union[int, float, str]]],
            List[Tuple[Union[int, float, datetime], Union[int, float, str]]],
        ],
    ) -> List[Dict[str, int]]:
        utils.assert_type(datapoints, "datapoints", list)
        assert len(datapoints) > 0, "No datapoints provided"
        utils.assert_type(datapoints[0], "datapoints element", tuple, dict)

        valid_datapoints = []
        if isinstance(datapoints[0], tuple):
            valid_datapoints = [{"timestamp": utils.timestamp_to_ms(t), "value": v} for t, v in datapoints]
        elif isinstance(datapoints[0], dict):
            for dp in datapoints:
                assert "timestamp" in dp, "A datapoint is missing the 'timestamp' key"
                assert "value" in dp, "A datapoint is missing the 'value' key"
                valid_datapoints.append({"timestamp": utils.timestamp_to_ms(dp["timestamp"]), "value": dp["value"]})
        return valid_datapoints
