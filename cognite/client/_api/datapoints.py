import math
import threading
from collections import defaultdict, namedtuple
from datetime import datetime
from typing import *

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Datapoints, DatapointsList, DatapointsQuery
from cognite.client.utils import _utils as utils


class DatapointsAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/data"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._DPS_LIMIT_AGG = 10000
        self._DPS_LIMIT = 100000

    def retrieve(
        self,
        start: Union[int, str, datetime],
        end: Union[int, str, datetime],
        id: Union[int, List[int], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]] = None,
        external_id: Union[
            str, List[str], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]
        ] = None,
        aggregates: List[str] = None,
        granularity: str = None,
        include_outside_points: bool = None,
        limit: int = None,
    ) -> Union[Datapoints, DatapointsList]:
        """Get datapoints for one or more time series

        Args:
            start (Union[int, str, datetime]): Inclusive start.
            end (Union[int, str, datetime]): Exclusive end.
            id (Union[int, List[int], Dict[str, Any], List[Dict[str, Any]]]: Id or list of ids. Can also be object
                specifying aggregates. See example below.
            external_id (Union[str, List[str], Dict[str, Any], List[Dict[str, Any]]]): External id or list of external
                ids. Can also be object specifying aggregates. See example below.
            aggregates (List[str]): List of aggregate functions to apply.
            granularity (str): The granularity to fetch aggregates at. e.g. '1s', '2h', '10d'.
            include_outside_points (bool): Whether or not to include outside points.
            limit (int): Maximum number of datapoints to return for each time series.

        Returns:
            Union[Datapoints, DatapointsList]: A Datapoints object containing the requested data, or a list of such objects.

        Examples:

            You can get specify the ids of the datapoints you wish to retrieve in a number of ways. In this example
            we are using the time-ago format to get raw data for the time series with id 1::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dps = c.datapoints.retrieve(id=1, start="2w-ago", end="now")

            We can also get aggregated values, such as average. Here we are getting daily averages for all of 2018 for
            two different time series. Note that we arefetching them using their external ids::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dps = c.datapoints.retrieve(external_id=["abc", "def"],
                ...                         start=datetime(2018,1,1),
                ...                         end=datetime(2019,1,1),
                ...                         aggregates=["avg"],
                ...                         granularity="1d")

            If you want different aggregates for different time series specify your ids like this::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dps = c.datapoints.retrieve(id=[{"id": 1, "aggregates": ["avg"]},
                ...                             {"id": 1, "aggregates": ["min"]}],
                ...                         external_id={"externalId": "1", "aggregates": ["max"]},
                ...                         start="1d-ago", end="now", granularity="1h")
        """
        ts_items, is_single_id = self._process_ts_identifiers(id, external_id)

        dps_queries = []
        for ts_item in ts_items:
            dps_queries.append(_DPQuery(start, end, ts_item, aggregates, granularity, include_outside_points, limit))

        dps_list = _DatapointsFetcher(client=self).fetch(dps_queries)

        if is_single_id:
            return dps_list[0]
        return dps_list

    def retrieve_latest(
        self,
        id: Union[int, List[int]] = None,
        external_id: Union[str, List[str]] = None,
        before: Union[int, str, datetime] = None,
    ) -> Union[Datapoints, DatapointsList]:
        """Get the latest datapoint for one or more time series

        Args:
            id (Union[int, List[int]]: Id or list of ids.
            external_id (Union[str, List[str]): External id or list of external ids.
            before: Union[int, str, datetime]: Get latest datapoint before this time.

        Returns:
            Union[Datapoints, DatapointsList]: A Datapoints object containing the requested data, or a list of such objects.

        Examples:

            Getting the latest datapoint in a time series. This method returns a Datapoints object, so the datapoint will
            be the first element::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datapoints.retrieve_latest(id=1)[0]

            You can also get the first datapoint before a specific time::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datapoints.retrieve_latest(id=1, before="2d-ago")[0]

            If you need the latest datapoint for multiple time series simply give a list of ids. Note that we are
            using external ids here, but either will work::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datapoints.retrieve_latest(external_id=["abc", "def"])
                >>> latest_abc = res[0][0]
                >>> latest_def = res[1][0]
        """
        before = utils.timestamp_to_ms(before) if before else None
        all_ids = self._process_ids(id, external_id, wrap_ids=True)
        is_single_id = self._is_single_identifier(id, external_id)
        if before:
            for id in all_ids:
                id.update({"before": before})

        res = self._post(url_path=self._RESOURCE_PATH + "/latest", json={"items": all_ids}).json()["items"]
        if is_single_id:
            return Datapoints._load(res[0], cognite_client=self._cognite_client)
        return DatapointsList._load(res, cognite_client=self._cognite_client)

    def query(self, query: Union[DatapointsQuery, List[DatapointsQuery]]) -> Union[Datapoints, DatapointsList]:
        """Get datapoints for one or more time series

        This method is different from get() in that you can specify different start times, end times, and granularities
        for each requested time series.

        Args:
            query (Union[DatapointsQuery, List[DatapointsQuery]): List of datapoint queries.

        Returns:
            Union[Datapoints, DatapointsList]: A Datapoints object containing the requested data, or a list of such objects.

        Examples:

            This method is useful if you want to get multiple time series, but you want to specify different starts,
            ends, or granularities for each. e.g.::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DatapointsQuery
                >>> c = CogniteClient()
                >>> queries = [DatapointsQuery(id=1, start="2d-ago", end="now"),
                ...             DatapointsQuery(external_id="abc",
                ...                             start="10d-ago",
                ...                             end="now",
                ...                             aggregates=["avg"],
                ...                             granularity="1m")]
                >>> res = c.datapoints.query(queries)
        """
        is_single_query = False
        if isinstance(query, DatapointsQuery):
            is_single_query = True
            query = [query]

        dps_queries = []
        for q in query:
            utils.assert_exactly_one_of_id_or_external_id(q.id, q.external_id)
            ts_items, _ = self._process_ts_identifiers(q.id, q.external_id)
            dps_queries.append(
                _DPQuery(q.start, q.end, ts_items[0], q.aggregates, q.granularity, q.include_outside_points, q.limit)
            )
        results = _DatapointsFetcher(self).fetch(dps_queries)
        if is_single_query:
            return results[0]
        return results

    def insert(
        self,
        datapoints: Union[
            List[Dict[Union[int, float, datetime], Union[int, float, str]]],
            List[Tuple[Union[int, float, datetime], Union[int, float, str]]],
        ],
        id: int = None,
        external_id: str = None,
    ) -> None:
        """Insert datapoints into a time series

        Timestamps can be represented as milliseconds since epoch or datetime objects.

        Args:
            datapoints(Union[List[Dict, Tuple]]): The datapoints you wish to insert. Can either be a list of tuples or
                a list of dictionaries. See examples below.
            id (int): Id of time series to insert datapoints into.
            external_id (str): External id of time series to insert datapoint into.

        Returns:
            None

        Examples:

            Your datapoints can be a list of tuples where the first element is the timestamp and the second element is
            the value::


                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> # with datetime objects
                >>> datapoints = [(datetime(2018,1,1), 1000), (datetime(2018,1,2), 2000)]
                >>> res1 = c.datapoints.insert(datapoints, id=1)
                >>> # with ms since epoch
                >>> datapoints = [(150000000000, 1000), (160000000000, 2000)]
                >>> res2 = c.datapoints.insert(datapoints, id=2)

            Or they can be a list of dictionaries::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> # with datetime objects
                >>> datapoints = [{"timestamp": datetime(2018,1,1), "value": 1000},
                ...    {"timestamp": datetime(2018,1,2), "value": 2000}]
                >>> res1 = c.datapoints.insert(datapoints, external_id="abc")
                >>> # with ms since epoch
                >>> datapoints = [{"timestamp": 150000000000, "value": 1000},
                ...    {"timestamp": 160000000000, "value": 2000}]
                >>> res2 = c.datapoints.insert(datapoints, external_id="def")
        """
        utils.assert_exactly_one_of_id_or_external_id(id, external_id)
        datapoints = self._validate_and_format_datapoints(datapoints)
        utils.assert_timestamp_not_in_jan_in_1970(datapoints[0]["timestamp"])
        post_dps_object = self._process_ids(id, external_id, wrap_ids=True)[0]
        post_dps_object.update({"datapoints": datapoints})
        self._insert_datapoints_concurrently([post_dps_object])

    def insert_multiple(self, datapoints: List[Dict[str, Union[str, int, List]]]) -> None:
        """Insert datapoints into multiple time series

        Args:
            datapoints (List[Dict]): The datapoints you wish to insert along with the ids of the time series.
                See examples below.

        Returns:
            None

        Examples:

            Your datapoints can be a list of tuples where the first element is the timestamp and the second element is
            the value::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()

                >>> datapoints = []
                >>> # with datetime objects and id
                >>> datapoints.append({"id": 1, "datapoints": [(datetime(2018,1,1), 1000), (datetime(2018,1,2), 2000)]})
                >>> # with ms since epoch and externalId
                >>> datapoints.append({"externalId": 1, "datapoints": [(150000000000, 1000), (160000000000, 2000)]})

                >>> res = c.datapoints.insert_multiple(datapoints)

            Or they can be a list of dictionaries::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()

                >>> datapoints = []
                >>> # with datetime objects and external id
                >>> datapoints.append({"externalId": "1", "datapoints": [{"timestamp": datetime(2018,1,1), "value": 1000},
                ...                     {"timestamp": datetime(2018,1,2), "value": 2000}]})
                >>> # with ms since epoch and id
                >>> datapoints.append({"id": 1, "datapoints": [{"timestamp": 150000000000, "value": 1000},
                ...                     {"timestamp": 160000000000, "value": 2000}]})

                >>> res = c.datapoints.insert_multiple(datapoints)
        """
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

    def delete_range(
        self, start: Union[int, str, datetime], end: Union[int, str, datetime], id: int = None, external_id: str = None
    ) -> None:
        """Delete a range of datapoints from a time series.

        Args:
            start (Union[int, str, datetime]): Inclusive start of delete range
            end (Union[int, str, datetime]): Exclusvie end of delete range
            id (int): Id of time series to delete data from
            external_id (str): External id of time series to delete data from

        Returns:
            None

        Examples:

            Deleting the last week of data from a time series::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datapoints.delete_range(start="1w-ago", end="now", id=1)
        """
        utils.assert_exactly_one_of_id_or_external_id(id, external_id)
        start = utils.timestamp_to_ms(start)
        end = utils.timestamp_to_ms(end)
        assert end > start, "end must be larger than start"

        delete_dps_object = self._process_ids(id, external_id, wrap_ids=True)[0]
        delete_dps_object.update({"inclusiveBegin": start, "exclusiveEnd": end})
        self._delete_datapoints_ranges([delete_dps_object])

    def delete_ranges(self, ranges: List[Dict[str, Any]]) -> None:
        """Delete a range of datapoints from multiple time series.

        Args:
            ranges (List[Dict[str, Any]]): The ids an ranges to delete. See examples below.

        Returns:
            None

        Examples:

            Each element in the list ranges must be specify either id or externalId, and a range::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> ranges = [{"id": 1, "start": "2d-ago", "end": "now"},
                ...             {"externalId": "abc", "start": "2d-ago", "end": "now"}]
                >>> res = c.datapoints.delete_ranges(ranges)
        """
        valid_ranges = []
        for range in ranges:
            for key in range:
                if key not in ("id", "externalId", "start", "end"):
                    raise AssertionError(
                        "Invalid key '{}' in range. Must contain 'start', 'end', and 'id' or 'externalId".format(key)
                    )
            id = range.get("id")
            external_id = range.get("externalId")
            utils.assert_exactly_one_of_id_or_external_id(id, external_id)
            valid_range = self._process_ids(id, external_id, wrap_ids=True)[0]
            valid_range.update({"inclusiveBegin": range["start"], "exclusiveEnd": range["end"]})
            valid_ranges.append(valid_range)
        self._delete_datapoints_ranges(valid_ranges)

    def _delete_datapoints_ranges(self, delete_range_objects):
        self._post(url_path=self._RESOURCE_PATH + "/delete", json={"items": delete_range_objects})

    def retrieve_dataframe(
        self,
        start: Union[int, str, datetime],
        end: Union[int, str, datetime],
        aggregates: List[str],
        granularity: str,
        id: Union[int, List[int], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]] = None,
        external_id: Union[
            str, List[str], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]
        ] = None,
        limit: int = None,
    ):
        """Get a pandas dataframe describing the requested data.

        Args:
            start (Union[int, str, datetime]): Inclusive start.
            end (Union[int, str, datetime]): Exclusive end.
            aggregates (List[str]): List of aggregate functions to apply.
            granularity (str): The granularity to fetch aggregates at. e.g. '1s', '2h', '10d'.
            id (Union[int, List[int], Dict[str, Any], List[Dict[str, Any]]]: Id or list of ids. Can also be object
                specifying aggregates. See example below.
            external_id (Union[str, List[str], Dict[str, Any], List[Dict[str, Any]]]): External id or list of external
                ids. Can also be object specifying aggregates. See example below.
            limit (int): Maximum number of datapoints to return for each time series.

        Returns:
            pandas.DataFrame: The requested dataframe

        Examples:

            Get a pandas dataframe::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> df = c.datapoints.retrieve_dataframe(id=[1,2,3], start="2w-ago", end="now",
                ...         aggregates=["average"], granularity="1h")
        """
        utils.local_import("pandas")
        return self.retrieve(
            id=id,
            external_id=external_id,
            start=start,
            end=end,
            aggregates=aggregates,
            granularity=granularity,
            limit=limit,
        ).to_pandas()

    def insert_dataframe(self, dataframe):
        """Insert a dataframe.

        The index of the dataframe must contain the timestamps. The names of the remaining columns specify the ids of
        the time series to which column contents will be written.

        Said time series must already exist.

        Args:
            dataframe (pandas.DataFrame):  Pandas DataFrame Object containing the time series.

        Returns:
            None

        Examples:
            Post a dataframe with white noise::

                >>> import numpy as np
                >>> import pandas as pd
                >>> from cognite.client import CogniteClient
                >>> from datetime import datetime, timedelta
                >>>
                >>> c = CogniteClient()
                >>> ts_id = 123
                >>> start = datetime(2018, 1, 1)
                >>> # The scaling by 1000 is important: timestamp() returns seconds
                >>> x = pd.DatetimeIndex([start + timedelta(days=d) for d in range(100)])
                >>> y = np.random.normal(0, 1, 100)
                >>> df = pd.DataFrame({ts_id: y}, index=x)
                >>> res = c.datapoints.insert_dataframe(df)
        """
        assert not dataframe.isnull().values.any(), "Dataframe contains NaNs. Remove them in order to insert the data."
        dps = []
        for col in dataframe.columns:
            dps.append(
                {
                    "id": int(col),
                    "datapoints": list(
                        zip(dataframe.index.values.astype("datetime64[ms]").astype("int64").tolist(), dataframe[col])
                    ),
                }
            )
        self.insert_multiple(dps)

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
            for i in range(0, len(dps_object["datapoints"]), self._DPS_LIMIT):
                dps_object_chunk = dps_object.copy()
                dps_object_chunk["datapoints"] = dps_object["datapoints"][i : i + self._DPS_LIMIT]
                tasks.append(([dps_object_chunk],))
        utils.execute_tasks_concurrently(self._insert_datapoints, tasks, max_workers=self._max_workers)

    def _insert_datapoints(self, post_dps_objects: List[Dict[str, Any]]):
        self._post(url_path=self._RESOURCE_PATH, json={"items": post_dps_objects})

    @staticmethod
    def _validate_and_format_datapoints(
        datapoints: Union[
            List[Dict[Union[int, float, datetime], Union[int, float, str]]],
            List[Tuple[Union[int, float, datetime], Union[int, float, str]]],
        ],
    ) -> List[Dict[str, int]]:
        utils.assert_type(datapoints, "datapoints", [list])
        assert len(datapoints) > 0, "No datapoints provided"
        utils.assert_type(datapoints[0], "datapoints element", [tuple, dict])

        valid_datapoints = []
        if isinstance(datapoints[0], tuple):
            valid_datapoints = [{"timestamp": utils.timestamp_to_ms(t), "value": v} for t, v in datapoints]
        elif isinstance(datapoints[0], dict):
            for dp in datapoints:
                assert "timestamp" in dp, "A datapoint is missing the 'timestamp' key"
                assert "value" in dp, "A datapoint is missing the 'value' key"
                valid_datapoints.append({"timestamp": utils.timestamp_to_ms(dp["timestamp"]), "value": dp["value"]})
        return valid_datapoints


_DPWindow = namedtuple("_DPWindow", ["start", "end"])


class _DPQuery:
    def __init__(self, start, end, ts_item, aggregates, granularity, include_outside_points, limit):
        self.start = utils.timestamp_to_ms(start)
        self.end = utils.timestamp_to_ms(end)
        self.ts_item = ts_item
        self.aggregates = aggregates
        self.granularity = granularity
        self.include_outside_points = include_outside_points
        self.limit = limit
        self.dps_result = None

    def as_tuple(self):
        return (
            self.start,
            self.end,
            self.ts_item,
            self.aggregates,
            self.granularity,
            self.include_outside_points,
            self.limit,
        )


class _DatapointsFetcher:
    def __init__(self, client: DatapointsAPI):
        self.client = client
        self.id_to_datapoints = defaultdict(lambda: Datapoints())
        self.id_to_finalized_query = {}
        self.lock = threading.Lock()

    def fetch(self, dps_queries: List[_DPQuery]) -> DatapointsList:
        self._preprocess_queries(dps_queries)
        self._fetch_datapoints(dps_queries)
        dps_list = self._get_dps_results()
        return dps_list

    def _preprocess_queries(self, queries: List[_DPQuery]):
        for query in queries:
            new_start = utils.timestamp_to_ms(query.start)
            new_end = utils.timestamp_to_ms(query.end)
            if query.aggregates:
                new_start = self._align_with_granularity_unit(new_start, query.granularity)
                new_end = self._align_with_granularity_unit(new_end, query.granularity)
            query.start = new_start
            query.end = new_end

    def _get_dps_results(self):
        dps_list = DatapointsList([], cognite_client=self.client._cognite_client)
        for id, dps in self.id_to_datapoints.items():
            finalized_query = self.id_to_finalized_query[id]
            if finalized_query.include_outside_points:
                dps = self._remove_duplicates(dps)
            if finalized_query.limit and len(dps) > finalized_query.limit:
                dps = dps[: finalized_query.limit]
            dps_list.append(dps)
        return dps_list

    def _fetch_datapoints(self, dps_queries: List[_DPQuery]):
        tasks_summary = utils.execute_tasks_concurrently(
            self._fetch_dps_initial_and_return_remaining_queries,
            [(q,) for q in dps_queries],
            max_workers=self.client._max_workers,
        )
        if tasks_summary.exceptions:
            raise tasks_summary.exceptions[0]

        remaining_queries = tasks_summary.joined_results()
        if len(remaining_queries) > 0:
            self._fetch_datapoints_for_remaining_queries(remaining_queries)

    def _fetch_dps_initial_and_return_remaining_queries(self, query: _DPQuery) -> List[_DPQuery]:
        request_limit = self.client._DPS_LIMIT if query.aggregates is None else self.client._DPS_LIMIT_AGG
        if query.limit is not None and query.limit <= request_limit:
            query.dps_result = self._get_datapoints_with_paging(*query.as_tuple())
            self._store_finalized_query(query)
            return []
        user_limit = query.limit
        query.limit = None
        query.dps_result = self._get_datapoints(*query.as_tuple())
        query.limit = user_limit
        self._store_finalized_query(query)

        dps_in_first_query = len(query.dps_result)
        if dps_in_first_query < request_limit:
            return []

        if user_limit:
            user_limit -= dps_in_first_query
        next_start_offset = utils.granularity_to_ms(query.granularity) if query.granularity else 1
        query.start = query.dps_result[-1].timestamp + next_start_offset
        queries = self._split_query_into_windows(query.dps_result.id, query, request_limit, user_limit)

        return queries

    def _fetch_datapoints_for_remaining_queries(self, queries: List[_DPQuery]):
        tasks_summary = utils.execute_tasks_concurrently(
            self._get_datapoints_with_paging, [q.as_tuple() for q in queries], max_workers=self.client._max_workers
        )
        if tasks_summary.exceptions:
            raise tasks_summary.exceptions[0]
        res_list = tasks_summary.results
        for i, res in enumerate(res_list):
            queries[i].dps_result = res
            self._store_finalized_query(queries[i])

    def _store_finalized_query(self, query: _DPQuery):
        with self.lock:
            self.id_to_datapoints[query.dps_result.id]._insert(query.dps_result)
            self.id_to_finalized_query[query.dps_result.id] = query

    @staticmethod
    def _align_with_granularity_unit(ts: int, granularity: str):
        gms = utils.granularity_unit_to_ms(granularity)
        if ts % gms == 0:
            return ts
        return ts - (ts % gms) + gms

    def _split_query_into_windows(self, id: int, query: _DPQuery, request_limit, user_limit):
        windows = self._get_windows(id, query.start, query.end, query.granularity, request_limit, user_limit)
        return [
            _DPQuery(
                w.start,
                w.end,
                query.ts_item,
                query.aggregates,
                query.granularity,
                query.include_outside_points,
                query.limit,
            )
            for w in windows
        ]

    def _get_windows(self, id, start, end, granularity, request_limit, user_limit):
        count_granularity = "1d"
        if granularity and utils.granularity_to_ms("1d") < utils.granularity_to_ms(granularity):
            count_granularity = granularity
        res = self._get_datapoints_with_paging(
            start=start,
            end=end,
            ts_item={"id": id},
            aggregates=["count"],
            granularity=count_granularity,
            include_outside_points=False,
            limit=None,
        )
        counts = list(zip(res.timestamp, res.count))
        windows = []
        total_count = 0
        current_window_count = 0
        window_start = start
        granularity_ms = utils.granularity_to_ms(granularity) if granularity else None
        agg_count = lambda count: int(
            min(math.ceil(utils.granularity_to_ms(count_granularity) / granularity_ms), count)
        )
        for i, (ts, count) in enumerate(counts):
            if i < len(counts) - 1:
                next_timestamp = counts[i + 1][0]
                next_raw_count = counts[i + 1][1]
                next_count = next_raw_count if granularity is None else agg_count(next_raw_count)
            else:
                next_timestamp = end
                next_count = 0
            current_count = count if granularity is None else agg_count(count)
            total_count += current_count
            current_window_count += current_count
            if current_window_count + next_count > request_limit or i == len(counts) - 1:
                window_end = next_timestamp
                if granularity:
                    window_end = self._align_window_end(start, next_timestamp, granularity)
                windows.append(_DPWindow(window_start, window_end))
                window_start = window_end
                current_window_count = 0
                if user_limit and total_count >= user_limit:
                    break
        return windows

    @staticmethod
    def _align_window_end(start: int, end: int, granularity: str):
        gms = utils.granularity_to_ms(granularity)
        diff = end - start
        end -= diff % gms
        return end

    @staticmethod
    def _remove_duplicates(dps_object: Datapoints) -> Datapoints:
        frequencies = defaultdict(lambda: [0, []])
        for i, timestamp in enumerate(dps_object.timestamp):
            frequencies[timestamp][0] += 1
            frequencies[timestamp][1].append(i)

        indices_to_remove = []
        for timestamp, freq in frequencies.items():
            if freq[0] > 1:
                indices_to_remove += freq[1][1:]

        dps_object_without_duplicates = Datapoints(id=dps_object.id, external_id=dps_object.external_id)
        for attr, values in dps_object._get_non_empty_data_fields():
            filtered_values = [elem for i, elem in enumerate(values) if i not in indices_to_remove]
            setattr(dps_object_without_duplicates, attr, filtered_values)

        return dps_object_without_duplicates

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
        per_request_limit = self.client._DPS_LIMIT_AGG if aggregates else self.client._DPS_LIMIT
        limit_next_request = per_request_limit
        next_start = start
        datapoints = Datapoints()
        all_datapoints = Datapoints()
        while (
            (len(all_datapoints) == 0 or len(datapoints) == per_request_limit)
            and end > next_start
            and len(all_datapoints) < (limit or float("inf"))
        ):
            datapoints = self._get_datapoints(
                next_start, end, ts_item, aggregates, granularity, include_outside_points, limit_next_request
            )
            if len(datapoints) == 0:
                break

            if limit:
                remaining_datapoints = limit - len(datapoints)
                if remaining_datapoints < per_request_limit:
                    limit_next_request = remaining_datapoints
            latest_timestamp = int(datapoints.timestamp[-1])
            next_start = latest_timestamp + (utils.granularity_to_ms(granularity) if granularity else 1)
            all_datapoints._insert(datapoints)
        return all_datapoints

    def _get_datapoints(
        self,
        start: int,
        end: int,
        ts_item: Dict[str, Any],
        aggregates: List[str],
        granularity: str,
        include_outside_points: bool,
        limit: int,
    ) -> Datapoints:
        payload = {
            "items": [ts_item],
            "start": start,
            "end": end,
            "aggregates": aggregates,
            "granularity": granularity,
            "includeOutsidePoints": include_outside_points,
            "limit": limit or (self.client._DPS_LIMIT_AGG if aggregates else self.client._DPS_LIMIT),
        }
        res = self.client._post(self.client._RESOURCE_PATH + "/list", json=payload).json()["items"][0]
        return Datapoints._load(res, cognite_client=self.client._cognite_client)
