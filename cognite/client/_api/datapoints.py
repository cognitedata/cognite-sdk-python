import threading
from collections import defaultdict, namedtuple
from datetime import datetime

from cognite.client._api_client import APIClient
from cognite.client._base import *


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

    def to_pandas(self) -> "pandas.DataFrame":
        """Convert the datapoint into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = utils.local_import("pandas")

        dumped = self.dump(camel_case=True)
        timestamp = dumped.pop("timestamp")

        for k, v in dumped.items():
            dumped[k] = [v]
        df = pd.DataFrame(dumped, index=[utils.ms_to_datetime(timestamp)])

        return df


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
        **kwargs
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
        self._client = None
        if "cognite_client" in kwargs:
            self._client = kwargs["cognite_client"]

    def __str__(self):
        return json.dumps(self.dump(), indent=4)

    def __repr__(self):
        return self.__str__()

    def __len__(self) -> int:
        return len(self.timestamp)

    def __eq__(self, other):
        return (
            type(self) == type(other)
            and self.id == other.id
            and self.external_id == other.external_id
            and list(self._get_non_empty_data_fields()) == list(other._get_non_empty_data_fields())
        )

    def __getitem__(self, item) -> Union[Datapoint, "Datapoints"]:
        if isinstance(item, slice):
            return self._slice(item)
        dp_args = {}
        for attr, values in self._get_non_empty_data_fields():
            dp_args[attr] = values[item]
        return Datapoint(**dp_args)

    def __iter__(self) -> Generator[Datapoint, None, None]:
        yield from self.__get_datapoint_objects()

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the datapoints into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            List[Dict[str, Any]]: A list of dicts representing the instance.
        """
        dumped = {
            "id": self.id,
            "external_id": self.external_id,
            "datapoints": [dp.dump(camel_case=camel_case) for dp in self.__get_datapoint_objects()],
        }
        if camel_case:
            dumped = {utils.to_camel_case(key): value for key, value in dumped.items()}
        return {key: value for key, value in dumped.items() if value is not None}

    def to_pandas(self) -> "pandas.DataFrame":
        """Convert the datapoints into a pandas DataFrame.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        np, pd = utils.local_import("numpy", "pandas")
        data_fields = {}
        timestamps = []
        identifier = self.id if self.id is not None else self.external_id
        for attr, value in self._get_non_empty_data_fields():
            if attr == "timestamp":
                timestamps = value
            else:
                id_with_agg = str(identifier)
                if attr != "value":
                    id_with_agg += "|{}".format(utils.to_camel_case(attr))
                data_fields[id_with_agg] = value
        return pd.DataFrame(data_fields, index=pd.DatetimeIndex(data=np.array(timestamps, dtype="datetime64[ms]")))

    def plot(self) -> None:
        """Plot the datapoints."""
        plt = utils.local_import("matplotlib.pyplot")
        self.to_pandas().plot()
        plt.show()

    @classmethod
    def _load(cls, dps_object, cognite_client=None):
        instance = cls(cognite_client=cognite_client)
        instance.id = dps_object["id"]
        instance.external_id = dps_object["externalId"]
        for dp in dps_object["datapoints"]:
            for key, value in dp.items():
                snake_key = utils.to_snake_case(key)
                current_attr = getattr(instance, snake_key) or []
                current_attr.append(value)
                setattr(instance, snake_key, current_attr)
        return instance

    def _insert(self, other_dps):
        if not other_dps.timestamp:
            return

        if self._client is None:
            self._client = other_dps._client
        if self.id is None and self.external_id is None:
            self.id = other_dps.id
            self.external_id = other_dps.external_id

        other_first_ts = other_dps.timestamp[0]
        index_to_split_on = None
        for i, ts in enumerate(self.timestamp):
            if ts > other_first_ts:
                index_to_split_on = i
                break

        for attr, other_value in other_dps._get_non_empty_data_fields():
            value = getattr(self, attr)
            if not value:
                setattr(self, attr, other_value)
            else:
                if index_to_split_on is not None:
                    new_value = value[:index_to_split_on] + other_value + value[index_to_split_on:]
                else:
                    new_value = value + other_value
                setattr(self, attr, new_value)

    def _get_non_empty_data_fields(self) -> Generator[Tuple[str, Any], None, None]:
        for attr, value in self.__dict__.copy().items():
            if attr not in ["id", "external_id", "_Datapoints__datapoint_objects", "_client"] and value is not None:
                yield attr, value

    def __get_datapoint_objects(self) -> List[Datapoint]:
        if self.__datapoint_objects is None:
            self.__datapoint_objects = []
            for i in range(len(self)):
                dp_args = {}
                for attr, value in self._get_non_empty_data_fields():
                    dp_args[attr] = value[i]
                self.__datapoint_objects.append(Datapoint(**dp_args))
        return self.__datapoint_objects

    def _slice(self, slice: slice):
        truncated_datapoints = Datapoints(id=self.id, external_id=self.external_id)
        for attr, value in self._get_non_empty_data_fields():
            setattr(truncated_datapoints, attr, value[slice])
        return truncated_datapoints


class DatapointsList(CogniteResourceList):
    _RESOURCE = Datapoints
    _ASSERT_CLASSES = False

    def to_pandas(self) -> "pandas.DataFrame":
        """Convert the datapoints list into a pandas DataFrame."""
        pd = utils.local_import("pandas")
        dfs = [df.to_pandas() for df in self.data]
        if dfs:
            return pd.concat(dfs, axis="columns")
        return pd.DataFrame()

    def plot(self) -> None:
        """Plot the list of datapoints."""
        plt = utils.local_import("matplotlib.pyplot")
        self.to_pandas().plot()
        plt.show()


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
                >>> dps = c.datapoints.get(id=1, start="2w-ago", end="now")

            We can also get aggregated values, such as average. Here we are getting daily averages for all of 2018 for
            two different time series. Note that we arefetching them using their external ids::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dps = c.datapoints.get(external_id=["abc", "def"],
                ...                         start=datetime(2018,1,1),
                ...                         end=datetime(2019,1,1),
                ...                         aggregates=["avg"],
                ...                         granularity="1d")

            If you want different aggregates for different time series specify your ids like this::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dps = c.datapoints.get(id=[{"id": 1, "aggregates": ["avg"]},
                ...                             {"id": 1, "aggregates": ["min"]}],
                ...                         external_id={"externalId": "1", "aggregates": ["max"]},
                ...                         start="1d-ago", end="now", granularity="1h")
        """
        ts_items, is_single_id = self._process_ts_identifiers(id, external_id)

        dps_queries = []
        for ts_item in ts_items:
            dps_queries.append(
                _DPQuery(
                    utils.timestamp_to_ms(start),
                    utils.timestamp_to_ms(end),
                    ts_item,
                    aggregates,
                    granularity,
                    include_outside_points,
                    limit,
                )
            )

        dps_list = _DatapointsFetcher(client=self).fetch(dps_queries)

        if is_single_id:
            return dps_list[0]
        return dps_list

    def get_latest(
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
                >>> res = c.datapoints.get_latest(id=1)[0]

            You can also get the first datapoint before a specific time::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datapoints.get_latest(id=1, before="2d-ago")[0]

            If you need the latest datapoint for multiple time series simply give a list of ids. Note that we are
            using external ids here, but either will work::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datapoints.get_latest(external_id=["abc", "def"])
                >>> latest_abc = res[0][0]
                >>> latest_def = res[1][0]
        """
        before = utils.timestamp_to_ms(before) if before else None
        all_ids = self._process_ids(id, external_id, wrap_ids=True)
        is_single_id = self._is_single_identifier(id, external_id)
        if before:
            for id in all_ids:
                id.update({"before": before})

        res = self._post(url_path=self._RESOURCE_PATH + "/latest", json={"items": all_ids}).json()["data"]["items"]
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
                _DPQuery(
                    utils.timestamp_to_ms(q.start),
                    utils.timestamp_to_ms(q.end),
                    ts_items[0],
                    q.aggregates,
                    q.granularity,
                    q.include_outside_points,
                    q.limit,
                )
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

    def get_dataframe(
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
                >>> df = c.datapoints.get_dataframe(id=[1,2,3], start="2w-ago", end="now",
                ...         aggregates=["average"], granularity="1h")
        """
        utils.local_import("pandas")
        return self.get(
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
            for i in range(0, len(dps_object["datapoints"]), self._LIMIT):
                dps_object_chunk = dps_object.copy()
                dps_object_chunk["datapoints"] = dps_object["datapoints"][i : i + self._LIMIT]
                tasks.append(([dps_object_chunk],))
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


_DPWindow = namedtuple("Window", ["start", "end"])


class _DPQuery:
    def __init__(self, start, end, ts_item, aggregates, granularity, include_outside_points, limit):
        self.start = start
        self.end = end
        self.ts_item = ts_item
        self.aggregates = aggregates
        self.granularity = granularity
        self.include_outside_points = include_outside_points
        self.limit = limit

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
        self.semaphore = threading.Semaphore(self.client._max_workers)
        self.lock = threading.Lock()
        self.id_to_datapoints = defaultdict(lambda: Datapoints())
        self.id_to_include_outside_dps = defaultdict(lambda: False)

    def fetch(self, dps_queries: List[_DPQuery]) -> DatapointsList:
        queries = []
        for dps_query in dps_queries:
            first_timestamp = self._get_first_timestamp(dps_query)
            if first_timestamp is not None:
                dps_query.start = first_timestamp

            if first_timestamp is not None and self._should_split(dps_query):
                split_queries = self._split_query_into_windows(dps_query)
                for query in split_queries:
                    queries.append((query,))
            else:
                queries.append((dps_query,))

        utils.execute_tasks_concurrently(self._do_dps_query, queries, max_workers=self.client._max_workers)

        dps_list = DatapointsList([], cognite_client=self.client._cognite_client)

        for id, dps in self.id_to_datapoints.items():
            if self.id_to_include_outside_dps[id]:
                dps = self._remove_duplicates(dps)
            dps_list.append(dps)

        return dps_list

    def _do_dps_query(self, query: _DPQuery):
        with self.semaphore:
            dps = self._get_datapoints_with_paging(*query.as_tuple())
        with self.lock:
            self.id_to_datapoints[dps.id]._insert(dps)
            self.id_to_include_outside_dps[dps.id] = query.include_outside_points

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
        per_request_limit = self.client._LIMIT_AGG if aggregates else self.client._LIMIT
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
            "limit": limit or (self.client._LIMIT_AGG if aggregates else self.client._LIMIT),
        }
        res = self.client._post(self.client._RESOURCE_PATH + "/get", json=payload).json()["data"]["items"][0]
        return Datapoints._load(res, cognite_client=self.client._cognite_client)

    def _should_split(self, query: _DPQuery):
        if query.limit is not None:
            return False
        return True

    def _split_query_into_windows(self, dps_query: _DPQuery) -> List[_DPQuery]:
        if dps_query.aggregates:
            dps_per_window = self.client._LIMIT_AGG * 5
        else:
            dps_per_window = self.client._LIMIT * 500
        windows = self._get_windows(dps_query.start, dps_query.end, dps_query.granularity, dps_per_window)
        return [
            _DPQuery(
                w.start,
                w.end,
                dps_query.ts_item,
                dps_query.aggregates,
                dps_query.granularity,
                dps_query.include_outside_points,
                dps_query.limit,
            )
            for w in windows
        ]

    def _get_first_timestamp(self, dps_query: _DPQuery) -> Union[int, None]:
        dps = self._get_datapoints(
            dps_query.start, dps_query.end, dps_query.ts_item, dps_query.aggregates, dps_query.granularity, False, 1
        )
        if len(dps) > 0:
            return dps[0].timestamp

    @staticmethod
    def _get_windows(start: int, end: int, granularity: str, dps_per_window: int) -> List[_DPWindow]:
        granularity_ms = utils.granularity_to_ms(granularity) if granularity else 1
        diff = end - start
        estimated_num_of_dps = diff // granularity_ms
        num_of_windows = max(1, int(estimated_num_of_dps // dps_per_window))

        windows = []
        next_start = start
        window_size = diff // num_of_windows
        next_end = next_start + window_size
        while (not windows or windows[-1].end < end) and next_start < next_end:
            windows.append(_DPWindow(start=next_start, end=next_end))
            next_start += window_size + granularity_ms
            next_end = next_start + window_size
            if next_end > end:
                next_end = end
        return windows

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
