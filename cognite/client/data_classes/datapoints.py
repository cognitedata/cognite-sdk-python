import collections
import re as regexp
from datetime import datetime
from typing import *

import cognite.client.utils._time
from cognite.client.data_classes._base import *
from cognite.client.exceptions import CogniteDuplicateColumnsError


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

    def to_pandas(self, camel_case=True) -> "pandas.DataFrame":
        """Convert the datapoint into a pandas DataFrame.
             camel_case (bool): Convert column names to camel case (e.g. `stepInterpolation` instead of `step_interpolation`)

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = utils._auxiliary.local_import("pandas")

        dumped = self.dump(camel_case=camel_case)
        timestamp = dumped.pop("timestamp")

        for k, v in dumped.items():
            dumped[k] = [v]
        df = pd.DataFrame(dumped, index=[cognite.client.utils._time.ms_to_datetime(timestamp)])

        return df


class Datapoints:
    """An object representing a list of datapoints.

    Args:
        id (int): Id of the timeseries the datapoints belong to
        external_id (str): External id of the timeseries the datapoints belong to (Only if id is not set)
        is_string (bool): Whether the time series is string valued or not.
        is_step (bool): Whether the time series is a step series or not.
        unit (str): The physical unit of the time series.
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
        is_string: bool = None,
        is_step: bool = None,
        unit: str = None,
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
        error: List[Union[None, str]] = None,
    ):
        self.id = id
        self.external_id = external_id
        self.is_string = is_string
        self.is_step = is_step
        self.unit = unit
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
        self.error = error

        self.__datapoint_objects = None

    def __str__(self):
        item = self.dump()
        item["datapoints"] = utils._time.convert_time_attributes_to_datetime(item["datapoints"])
        return json.dumps(item, indent=4)

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
            "is_string": self.is_string,
            "is_step": self.is_step,
            "unit": self.unit,
            "datapoints": [dp.dump(camel_case=camel_case) for dp in self.__get_datapoint_objects()],
        }
        if camel_case:
            dumped = {utils._auxiliary.to_camel_case(key): value for key, value in dumped.items()}
        return {key: value for key, value in dumped.items() if value is not None}

    def to_pandas(
        self, column_names: str = "externalId", include_aggregate_name: bool = True, include_errors: bool = False
    ) -> "pandas.DataFrame":
        """Convert the datapoints into a pandas DataFrame.

        Args:
            column_names (str):  Which field to use as column header. Defaults to "externalId", can also be "id".
            include_aggregate_name (bool): Include aggregate in the column name
            include_errors (bool): For synthetic datapoint queries, include a column with errors.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        np, pd = utils._auxiliary.local_import("numpy", "pandas")
        data_fields = {}
        timestamps = []
        if column_names == "externalId":
            identifier = self.external_id if self.external_id is not None else self.id
        elif column_names == "id":
            identifier = self.id
        else:
            raise ValueError("column_names must be 'externalId' or 'id'")
        for attr, value in self._get_non_empty_data_fields(get_empty_lists=True, get_error=include_errors):
            if attr == "timestamp":
                timestamps = value
            else:
                id_with_agg = str(identifier)
                if attr != "value":
                    id_with_agg += "|{}".format(utils._auxiliary.to_camel_case(attr))
                data_fields[id_with_agg] = value
        df = pd.DataFrame(data_fields, index=pd.DatetimeIndex(data=np.array(timestamps, dtype="datetime64[ms]")))
        if not include_aggregate_name:
            Datapoints._strip_aggregate_names(df)
        return df

    def plot(self, *args, **kwargs) -> None:
        """Plot the datapoints."""
        plt = utils._auxiliary.local_import("matplotlib.pyplot")
        self.to_pandas().plot(*args, **kwargs)
        plt.show()

    @staticmethod
    def _strip_aggregate_names(df):
        df.rename(columns=lambda s: regexp.sub(r"\|\w+$", "", s), inplace=True)
        if len(set(df.columns)) < df.shape[1]:
            raise CogniteDuplicateColumnsError(
                [item for item, count in collections.Counter(df.columns).items() if count > 1]
            )
        return df

    @classmethod
    def _load(cls, dps_object, expected_fields: List[str] = None, cognite_client=None):
        instance = cls()
        instance.id = dps_object.get("id")
        instance.external_id = dps_object.get("externalId")
        instance.is_string = dps_object["isString"]  # should never be missing
        instance.is_step = dps_object.get("isStep")  # NB can be null if isString is true
        instance.unit = dps_object.get("unit")
        expected_fields = (expected_fields or ["value"]) + ["timestamp"]
        if len(dps_object["datapoints"]) == 0:
            for key in expected_fields:
                snake_key = utils._auxiliary.to_snake_case(key)
                setattr(instance, snake_key, [])
        else:
            for key in expected_fields:
                data = [dp[key] if key in dp else None for dp in dps_object["datapoints"]]
                snake_key = utils._auxiliary.to_snake_case(key)
                setattr(instance, snake_key, data)
        return instance

    def _extend(self, other_dps):
        if self.id is None and self.external_id is None:
            self.id = other_dps.id
            self.external_id = other_dps.external_id
            self.is_string = other_dps.is_string
            self.is_step = other_dps.is_step
            self.unit = other_dps.unit

        for attr, other_value in other_dps._get_non_empty_data_fields(get_empty_lists=True):
            value = getattr(self, attr)
            if not value:
                setattr(self, attr, other_value)
            else:
                value.extend(other_value)

    def _get_non_empty_data_fields(self, get_empty_lists=False, get_error=True) -> List[Tuple[str, Any]]:
        non_empty_data_fields = []
        for attr, value in self.__dict__.copy().items():
            if (
                attr not in ["id", "external_id", "is_string", "is_step", "unit"]
                and attr[0] != "_"
                and (attr != "error" or get_error)
            ):
                if value is not None or attr == "timestamp":
                    if len(value) > 0 or get_empty_lists or attr == "timestamp":
                        non_empty_data_fields.append((attr, value))
        return non_empty_data_fields

    def __get_datapoint_objects(self) -> List[Datapoint]:
        if self.__datapoint_objects is None:
            fields = self._get_non_empty_data_fields(get_error=False)
            self.__datapoint_objects = []
            for i in range(len(self)):
                dp_args = {}
                for attr, value in fields:
                    dp_args[attr] = value[i]
                self.__datapoint_objects.append(Datapoint(**dp_args))
        return self.__datapoint_objects

    def _slice(self, slice: slice):
        truncated_datapoints = Datapoints(id=self.id, external_id=self.external_id)
        for attr, value in self._get_non_empty_data_fields():
            setattr(truncated_datapoints, attr, value[slice])
        return truncated_datapoints

    def _repr_html_(self):
        return self.to_pandas(include_errors=True)._repr_html_()


class DatapointsList(CogniteResourceList):
    _RESOURCE = Datapoints
    _ASSERT_CLASSES = False

    def __str__(self):
        item = self.dump()
        for i in item:
            i["datapoints"] = utils._time.convert_time_attributes_to_datetime(i["datapoints"])
        return json.dumps(item, default=lambda x: x.__dict__, indent=4)

    def to_pandas(self, column_names: str = "externalId", include_aggregate_name: bool = True) -> "pandas.DataFrame":
        """Convert the datapoints list into a pandas DataFrame.

        Args:
            column_names (str): Which field to use as column header. Defaults to "externalId", can also be "id".
            include_aggregate_name (bool): Include aggregate in the column name

        Returns:
            pandas.DataFrame: The datapoints list as a pandas DataFrame.
        """
        pd = utils._auxiliary.local_import("pandas")

        dfs = [df.to_pandas(column_names=column_names) for df in self.data]
        if dfs:
            df = pd.concat(dfs, axis="columns")
            if not include_aggregate_name:  # do not pass in to_pandas above, so we check for duplicate columns
                Datapoints._strip_aggregate_names(df)
            return df

        return pd.DataFrame()

    def _repr_html_(self):
        return self.to_pandas()._repr_html_()

    def plot(self, *args, **kwargs) -> None:
        """Plot the list of datapoints."""
        plt = utils._auxiliary.local_import("matplotlib.pyplot")
        self.to_pandas().plot(*args, **kwargs)
        plt.show()


class DatapointsQuery(CogniteResource):
    """Parameters describing a query for datapoints.

    Args:
        start (Union[str, int, datetime]): Get datapoints after this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since epoch.
        end (Union[str, int, datetime]): Get datapoints up to this time. The format is the same as for start.
        id (Union[int, List[int], Dict[str, Any], List[Dict[str, Any]]]: Id or list of ids. Can also be object
                specifying aggregates. See example below.
            external_id (Union[str, List[str], Dict[str, Any], List[Dict[str, Any]]]): External id or list of external
                ids. Can also be object specifying aggregates. See example below.
        limit (int): Return up to this number of datapoints.
        aggregates (List[str]): The aggregates to be returned.  Use default if null. An empty string must be sent to get raw data if the default is a set of aggregates.
        granularity (str): The granularity size and granularity of the aggregates.
        include_outside_points (bool): Whether to include the last datapoint before the requested time period,and the first one after the requested period. This can be useful for interpolating data. Not available for aggregates.
        ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception. Note that in this case the function always returns a DatapointsList even when a single id is requested.
    """

    def __init__(
        self,
        start: Union[str, int, datetime],
        end: Union[str, int, datetime],
        id: Union[int, List[int], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]] = None,
        external_id: Union[
            str, List[str], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]
        ] = None,
        limit: int = None,
        aggregates: List[str] = None,
        granularity: str = None,
        include_outside_points: bool = None,
        ignore_unknown_ids: bool = False,
    ):
        self.id = id
        self.external_id = external_id
        self.start = start
        self.end = end
        self.limit = limit
        self.aggregates = aggregates
        self.granularity = granularity
        self.include_outside_points = include_outside_points
        self.ignore_unknown_ids = ignore_unknown_ids
