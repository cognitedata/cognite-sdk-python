from __future__ import annotations

import dataclasses
import json
import math
import numbers
import re as regexp
import warnings
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    NoReturn,
    Optional,
    Tuple,
    TypedDict,
    Union,
    cast,
)

import numpy.typing as npt

from cognite.client import utils
from cognite.client._api.datapoint_constants import (
    DPS_LIMIT,
    DPS_LIMIT_AGG,
    DatapointsExternalIdTypes,
    DatapointsIdTypes,
    DatapointsQueryExternalId,
    DatapointsQueryId,
    NumpyFloat64Array,
    NumpyInt64Array,
    NumpyObjArray,
)
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.exceptions import CogniteDuplicateColumnsError
from cognite.client.utils._auxiliary import to_camel_case, to_snake_case, valfilter
from cognite.client.utils._time import align_start_and_end_for_granularity, timestamp_to_ms

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient


ALL_DATAPOINT_AGGREGATES = [
    "average",
    "max",
    "min",
    "count",
    "sum",
    "interpolation",
    "step_interpolation",
    "continuous_variance",
    "discrete_variance",
    "total_variation",
]


class Datapoint(CogniteResource):
    """An object representing a datapoint.

    Args:
        timestamp (int): The data timestamp in milliseconds since the epoch (Jan 1, 1970).
        value (Union[str, float]): The data value. Can be String or numeric depending on the metric
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
        timestamp: int = None,
        value: Union[str, float] = None,
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

    def to_pandas(self, camel_case: bool = True) -> "pandas.DataFrame":  # type: ignore[override]
        """Convert the datapoint into a pandas DataFrame.
             camel_case (bool): Convert column names to camel case (e.g. `stepInterpolation` instead of `step_interpolation`)

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = cast(Any, utils._auxiliary.local_import("pandas"))

        dumped = self.dump(camel_case=camel_case)
        timestamp = dumped.pop("timestamp")

        return pd.DataFrame(dumped, index=[pd.Timestamp(timestamp, unit="ms")])


class DatapointsArray(CogniteResource):
    """An object representing datapoints using numpy arrays."""

    def __init__(
        self,
        id: int = None,
        external_id: str = None,
        is_string: bool = None,
        is_step: bool = None,
        unit: str = None,
        timestamp: NumpyInt64Array = None,
        value: Union[NumpyFloat64Array, NumpyObjArray] = None,
        average: NumpyFloat64Array = None,
        max: NumpyFloat64Array = None,
        min: NumpyFloat64Array = None,
        count: NumpyInt64Array = None,
        sum: NumpyFloat64Array = None,
        interpolation: NumpyFloat64Array = None,
        step_interpolation: NumpyFloat64Array = None,
        continuous_variance: NumpyFloat64Array = None,
        discrete_variance: NumpyFloat64Array = None,
        total_variation: NumpyFloat64Array = None,
    ):
        self.id = id
        self.external_id = external_id
        self.is_string = is_string
        self.is_step = is_step
        self.unit = unit
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

    @classmethod
    def _load(
        cls,
        dps_dct: Dict[str, Union[int, str, bool, npt.NDArray]],
        cognite_client: "CogniteClient" = None,
    ) -> DatapointsArray:
        del cognite_client  # Just needed for signature
        return cls(**dict(zip(map(to_snake_case, dps_dct.keys()), dps_dct.values())))

    def __len__(self):
        if self.timestamp is None:
            return 0
        return len(self.timestamp)

    def to_pandas(self, column_names: str = "external_id", include_aggregate_name: bool = True) -> "pandas.DataFrame":
        pd = utils._auxiliary.local_import("pandas")
        if column_names not in {"id", "external_id"}:
            raise ValueError("Argument `column_names` must be either 'external_id' or 'id'")
        identifier = {"id": str(self.id), "external_id": self.external_id}[column_names]
        if identifier is None:
            identifier = self.id
            warnings.warn(
                f"Time series does not have an external ID, so its ID ({self.id}) was used instead as "
                'the column name in the DataFrame. If this is expected, consider passing `column_names="id"` '
                "to silence this warning.",
                UserWarning,
            )

        if self.value is not None:
            columns = {identifier: self.value}
        else:

            def col_name(agg):
                return identifier + include_aggregate_name * f"|{to_camel_case(agg)}"

            columns = valfilter({col_name(agg): getattr(self, agg) for agg in ALL_DATAPOINT_AGGREGATES})

        return pd.DataFrame(columns, index=pd.to_datetime(self.timestamp, unit="ms"))


class Datapoints(CogniteResource):
    """An object representing a list of datapoints.

    Args:
        id (int): Id of the timeseries the datapoints belong to
        external_id (str): External id of the timeseries the datapoints belong to
        is_string (bool): Whether the time series is string valued or not.
        is_step (bool): Whether the time series is a step series or not.
        unit (str): The physical unit of the time series.
        timestamp (List[int]): The data timestamps in milliseconds since the epoch (Jan 1, 1970).
        value (List[Union[str, float]]): The data values. Can be String or numeric depending on the metric
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
        timestamp: List[int] = None,
        value: List[Union[str, float]] = None,
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
        self.timestamp = timestamp or []  # Needed in __len__
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

        self.__datapoint_objects: Optional[List[Datapoint]] = None

    def __str__(self) -> str:
        item = self.dump()
        item["datapoints"] = utils._time.convert_time_attributes_to_datetime(item["datapoints"])
        return json.dumps(item, indent=4)

    def __len__(self) -> int:
        return len(self.timestamp)

    def __eq__(self, other: Any) -> bool:
        return (
            type(self) == type(other)
            and self.id == other.id
            and self.external_id == other.external_id
            and list(self._get_non_empty_data_fields()) == list(other._get_non_empty_data_fields())
        )

    def __getitem__(self, item: Any) -> Union[Datapoint, "Datapoints"]:
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

    def to_pandas(  # type: ignore[override]
        self, column_names: str = "external_id", include_aggregate_name: bool = True, include_errors: bool = False
    ) -> "pandas.DataFrame":
        """Convert the datapoints into a pandas DataFrame.

        Args:
            column_names (str):  Which field to use as column header. Defaults to "external_id", can also be "id". For time series with no external ID, ID will be used instead.
            include_aggregate_name (bool): Include aggregate in the column name
            include_errors (bool): For synthetic datapoint queries, include a column with errors.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = utils._auxiliary.local_import("pandas")
        data_fields = {}
        timestamps = []
        if column_names in ["external_id", "externalId"]:  # Camel case for backwards compat
            identifier = self.external_id if self.external_id is not None else self.id
        elif column_names == "id":
            identifier = self.id
        else:
            raise ValueError("column_names must be 'external_id' or 'id'")
        for attr, value in self._get_non_empty_data_fields(get_empty_lists=True, get_error=include_errors):
            if attr == "timestamp":
                timestamps = value
            else:
                id_with_agg = str(identifier)
                if attr != "value":
                    id_with_agg += "|{}".format(utils._auxiliary.to_camel_case(attr))
                data_fields[id_with_agg] = value
        df = pd.DataFrame(data_fields, index=pd.to_datetime(timestamps, unit="ms"))
        if not include_aggregate_name:
            df = Datapoints._strip_aggregate_names(df)
        return df

    def plot(self, *args: Any, **kwargs: Any) -> None:
        """Plot the datapoints."""
        plt = cast(Any, utils._auxiliary.local_import("matplotlib.pyplot"))
        self.to_pandas().plot(*args, **kwargs)
        plt.show()

    @staticmethod
    def _strip_aggregate_names(df: "pandas.DataFrame") -> "pandas.DataFrame":
        expr = f"\\|({'|'.join(ALL_DATAPOINT_AGGREGATES)})$"
        df = df.rename(columns=lambda col: regexp.sub(expr, "", col))
        if not df.columns.is_unique:
            raise CogniteDuplicateColumnsError([col for col, count in df.columns.value_counts().items() if count > 1])
        return df

    @classmethod
    def _load(  # type: ignore[override]
        cls, dps_object: Dict[str, Any], expected_fields: List[str] = None, cognite_client: "CogniteClient" = None
    ) -> "Datapoints":
        instance = cls(
            id=dps_object["id"],
            external_id=dps_object.get("externalId"),
            is_string=dps_object["isString"],
            is_step=dps_object["isStep"],
            unit=dps_object.get("unit"),
        )
        expected_fields = (expected_fields or ["value"]) + ["timestamp"]
        if len(dps_object["datapoints"]) == 0:
            for key in expected_fields:
                snake_key = utils._auxiliary.to_snake_case(key)
                setattr(instance, snake_key, [])
        else:
            for key in expected_fields:
                data = [dp.get(key) for dp in dps_object["datapoints"]]
                snake_key = utils._auxiliary.to_snake_case(key)
                setattr(instance, snake_key, data)
        return instance

    def _extend(self, other_dps: "Datapoints") -> None:
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

    def _get_non_empty_data_fields(
        self, get_empty_lists: bool = False, get_error: bool = True
    ) -> List[Tuple[str, Any]]:
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
        if self.__datapoint_objects is not None:
            return self.__datapoint_objects
        fields = self._get_non_empty_data_fields(get_error=False)
        new_dps_objects = []
        for i in range(len(self)):
            dp_args = {}
            for attr, value in fields:
                dp_args[attr] = value[i]
            new_dps_objects.append(Datapoint(**dp_args))
        self.__datapoint_objects = new_dps_objects
        return self.__datapoint_objects

    def _slice(self, slice: slice) -> "Datapoints":
        truncated_datapoints = Datapoints(id=self.id, external_id=self.external_id)
        for attr, value in self._get_non_empty_data_fields():
            setattr(truncated_datapoints, attr, value[slice])
        return truncated_datapoints

    def _repr_html_(self) -> str:
        return self.to_pandas(include_errors=True)._repr_html_()


class DatapointsArrayList(CogniteResourceList):
    _RESOURCE = DatapointsArray

    def __str__(self) -> str:
        return "DatapointsArrayList.__str__ not implemented"  # No really, TODO
        # item = utils._time.convert_time_attributes_to_datetime(self.dump())
        # return json.dumps(item, default=utils._auxiliary.json_dump_default, indent=4)

    def to_pandas(self, column_names: str = "external_id", include_aggregate_name: bool = True) -> "pandas.DataFrame":
        pd = cast(Any, utils._auxiliary.local_import("pandas"))
        dfs = [dps_arr.to_pandas(column_names=column_names) for dps_arr in self.data]
        if dfs:
            df = pd.concat(dfs, axis="columns")
            if not include_aggregate_name:  # do not pass in to_pandas above, so we check for duplicate columns
                df = Datapoints._strip_aggregate_names(df)
            return df

        return pd.DataFrame()


class DatapointsList(CogniteResourceList):
    _RESOURCE = Datapoints

    def __str__(self) -> str:
        item = self.dump()
        for i in item:
            i["datapoints"] = utils._time.convert_time_attributes_to_datetime(i["datapoints"])
        return json.dumps(item, default=lambda x: x.__dict__, indent=4)

    def to_pandas(  # type: ignore[override]
        self, column_names: str = "external_id", include_aggregate_name: bool = True
    ) -> "pandas.DataFrame":
        """Convert the datapoints list into a pandas DataFrame.

        Args:
            column_names (str): Which field to use as column header. Defaults to "external_id", can also be "id". For time series with no external ID, ID will be used instead.
            include_aggregate_name (bool): Include aggregate in the column name

        Returns:
            pandas.DataFrame: The datapoints list as a pandas DataFrame.
        """
        pd = cast(Any, utils._auxiliary.local_import("pandas"))

        dfs = [df.to_pandas(column_names=column_names) for df in self.data]
        if dfs:
            df = pd.concat(dfs, axis="columns")
            if not include_aggregate_name:  # do not pass in to_pandas above, so we check for duplicate columns
                df = Datapoints._strip_aggregate_names(df)
            return df

        return pd.DataFrame()

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()

    def plot(self, *args: Any, **kwargs: Any) -> None:
        """Plot the list of datapoints."""
        plt = utils._auxiliary.local_import("matplotlib.pyplot")
        self.to_pandas().plot(*args, **kwargs)
        plt.show()  # type: ignore


DatapointsIdMaybeAggregate = Union[
    int, List[int], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]
]
DatapointsExternalIdMaybeAggregate = Union[
    str, List[str], Dict[str, Union[str, List[str]]], List[Dict[str, Union[str, List[str]]]]
]


class DatapointsQuery(CogniteResource):
    """Parameters describing a query for datapoints.

    Args:
        start (Union[str, int, datetime]): Get datapoints starting from this time. Format is N[timeunit]-ago where timeunit is w,d,h,m,s. Example: '2d-ago' will get everything that is up to 2 days old. Can also send time in ms since epoch.
        end (Union[str, int, datetime]): Get datapoints up to this time. The format is the same as for start.
        id (Union[int, List[int], Dict[str, Any], List[Dict[str, Any]]]: Id or list of ids. Can also be object
                specifying aggregates. See example below.
        external_id (Union[str, List[str], Dict[str, Any], List[Dict[str, Any]]]): External id or list of external
            ids. Can also be object specifying aggregates. See example below (TODO: Where?).
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
        id: DatapointsIdMaybeAggregate = None,
        external_id: DatapointsExternalIdMaybeAggregate = None,
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


@dataclass
class DatapointsQueryNew(CogniteResource):
    start: Union[int, str, datetime, None] = None
    end: Union[int, str, datetime, None] = None
    id: Optional[DatapointsIdTypes] = None
    external_id: Optional[DatapointsExternalIdTypes] = None
    aggregates: Optional[List[str]] = None
    granularity: Optional[str] = None
    limit: Optional[int] = None
    include_outside_points: bool = False
    ignore_unknown_ids: bool = False

    @cached_property
    def defaults(self):
        return dict(
            id=None,  # We have to parse these
            external_id=None,  # We have to parse these
            start=self.start,
            end=self.end,
            limit=self.limit,
            aggregates=self.aggregates,
            granularity=self.granularity,
            include_outside_points=self.include_outside_points,
            ignore_unknown_ids=self.ignore_unknown_ids,
        )

    @property
    def is_single_identifier(self):
        # No lists given and exactly one of id/xid was given:
        return (
            isinstance(self.id, (dict, numbers.Integral))
            and self.external_id is None
            or isinstance(self.external_id, (dict, str))
            and self.id is None
        )

    def validate(self) -> None:
        self.validate_and_create_queries()

    def validate_and_create_queries(self) -> List[SingleTSQuery]:
        queries = []
        if self.id is not None:
            queries.extend(self._validate_id_or_xid(id_or_xid=self.id, is_external_id=False))
        if self.external_id is not None:
            queries.extend(self._validate_id_or_xid(id_or_xid=self.external_id, is_external_id=True))
        if queries:
            return queries
        raise ValueError("Pass at least one time series `id` or `external_id`!")

    def _validate_id_or_xid(self, id_or_xid, is_external_id: bool):
        if is_external_id:
            arg_name, exp_type = "external_id", str
        else:
            arg_name, exp_type = "id", numbers.Integral

        if isinstance(id_or_xid, (exp_type, dict)):
            id_or_xid = [id_or_xid]  # Lazy - we postpone evaluation

        if not isinstance(id_or_xid, Iterable):
            self._raise_on_wrong_ts_identifier_type(id_or_xid, arg_name, exp_type)

        queries = []
        for ts in id_or_xid:
            if isinstance(ts, exp_type):
                queries.append(SingleTSQuery.from_dict_with_validation({arg_name: ts}, self.defaults))

            elif isinstance(ts, dict):
                ts_validated = self._validate_ts_query_dct(ts, arg_name, exp_type)
                queries.append(SingleTSQuery.from_dict_with_validation(ts_validated, self.defaults))
            else:
                self._raise_on_wrong_ts_identifier_type(ts, arg_name, exp_type)
        return queries

    @staticmethod
    def _raise_on_wrong_ts_identifier_type(id_or_xid, arg_name, exp_type) -> NoReturn:
        raise TypeError(
            f"Got unsupported type {type(id_or_xid)}, as, or part of argument `{arg_name}`. Expected one of "
            f"{exp_type}, {dict} or a (mixed) list of these, but got `{id_or_xid}`."
        )

    @staticmethod
    def _validate_ts_query_dct(dct, arg_name, exp_type) -> Union[DatapointsQueryId, DatapointsQueryExternalId]:
        if arg_name not in dct:
            if to_camel_case(arg_name) in dct:
                # For backwards compatability we accept identifier in camel case:
                dct = dct.copy()  # Avoid side effects for user's input. Also means we need to return it.
                dct[arg_name] = dct.pop(to_camel_case(arg_name))
            else:
                raise KeyError(f"Missing key `{arg_name}` in dict passed as, or part of argument `{arg_name}`")

        ts_identifier = dct[arg_name]
        if not isinstance(ts_identifier, exp_type):
            DatapointsQueryNew._raise_on_wrong_ts_identifier_type(ts_identifier, arg_name, exp_type)

        opt_dct_keys = {"start", "end", "aggregates", "granularity", "include_outside_points", "limit"}
        bad_keys = set(dct) - opt_dct_keys - {arg_name}
        if not bad_keys:
            return dct
        raise KeyError(
            f"Dict provided by argument `{arg_name}` included key(s) not understood: {sorted(bad_keys)}. "
            f"Required key: `{arg_name}`. Optional: {list(opt_dct_keys)}."
        )


# Note on `@dataclass(eq=False)`: We need all individual queries to be hashable (and unique), even two exactly
# similar queries. With `eq=False`, we inherit __hash__ from `object` (just id(self)) - exactly what we need!
@dataclass(eq=False)
class SingleTSQuery:
    id: Optional[int] = None
    external_id: Optional[str] = None
    start: Union[int, str, datetime, None] = None
    end: Union[int, str, datetime, None] = None
    aggregates: Optional[List[str]] = None
    granularity: Optional[str] = None
    limit: Optional[int] = None
    include_outside_points: Optional[bool] = False
    ignore_unknown_ids: Optional[bool] = False

    def __post_init__(self):
        # NB: Do not change order of _verify fns without care:
        self._verify_identifier()
        self._verify_limit()
        self._verify_time_range()
        self._is_missing = None  # I.e. not set...
        self._is_string = None  # ...or unknown
        if not self.is_raw_query:
            self._is_string = False  # No aggregates exist for string time series, yet ;)
        if self.include_outside_points and self.limit is not None:
            warnings.warn(
                "When using `include_outside_points=True` with a non-infinite `limit` you may get a large gap "
                "between the last 'inside datapoint' and the 'after/outside' datapoint. Note also that the "
                "up-to-two outside points come in addition to your given `limit`; asking for 5 datapoints might "
                "yield 5, 6 or 7. It's a feature, not a bug ;)",
                UserWarning,
            )

    def to_payload(self):
        payload = dataclasses.asdict(self)
        for k in ("id", "external_id", "ignore_unknown_ids", "include_outside_points"):
            del payload[k]
        payload["includeOutsidePoints"] = self.include_outside_points  # Camel case...
        return {**payload, **self.identifier_dct}

    @classmethod
    def from_dict_with_validation(cls, ts_dct, defaults) -> SingleTSQuery:
        # We merge 'defaults' and given ts-dict, ts-dict takes precedence:
        dct = {**defaults, **ts_dct}
        granularity, aggregates = dct["granularity"], dct["aggregates"]

        if not (granularity is None or isinstance(granularity, str)):
            raise TypeError(f"Expected `granularity` to be of type `str` or None, not {type(granularity)}")

        elif not (aggregates is None or isinstance(aggregates, list)):
            raise TypeError(f"Expected `aggregates` to be of type `list[str]` or None, not {type(aggregates)}")

        elif aggregates is None:
            if granularity is None:
                return cls(**dct)  # Request for raw datapoints
            raise ValueError("When passing `granularity`, argument `aggregates` is also required.")

        # Aggregates must be a list at this point:
        elif len(aggregates) == 0:
            raise ValueError("Empty list of `aggregates` passed, expected at least one!")

        elif granularity is None:
            raise ValueError("When passing `aggregates`, argument `granularity` is also required.")

        elif dct["include_outside_points"] is True:
            raise ValueError("'Include outside points' is not supported for aggregates.")
        return cls(**dct)  # Request for one or more aggregates

    def _verify_identifier(self):
        if self.id is not None:
            self.identifier_type = "id"
            self.identifier = int(self.id)
        elif self.external_id is not None:
            self.identifier_type = "externalId"
            self.identifier = str(self.external_id)
        else:
            raise ValueError("Pass exactly one of `id` or `external_id`. Got neither.")
        # Shortcuts for hashing and API queries:
        self.identifier_tpl = (self.identifier_type, self.identifier)
        self.identifier_dct = {self.identifier_type: self.identifier}
        self.identifier_dct_sc = {to_snake_case(self.identifier_type): self.identifier}

    def _verify_limit(self):
        if self.limit in {None, -1, math.inf}:
            self.limit = None
        elif isinstance(self.limit, numbers.Integral):  # limit=0 is accepted by the API
            self.limit = int(self.limit)  # We don't want weird stuff like numpy dtypes etc.
        else:
            raise TypeError(f"Limit must be an integer or one of [None, -1, inf], got {type(self.limit)}")

    def _verify_time_range(self):
        if self.start is None:
            self.start = 0  # 1970-01-01
        else:
            self.start = timestamp_to_ms(self.start)
        if self.end is None:
            self.end = "now"
        self.end = timestamp_to_ms(self.end)

        if self.end <= self.start:
            raise ValueError(
                f"Invalid time range, `end` must be later than `start` (from query: {self.identifier_dct_sc})"
            )

        if not self.is_raw_query:  # API rounds aggregate queries in a very particular fashion
            self.start, self.end = align_start_and_end_for_granularity(self.start, self.end, self.granularity)

    @property
    def is_missing(self):
        if self._is_missing is None:
            raise RuntimeError("Before making API-calls the `is_missing` status is unknown")
        return self._is_missing

    @is_missing.setter
    def is_missing(self, value):
        assert isinstance(value, bool)
        self._is_missing = value

    @property
    def is_string(self):
        if self._is_string is None:
            raise RuntimeError(
                "For queries asking for raw datapoints, the `is_string` status is unknown before "
                "any API-calls have been made"
            )
        return self._is_string

    @is_string.setter
    def is_string(self, value):
        assert isinstance(value, bool)
        self._is_string = value

    @property
    def is_raw_query(self):
        return self.aggregates is None

    @property
    def max_query_limit(self):
        if self.is_raw_query:
            return DPS_LIMIT
        return DPS_LIMIT_AGG
