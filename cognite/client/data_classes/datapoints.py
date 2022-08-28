from __future__ import annotations

import dataclasses
import json
import math
import numbers
import re as regexp
import warnings
from dataclasses import InitVar, dataclass
from datetime import datetime
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Literal,
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
from cognite.client.utils._auxiliary import convert_all_keys_to_camel_case, local_import, to_camel_case, to_snake_case
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._time import (
    UNIT_IN_MS,
    align_start_and_end_for_granularity,
    granularity_to_ms,
    timestamp_to_ms,
)

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
        timestamp (Union[int, float]): The data timestamp in milliseconds since the epoch (Jan 1, 1970). Can be negative to define a date before 1970. Minimum timestamp is 1900.01.01 00:00:00 UTC
        value (Union[str, int, float]): The data value. Can be string or numeric depending on the metric
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

        Args:
            camel_case (bool): Convert column names to camel case (e.g. `stepInterpolation` instead of `step_interpolation`)

        Returns:
            pandas.DataFrame
        """
        pd = cast(Any, local_import("pandas"))

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

    def __str__(self) -> str:
        return json.dumps(self.dump(convert_timestamps=True), indent=4)

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()

    def dump(self, camel_case: bool = False, convert_timestamps: bool = False) -> Dict[str, Any]:
        """Dump the datapoints into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Default: False.
            convert_timestamps (bool): Convert integer timestamps to ISO 8601 formatted strings. Default: False.

        Returns:
            List[Dict[str, Any]]: A list of dicts representing the instance.
        """
        dps_to_dump = {
            attr: dps
            for attr in ["timestamp", "value", *ALL_DATAPOINT_AGGREGATES]
            if (dps := getattr(attr, self)) is not None
        }
        if convert_timestamps:
            # Note: numpy does not have a strftime method to get the exact format we want (hence the datetime detour):
            dps_to_dump["timestamp"] = dps_to_dump["timestamp"].astype("datetime64[ms]").astype(datetime).astype(str)

        if camel_case:
            dps_to_dump = convert_all_keys_to_camel_case(dps_to_dump)

        dumped = {
            "id": self.id,
            "external_id": self.external_id,
            "is_string": self.is_string,
            "is_step": self.is_step,
            "unit": self.unit,
            "datapoints": [dict(zip(dps_to_dump.keys(), row)) for row in zip(*dps_to_dump.values())],
        }
        if camel_case:
            dumped = convert_all_keys_to_camel_case(dumped)
        return {k: v for k, v in dumped.items() if v is not None}

    def to_pandas(
        self, column_names: Literal["id", "external_id"] = "external_id", include_aggregate_name: bool = True
    ) -> "pandas.DataFrame":
        pd = local_import("pandas")
        identifier_dct = {"id": str(self.id), "external_id": self.external_id}
        if column_names not in identifier_dct:
            raise ValueError("Argument `column_names` must be either 'external_id' or 'id'")
        identifier = identifier_dct[column_names]
        if identifier is None:  # Time series are not required to have an external_id unfortunately...
            identifier = self.id
            warnings.warn(
                f"Time series does not have an external ID, so its ID ({self.id}) was used instead as "
                'the column name in the DataFrame. If this is expected, consider passing `column_names="id"` '
                "to silence this warning.",
                UserWarning,
            )
        idx = pd.to_datetime(self.timestamp, unit="ms")
        if self.value is not None:
            return pd.DataFrame({identifier: self.value}, index=idx)

        def col_name(agg):
            return identifier + include_aggregate_name * f"|{agg}"

        return pd.DataFrame(
            {col_name(agg): arr for agg in ALL_DATAPOINT_AGGREGATES if (arr := getattr(self, agg)) is not None},
            index=idx,
        )


class Datapoints(CogniteResource):
    """An object representing a list of datapoints.

    Args:
        id (int): Id of the timeseries the datapoints belong to
        external_id (str): External id of the timeseries the datapoints belong to
        is_string (bool): Whether the time series is string valued or not.
        is_step (bool): Whether the time series is a step series or not.
        unit (str): The physical unit of the time series.
        timestamp (List[Union[int, float]]): The data timestamps in milliseconds since the epoch (Jan 1, 1970). Can be negative to define a date before 1970. Minimum timestamp is 1900.01.01 00:00:00 UTC
        value (List[Union[int, str, float]]): The data values. Can be string or numeric depending on the metric
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
        value: Union[List[str], List[float]] = None,
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
            # TODO: Keys in dicts in `datapoints`, i.e. `step_interpolation` are still snake_cased.
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
        pd = local_import("pandas")
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
        plt = cast(Any, local_import("matplotlib.pyplot"))
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
    def _load(
        cls, dps_object: Dict[str, Any], expected_fields: List[str] = None, cognite_client: "CogniteClient" = None
    ) -> "Datapoints":
        del cognite_client  # just needed for signature
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
        return json.dumps(self.dump(convert_timestamps=True), indent=4)

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()

    def to_pandas(
        self, column_names: Literal["id", "external_id"] = "external_id", include_aggregate_name: bool = True
    ) -> "pandas.DataFrame":
        pd = cast(Any, local_import("pandas"))
        if dfs := [arr.to_pandas(column_names, include_aggregate_name) for arr in self.data]:
            return pd.concat(dfs, axis="columns")
        return pd.DataFrame()

    def dump(self, camel_case: bool = False, convert_timestamps: bool = False) -> List[Dict[str, Any]]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Default: False.
            convert_timestamps (bool): Convert integer timestamps to ISO 8601 formatted strings. Default: False.

        Returns:
            List[Dict[str, Any]]: A list of dicts representing the instance.
        """
        return [dps.dump(camel_case, convert_timestamps) for dps in self.data]


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
        pd = cast(Any, local_import("pandas"))

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
        plt = local_import("matplotlib.pyplot")
        self.to_pandas().plot(*args, **kwargs)
        plt.show()  # type: ignore


@dataclass
class DatapointsQuery(CogniteResource):
    """Parameters describing a query for datapoints.

    See `DatapointsAPI.retrieve` method for a description of the parameters.
    """

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

    def validate_and_create_queries(self) -> List[_SingleTSQuery]:
        queries = []
        if self.id is not None:
            queries.extend(self._validate_id_or_xid(id_or_xid=self.id, is_external_id=False))
        if self.external_id is not None:
            queries.extend(self._validate_id_or_xid(id_or_xid=self.external_id, is_external_id=True))
        if queries:
            return queries
        raise ValueError("Pass at least one time series `id` or `external_id`!")

    def _validate_id_or_xid(
        self, id_or_xid: Union[DatapointsIdTypes, DatapointsExternalIdTypes], is_external_id: bool
    ) -> List[_SingleTSQuery]:
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
                queries.append(_SingleTSQuery.from_dict_with_validation({arg_name: ts}, self.defaults))

            elif isinstance(ts, dict):
                ts_validated = self._validate_ts_query_dct(ts, arg_name, exp_type)
                queries.append(_SingleTSQuery.from_dict_with_validation(ts_validated, self.defaults))
            else:
                self._raise_on_wrong_ts_identifier_type(ts, arg_name, exp_type)
        return queries

    @staticmethod
    def _raise_on_wrong_ts_identifier_type(
        id_or_xid: Union[DatapointsIdTypes, DatapointsExternalIdTypes],
        arg_name: str,
        exp_type: Union[str, numbers.Integral],
    ) -> NoReturn:
        raise TypeError(
            f"Got unsupported type {type(id_or_xid)}, as, or part of argument `{arg_name}`. Expected one of "
            f"{exp_type}, {dict} or a (mixed) list of these, but got `{id_or_xid}`."
        )

    @staticmethod
    def _validate_ts_query_dct(
        dct: Dict[str, Any], arg_name: str, exp_type: Union[str, numbers.Integral]
    ) -> Union[DatapointsQueryId, DatapointsQueryExternalId]:
        if arg_name not in dct:
            if (arg_name_cc := to_camel_case(arg_name)) not in dct:
                raise KeyError(f"Missing key `{arg_name}` in dict passed as, or part of argument `{arg_name}`")
            # For backwards compatability we accept identifier in camel case: (Make copy to avoid side effects
            # for user's input). Also means we need to return it.
            dct[arg_name] = (dct := dct.copy()).pop(arg_name_cc)

        ts_identifier = dct[arg_name]
        if not isinstance(ts_identifier, exp_type):
            DatapointsQuery._raise_on_wrong_ts_identifier_type(ts_identifier, arg_name, exp_type)

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
class _SingleTSQuery:
    id: InitVar[int] = None
    external_id: InitVar[str] = None
    start: Union[int, str, datetime, None] = None
    end: Union[int, str, datetime, None] = None
    aggregates: Optional[List[str]] = None
    granularity: Optional[str] = None
    limit: Optional[int] = None
    include_outside_points: Optional[bool] = False
    ignore_unknown_ids: Optional[bool] = False
    identifier: Identifier = dataclasses.field(default=None, init=False)

    def __post_init__(self, id, external_id):
        self.identifier = Identifier.of_either(id, external_id)
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
        return {
            **self.identifier.as_dict(),
            "start": self.start,
            "end": self.end,
            "aggregates": self.aggregates_cc,  # camel case
            "granularity": self.granularity,
            "limit": self.capped_limit,
            "includeOutsidePoints": self.include_outside_points,
        }

    @classmethod
    def from_dict_with_validation(
        cls,
        ts_dct: Union[DatapointsQueryId, DatapointsQueryExternalId],
        defaults: Union[DatapointsQueryId, DatapointsQueryExternalId],
    ) -> _SingleTSQuery:
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

    def _verify_limit(self) -> None:
        if self.limit in {None, -1, math.inf}:
            self.limit = None
        elif isinstance(self.limit, numbers.Integral) and self.limit >= 0:  # limit=0 is accepted by the API
            self.limit = int(self.limit)  # We don't want weird stuff like numpy dtypes etc.
        else:
            raise TypeError(f"Limit must be a non-negative integer -or- one of [None, -1, inf], got {type(self.limit)}")
        self._max_raw_query_limit = DPS_LIMIT
        self._max_agg_query_limit = DPS_LIMIT_AGG

    def _verify_time_range(self) -> None:
        if self.start is None:
            self.start = 0  # 1970-01-01
        else:
            self.start = timestamp_to_ms(self.start)
        if self.end is None:
            self.end = "now"
        self.end = timestamp_to_ms(self.end)

        if self.end <= self.start:
            raise ValueError(
                f"Invalid time range, `end` must be later than `start` (from query: {self.identifier.as_dict(camel_case=False)})"
            )
        if not self.is_raw_query:  # API rounds aggregate queries in a very particular fashion
            self.start, self.end = align_start_and_end_for_granularity(self.start, self.end, self.granularity)

    @property
    def is_missing(self) -> bool:
        if self._is_missing is None:
            raise RuntimeError("Before making API-calls the `is_missing` status is unknown")
        return self._is_missing

    @is_missing.setter
    def is_missing(self, value) -> None:
        assert isinstance(value, bool)
        self._is_missing = value

    @property
    def is_string(self) -> bool:
        if self._is_string is None:
            raise RuntimeError(
                "For queries asking for raw datapoints, the `is_string` status is unknown before "
                "any API-calls have been made"
            )
        return self._is_string

    @is_string.setter
    def is_string(self, value) -> None:
        assert isinstance(value, bool)
        self._is_string = value

    @property
    def is_raw_query(self) -> bool:
        return self.aggregates is None

    @cached_property
    def aggregates_cc(self) -> List[str]:
        if not self.is_raw_query:
            return list(map(to_camel_case, self.aggregates))

    @property
    def capped_limit(self) -> int:
        if self.limit is None:
            return self.max_query_limit
        return min(self.limit, self.max_query_limit)

    def override_max_limits(self, max_raw: int, max_agg: int) -> None:
        assert isinstance(max_raw, int) and isinstance(max_agg, int)
        assert max_raw <= DPS_LIMIT and max_agg <= DPS_LIMIT_AGG
        self._max_raw_query_limit = max_raw
        self._max_agg_query_limit = max_agg

    @property
    def max_query_limit(self) -> int:
        if self.is_raw_query:
            return self._max_raw_query_limit
        return self._max_agg_query_limit
