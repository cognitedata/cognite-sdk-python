from __future__ import annotations

import json
import numbers
import re as regexp
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Literal,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypedDict,
    Union,
    cast,
)

from cognite.client import utils
from cognite.client._api.datapoint_constants import (
    DPS_LIMIT,
    DPS_LIMIT_AGG,
    CustomDatapointsQuery,
    DatapointsExternalIdTypes,
    DatapointsIdTypes,
    DatapointsQueryExternalId,
    DatapointsQueryId,
)
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.exceptions import CogniteDuplicateColumnsError
from cognite.client.utils._auxiliary import (
    convert_all_keys_to_camel_case,
    convert_all_keys_to_snake_case,
    local_import,
    to_camel_case,
)
from cognite.client.utils._time import (
    UNIT_IN_MS,
    align_start_and_end_for_granularity,
    granularity_to_ms,
    timestamp_to_ms,
)

if TYPE_CHECKING:
    import numpy.typing as npt
    import pandas

    from cognite.client import CogniteClient
    from cognite.client._api.datapoint_constants import (
        NumpyDatetime64NSArray,
        NumpyFloat64Array,
        NumpyInt64Array,
        NumpyObjArray,
    )


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

    def to_pandas(self, camel_case: bool = False) -> "pandas.DataFrame":  # type: ignore[override]
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
        timestamp: NumpyDatetime64NSArray = None,
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

    @property
    def _ts_info(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "external_id": self.external_id,
            "is_string": self.is_string,
            "is_step": self.is_step,
            "unit": self.unit,
        }

    @classmethod
    def _load(  # type: ignore [override]
        cls,
        dps_dct: Dict[str, Union[int, str, bool, npt.NDArray]],
    ) -> DatapointsArray:
        # Since pandas always uses nanoseconds for datetime, we stick with the same:
        dps_dct["timestamp"] = dps_dct["timestamp"].astype("datetime64[ms]").astype("datetime64[ns]")
        return cls(**convert_all_keys_to_snake_case(dps_dct))

    def __len__(self) -> int:
        if self.timestamp is None:
            return 0
        return len(self.timestamp)

    def __eq__(self, other: Any) -> bool:
        # Override CogniteResource __eq__ which checks exact type & dump being equal. We do not want
        # this: comparing arrays with (mostly) floats is a very bad idea; also dump is exceedingly expensive.
        return id(self) == id(other)

    def __str__(self) -> str:
        return json.dumps(self.dump(convert_timestamps=True), indent=4)

    def _repr_html_(self) -> str:
        return self.to_pandas()._repr_html_()

    def __getitem__(self, item: Any) -> Union[Datapoint, DatapointsArray]:
        if isinstance(item, slice):
            return self._slice(item)
        return Datapoint(**{attr: arr[item].item() for attr, arr in zip(*self._data_fields())})

    def _slice(self, part: slice) -> DatapointsArray:
        return DatapointsArray(
            **self._ts_info, **{attr: arr[part] for attr, arr in zip(*self._data_fields())}  # type: ignore [arg-type]
        )

    def __iter__(self) -> Iterator[Datapoint]:
        # Let's not create a single Datapoint more than we have too:
        attrs, arrays = self._data_fields()
        yield from (Datapoint(**dict(zip(attrs, row))) for row in zip(*arrays))

    def _data_fields(self) -> Tuple[List[str], List[npt.NDArray]]:
        data_field_tuples = [
            (attr, arr)
            for attr in ("timestamp", "value", *ALL_DATAPOINT_AGGREGATES)
            if (arr := getattr(self, attr)) is not None
        ]
        attrs, arrays = map(list, zip(*data_field_tuples))
        return attrs, arrays  # type: ignore [return-value]

    def dump(self, camel_case: bool = False, convert_timestamps: bool = False) -> Dict[str, Any]:
        """Dump the datapoints into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Default: False.
            convert_timestamps (bool): Convert integer timestamps to ISO 8601 formatted strings. Default: False.

        Returns:
            List[Dict[str, Any]]: A list of dicts representing the instance.
        """
        attrs, arrays = self._data_fields()
        if convert_timestamps:
            assert attrs[0] == "timestamp"
            # Note: numpy does not have a strftime method to get the exact format we want (hence the datetime detour):
            arrays[0] = arrays[0].astype("datetime64[ms]").astype(datetime).astype(str)

        if camel_case:
            attrs = list(map(to_camel_case, attrs))

        dumped = {
            **self._ts_info,
            # Using .item() is not strictly necessary, but it gives us vanilla python types:
            "datapoints": [dict(zip(attrs, [v.item() for v in row])) for row in zip(*arrays)],
        }
        if camel_case:
            dumped = convert_all_keys_to_camel_case(dumped)
        return {k: v for k, v in dumped.items() if v is not None}

    def to_pandas(  # type: ignore [override]
        self, column_names: Literal["id", "external_id"] = "external_id", include_aggregate_name: bool = True
    ) -> "pandas.DataFrame":
        pd = cast(Any, local_import("pandas"))
        identifier_dct = {"id": self.id, "external_id": self.external_id}
        if column_names not in identifier_dct:
            raise ValueError("Argument `column_names` must be either 'external_id' or 'id'")
        identifier = identifier_dct[column_names]
        if identifier is None:  # Time series are not required to have an external_id unfortunately...
            identifier = identifier_dct["id"]
            assert identifier is not None  # Only happens if a user has created DatapointsArray themselves
            warnings.warn(
                f"Time series does not have an external ID, so its ID ({self.id}) was used instead as "
                'the column name in the DataFrame. If this is expected, consider passing `column_names="id"` '
                "to silence this warning.",
                UserWarning,
            )
        idx = pd.to_datetime(self.timestamp, unit="ms")
        if self.value is not None:
            return pd.DataFrame({identifier: self.value}, index=idx)

        def col_name(agg: str) -> str:
            return f"{identifier}{include_aggregate_name * f'|{agg}'}"

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
        pd = cast(Any, local_import("pandas"))
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

    @staticmethod
    def _strip_aggregate_names(df: "pandas.DataFrame") -> "pandas.DataFrame":
        expr = f"\\|({'|'.join(ALL_DATAPOINT_AGGREGATES)})$"
        df = df.rename(columns=lambda col: regexp.sub(expr, "", col))
        if not df.columns.is_unique:
            raise CogniteDuplicateColumnsError([col for col, count in df.columns.value_counts().items() if count > 1])
        return df

    @classmethod
    def _load(  # type: ignore [override]
        cls, dps_object: Dict[str, Any], expected_fields: List[str] = None, cognite_client: "CogniteClient" = None
    ) -> "Datapoints":
        del cognite_client  # just needed for signature
        instance = cls(
            id=dps_object.get("id"),
            external_id=dps_object.get("externalId"),
            is_string=dps_object["isString"],
            is_step=dps_object.get("isStep"),
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

    def to_pandas(  # type: ignore [override]
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

    @property
    def is_single_identifier(self) -> bool:
        # No lists given and exactly one of id/xid was given:
        return (
            isinstance(self.id, (dict, numbers.Integral))
            and self.external_id is None
            or isinstance(self.external_id, (dict, str))
            and self.id is None
        )
