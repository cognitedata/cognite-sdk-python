from __future__ import annotations

import json
import operator as op
import typing
import warnings
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Iterator,
    Literal,
    Sequence,
    cast,
    overload,
)

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._auxiliary import find_duplicates, local_import
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._pandas_helpers import (
    concat_dataframes_with_nullable_int_cols,
    notebook_display_with_fallback,
)
from cognite.client.utils._text import (
    convert_all_keys_to_camel_case,
    convert_all_keys_to_snake_case,
    to_camel_case,
    to_snake_case,
)
from cognite.client.utils._time import convert_time_attributes_to_datetime

Aggregate = Literal[
    "average",
    "continuous_variance",
    "count",
    "discrete_variance",
    "interpolation",
    "max",
    "min",
    "step_interpolation",
    "sum",
    "total_variation",
]

ALL_SORTED_DP_AGGS = sorted(typing.get_args(Aggregate))

try:
    import numpy as np

    NUMPY_IS_AVAILABLE = True

except ImportError:  # pragma no cover
    NUMPY_IS_AVAILABLE = False

if TYPE_CHECKING:
    import numpy.typing as npt
    import pandas

    from cognite.client import CogniteClient

    NumpyDatetime64NSArray = npt.NDArray[np.datetime64]
    NumpyInt64Array = npt.NDArray[np.int64]
    NumpyFloat64Array = npt.NDArray[np.float64]
    NumpyObjArray = npt.NDArray[np.object_]


@dataclass(frozen=True)
class LatestDatapointQuery:
    """Parameters describing a query for the latest datapoint from a time series.

    Note:
        Pass either ID or external ID.

    Args:
        id (Optional[int]): The internal ID of the time series to query.
        external_id (Optional[str]): The external ID of the time series to query.
        before (Union[None, int, str, datetime]): Get latest datapoint before this time. None means 'now'.
    """

    id: int | None = None
    external_id: str | None = None
    before: None | int | str | datetime = None

    def __post_init__(self) -> None:
        # Ensure user have just specified one of id/xid:
        Identifier.of_either(self.id, self.external_id)


class Datapoint(CogniteResource):
    """An object representing a datapoint.

    Args:
        timestamp (int | None): The data timestamp in milliseconds since the epoch (Jan 1, 1970). Can be negative to define a date before 1970. Minimum timestamp is 1900.01.01 00:00:00 UTC
        value (str | float | None): The data value. Can be string or numeric
        average (float | None): The integral average value in the aggregate period
        max (float | None): The maximum value in the aggregate period
        min (float | None): The minimum value in the aggregate period
        count (int | None): The number of datapoints in the aggregate period
        sum (float | None): The sum of the datapoints in the aggregate period
        interpolation (float | None): The interpolated value of the series in the beginning of the aggregate
        step_interpolation (float | None): The last value before or at the beginning of the aggregate.
        continuous_variance (float | None): The variance of the interpolated underlying function.
        discrete_variance (float | None): The variance of the datapoint values.
        total_variation (float | None): The total variation of the interpolated underlying function.
    """

    def __init__(
        self,
        timestamp: int | None = None,
        value: str | float | None = None,
        average: float | None = None,
        max: float | None = None,
        min: float | None = None,
        count: int | None = None,
        sum: float | None = None,
        interpolation: float | None = None,
        step_interpolation: float | None = None,
        continuous_variance: float | None = None,
        discrete_variance: float | None = None,
        total_variation: float | None = None,
    ) -> None:
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

    def to_pandas(self, camel_case: bool = False) -> pandas.DataFrame:  # type: ignore[override]
        """Convert the datapoint into a pandas DataFrame.

        Args:
            camel_case (bool): Convert column names to camel case (e.g. `stepInterpolation` instead of `step_interpolation`)

        Returns:
            pandas.DataFrame: pandas.DataFrame
        """
        pd = cast(Any, local_import("pandas"))

        dumped = self.dump(camel_case=camel_case)
        timestamp = dumped.pop("timestamp")

        return pd.DataFrame(dumped, index=[pd.Timestamp(timestamp, unit="ms")])


class DatapointsArray(CogniteResource):
    """An object representing datapoints using numpy arrays."""

    def __init__(
        self,
        id: int | None = None,
        external_id: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        granularity: str | None = None,
        timestamp: NumpyDatetime64NSArray | None = None,
        value: NumpyFloat64Array | NumpyObjArray | None = None,
        average: NumpyFloat64Array | None = None,
        max: NumpyFloat64Array | None = None,
        min: NumpyFloat64Array | None = None,
        count: NumpyInt64Array | None = None,
        sum: NumpyFloat64Array | None = None,
        interpolation: NumpyFloat64Array | None = None,
        step_interpolation: NumpyFloat64Array | None = None,
        continuous_variance: NumpyFloat64Array | None = None,
        discrete_variance: NumpyFloat64Array | None = None,
        total_variation: NumpyFloat64Array | None = None,
    ) -> None:
        self.id = id
        self.external_id = external_id
        self.is_string = is_string
        self.is_step = is_step
        self.unit = unit
        self.unit_external_id = unit_external_id
        self.granularity = granularity
        self.timestamp = timestamp if timestamp is not None else np.array([], dtype="datetime64[ns]")
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
    def _ts_info(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "external_id": self.external_id,
            "is_string": self.is_string,
            "is_step": self.is_step,
            "unit": self.unit,
            "unit_external_id": self.unit_external_id,
            "granularity": self.granularity,
        }

    @classmethod
    def _load(  # type: ignore [override]
        cls,
        dps_dct: dict[str, int | str | bool | npt.NDArray],
    ) -> DatapointsArray:
        assert isinstance(dps_dct["timestamp"], np.ndarray)  # mypy love
        # Since pandas always uses nanoseconds for datetime, we stick with the same
        # (also future-proofs the SDK; ns is coming!):
        dps_dct["timestamp"] = dps_dct["timestamp"].astype("datetime64[ms]").astype("datetime64[ns]")
        return cls(**convert_all_keys_to_snake_case(dps_dct))

    @classmethod
    def create_from_arrays(cls, *arrays: DatapointsArray) -> DatapointsArray:
        sort_by_time = sorted((a for a in arrays if len(a.timestamp) > 0), key=lambda a: a.timestamp[0])
        if len(sort_by_time) == 0:
            return arrays[0]
        elif len(sort_by_time) == 1:
            return sort_by_time[0]

        first = sort_by_time[0]

        arrays_by_attribute = defaultdict(list)
        for array in sort_by_time:
            for attr, arr in zip(*array._data_fields()):
                arrays_by_attribute[attr].append(arr)
        arrays_by_attribute = {attr: np.concatenate(arrs) for attr, arrs in arrays_by_attribute.items()}  # type: ignore [assignment]

        return cls(**first._ts_info, **arrays_by_attribute)  # type: ignore [arg-type]

    def __len__(self) -> int:
        return len(self.timestamp)

    def __eq__(self, other: Any) -> bool:
        # Override CogniteResource __eq__ which checks exact type & dump being equal. We do not want
        # this: comparing arrays with (mostly) floats is a very bad idea; also dump is exceedingly expensive.
        return id(self) == id(other)

    def __str__(self) -> str:
        return json.dumps(self.dump(convert_timestamps=True), indent=4)

    @overload
    def __getitem__(self, item: int) -> Datapoint:
        ...

    @overload
    def __getitem__(self, item: slice) -> DatapointsArray:
        ...

    def __getitem__(self, item: int | slice) -> Datapoint | DatapointsArray:
        if isinstance(item, slice):
            return self._slice(item)
        attrs, arrays = self._data_fields()
        return Datapoint(
            timestamp=self._dtype_fix(arrays[0][item]) // 1_000_000,
            **{attr: self._dtype_fix(arr[item]) for attr, arr in zip(attrs[1:], arrays[1:])},
        )

    def _slice(self, part: slice) -> DatapointsArray:
        data: dict[str, Any] = {attr: arr[part] for attr, arr in zip(*self._data_fields())}
        return DatapointsArray(**self._ts_info, **data)

    def __iter__(self) -> Iterator[Datapoint]:
        """Iterate over datapoints

        Warning:
            For efficient storage, datapoints are not stored as a sequence of (singular) Datapoint
            objects, so these are created on demand while iterating (slow).

        Returns:
            Iterator[Datapoint]: No description."""
        # Let's not create a single Datapoint more than we have too:
        attrs, arrays = self._data_fields()
        return (
            Datapoint(
                timestamp=self._dtype_fix(row[0]) // 1_000_000, **dict(zip(attrs[1:], map(self._dtype_fix, row[1:])))
            )
            for row in zip(*arrays)
        )

    @cached_property
    def _dtype_fix(self) -> Callable:
        if self.is_string:
            # Return no-op as array contains just references to vanilla python objects:
            return lambda s: s
        # Using .item() on numpy scalars gives us vanilla python types:
        return op.methodcaller("item")

    def _data_fields(self) -> tuple[list[str], list[npt.NDArray]]:
        data_field_tuples = [
            (attr, arr)
            for attr in ("timestamp", "value", *ALL_SORTED_DP_AGGS)  # ts must be first!
            if (arr := getattr(self, attr)) is not None
        ]
        attrs, arrays = map(list, zip(*data_field_tuples))
        return attrs, arrays  # type: ignore [return-value]

    def dump(self, camel_case: bool = False, convert_timestamps: bool = False) -> dict[str, Any]:
        """Dump the DatapointsArray into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Default: False.
            convert_timestamps (bool): Convert timestamps to ISO 8601 formatted strings. Default: False (returns as integer, milliseconds since epoch)

        Returns:
            dict[str, Any]: A dictionary representing the instance.
        """
        attrs, arrays = self._data_fields()
        if not convert_timestamps:  # Eh.. so.. we still have to convert...
            arrays[0] = arrays[0].astype("datetime64[ms]").astype(np.int64)
        else:
            # Note: numpy does not have a strftime method to get the exact format we want (hence the datetime detour)
            #       and for some weird reason .astype(datetime) directly from dt64 returns native integer... whatwhyy
            arrays[0] = arrays[0].astype("datetime64[ms]").astype(datetime).astype(str)

        if camel_case:
            attrs = list(map(to_camel_case, attrs))

        dumped = {**self._ts_info, "datapoints": [dict(zip(attrs, map(self._dtype_fix, row))) for row in zip(*arrays)]}
        if camel_case:
            dumped = convert_all_keys_to_camel_case(dumped)
        return {k: v for k, v in dumped.items() if v is not None}

    def to_pandas(  # type: ignore [override]
        self,
        column_names: Literal["id", "external_id"] = "external_id",
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
    ) -> pandas.DataFrame:
        """Convert the DatapointsArray into a pandas DataFrame.

        Args:
            column_names (Literal["id", "external_id"]): Which field to use as column header. Defaults to "external_id", can also be "id". For time series with no external ID, ID will be used instead.
            include_aggregate_name (bool): Include aggregate in the column name
            include_granularity_name (bool): Include granularity in the column name (after aggregate if present)

        Returns:
            pandas.DataFrame: The datapoints as a pandas DataFrame.
        """
        pd = cast(Any, local_import("pandas"))
        if column_names == "id":
            if self.id is None:
                raise ValueError("Unable to use `id` as column name(s), not set on object")
            identifier = str(self.id)

        elif column_names == "external_id":
            if self.external_id is not None:
                identifier = self.external_id
            elif self.id is not None:
                # Time series are not required to have an external_id unfortunately...
                identifier = str(self.id)
                warnings.warn(
                    f"Time series does not have an external ID, so its ID ({self.id}) was used instead as "
                    'the column name in the DataFrame. If this is expected, consider passing `column_names="id"` '
                    "to silence this warning.",
                    UserWarning,
                )
            else:
                raise ValueError("Object missing both `id` and `external_id` attributes")
        else:
            raise ValueError("Argument `column_names` must be either 'external_id' or 'id'")

        if self.value is not None:
            return pd.DataFrame({identifier: self.value}, index=self.timestamp, copy=False)

        (_, *agg_names), (_, *arrays) = self._data_fields()
        columns = [
            str(identifier) + include_aggregate_name * f"|{agg}" + include_granularity_name * f"|{self.granularity}"
            for agg in agg_names
        ]
        # Since columns might contain duplicates, we can't instantiate from dict as only the
        # last key (array/column) would be kept:
        (df := pd.DataFrame(dict(enumerate(arrays)), index=self.timestamp, copy=False)).columns = columns
        return df


class Datapoints(CogniteResource):
    """An object representing a list of datapoints.

    Args:
        id (int | None): Id of the timeseries the datapoints belong to
        external_id (str | None): External id of the timeseries the datapoints belong to
        is_string (bool | None): Whether the time series is string valued or not.
        is_step (bool | None): Whether the time series is a step series or not.
        unit (str | None): The physical unit of the time series (free-text field).
        unit_external_id (str | None): The unit_external_id (as defined in the unit catalog) of the returned data points. If the datapoints were converted to a compatible unit, this will equal the converted unit, not the one defined on the time series.
        granularity (str | None): The granularity of the aggregate datapoints (does not apply to raw data)
        timestamp (Sequence[int] | None): The data timestamps in milliseconds since the epoch (Jan 1, 1970). Can be negative to define a date before 1970. Minimum timestamp is 1900.01.01 00:00:00 UTC
        value (Sequence[str] | Sequence[float] | None): The data values. Can be string or numeric
        average (list[float] | None): The integral average values in the aggregate period
        max (list[float] | None): The maximum values in the aggregate period
        min (list[float] | None): The minimum values in the aggregate period
        count (list[int] | None): The number of datapoints in the aggregate periods
        sum (list[float] | None): The sum of the datapoints in the aggregate periods
        interpolation (list[float] | None): The interpolated values of the series in the beginning of the aggregates
        step_interpolation (list[float] | None): The last values before or at the beginning of the aggregates.
        continuous_variance (list[float] | None): The variance of the interpolated underlying function.
        discrete_variance (list[float] | None): The variance of the datapoint values.
        total_variation (list[float] | None): The total variation of the interpolated underlying function.
        error (list[None | str] | None): No description.
    """

    def __init__(
        self,
        id: int | None = None,
        external_id: str | None = None,
        is_string: bool | None = None,
        is_step: bool | None = None,
        unit: str | None = None,
        unit_external_id: str | None = None,
        granularity: str | None = None,
        timestamp: Sequence[int] | None = None,
        value: Sequence[str] | Sequence[float] | None = None,
        average: list[float] | None = None,
        max: list[float] | None = None,
        min: list[float] | None = None,
        count: list[int] | None = None,
        sum: list[float] | None = None,
        interpolation: list[float] | None = None,
        step_interpolation: list[float] | None = None,
        continuous_variance: list[float] | None = None,
        discrete_variance: list[float] | None = None,
        total_variation: list[float] | None = None,
        error: list[None | str] | None = None,
    ) -> None:
        self.id = id
        self.external_id = external_id
        self.is_string = is_string
        self.is_step = is_step
        self.unit = unit
        self.unit_external_id = unit_external_id
        self.granularity = granularity
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

        self.__datapoint_objects: list[Datapoint] | None = None

    def __str__(self) -> str:
        item = self.dump()
        item["datapoints"] = convert_time_attributes_to_datetime(item["datapoints"])
        return json.dumps(item, indent=4)

    def __len__(self) -> int:
        return len(self.timestamp)

    def __eq__(self, other: Any) -> bool:
        return (
            type(self) is type(other)
            and self.id == other.id
            and self.external_id == other.external_id
            and list(self._get_non_empty_data_fields()) == list(other._get_non_empty_data_fields())
        )

    @overload
    def __getitem__(self, item: int) -> Datapoint:
        ...

    @overload
    def __getitem__(self, item: slice) -> Datapoints:
        ...

    def __getitem__(self, item: int | slice) -> Datapoint | Datapoints:
        if isinstance(item, slice):
            return self._slice(item)
        dp_args = {}
        for attr, values in self._get_non_empty_data_fields():
            dp_args[attr] = values[item]
        return Datapoint(**dp_args)

    def __iter__(self) -> Iterator[Datapoint]:
        yield from self.__get_datapoint_objects()

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        """Dump the datapoints into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            dict[str, Any]: A dictionary representing the instance.
        """
        dumped = {
            "id": self.id,
            "external_id": self.external_id,
            "is_string": self.is_string,
            "is_step": self.is_step,
            "unit": self.unit,
            "unit_external_id": self.unit_external_id,
            "datapoints": [dp.dump(camel_case=camel_case) for dp in self.__get_datapoint_objects()],
        }
        if camel_case:
            dumped = convert_all_keys_to_camel_case(dumped)
        return {key: value for key, value in dumped.items() if value is not None}

    def to_pandas(  # type: ignore [override]
        self,
        column_names: str = "external_id",
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        include_errors: bool = False,
    ) -> pandas.DataFrame:
        """Convert the datapoints into a pandas DataFrame.

        Args:
            column_names (str): Which field to use as column header. Defaults to "external_id", can also be "id". For time series with no external ID, ID will be used instead.
            include_aggregate_name (bool): Include aggregate in the column name
            include_granularity_name (bool): Include granularity in the column name (after aggregate if present)
            include_errors (bool): For synthetic datapoint queries, include a column with errors.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = cast(Any, local_import("pandas"))
        if column_names in ["external_id", "externalId"]:  # Camel case for backwards compat
            identifier = self.external_id if self.external_id is not None else self.id
        elif column_names == "id":
            identifier = self.id
        else:
            raise ValueError("column_names must be 'external_id' or 'id'")

        if include_errors and self.error is None:
            raise ValueError("Unable to 'include_errors', only available for data from synthetic datapoint queries")

        # Make sure columns (aggregates) always come in alphabetical order (e.g. "average" before "max"):
        field_names, data_lists = [], []
        data_fields = self._get_non_empty_data_fields(get_empty_lists=True, get_error=include_errors)
        if not include_errors:  # We do not touch column ordering for synthetic datapoints
            data_fields = sorted(data_fields)
        for attr, data in data_fields:
            if attr == "timestamp":
                continue
            id_col_name = str(identifier)
            if attr == "value":
                field_names.append(id_col_name)
                data_lists.append(data)
                continue
            if include_aggregate_name:
                id_col_name += f"|{attr}"
            if include_granularity_name and self.granularity is not None:
                id_col_name += f"|{self.granularity}"
            field_names.append(id_col_name)
            if attr == "error":
                data_lists.append(data)
                continue  # Keep string (object) column non-numeric
            data = pd.to_numeric(data, errors="coerce")  # Avoids object dtype for missing aggs
            if attr == "count":
                data_lists.append(data.astype("int64"))
            else:
                data_lists.append(data.astype("float64"))

        idx = pd.to_datetime(self.timestamp, unit="ms")
        (df := pd.DataFrame(dict(enumerate(data_lists)), index=idx)).columns = field_names
        return df

    @classmethod
    def _load(  # type: ignore [override]
        cls,
        dps_object: dict[str, Any],
        expected_fields: list[str] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> Datapoints:
        del cognite_client  # just needed for signature
        instance = cls(
            id=dps_object.get("id"),
            external_id=dps_object.get("externalId"),
            is_string=dps_object["isString"],
            is_step=dps_object.get("isStep"),
            unit=dps_object.get("unit"),
            unit_external_id=dps_object.get("unitExternalId"),
        )
        expected_fields = (expected_fields or ["value"]) + ["timestamp"]
        if len(dps_object["datapoints"]) == 0:
            for key in expected_fields:
                snake_key = to_snake_case(key)
                setattr(instance, snake_key, [])
        else:
            for key in expected_fields:
                data = [dp.get(key) for dp in dps_object["datapoints"]]
                snake_key = to_snake_case(key)
                setattr(instance, snake_key, data)
        return instance

    def _extend(self, other_dps: Datapoints) -> None:
        if self.id is None and self.external_id is None:
            self.id = other_dps.id
            self.external_id = other_dps.external_id
            self.is_string = other_dps.is_string
            self.is_step = other_dps.is_step
            self.unit = other_dps.unit
            self.unit_external_id = other_dps.unit_external_id

        for attr, other_value in other_dps._get_non_empty_data_fields(get_empty_lists=True):
            value = getattr(self, attr)
            if not value:
                setattr(self, attr, other_value)
            else:
                value.extend(other_value)

    def _get_non_empty_data_fields(
        self, get_empty_lists: bool = False, get_error: bool = True
    ) -> list[tuple[str, Any]]:
        non_empty_data_fields = []
        skip_attrs = {"id", "external_id", "is_string", "is_step", "unit", "unit_external_id", "granularity"}
        for attr, value in self.__dict__.copy().items():
            if attr not in skip_attrs and attr[0] != "_" and (attr != "error" or get_error):
                if value is not None or attr == "timestamp":
                    if len(value) > 0 or get_empty_lists or attr == "timestamp":
                        non_empty_data_fields.append((attr, value))
        return non_empty_data_fields

    def __get_datapoint_objects(self) -> list[Datapoint]:
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

    def _slice(self, slice: slice) -> Datapoints:
        truncated_datapoints = Datapoints(id=self.id, external_id=self.external_id)
        for attr, value in self._get_non_empty_data_fields():
            setattr(truncated_datapoints, attr, value[slice])
        return truncated_datapoints

    def _repr_html_(self) -> str:
        is_synthetic_dps = self.error is not None
        return notebook_display_with_fallback(self, include_errors=is_synthetic_dps)


class DatapointsArrayList(CogniteResourceList[DatapointsArray]):
    _RESOURCE = DatapointsArray

    def __init__(self, resources: Collection[Any], cognite_client: CogniteClient | None = None) -> None:
        super().__init__(resources, cognite_client)

        # Fix what happens for duplicated identifiers:
        ids = [dps.id for dps in self if dps.id is not None]
        xids = [dps.external_id for dps in self if dps.external_id is not None]
        dupe_ids, id_dct = find_duplicates(ids), defaultdict(list)
        dupe_xids, xid_dct = find_duplicates(xids), defaultdict(list)

        for dps in self:
            if (id_ := dps.id) is not None and id_ in dupe_ids:
                id_dct[id_].append(dps)
            if (xid := dps.external_id) is not None and xid in dupe_xids:
                xid_dct[xid].append(dps)

        self._id_to_item.update(id_dct)
        self._external_id_to_item.update(xid_dct)

    def concat_duplicate_ids(self) -> None:
        """
        Concatenates all arrays with duplicated IDs.

        Arrays with the same ids are stacked in chronological order.

        **Caveat** This method is not guaranteed to preserve the order of the list.
        """
        # Rebuilt list instead of removing duplicated one at a time at the cost of O(n).
        self.data.clear()

        # This implementation takes advantage of the ordering of the duplicated in the __init__ method
        has_external_ids = set()
        for ext_id, items in self._external_id_to_item.items():
            if not isinstance(items, list):
                self.data.append(items)
                if items.id is not None:
                    has_external_ids.add(items.id)
                continue
            concatenated = DatapointsArray.create_from_arrays(*items)
            self._external_id_to_item[ext_id] = concatenated
            if concatenated.id is not None:
                has_external_ids.add(concatenated.id)
                self._id_to_item[concatenated.id] = concatenated
            self.data.append(concatenated)

        if not (only_ids := set(self._id_to_item) - has_external_ids):
            return

        for id_, items in self._id_to_item.items():
            if id_ not in only_ids:
                continue
            if not isinstance(items, list):
                self.data.append(items)
                continue
            concatenated = DatapointsArray.create_from_arrays(*items)
            self._id_to_item[id_] = concatenated
            self.data.append(concatenated)

    def get(  # type: ignore [override]
        self,
        id: int | None = None,
        external_id: str | None = None,
    ) -> DatapointsArray | list[DatapointsArray] | None:
        """Get a specific DatapointsArray from this list by id or external_id.

        Note: For duplicated time series, returns a list of DatapointsArray.

        Args:
            id (int | None): The id of the item(s) to get.
            external_id (str | None): The external_id of the item(s) to get.

        Returns:
            DatapointsArray | list[DatapointsArray] | None: The requested item(s)
        """
        # TODO: Question, can we type annotate without specifying the function?
        return super().get(id, external_id)

    def __str__(self) -> str:
        return json.dumps(self.dump(convert_timestamps=True), indent=4)

    def to_pandas(  # type: ignore [override]
        self,
        column_names: Literal["id", "external_id"] = "external_id",
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
    ) -> pandas.DataFrame:
        """Convert the DatapointsArrayList into a pandas DataFrame.

        Args:
            column_names (Literal["id", "external_id"]): Which field to use as column header. Defaults to "external_id", can also be "id". For time series with no external ID, ID will be used instead.
            include_aggregate_name (bool): Include aggregate in the column name
            include_granularity_name (bool): Include granularity in the column name (after aggregate if present)

        Returns:
            pandas.DataFrame: The datapoints as a pandas DataFrame.
        """
        pd = cast(Any, local_import("pandas"))
        dfs = [dps.to_pandas(column_names, include_aggregate_name, include_granularity_name) for dps in self]
        if not dfs:
            return pd.DataFrame(index=pd.to_datetime([]))

        return concat_dataframes_with_nullable_int_cols(dfs)

    def dump(self, camel_case: bool = False, convert_timestamps: bool = False) -> list[dict[str, Any]]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Default: False.
            convert_timestamps (bool): Convert timestamps to ISO 8601 formatted strings. Default: False (returns as integer, milliseconds since epoch)

        Returns:
            list[dict[str, Any]]: A list of dicts representing the instance.
        """
        return [dps.dump(camel_case, convert_timestamps) for dps in self]


class DatapointsList(CogniteResourceList[Datapoints]):
    _RESOURCE = Datapoints

    def __init__(self, resources: Collection[Any], cognite_client: CogniteClient | None = None) -> None:
        super().__init__(resources, cognite_client)

        # Fix what happens for duplicated identifiers:
        ids = [dps.id for dps in self if dps.id is not None]
        xids = [dps.external_id for dps in self if dps.external_id is not None]
        dupe_ids, id_dct = find_duplicates(ids), defaultdict(list)
        dupe_xids, xid_dct = find_duplicates(xids), defaultdict(list)

        for dps in self:
            if (id_ := dps.id) is not None and id_ in dupe_ids:
                id_dct[id_].append(dps)
            if (xid := dps.external_id) is not None and xid in dupe_xids:
                xid_dct[xid].append(dps)

        self._id_to_item.update(id_dct)
        self._external_id_to_item.update(xid_dct)

    def get(  # type: ignore [override]
        self,
        id: int | None = None,
        external_id: str | None = None,
    ) -> Datapoints | list[Datapoints] | None:
        """Get a specific Datapoints from this list by id or external_id.

        Note: For duplicated time series, returns a list of Datapoints.

        Args:
            id (int | None): The id of the item(s) to get.
            external_id (str | None): The external_id of the item(s) to get.

        Returns:
            Datapoints | list[Datapoints] | None: The requested item(s)
        """
        # TODO: Question, can we type annotate without specifying the function?
        return super().get(id, external_id)

    def __str__(self) -> str:
        item = self.dump()
        for i in item:
            i["datapoints"] = convert_time_attributes_to_datetime(i["datapoints"])
        return json.dumps(item, default=lambda x: x.__dict__, indent=4)

    def to_pandas(  # type: ignore [override]
        self,
        column_names: Literal["id", "external_id"] = "external_id",
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
    ) -> pandas.DataFrame:
        """Convert the datapoints list into a pandas DataFrame.

        Args:
            column_names (Literal["id", "external_id"]): Which field to use as column header. Defaults to "external_id", can also be "id". For time series with no external ID, ID will be used instead.
            include_aggregate_name (bool): Include aggregate in the column name
            include_granularity_name (bool): Include granularity in the column name (after aggregate if present)

        Returns:
            pandas.DataFrame: The datapoints list as a pandas DataFrame.
        """
        pd = cast(Any, local_import("pandas"))
        dfs = [dps.to_pandas(column_names, include_aggregate_name, include_granularity_name) for dps in self]
        if not dfs:
            return pd.DataFrame(index=pd.to_datetime([]))

        return concat_dataframes_with_nullable_int_cols(dfs)
