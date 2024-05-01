from __future__ import annotations

import json
import typing
import warnings
from collections import defaultdict
from dataclasses import InitVar, dataclass, fields
from datetime import datetime
from enum import IntEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Iterator,
    Literal,
    Sequence,
    overload,
)

from cognite.client._constants import NUMPY_IS_AVAILABLE
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import find_duplicates
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._importing import local_import
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
from cognite.client.utils._time import convert_and_isoformat_time_attrs
from cognite.client.utils.useful_types import SequenceNotStr


class StatusCode(IntEnum):
    Good = 0x0
    Uncertain = 0x40000000  # aka 1 << 30 aka 1073741824
    Bad = 0x80000000  # aka 1 << 31 aka 2147483648


Aggregate = Literal[
    "average",
    "continuous_variance",
    "count",
    "count_bad",
    "count_good",
    "count_uncertain",
    "discrete_variance",
    "duration_bad",
    "duration_good",
    "duration_uncertain",
    "interpolation",
    "max",
    "min",
    "step_interpolation",
    "sum",
    "total_variation",
]
_INT_AGGREGATES = frozenset(
    {
        "count",
        "countBad",
        "countGood",
        "countUncertain",
        "durationBad",
        "durationGood",
        "durationUncertain",
    }
)
ALL_SORTED_DP_AGGS = sorted(typing.get_args(Aggregate))

if NUMPY_IS_AVAILABLE:
    import numpy as np

if TYPE_CHECKING:
    import numpy.typing as npt
    import pandas

    from cognite.client import CogniteClient

    NumpyDatetime64NSArray = npt.NDArray[np.datetime64]
    NumpyUInt32Array = npt.NDArray[np.uint32]
    NumpyInt64Array = npt.NDArray[np.int64]
    NumpyFloat64Array = npt.NDArray[np.float64]
    NumpyObjArray = npt.NDArray[np.object_]


def numpy_dtype_fix(element: np.float64 | str) -> float | str:
    try:
        # Using .item() on numpy scalars gives us vanilla python types:
        return element.item()  # type: ignore [union-attr]
    except AttributeError:
        # Return no-op as array contains just references to vanilla python objects:
        if isinstance(element, str):
            return element
        raise


_NOT_SET = object()


@dataclass(frozen=True)
class DatapointsQuery:
    """Represent a user request for datapoints for a single time series"""

    id: InitVar[int | None] = None
    external_id: InitVar[str | None] = None
    start: int | str | datetime = _NOT_SET  # type: ignore [assignment]
    end: int | str | datetime = _NOT_SET  # type: ignore [assignment]
    aggregates: Aggregate | str | list[Aggregate | str] = _NOT_SET  # type: ignore [assignment]
    granularity: str = _NOT_SET  # type: ignore [assignment]
    target_unit: str = _NOT_SET  # type: ignore [assignment]
    target_unit_system: str = _NOT_SET  # type: ignore [assignment]
    limit: int | None = _NOT_SET  # type: ignore [assignment]
    include_outside_points: bool = _NOT_SET  # type: ignore [assignment]
    ignore_unknown_ids: bool = _NOT_SET  # type: ignore [assignment]
    include_status: bool = _NOT_SET  # type: ignore [assignment]
    ignore_bad_datapoints: bool = _NOT_SET  # type: ignore [assignment]
    treat_uncertain_as_bad: bool = _NOT_SET  # type: ignore [assignment]

    def __post_init__(self, id: int | None, external_id: str | None) -> None:
        # Ensure user have just specified one of id/xid:
        object.__setattr__(self, "_identifier", Identifier.of_either(id, external_id))

    @property
    def identifier(self) -> Identifier:
        return self._identifier  # type: ignore [attr-defined]

    def __repr__(self) -> str:
        return json.dumps(self.dump(), indent=4)

    def dump(self) -> dict[str, Any]:
        # We need to dump only those fields specifically passed by the user:
        return {
            **self.identifier.as_dict(camel_case=False),
            **dict((fld.name, val) for fld in fields(self) if (val := getattr(self, fld.name)) is not _NOT_SET),
        }


@dataclass(frozen=True)
class LatestDatapointQuery:
    """Parameters describing a query for the latest datapoint from a time series.

    Note:
        Pass either ID or external ID.

    Args:
        id (Optional[int]): The internal ID of the time series to query.
        external_id (Optional[str]): The external ID of the time series to query.
        before (Union[None, int, str, datetime]): Get latest datapoint before this time. None means 'now'.
        target_unit (str | None): The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
        target_unit_system (str | None): The unit system of the data points returned. Cannot be used with target_unit.
        include_status (bool): Also return the status code, an integer, for each datapoint in the response.
        ignore_bad_datapoints (bool): Prevent data points with a bad status code to be returned. Default: True.
        treat_uncertain_as_bad (bool): Treat uncertain status codes as bad. If false, treat uncertain as good. Default: True.
    """

    id: InitVar[int | None] = None
    external_id: InitVar[str | None] = None
    before: None | int | str | datetime = None
    target_unit: str | None = None
    target_unit_system: str | None = None
    include_status: bool | None = None
    ignore_bad_datapoints: bool | None = None
    treat_uncertain_as_bad: bool | None = None

    def __post_init__(self, id: int | None, external_id: str | None) -> None:
        # Ensure user have just specified one of id/xid:
        object.__setattr__(self, "_identifier", Identifier.of_either(id, external_id))

    @property
    def identifier(self) -> Identifier:
        return self._identifier  # type: ignore [attr-defined]


class Datapoint(CogniteResource):
    """An object representing a datapoint.

    Args:
        timestamp (int | None): The data timestamp in milliseconds since the epoch (Jan 1, 1970). Can be negative to define a date before 1970. Minimum timestamp is 1900.01.01 00:00:00 UTC
        value (str | float | None): The raw data value. Can be string or numeric.
        average (float | None): The time-weighted average value in the aggregate interval.
        max (float | None): The maximum value in the aggregate interval.
        min (float | None): The minimum value in the aggregate interval.
        count (int | None): The number of raw datapoints in the aggregate interval.
        sum (float | None): The sum of the raw datapoints in the aggregate interval.
        interpolation (float | None): The interpolated value at the beginning of the aggregate interval.
        step_interpolation (float | None): The interpolated value at the beginning of the aggregate interval using stepwise interpretation.
        continuous_variance (float | None): The variance of the interpolated underlying function.
        discrete_variance (float | None): The variance of the datapoint values.
        total_variation (float | None): The total variation of the interpolated underlying function.
        count_bad (int | None): The number of raw datapoints with a bad status code, in the aggregate interval.
        count_good (int | None): The number of raw datapoints with a good status code, in the aggregate interval.
        count_uncertain (int | None): The number of raw datapoints with a uncertain status code, in the aggregate interval.
        duration_bad (int | None): The duration the aggregate is defined and marked as bad (measured in milliseconds).
        duration_good (int | None): The duration the aggregate is defined and marked as good (measured in milliseconds).
        duration_uncertain (int | None): The duration the aggregate is defined and marked as uncertain (measured in milliseconds).
        status_code (int | None): The status code for the raw datapoint.
        status_symbol (str | None): The status symbol for the raw datapoint.
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
        count_bad: int | None = None,
        count_good: int | None = None,
        count_uncertain: int | None = None,
        duration_bad: int | None = None,
        duration_good: int | None = None,
        duration_uncertain: int | None = None,
        status_code: int | None = None,
        status_symbol: str | None = None,
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
        self.count_bad = count_bad
        self.count_good = count_good
        self.count_uncertain = count_uncertain
        self.duration_bad = duration_bad
        self.duration_good = duration_good
        self.duration_uncertain = duration_uncertain
        self.status_code = status_code
        self.status_symbol = status_symbol

    def to_pandas(self, camel_case: bool = False) -> pandas.DataFrame:  # type: ignore[override]
        """Convert the datapoint into a pandas DataFrame.

        Args:
            camel_case (bool): Convert column names to camel case (e.g. `stepInterpolation` instead of `step_interpolation`)

        Returns:
            pandas.DataFrame: pandas.DataFrame
        """
        pd = local_import("pandas")

        dumped = self.dump(camel_case=camel_case)
        timestamp = dumped.pop("timestamp")

        return pd.DataFrame(dumped, index=[pd.Timestamp(timestamp, unit="ms")])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        # Keep value even if None (bad status codes support missing):
        return {"value": self.value, **super().dump(camel_case=camel_case)}


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
        count_bad: NumpyInt64Array | None = None,
        count_good: NumpyInt64Array | None = None,
        count_uncertain: NumpyInt64Array | None = None,
        duration_bad: NumpyInt64Array | None = None,
        duration_good: NumpyInt64Array | None = None,
        duration_uncertain: NumpyInt64Array | None = None,
        status_code: NumpyUInt32Array | None = None,
        status_symbol: NumpyObjArray | None = None,
        null_timestamps: set[int] | None = None,
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
        self.count_bad = count_bad
        self.count_good = count_good
        self.count_uncertain = count_uncertain
        self.duration_bad = duration_bad
        self.duration_good = duration_good
        self.duration_uncertain = duration_uncertain
        self.status_code = status_code
        self.status_symbol = status_symbol
        self.null_timestamps = null_timestamps

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
    def _load_from_arrays(
        cls,
        dps_dct: dict[str, Any],
        cognite_client: CogniteClient | None = None,
    ) -> DatapointsArray:
        assert isinstance(dps_dct["timestamp"], np.ndarray)  # mypy love
        # Since pandas always uses nanoseconds for datetime, we stick with the same
        # (also future-proofs the SDK; ns may be coming!):
        dps_dct["timestamp"] = dps_dct["timestamp"].astype("datetime64[ms]").astype("datetime64[ns]")
        return cls(**convert_all_keys_to_snake_case(dps_dct))

    @classmethod
    def _load(
        cls,
        dps_dct: dict[str, Any],
        cognite_client: CogniteClient | None = None,
    ) -> DatapointsArray:
        array_by_attr = {}
        if "datapoints" in dps_dct:
            datapoints_by_attr = defaultdict(list)
            for row in dps_dct["datapoints"]:
                for attr, value in row.items():
                    datapoints_by_attr[attr].append(value)
            status = datapoints_by_attr.pop("status", None)
            for attr, values in datapoints_by_attr.items():
                if attr == "timestamp":
                    array_by_attr[attr] = np.array(values, dtype="datetime64[ms]").astype("datetime64[ns]")
                elif attr in _INT_AGGREGATES:
                    array_by_attr[attr] = np.array(values, dtype=np.int64)
                else:
                    try:
                        array_by_attr[attr] = np.array(values, dtype=np.float64)
                    except ValueError:
                        array_by_attr[attr] = np.array(values, dtype=np.object_)
            if status is not None:
                array_by_attr["statusCode"] = np.array([s["code"] for s in status], dtype=np.uint32)
                array_by_attr["statusSymbol"] = np.array([s["symbol"] for s in status], dtype=np.object_)

        return cls(
            id=dps_dct.get("id"),
            external_id=dps_dct.get("externalId"),
            is_step=dps_dct.get("isStep"),
            is_string=dps_dct.get("isString"),
            unit=dps_dct.get("unit"),
            granularity=dps_dct.get("granularity"),
            unit_external_id=dps_dct.get("unitExternalId"),
            timestamp=array_by_attr.get("timestamp"),
            value=array_by_attr.get("value"),
            average=array_by_attr.get("average"),
            max=array_by_attr.get("max"),
            min=array_by_attr.get("min"),
            count=array_by_attr.get("count"),
            sum=array_by_attr.get("sum"),
            interpolation=array_by_attr.get("interpolation"),
            step_interpolation=array_by_attr.get("stepInterpolation"),
            continuous_variance=array_by_attr.get("continuousVariance"),
            discrete_variance=array_by_attr.get("discreteVariance"),
            total_variation=array_by_attr.get("totalVariation"),
            count_bad=array_by_attr.get("countBad"),
            count_good=array_by_attr.get("countGood"),
            count_uncertain=array_by_attr.get("countUncertain"),
            duration_bad=array_by_attr.get("durationBad"),
            duration_good=array_by_attr.get("durationGood"),
            duration_uncertain=array_by_attr.get("durationUncertain"),
            status_code=array_by_attr.get("statusCode"),
            status_symbol=array_by_attr.get("statusSymbol"),
            null_timestamps=set(dps_dct["nullTimestamps"]) if "nullTimestamps" in dps_dct else None,
        )

    @classmethod
    def create_from_arrays(cls, *arrays: DatapointsArray) -> DatapointsArray:
        sort_by_time = sorted((a for a in arrays if len(a.timestamp) > 0), key=lambda a: a.timestamp[0])
        if len(sort_by_time) == 0:
            return arrays[0]

        first = sort_by_time[0]
        if len(sort_by_time) == 1:
            return first

        arrays_by_attribute = defaultdict(list)
        for array in sort_by_time:
            for attr, arr in zip(*array._data_fields()):
                arrays_by_attribute[attr].append(arr)
        arrays_by_attribute = {attr: np.concatenate(arrs) for attr, arrs in arrays_by_attribute.items()}  # type: ignore [assignment]

        all_null_ts = set().union(*(arr.null_timestamps for arr in sort_by_time if arr.null_timestamps))
        return cls(
            **first._ts_info,
            **arrays_by_attribute,  # type: ignore [arg-type]
            null_timestamps=all_null_ts,
        )

    def __len__(self) -> int:
        return len(self.timestamp)

    def __eq__(self, other: Any) -> bool:
        # Override CogniteResource __eq__ which checks exact type & dump being equal. We do not want
        # this: comparing arrays with (mostly) floats is a very bad idea; also dump is exceedingly expensive.
        return id(self) == id(other)

    def __str__(self) -> str:
        return _json.dumps(self.dump(convert_timestamps=True), indent=4)

    @overload
    def __getitem__(self, item: int) -> Datapoint: ...

    @overload
    def __getitem__(self, item: slice) -> DatapointsArray: ...

    def __getitem__(self, item: int | slice) -> Datapoint | DatapointsArray:
        if isinstance(item, slice):
            return self._slice(item)
        attrs, arrays = self._data_fields()
        timestamp = arrays[0][item].item() // 1_000_000
        data: dict[str, float | str | None] = {
            attr: numpy_dtype_fix(arr[item]) for attr, arr in zip(attrs[1:], arrays[1:])
        }

        if self.status_code is not None:
            data.update(status_code=self.status_code[item], status_symbol=self.status_symbol[item])  # type: ignore [index]
        if self.null_timestamps and timestamp in self.null_timestamps:
            data["value"] = None
        return Datapoint(timestamp=timestamp, **data)  # type: ignore [arg-type]

    def _slice(self, part: slice) -> DatapointsArray:
        data: dict[str, Any] = {attr: arr[part] for attr, arr in zip(*self._data_fields())}
        if self.status_code is not None:
            data.update(status_code=self.status_code[part], status_symbol=self.status_symbol[part])  # type: ignore [index]
        if self.null_timestamps is not None:
            data["null_timestamps"] = self.null_timestamps.intersection(
                data["timestamp"].astype("datetime64[ms]").astype(np.int64).tolist()
            )
        return DatapointsArray(**self._ts_info, **data)

    def __iter__(self) -> Iterator[Datapoint]:
        """Iterate over datapoints

        Warning:
            For efficient storage, datapoints are not stored as a sequence of (singular) Datapoint
            objects, so these are created on demand while iterating (slow).

        Yields:
            Datapoint: No description.
        """
        warnings.warn(
            "Iterating through a DatapointsArray is very inefficient. Tip: Access the arrays directly and use "
            "vectorised numpy ops on those. E.g. `dps.average` for the 'average' aggregate, `dps.value` for the "
            "raw datapoints or `dps.timestamp` for the timestamps. You may also convert to a pandas DataFrame using "
            "`dps.to_pandas()`.",
            UserWarning,
        )
        attrs, arrays = self._data_fields()
        # Let's not create a single Datapoint more than we have too:
        for i, row in enumerate(zip(*arrays)):
            timestamp = row[0].item() // 1_000_000
            data: dict[str, float | str | None] = dict(zip(attrs[1:], map(numpy_dtype_fix, row[1:])))
            if self.status_code is not None:
                data.update(status_code=self.status_code[i], status_symbol=self.status_symbol[i])  # type: ignore [index]
            if self.null_timestamps and timestamp in self.null_timestamps:
                data["value"] = None

            yield Datapoint(timestamp=timestamp, **data)  # type: ignore [arg-type]

    def _data_fields(self) -> tuple[list[str], list[npt.NDArray]]:
        # Note: Does not return status-related fields
        data_field_tuples = [
            (attr, arr)
            for attr in ("timestamp", "value", *ALL_SORTED_DP_AGGS)  # ts must be first
            if (arr := getattr(self, attr)) is not None
        ]
        attrs, arrays = map(list, zip(*data_field_tuples))
        return attrs, arrays

    def dump(self, camel_case: bool = True, convert_timestamps: bool = False) -> dict[str, Any]:
        """Dump the DatapointsArray into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.
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

        dumped = self._ts_info
        datapoints = [dict(zip(attrs, map(numpy_dtype_fix, row))) for row in zip(*arrays)]

        if self.status_code is not None or self.status_symbol is not None:
            if (
                self.status_code is None
                or self.status_symbol is None
                or not len(self.status_symbol) == len(datapoints) == len(self.status_code)
            ):
                raise ValueError("The number of status codes/symbols does not match the number of datapoints")

            for dp, code, symbol in zip(datapoints, map(numpy_dtype_fix, self.status_code), self.status_symbol):
                dp["status"] = {"code": code, "symbol": symbol}  # type: ignore [assignment]

        # When we're dealing with datapoints with bad status codes, NaN might be either one of [<missing>, nan]:
        if self.null_timestamps:
            for dp in datapoints:
                if dp["timestamp"] in self.null_timestamps:  # ...luckily, we know :3
                    dp["value"] = None  # type: ignore [assignment]
        dumped["datapoints"] = datapoints

        if camel_case:
            dumped = convert_all_keys_to_camel_case(dumped)
        return {k: v for k, v in dumped.items() if v is not None}

    def to_pandas(  # type: ignore [override]
        self,
        column_names: Literal["id", "external_id"] = "external_id",
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        include_status: bool = True,
    ) -> pandas.DataFrame:
        """Convert the DatapointsArray into a pandas DataFrame.

        Args:
            column_names (Literal["id", "external_id"]): Which field to use as column header. Defaults to "external_id", can also be "id". For time series with no external ID, ID will be used instead.
            include_aggregate_name (bool): Include aggregate in the column name
            include_granularity_name (bool): Include granularity in the column name (after aggregate if present)
            include_status (bool): Include status code and status symbol as separate columns, if available.

        Returns:
            pandas.DataFrame: The datapoints as a pandas DataFrame.
        """
        pd = local_import("pandas")
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
            raw_columns: dict[str, npt.NDArray] = {identifier: self.value}
            if include_status:
                if self.status_code is not None:
                    raw_columns[f"{identifier}|status_code"] = self.status_code
                if self.status_symbol is not None:
                    raw_columns[f"{identifier}|status_symbol"] = self.status_symbol
            return pd.DataFrame(raw_columns, index=self.timestamp, copy=False)

        (_, *agg_names), (_, *arrays) = self._data_fields()
        aggregate_columns = [
            str(identifier) + include_aggregate_name * f"|{agg}" + include_granularity_name * f"|{self.granularity}"
            for agg in agg_names
        ]
        # Since columns might contain duplicates, we can't instantiate from dict as only the
        # last key (array/column) would be kept:
        (df := pd.DataFrame(dict(enumerate(arrays)), index=self.timestamp, copy=False)).columns = aggregate_columns
        return df


class Datapoints(CogniteResource):
    """An object representing a list of datapoints.

    Args:
        id (int | None): Id of the time series the datapoints belong to
        external_id (str | None): External id of the time series the datapoints belong to
        is_string (bool | None): Whether the time series contains numerical or string data.
        is_step (bool | None): Whether the time series is stepwise or continuous.
        unit (str | None): The physical unit of the time series (free-text field). Omitted if the datapoints were converted to another unit.
        unit_external_id (str | None): The unit_external_id (as defined in the unit catalog) of the returned data points. If the datapoints were converted to a compatible unit, this will equal the converted unit, not the one defined on the time series.
        granularity (str | None): The granularity of the aggregate datapoints (does not apply to raw data)
        timestamp (Sequence[int] | None): The data timestamps in milliseconds since the epoch (Jan 1, 1970). Can be negative to define a date before 1970. Minimum timestamp is 1900.01.01 00:00:00 UTC
        value (SequenceNotStr[str] | Sequence[float] | None): The raw data values. Can be string or numeric.
        average (list[float] | None): The time-weighted average values per aggregate interval.
        max (list[float] | None): The maximum values per aggregate interval.
        min (list[float] | None): The minimum values per aggregate interval.
        count (list[int] | None): The number of raw datapoints per aggregate interval.
        sum (list[float] | None): The sum of the raw datapoints per aggregate interval.
        interpolation (list[float] | None): The interpolated values at the beginning of each the aggregate interval.
        step_interpolation (list[float] | None): The interpolated values at the beginning of each the aggregate interval using stepwise interpretation.
        continuous_variance (list[float] | None): The variance of the interpolated underlying function.
        discrete_variance (list[float] | None): The variance of the datapoint values.
        total_variation (list[float] | None): The total variation of the interpolated underlying function.
        count_bad (list[int] | None): The number of raw datapoints with a bad status code, per aggregate interval.
        count_good (list[int] | None): The number of raw datapoints with a good status code, per aggregate interval.
        count_uncertain (list[int] | None): The number of raw datapoints with a uncertain status code, per aggregate interval.
        duration_bad (list[int] | None): The duration the aggregate is defined and marked as bad (measured in milliseconds).
        duration_good (list[int] | None): The duration the aggregate is defined and marked as good (measured in milliseconds).
        duration_uncertain (list[int] | None): The duration the aggregate is defined and marked as uncertain (measured in milliseconds).
        status_code (list[int] | None): The status codes for the raw datapoints.
        status_symbol (list[str] | None): The status symbols for the raw datapoints.
        error (list[None | str] | None): Human readable strings with description of what went wrong (returned by synthetic datapoints queries).
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
        value: SequenceNotStr[str] | Sequence[float] | None = None,
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
        count_bad: list[int] | None = None,
        count_good: list[int] | None = None,
        count_uncertain: list[int] | None = None,
        duration_bad: list[int] | None = None,
        duration_good: list[int] | None = None,
        duration_uncertain: list[int] | None = None,
        status_code: list[int] | None = None,
        status_symbol: list[str] | None = None,
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
        self.count_bad = count_bad
        self.count_good = count_good
        self.count_uncertain = count_uncertain
        self.duration_bad = duration_bad
        self.duration_good = duration_good
        self.duration_uncertain = duration_uncertain
        self.status_code = status_code
        self.status_symbol = status_symbol
        self.error = error

        self.__datapoint_objects: list[Datapoint] | None = None

    def __str__(self) -> str:
        item = self.dump()
        item["datapoints"] = convert_and_isoformat_time_attrs(item["datapoints"])
        return _json.dumps(item, indent=4)

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
    def __getitem__(self, item: int) -> Datapoint: ...

    @overload
    def __getitem__(self, item: slice) -> Datapoints: ...

    def __getitem__(self, item: int | slice) -> Datapoint | Datapoints:
        if isinstance(item, slice):
            return self._slice(item)
        dp_args = {}
        for attr, values in self._get_non_empty_data_fields():
            dp_args[attr] = values[item]

        if self.status_code is not None:
            dp_args.update(status_code=self.status_code[item], status_symbol=self.status_symbol[item])  # type: ignore [index]

        return Datapoint(**dp_args)

    def __iter__(self) -> Iterator[Datapoint]:
        yield from self.__get_datapoint_objects()

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the datapoints into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representing the instance.
        """
        dumped: dict[str, Any] = {
            "id": self.id,
            "external_id": self.external_id,
            "is_string": self.is_string,
            "is_step": self.is_step,
            "unit": self.unit,
            "unit_external_id": self.unit_external_id,
        }
        datapoints = [dp.dump(camel_case=camel_case) for dp in self.__get_datapoint_objects()]
        if self.status_code is not None or self.status_symbol is not None:
            if (
                self.status_code is None
                or self.status_symbol is None
                or not len(self.status_symbol) == len(datapoints) == len(self.status_code)
            ):
                raise ValueError("The number of status codes/symbols does not match the number of datapoints")
            for dp in datapoints:
                dp["status"] = {"code": dp.pop("statusCode"), "symbol": dp.pop("statusSymbol")}
        dumped["datapoints"] = datapoints

        if camel_case:
            dumped = convert_all_keys_to_camel_case(dumped)
        return {key: value for key, value in dumped.items() if value is not None}

    def to_pandas(  # type: ignore [override]
        self,
        column_names: str = "external_id",
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        include_errors: bool = False,
        include_status: bool = True,
    ) -> pandas.DataFrame:
        """Convert the datapoints into a pandas DataFrame.

        Args:
            column_names (str): Which field to use as column header. Defaults to "external_id", can also be "id". For time series with no external ID, ID will be used instead.
            include_aggregate_name (bool): Include aggregate in the column name
            include_granularity_name (bool): Include granularity in the column name (after aggregate if present)
            include_errors (bool): For synthetic datapoint queries, include a column with errors.
            include_status (bool): Include status code and status symbol as separate columns, if available.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = local_import("pandas")
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
                if include_status:
                    if self.status_code is not None:
                        field_names.append(f"{identifier}|status_code")
                        data_lists.append(self.status_code)
                    if self.status_symbol is not None:
                        field_names.append(f"{identifier}|status_symbol")
                        data_lists.append(self.status_symbol)
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
            if to_camel_case(attr) in _INT_AGGREGATES:
                data_lists.append(data.astype("int64"))
            else:
                data_lists.append(data.astype("float64"))

        idx = pd.to_datetime(self.timestamp, unit="ms")
        (df := pd.DataFrame(dict(enumerate(data_lists)), index=idx)).columns = field_names
        return df

    @classmethod
    def _load_from_synthetic(
        cls,
        dps_object: dict[str, Any],
        cognite_client: CogniteClient | None = None,
    ) -> Datapoints:
        if dps := dps_object["datapoints"]:
            for dp in dps:
                dp.setdefault("error", None)
                dp.setdefault("value", None)
            return cls._load(dps_object, cognite_client=cognite_client)

        instance = cls._load(dps_object, cognite_client=cognite_client)
        instance.error, instance.value = [], []
        return instance

    # TODO: remove 'expected_fields' in the next major version:
    #       the method should not need to be told what to load...
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
            is_string=dps_object.get("isString"),
            is_step=dps_object.get("isStep"),
            unit=dps_object.get("unit"),
            unit_external_id=dps_object.get("unitExternalId"),
        )
        expected_fields = (expected_fields or ["value"]) + ["timestamp"]
        if len(dps_object["datapoints"]) == 0:
            for key in expected_fields:
                snake_key = to_snake_case(key)
                setattr(instance, snake_key, [])
            return instance

        data_lists = defaultdict(list)
        for row in dps_object["datapoints"]:
            for attr, value in row.items():
                data_lists[attr].append(value)
        if (status := data_lists.pop("status", None)) is not None:
            data_lists["status_code"] = [s["code"] for s in status]
            data_lists["status_symbol"] = [s["symbol"] for s in status]

        for key, data in data_lists.items():
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
        skip_attrs = {
            "id",
            "external_id",
            "is_string",
            "is_step",
            "unit",
            "unit_external_id",
            "granularity",
            "status_code",
            "status_symbol",
        }
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
                if self.status_code is not None:
                    dp_args.update(
                        statusCode=self.status_code[i],
                        statusSymbol=self.status_symbol[i],  # type: ignore [index]
                    )
            new_dps_objects.append(Datapoint.load(dp_args))
        self.__datapoint_objects = new_dps_objects
        return self.__datapoint_objects

    def _slice(self, slice: slice) -> Datapoints:
        truncated_datapoints = Datapoints(
            id=self.id,
            external_id=self.external_id,
            is_string=self.is_string,
            is_step=self.is_step,
            unit=self.unit,
            unit_external_id=self.unit_external_id,
            granularity=self.granularity,
        )
        for attr, value in self._get_non_empty_data_fields():
            setattr(truncated_datapoints, attr, value[slice])
        if self.status_code is not None:
            truncated_datapoints.status_code = self.status_code[slice]
            truncated_datapoints.status_symbol = self.status_symbol[slice]  # type: ignore [index]
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
        return _json.dumps(self.dump(convert_timestamps=True), indent=4)

    def to_pandas(  # type: ignore [override]
        self,
        column_names: Literal["id", "external_id"] = "external_id",
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        include_status: bool = True,
    ) -> pandas.DataFrame:
        """Convert the DatapointsArrayList into a pandas DataFrame.

        Args:
            column_names (Literal["id", "external_id"]): Which field to use as column header. Defaults to "external_id", can also be "id". For time series with no external ID, ID will be used instead.
            include_aggregate_name (bool): Include aggregate in the column name
            include_granularity_name (bool): Include granularity in the column name (after aggregate if present)
            include_status (bool): Include status code and status symbol as separate columns, if available.

        Returns:
            pandas.DataFrame: The datapoints as a pandas DataFrame.
        """
        pd = local_import("pandas")
        dfs = [
            dps.to_pandas(
                column_names=column_names,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
                include_status=include_status,
            )
            for dps in self
        ]
        if not dfs:
            return pd.DataFrame(index=pd.to_datetime([]))

        return concat_dataframes_with_nullable_int_cols(dfs)

    def dump(self, camel_case: bool = True, convert_timestamps: bool = False) -> list[dict[str, Any]]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.
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
            i["datapoints"] = convert_and_isoformat_time_attrs(i["datapoints"])
        return _json.dumps(item, indent=4)

    def to_pandas(  # type: ignore [override]
        self,
        column_names: Literal["id", "external_id"] = "external_id",
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        include_status: bool = True,
    ) -> pandas.DataFrame:
        """Convert the datapoints list into a pandas DataFrame.

        Args:
            column_names (Literal["id", "external_id"]): Which field to use as column header. Defaults to "external_id", can also be "id". For time series with no external ID, ID will be used instead.
            include_aggregate_name (bool): Include aggregate in the column name
            include_granularity_name (bool): Include granularity in the column name (after aggregate if present)
            include_status (bool): Include status code and status symbol as separate columns, if available.

        Returns:
            pandas.DataFrame: The datapoints list as a pandas DataFrame.
        """
        pd = local_import("pandas")
        dfs = [
            dps.to_pandas(
                column_names=column_names,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
                include_status=include_status,
            )
            for dps in self
        ]
        if not dfs:
            return pd.DataFrame(index=pd.to_datetime([]))

        return concat_dataframes_with_nullable_int_cols(dfs)
