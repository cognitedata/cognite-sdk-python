from __future__ import annotations

import datetime
import itertools
import re
import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cache
from inspect import signature
from numbers import Integral
from typing import TYPE_CHECKING, Any, Literal
from zoneinfo import ZoneInfo

from cognite.client.data_classes.datapoint_aggregates import (
    _AGGREGATES_WITH_UNIT,
    _ALL_AGGREGATES,
    INT_AGGREGATES,
    OBJECT_AGGREGATES,
)
from cognite.client.exceptions import CogniteImportError
from cognite.client.utils._importing import local_import
from cognite.client.utils._text import shorten, to_camel_case
from cognite.client.utils._time import TIME_ATTRIBUTES

if TYPE_CHECKING:
    import numpy as np
    import numpy.typing as npt
    import pandas as pd

    from cognite.client.data_classes import Datapoints, DatapointsArray, DatapointsArrayList, DatapointsList
    from cognite.client.data_classes._base import CogniteResource, T_CogniteResource, T_CogniteResourceList
    from cognite.client.data_classes.data_modeling.ids import NodeId
    from cognite.client.data_classes.datapoints import (
        NumpyDatetime64NSArray,
        NumpyFloat64Array,
        NumpyInt64Array,
        NumpyObjArray,
        NumpyUInt32Array,
    )


NULLABLE_INT_COLS = {
    "start_time",
    "end_time",
    "asset_id",
    "parent_id",
    "data_set_id",
    "scheduled_time",
    "schedule_id",
    "session_id",
    "deleted_time",
    "last_success",
    "last_failure",
    "last_seen",
    "last_seen_time",
    "last_updated_time",
}
NULLABLE_INT_COLS |= set(map(to_camel_case, NULLABLE_INT_COLS))


def pandas_major_version() -> int:
    from pandas import __version__

    return int(__version__.split(".")[0])


@cache
def is_pandas_v2_or_lower() -> bool:
    """Pandas v2 (and lower) always defaulted to nanosecond-precision timestamps, so a lot of user code
    (implicitly or not) relies on that. Pandas v3 instead infers precision from the input on a case-by-case
    basis, which for us would give arbitrary/unpredictable results, so from v3 onwards we instead pin all
    timestamp columns/indices produced by the SDK to millisecond precision, matching CDF's native resolution.
    """
    return pandas_major_version() < 3


@cache
def timestamp_dtype_unit() -> Literal["ns", "ms"]:
    """The datetime64 resolution to use for all timestamp columns/indices produced by the SDK, see
    `is_pandas_v2_or_lower` for the reasoning."""
    return "ns" if is_pandas_v2_or_lower() else "ms"


def to_pandas_timestamp(ms: int, tz: str | datetime.timezone | None = None) -> pd.Timestamp:
    """Create a pandas Timestamp from milliseconds since epoch, at the resolution given by `timestamp_dtype_unit`."""
    import pandas as pd

    return pd.Timestamp(ms, unit="ms", tz=tz).as_unit(timestamp_dtype_unit())


def to_pandas_datetime_index(
    timestamps: list[int],
    timezone: str | datetime.timezone | ZoneInfo | None,
    assume_utc: bool = False,
) -> pd.DatetimeIndex:
    """Create a DatetimeIndex from epoch-millisecond timestamps with SDK timestamp resolution."""
    import pandas as pd

    if assume_utc or timezone is not None:
        dt_index = pd.to_datetime(timestamps, unit="ms", utc=True)
    else:
        dt_index = pd.to_datetime(timestamps, unit="ms")

    if timezone is not None:
        dt_index = dt_index.tz_convert(convert_tz_for_pandas(timezone))
    return dt_index.as_unit(timestamp_dtype_unit())


def _to_pandas_datetime_index_from_numpy(
    timestamps: NumpyDatetime64NSArray,
    timezone: str | datetime.timezone | ZoneInfo | None,
) -> pd.DatetimeIndex:
    import pandas as pd

    dt_index = pd.to_datetime(timestamps, utc=timezone is not None)
    if timezone is not None:
        dt_index = dt_index.tz_convert(convert_tz_for_pandas(timezone))
    return dt_index.as_unit(timestamp_dtype_unit())


def convert_tz_for_pandas(tz: str | datetime.timezone | ZoneInfo | None) -> str | datetime.timezone | None:
    if tz is None or isinstance(tz, (str, datetime.timezone)):
        return tz
    if isinstance(tz, ZoneInfo):
        # pandas is not happy about ZoneInfo :shrug:
        if tz.key is not None:
            return tz.key
        raise ValueError("timezone of type ZoneInfo does not have the required 'key' attribute set")
    raise ValueError(f"'timezone' not understood, expected one of: [None, datetime.timezone, ZoneInfo], got {type(tz)}")


def concat_dps_dataframe_list(
    dps_lst: DatapointsList | DatapointsArrayList,
    include_aggregate_name: bool,
    include_granularity_name: bool,
    include_status: bool,
    include_unit: bool,
    exclude_numeric_states: bool,
    exclude_string_states: bool,
) -> pd.DataFrame:
    import pandas as pd

    if not dps_lst.data:
        return pd.DataFrame(index=pd.to_datetime([]))

    timezones = set(dps.timezone for dps in dps_lst) - {None}
    # If attempting to join naive & aware, pandas will raise (so we don't need to):
    # TypeError: Cannot join tz-naive with tz-aware DatetimeIndex
    if len(timezones) > 1:
        warnings.warn(
            f"When concatenating datapoints localized to different timezones ({sorted(map(str, timezones))}), the "
            "final dataframe index (timestamps) will be a union of the UTC converted timestamps.",
            UserWarning,
        )
    # Since we use a MultiIndex for the dataframe columns, these do not join nicely in pd.concat, so we need
    # to do that manually ourselves after combining.
    columns_lst = [
        _extract_column_info_from_dps_for_dataframe(
            dps,
            include_status=include_status,
            exclude_numeric_states=exclude_numeric_states,
            exclude_string_states=exclude_string_states,
        )
        for dps in dps_lst
    ]
    counter = itertools.count()  # Ensure unique column names initially
    dfs = [
        pd.DataFrame(
            {i: col.as_array() for i, col in zip(counter, columns)},
            index=_create_timestamp_index(dps.timestamp, dps.timezone),
            copy=False,  # we pass arrays directly for O(1) conversion
        )
        for dps, columns in zip(dps_lst, columns_lst, strict=True)
    ]
    # Each df may have completely different timestamp (index) so we let pandas do the heavy lifting:
    df = concat_dataframes_with_nullable_int_cols(dfs)

    df.columns = _create_multi_index_from_columns(
        list(itertools.chain.from_iterable(columns_lst)),
        include_aggregate=include_aggregate_name,
        include_granularity=include_granularity_name,
        include_unit=include_unit,
    )
    return df


def notebook_display_with_fallback(inst: T_CogniteResource | T_CogniteResourceList, **kwargs: Any) -> str:
    params = signature(inst.to_pandas).parameters
    # Default of False enforced (when accepted by method):
    if "camel_case" in params:
        kwargs["camel_case"] = False
    try:
        return inst.to_pandas(**kwargs)._repr_html_()
    except CogniteImportError:
        warnings.warn(
            "The 'cognite-sdk' depends on 'pandas' for pretty-printing objects like 'Asset' or 'DatapointsList' in "
            "(Jupyter) notebooks and similar environments. Consider installing it! Using fallback method (string "
            "truncated to 10,000 characters).",
            UserWarning,
        )
        return shorten(str(inst), 10_000)


def convert_nullable_int_cols(df: pd.DataFrame) -> pd.DataFrame:
    to_convert = df.columns.intersection(NULLABLE_INT_COLS)
    df[to_convert] = df[to_convert].astype("Int64")
    return df


def convert_timestamp_columns_to_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Convert CDF time-attribute columns from epoch milliseconds to pandas datetimes.

    Expects a DataFrame produced from CDF resources where any columns in
    ``TIME_ATTRIBUTES`` (for example ``created_time`` or ``last_updated_time``)
    are integer Unix timestamps in milliseconds.

    The output datetime resolution depends on pandas version:
    ``datetime64[ns]`` for pandas <= 2 and ``datetime64[ms]`` for newer versions.

    Other columns are left unchanged.
    """
    to_convert = df.columns.intersection(TIME_ATTRIBUTES)
    if is_pandas_v2_or_lower():
        # astype("datetime64[ns]") interprets raw ints as nanoseconds, so we need to convert first:
        df[to_convert] = (1_000_000 * df[to_convert]).astype("datetime64[ns]")
    else:
        df[to_convert] = df[to_convert].astype("datetime64[ms]")
    return df


def concat_dataframes_with_nullable_int_cols(dfs: Sequence[pd.DataFrame]) -> pd.DataFrame:
    import pandas as pd

    int_cols = [
        i
        for i, dtype in enumerate(itertools.chain.from_iterable(df.dtypes for df in dfs))
        if issubclass(dtype.type, Integral)
    ]
    # TODO: Performance optimization possible: The more unique each df.index is to the rest of the dfs, the
    # slower `pd.concat` scales. A manual "union(df.index for df in dfs)" + column insertion is faster for large
    # arrays, but require quite a lot of extra code (e.g. dtypes can be str (object), null.int and float)
    df = pd.concat(dfs, axis="columns", sort=True)  # Sort sorts non-concat axis
    if not int_cols:
        return df

    if pandas_major_version() >= 2:
        df.isetitem(int_cols, df.iloc[:, int_cols].astype("Int64"))
    else:
        # As of pandas >=1.5.0, <2, converting float cols (that used to be int) to nullable int using iloc raises FutureWarning,
        # but the suggested code change (to use `frame.isetitem(...)`) results in the wrong dtype (object).
        # See Github Issue: https://github.com/pandas-dev/pandas/issues/49922
        with warnings.catch_warnings():
            warnings.filterwarnings(
                action="ignore",
                message=re.escape(
                    "In a future version, `df.iloc[:, i] = newvals` will attempt to set the values inplace"
                ),
                category=FutureWarning,
            )
            df.iloc[:, int_cols] = df.iloc[:, int_cols].astype("Int64")
    return df


def _resolve_ts_identifier_as_df_column_name(dps: Datapoints | DatapointsArray) -> NodeId | str | int:
    if dps.instance_id:
        return dps.instance_id
    elif dps.external_id is not None:  # "" is legal xid
        return dps.external_id
    elif dps.id:
        return dps.id
    else:
        raise ValueError(f"{type(dps).__name__} object has no identifier (id, external_id or instance_id)")


def convert_dps_to_dataframe(
    dps: Datapoints | DatapointsArray,
    include_aggregate_name: bool,
    include_granularity_name: bool,
    include_status: bool,
    include_unit: bool,
    exclude_numeric_states: bool,
    exclude_string_states: bool,
) -> pd.DataFrame:
    pd = local_import("pandas")
    columns = _extract_column_info_from_dps_for_dataframe(
        dps,
        include_status=include_status,
        exclude_numeric_states=exclude_numeric_states,
        exclude_string_states=exclude_string_states,
    )
    df = pd.DataFrame(
        # We initially use integer indexing to allow duplicate column names:
        {i: col.as_array() for i, col in enumerate(columns)},
        index=_create_timestamp_index(dps.timestamp, dps.timezone),
        copy=False,  # we pass arrays directly for O(1) conversion
    )
    df.columns = _create_multi_index_from_columns(
        columns,
        include_aggregate=include_aggregate_name,
        include_granularity=include_granularity_name,
        include_unit=include_unit,
    )
    return df


@dataclass(frozen=True, slots=True)
class _DpsColumnInfo:
    """
    Used when converting Datapoints/DatapointsArray/DatapointsList/DatapointsArrayList to pandas DataFrame to help
    avoid the madness of how many columns we should end up with based on status codes/symbols, number of aggregates etc.

    A single Datapoints/DatapointsArray can result in 10+ columns from aggregates, and 1 or 3 columns from raw datapoints,
    with or without the 2 extra status info columns.
    """

    column_id: NodeId | str | int
    data: list[float] | list[str] | list[int] | NumpyUInt32Array | NumpyInt64Array | NumpyFloat64Array | NumpyObjArray
    is_string: bool | None = None
    is_array: bool = False
    aggregate: str | None = None
    granularity: str | None = None
    unit_xid: str | None = None
    status_info: Literal["code", "symbol"] | None = None

    def as_multi_index_tuple(self, include_aggregate: bool, include_granularity: bool, include_unit: bool) -> tuple:
        return (
            self.column_id,
            self.status_info,  # since these split to separate cols, they are already filtered out if not wanted
            self.aggregate if include_aggregate else None,
            self.granularity if include_granularity else None,
            self.unit_xid if include_unit else None,
        )

    def as_array(self) -> NumpyObjArray | NumpyFloat64Array | NumpyInt64Array | NumpyUInt32Array:
        if self.is_array:
            return self.data  # type: ignore [return-value]

        elif self.aggregate is None:
            return self._convert_to_array_for_raw_dps()
        else:
            return self._convert_to_array_for_agg_dps()

    def _convert_to_array_for_raw_dps(
        self,
    ) -> npt.NDArray[np.object_] | npt.NDArray[np.float64] | npt.NDArray[np.uint32]:
        import numpy as np

        match self.is_string, self.status_info:
            case True, None:
                return np.array(self.data, dtype=np.object_)
            case False, None:
                return np.array(self.data, dtype=np.float64)
            case _, "code":
                return np.array(self.data, dtype=np.uint32)
            case _, "symbol":
                return np.array(self.data, dtype=np.object_)
            case _:
                # IsString is required in the response, so if we reach here, most likely a user has instantiated
                # the datapoints object themselves:
                raise ValueError(
                    f"Invalid combination of is_string={self.is_string} and status_info={self.status_info}"
                )

    def _convert_to_array_for_agg_dps(
        self,
    ) -> npt.NDArray[np.object_] | npt.NDArray[np.float64] | npt.NDArray[np.int64]:
        import numpy as np

        from cognite.client.utils._datapoints import ensure_int_numpy

        if self.aggregate in OBJECT_AGGREGATES:
            return np.array(self.data, dtype=np.object_)

        elif self.aggregate in INT_AGGREGATES:
            return ensure_int_numpy(np.array(self.data, dtype=np.float64))
        else:
            return np.array(self.data, dtype=np.float64)


def _extract_raw_states_column_info(
    dps: Datapoints,
    identifier: NodeId | str | int,
    include_status: bool,
    exclude_numeric_states: bool,
    exclude_string_states: bool,
) -> list[_DpsColumnInfo]:
    columns = []
    if not exclude_numeric_states:
        assert dps.numeric_states is not None
        columns.append(
            _DpsColumnInfo(
                identifier,
                data=dps.numeric_states,
                is_string=False,
                is_array=False,
                state_type="numeric",
            )
        )
    if not exclude_string_states:
        assert dps.string_states is not None
        columns.append(
            _DpsColumnInfo(
                identifier,
                data=dps.string_states,
                is_string=True,
                is_array=False,
                state_type="string",
            )
        )
    if include_status:
        if dps.status_code is not None:
            columns.append(_DpsColumnInfo(identifier, data=dps.status_code, is_array=False, status_info="code"))
        if dps.status_symbol is not None:
            columns.append(_DpsColumnInfo(identifier, data=dps.status_symbol, is_array=False, status_info="symbol"))

    return columns


def _extract_raw_column_info(
    dps: Datapoints | DatapointsArray,
    identifier: NodeId | str | int,
    is_array: bool,
    include_status: bool,
) -> list[_DpsColumnInfo]:
    assert dps.value is not None
    columns = [
        _DpsColumnInfo(
            identifier,
            data=dps.value,
            is_string=dps.is_string,
            is_array=is_array,
            unit_xid=dps.unit_external_id or None,
        )
    ]
    if include_status:
        if dps.status_code is not None:
            columns.append(_DpsColumnInfo(identifier, data=dps.status_code, is_array=is_array, status_info="code"))
        if dps.status_symbol is not None:
            columns.append(_DpsColumnInfo(identifier, data=dps.status_symbol, is_array=is_array, status_info="symbol"))

    return columns


def _extract_aggregate_column_info_from_dps(
    dps: Datapoints | DatapointsArray, identifier: NodeId | str | int, is_array: bool
) -> list[_DpsColumnInfo]:
    aggregates = sorted(_ALL_AGGREGATES.intersection(k for k, v in dps.__dict__.items() if v is not None))
    return [
        _DpsColumnInfo(
            identifier,
            data=getattr(dps, agg),
            is_array=is_array,
            aggregate=agg,
            granularity=dps.granularity,
            # We show physical unit if the aggregate somewhat makes sense (e.g. average, but also (..)_variance).
            # Note the '... or None' is there because the API returns empty string when missing for some reason:
            unit_xid=dps.unit_external_id or None if agg in _AGGREGATES_WITH_UNIT else None,
        )
        for agg in aggregates
    ]


def _extract_column_info_from_dps_for_dataframe(
    dps: Datapoints | DatapointsArray, include_status: bool
) -> list[_DpsColumnInfo]:
    from cognite.client.data_classes import DatapointsArray

    identifier = _resolve_ts_identifier_as_df_column_name(dps)
    is_array = isinstance(dps, DatapointsArray)
    if dps.value is not None:
        return _extract_raw_column_info(dps, identifier, is_array, include_status)
    return _extract_aggregate_column_info_from_dps(dps, identifier, is_array)


def _create_multi_index_from_columns(
    columns: list[_DpsColumnInfo],
    include_aggregate: bool,
    include_granularity: bool,
    include_unit: bool,
) -> pd.Index:
    import pandas as pd

    column_ids = pd.DataFrame(
        [
            col.as_multi_index_tuple(
                include_aggregate=include_aggregate,
                include_granularity=include_granularity,
                include_unit=include_unit,
            )
            for col in columns
        ],
        columns=["identifier", "status", "aggregate", "granularity", "unit"],
    )
    # Key operation is to drop all-nan columns, which in the multi-index translates to dropping
    # the corresponding levels:
    non_id_levels = column_ids.iloc[:, 1:].dropna(axis="columns", how="all").fillna("")
    # When none of the extra levels survive (status/agg./gran./unit), return a plain Index so
    # columns are the bare identifiers rather than 1-tuples:
    if non_id_levels.columns.empty:
        return pd.Index(column_ids["identifier"])
    # ...but we always keep the identifier column:
    return pd.MultiIndex.from_frame(pd.concat((column_ids[["identifier"]], non_id_levels), axis=1, copy=False))


def _create_timestamp_index(
    timestamps: list[int] | NumpyDatetime64NSArray, timezone: str | datetime.timezone | ZoneInfo | None
) -> pd.DatetimeIndex:
    import numpy as np

    match timestamps:
        case list():
            return to_pandas_datetime_index(timestamps, timezone)
        case np.ndarray():
            return _to_pandas_datetime_index_from_numpy(timestamps, timezone)
        case _:
            raise TypeError("Timestamps must be either list[int] or numpy.ndarray")


def squeeze_single_row_list_df(df: pd.DataFrame, ignore: list[str] | None) -> pd.DataFrame:
    """Turns the single row dataframe the list-class produces back into a single-column"""
    if ignore:
        df = df.drop(columns=[c for c in ignore if c in df.columns])
    # For historical reasons, we need to do an astype(object) here. It undoes pandas dtype inference
    # (e.g. numpy.bool_ instead of bool) so values keep their native Python types:
    return df.astype(object).iloc[0].rename("value").to_frame()


def base_resource_to_pandas_fallback(
    resource: CogniteResource,
    camel_case: bool,
    ignore: list[str] | None,
    convert_timestamps: bool,
    expand_metadata: bool,
    metadata_prefix: str,
) -> pd.DataFrame:
    pd = local_import("pandas")
    dumped = resource.dump(camel_case=camel_case)

    for element in ignore or []:
        dumped.pop(element, None)

    if convert_timestamps:
        for k in TIME_ATTRIBUTES.intersection(dumped):
            dumped[k] = to_pandas_timestamp(dumped[k])

    if expand_metadata and "metadata" in dumped and isinstance(dumped["metadata"], dict):
        dumped.update({f"{metadata_prefix}{k}": v for k, v in dumped.pop("metadata").items()})

    return pd.Series(dumped).to_frame(name="value")
