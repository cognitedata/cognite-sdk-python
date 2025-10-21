from __future__ import annotations

import datetime
import itertools
import re
import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from inspect import signature
from numbers import Integral
from typing import TYPE_CHECKING, Any, Literal
from zoneinfo import ZoneInfo

from typing_extensions import assert_never

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
    import pandas as pd

    from cognite.client.data_classes import Datapoints, DatapointsArray, DatapointsArrayList, DatapointsList
    from cognite.client.data_classes._base import T_CogniteResource, T_CogniteResourceList
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
    columns_lst = [_extract_column_info_from_dps_for_dataframe(dps, include_status=include_status) for dps in dps_lst]
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
    to_convert = df.columns.intersection(TIME_ATTRIBUTES)
    df[to_convert] = (1_000_000 * df[to_convert]).astype("datetime64[ns]")
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
    include_errors: bool = False,  # old leftover misuse of Datapoints class :(
):
    pd = local_import("pandas")
    columns = _extract_column_info_from_dps_for_dataframe(
        dps, include_status=include_status, include_errors=include_errors
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
        include_errors=include_errors,
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
    synth_query_info: str | None = None

    def as_multi_index_tuple(
        self, include_aggregate: bool, include_granularity: bool, include_unit: bool, include_errors: bool
    ) -> tuple:
        return (
            self.column_id,
            self.status_info,  # since these split to separate cols, they are already filtered out if not wanted
            self.aggregate if include_aggregate else None,
            self.granularity if include_granularity else None,
            self.unit_xid if include_unit else None,
            self.synth_query_info if include_errors else None,
        )

    def as_array(self) -> NumpyObjArray | NumpyFloat64Array | NumpyInt64Array | NumpyUInt32Array:
        if self.is_array:
            return self.data  # type: ignore [return-value]

        elif self.aggregate is None:
            return self._convert_to_array_for_raw_dps()
        else:
            return self._convert_to_array_for_agg_dps()

    def _convert_to_array_for_raw_dps(self):
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
                assert_never(f"Invalid combination of is_string={self.is_string} and status_info={self.status_info}")

    def _convert_to_array_for_agg_dps(self):
        import numpy as np

        from cognite.client.utils._datapoints import ensure_int_numpy

        if self.aggregate in OBJECT_AGGREGATES:
            return np.array(self.data, dtype=np.object_)

        elif self.aggregate in INT_AGGREGATES:
            return ensure_int_numpy(np.array(self.data, dtype=np.float64))
        else:
            return np.array(self.data, dtype=np.float64)


def _extract_raw_column_info(
    dps: Datapoints | DatapointsArray,
    identifier: NodeId | str | int,
    is_array: bool,
    include_status: bool,
    include_errors: bool,
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

    if include_errors:
        columns = _handle_synthetic_dps_with_errors(identifier, dps, columns)  # type: ignore [arg-type]
    return columns


def _handle_synthetic_dps_with_errors(
    identifier: str, dps: Datapoints, columns: list[_DpsColumnInfo]
) -> list[_DpsColumnInfo]:
    # EVERYTHING about this is ugly and hacky, but adding a separate SyntheticDatapoints class is such a waste of time
    # that we let it slide (literally zero usage of synthetic datapoints API from the SDK - which is understandable):
    import numpy as np

    from cognite.client.data_classes import Datapoints

    assert isinstance(dps, Datapoints)  # only Datapoints has error field
    if dps.error is None:
        raise ValueError("Unable to 'include_errors', only available for data from synthetic datapoint queries")

    errors = np.array([dp or "" for dp in dps.error], dtype=np.object_)
    columns.append(_DpsColumnInfo(identifier, data=errors, is_array=True, synth_query_info="errors"))
    # Override the synth. query results with info about what it is:
    object.__setattr__(columns[0], "synth_query_info", "results")
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
    dps: Datapoints | DatapointsArray, include_status: bool, include_errors: bool = False
) -> list[_DpsColumnInfo]:
    from cognite.client.data_classes import DatapointsArray

    identifier = _resolve_ts_identifier_as_df_column_name(dps)
    is_array = isinstance(dps, DatapointsArray)
    if dps.value is not None:
        return _extract_raw_column_info(dps, identifier, is_array, include_status, include_errors)
    return _extract_aggregate_column_info_from_dps(dps, identifier, is_array)


def _create_multi_index_from_columns(
    columns: list[_DpsColumnInfo],
    include_aggregate: bool,
    include_granularity: bool,
    include_unit: bool,
    include_errors: bool = False,
) -> pd.MultiIndex:
    import pandas as pd

    column_ids_df = pd.DataFrame(
        [
            col.as_multi_index_tuple(
                include_aggregate=include_aggregate,
                include_granularity=include_granularity,
                include_unit=include_unit,
                include_errors=include_errors,
            )
            for col in columns
        ],
        columns=["identifier", "status", "aggregate", "granularity", "unit", "synthetic query"],
    )
    # Key operation is to drop all-nan columns, which in the multi-index translates to dropping
    # the corresponding levels:
    return pd.MultiIndex.from_frame(column_ids_df.dropna(axis="columns", how="all").fillna(""))


def _create_timestamp_index(
    timestamps: list[int] | NumpyDatetime64NSArray, timezone: str | datetime.timezone | ZoneInfo | None
) -> pd.DatetimeIndex:
    import numpy as np
    import pandas as pd

    match timestamps, timezone:
        case list(), None:
            return pd.to_datetime(timestamps, unit="ms")
        case list(), _:
            return pd.to_datetime(timestamps, unit="ms", utc=True).tz_convert(convert_tz_for_pandas(timezone))
        case np.ndarray(), None:
            return pd.to_datetime(timestamps)
        case np.ndarray(), _:
            return pd.to_datetime(timestamps, utc=True).tz_convert(convert_tz_for_pandas(timezone))
        case _:
            assert_never("Timestamps must be either list[int] or numpy.ndarray")
