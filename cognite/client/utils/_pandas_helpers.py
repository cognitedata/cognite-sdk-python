from __future__ import annotations

import re
import warnings
from collections.abc import Sequence
from datetime import timezone
from inspect import signature
from itertools import chain
from numbers import Integral
from typing import TYPE_CHECKING, Any, Literal

from cognite.client.exceptions import CogniteImportError
from cognite.client.utils._importing import local_import
from cognite.client.utils._text import to_camel_case
from cognite.client.utils._time import TIME_ATTRIBUTES, ZoneInfo

if TYPE_CHECKING:
    import pandas as pd

    from cognite.client.data_classes import Datapoints, DatapointsArray, DatapointsArrayList, DatapointsList
    from cognite.client.data_classes._base import T_CogniteResource, T_CogniteResourceList


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


def convert_tz_for_pandas(tz: str | timezone | ZoneInfo | None) -> str | timezone | None:
    if tz is None or isinstance(tz, (str, timezone)):
        return tz
    if isinstance(tz, ZoneInfo):
        # pandas is not happy about ZoneInfo :shrug:
        if tz.key is not None:
            return tz.key
        raise ValueError("timezone of type ZoneInfo does not have the required 'key' attribute set")
    raise ValueError(f"'timezone' not understood, expected one of: [None, datetime.timezone, ZoneInfo], got {type(tz)}")


def concat_dps_dataframe_list(
    dps_lst: DatapointsList | DatapointsArrayList,
    column_names: Literal["id", "external_id", "instance_id"],
    include_aggregate_name: bool,
    include_granularity_name: bool,
    include_status: bool,
) -> pd.DataFrame:
    pd = local_import("pandas")
    dfs = [
        dps.to_pandas(
            column_names=column_names,
            include_aggregate_name=include_aggregate_name,
            include_granularity_name=include_granularity_name,
            include_status=include_status,
        )
        for dps in dps_lst
    ]
    if not dfs:
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
    return concat_dataframes_with_nullable_int_cols(dfs)


def notebook_display_with_fallback(inst: T_CogniteResource | T_CogniteResourceList, **kwargs: Any) -> str:
    params = signature(inst.to_pandas).parameters
    # Default of False enforced (when accepted by method):
    if "camel_case" in params:
        kwargs["camel_case"] = False
    # TODO: Next major, flip this default to True in the method itself (and delete here):
    if "expand_properties" in params:
        kwargs["expand_properties"] = True
    try:
        return inst.to_pandas(**kwargs)._repr_html_()
    except CogniteImportError:
        warnings.warn(
            "The 'cognite-sdk' depends on 'pandas' for pretty-printing objects like 'Asset' or 'DatapointsList' in "
            "(Jupyter) notebooks and similar environments. Consider installing it! Using fallback method.",
            UserWarning,
        )
        return str(inst)


def convert_nullable_int_cols(df: pd.DataFrame) -> pd.DataFrame:
    to_convert = df.columns.intersection(NULLABLE_INT_COLS)
    df[to_convert] = df[to_convert].astype("Int64")
    return df


def convert_timestamp_columns_to_datetime(df: pd.DataFrame) -> pd.DataFrame:
    to_convert = df.columns.intersection(TIME_ATTRIBUTES)
    df[to_convert] = (1_000_000 * df[to_convert]).astype("datetime64[ns]")
    return df


def concat_dataframes_with_nullable_int_cols(dfs: Sequence[pd.DataFrame]) -> pd.DataFrame:
    pd = local_import("pandas")
    int_cols = [
        i for i, dtype in enumerate(chain.from_iterable(df.dtypes for df in dfs)) if issubclass(dtype.type, Integral)
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


def resolve_ts_identifier_as_df_column_name(
    dps: Datapoints | DatapointsArray, column_names: Literal["id", "external_id", "instance_id"]
) -> str:
    # Note: Although pandas columns can support numbers and objects like tuples, we may need to pad with
    #       e.g. aggregate info, so we always ensure the column names are strings.
    if column_names not in {"id", "external_id", "instance_id"}:
        # Don't raise here, the user may have waited a long time for datapoints fetching
        warnings.warn(
            "Parameter `column_names` must be either 'instance_id', 'external_id' or 'id'. "
            "Falling back to instance_id -> external_id -> id (using first one defined).",
            UserWarning,
        )
        column_names = "instance_id"

    original = column_names
    if column_names == "instance_id":
        if dps.instance_id:
            # Keep column name as short as possible, default repr adds "space=..." and "external_id=..."
            return "NodeId({}, {})".format(*dps.instance_id.as_tuple())
        column_names = "external_id"  # Fallback to external_id

    if column_names == "external_id":
        if dps.external_id is not None:  # "" is legal xid
            return dps.external_id
        column_names = "id"  # Fallback to id

    if column_names == "id":
        if dps.id:
            return str(dps.id)

    fallbacks = {"instance_id": ["external_id", "id"], "external_id": ["id"], "id": []}
    raise ValueError(
        f"Unable to resolve column name for time series using columns_names={original!r}. "
        f"Neither the attribute or its fallbacks ({fallbacks[original]}) are set on the object."
    )
