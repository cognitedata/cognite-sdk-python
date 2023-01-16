from __future__ import annotations

import re
import warnings
from inspect import signature
from itertools import chain
from numbers import Integral
from typing import TYPE_CHECKING, Any, Sequence, Union, cast

from cognite.client.exceptions import CogniteImportError
from cognite.client.utils._auxiliary import local_import

if TYPE_CHECKING:
    import pandas as pd

    from cognite.client.data_classes._base import T_CogniteResource, T_CogniteResourceList


def notebook_display_with_fallback(inst: Union[T_CogniteResource, T_CogniteResourceList], **kwargs: Any) -> str:
    if "camel_case" in signature(inst.to_pandas).parameters:
        # Default of False enforced (when accepted by method):
        kwargs["camel_case"] = False
    try:
        return inst.to_pandas(**kwargs)._repr_html_()
    except CogniteImportError:
        warnings.warn(
            "The 'cognite-sdk' depends on 'pandas' for pretty-printing objects like 'Asset' or 'DatapointsList' in "
            "(Jupyter) notebooks and similar environments. Consider installing it! Using fallback method.",
            UserWarning,
        )
        return str(inst)


def concat_dataframes_with_nullable_int_cols(dfs: Sequence[pd.DataFrame]) -> pd.DataFrame:
    pd = cast(Any, local_import("pandas"))
    int_cols = [
        i for i, dtype in enumerate(chain.from_iterable(df.dtypes for df in dfs)) if issubclass(dtype.type, Integral)
    ]
    # TODO: Performance optimization possible: The more unique each df.index is to the rest of the dfs, the
    # slower `pd.concat` scales. A manual "union(df.index for df in dfs)" + column insertion is faster for large
    # arrays, but require quite a lot of extra code (e.g. dtypes can be str (object), null.int and float)
    df = pd.concat(dfs, axis="columns", sort=True)  # Sort sorts non-concat axis
    if not int_cols:
        return df

    # As of pandas>=1.5.0, converting float cols (that used to be int) to nullable int using iloc raises FutureWarning,
    # but the suggested code change (to use `frame.isetitem(...)`) results in the wrong dtype (object).
    # See Github Issue: https://github.com/pandas-dev/pandas/issues/49922
    with warnings.catch_warnings():
        warnings.filterwarnings(
            action="ignore",
            message=re.escape("In a future version, `df.iloc[:, i] = newvals` will attempt to set the values inplace"),
            category=FutureWarning,
        )
        df.iloc[:, int_cols] = df.iloc[:, int_cols].astype("Int64")
        return df
