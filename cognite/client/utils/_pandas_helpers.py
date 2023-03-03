
import re
import warnings
from inspect import signature
from itertools import chain
from numbers import Integral
from cognite.client.exceptions import CogniteImportError
from cognite.client.utils._auxiliary import local_import
if TYPE_CHECKING:
    import pandas as pd
    from cognite.client.data_classes._base import T_CogniteResource, T_CogniteResourceList

def pandas_major_version():
    from pandas import __version__
    return int(__version__.split('.')[0])

def notebook_display_with_fallback(inst, **kwargs: Any):
    if ('camel_case' in signature(inst.to_pandas).parameters):
        kwargs['camel_case'] = False
    try:
        return inst.to_pandas(**kwargs)._repr_html_()
    except CogniteImportError:
        warnings.warn("The 'cognite-sdk' depends on 'pandas' for pretty-printing objects like 'Asset' or 'DatapointsList' in (Jupyter) notebooks and similar environments. Consider installing it! Using fallback method.", UserWarning)
        return str(inst)

def concat_dataframes_with_nullable_int_cols(dfs):
    pd = cast(Any, local_import('pandas'))
    int_cols = [i for (i, dtype) in enumerate(chain.from_iterable((df.dtypes for df in dfs))) if issubclass(dtype.type, Integral)]
    df = pd.concat(dfs, axis='columns', sort=True)
    if (not int_cols):
        return df
    if (pandas_major_version() < 2):
        with warnings.catch_warnings():
            warnings.filterwarnings(action='ignore', message=re.escape('In a future version, `df.iloc[:, i] = newvals` will attempt to set the values inplace'), category=FutureWarning)
            df.iloc[:, int_cols] = df.iloc[:, int_cols].astype('Int64')
    else:
        df.isetitem(int_cols, df.iloc[:, int_cols].astype('Int64'))
    return df
