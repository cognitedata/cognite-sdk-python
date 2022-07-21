from datetime import datetime
from typing import Dict, Iterable, List, Optional, TypedDict, Union

import numpy as np
import numpy.typing as npt


class CustomDatapoints(TypedDict, total=False):
    # No field required
    start: Union[int, str, datetime, None]
    end: Union[int, str, datetime, None]
    aggregates: Optional[List[str]]
    granularity: Optional[str]
    limit: Optional[int]
    include_outside_points: bool


class DatapointsQueryId(CustomDatapoints):
    id: int  # required field


class DatapointsQueryExternalId(CustomDatapoints):
    external_id: str  # required field


class DatapointsFromAPI(TypedDict):
    id: int
    externalId: Optional[str]
    isString: bool
    isStep: bool
    datapoints: List[Dict[str, Union[int, float, str]]]


DatapointsIdTypes = Union[int, DatapointsQueryId, Iterable[Union[int, DatapointsQueryId]]]
DatapointsExternalIdTypes = Union[str, DatapointsQueryExternalId, Iterable[Union[str, DatapointsQueryExternalId]]]

NumpyInt64Array = npt.NDArray[np.int64]
NumpyFloat64Array = npt.NDArray[np.float64]
NumpyObjArray = npt.NDArray[np.object_]
