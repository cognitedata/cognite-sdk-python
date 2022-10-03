from datetime import datetime
from typing import Dict, Iterable, List, Optional, TypedDict, Union

try:
    import numpy as np
    import numpy.typing as npt

    NUMPY_IS_AVAILABLE = True
except ImportError:  # pragma no cover
    NUMPY_IS_AVAILABLE = False

if NUMPY_IS_AVAILABLE:
    NumpyDatetime64NSArray = npt.NDArray[np.datetime64]
    NumpyInt64Array = npt.NDArray[np.int64]
    NumpyFloat64Array = npt.NDArray[np.float64]
    NumpyObjArray = npt.NDArray[np.object_]

# Datapoints API-limits:
DPS_LIMIT_AGG = 10_000
DPS_LIMIT = 100_000
POST_DPS_OBJECTS_LIMIT = 10_000
FETCH_TS_LIMIT = 100
RETRIEVE_LATEST_LIMIT = 100


ALL_SORTED_DP_AGGS = sorted(
    [
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
)


class CustomDatapointsQuery(TypedDict, total=False):
    # No field required
    start: Union[int, str, datetime, None]
    end: Union[int, str, datetime, None]
    aggregates: Optional[List[str]]
    granularity: Optional[str]
    limit: Optional[int]
    include_outside_points: Optional[bool]
    ignore_unknown_ids: Optional[bool]


class DatapointsQueryId(CustomDatapointsQuery):
    id: int  # required field


class DatapointsQueryExternalId(CustomDatapointsQuery):
    external_id: str  # required field


class CustomDatapoints(TypedDict, total=False):
    # No field required
    start: int
    end: int
    aggregates: Optional[List[str]]
    granularity: Optional[str]
    limit: int
    include_outside_points: bool


class DatapointsPayload(CustomDatapoints):
    items: List[CustomDatapoints]


DatapointsTypes = Union[int, float, str]


class DatapointsFromAPI(TypedDict):
    id: int
    externalId: Optional[str]
    isString: bool
    isStep: bool
    datapoints: List[Dict[str, DatapointsTypes]]


DatapointsIdTypes = Union[int, DatapointsQueryId, Iterable[Union[int, DatapointsQueryId]]]
DatapointsExternalIdTypes = Union[str, DatapointsQueryExternalId, Iterable[Union[str, DatapointsQueryExternalId]]]
