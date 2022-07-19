from __future__ import annotations

from dataclasses import InitVar, dataclass
from typing import TYPE_CHECKING

from cognite.client.data_classes.datapoints import DatapointsFromAPI, SingleTSQuery

if TYPE_CHECKING:
    pass


def dps_task_type_selector(query):
    # Note: We could add "include-outside-points" to our condition, but since
    # the "initial query" already includes this, when True, we don't.
    condition = (query.is_string, query.is_raw_query, query.limit is None)
    return {  # O pattern matching, where art thou?
        # String tasks:
        (True, True, True): UnlimitedStringTask,
        (True, True, False): LimitedStringTask,
        # Raw numeric tasks:
        (False, True, True): UnlimitedRawNumericTask,
        (False, True, False): LimitedRawNumericTask,
        # Agg. numeric tasks:
        (False, False, True): UnlimitedAggNumericTask,
        (False, False, False): LimitedAggNumericTask,
    }[condition]


@dataclass(eq=False)  # __hash__ cant be inherited for safety, so we add eq=False for all
class UnlimitedStringTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        # TODO: Handle outside points, if any
        pass


@dataclass(eq=False)
class LimitedStringTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        # TODO: Handle outside points, if any
        pass


@dataclass(eq=False)
class UnlimitedRawNumericTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        # TODO: Handle outside points, if any
        pass


@dataclass(eq=False)
class LimitedRawNumericTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        # TODO: Handle outside points, if any
        pass


@dataclass(eq=False)
class UnlimitedAggNumericTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        pass


@dataclass(eq=False)
class LimitedAggNumericTask:
    query: SingleTSQuery
    first_dps_batch: InitVar[DatapointsFromAPI]

    def __post_init__(self, first_dps_batch):
        pass
