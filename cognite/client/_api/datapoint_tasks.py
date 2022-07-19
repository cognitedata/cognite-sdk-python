from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from cognite.client.data_classes.datapoints import DatapointsFromAPI, SingleTSQuery

if TYPE_CHECKING:
    pass


def dps_task_type_selector(query):
    condition = (query.is_string, query.is_raw_query, query.limit is None)
    return {
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
    first_dps_batch: DatapointsFromAPI


@dataclass(eq=False)
class LimitedStringTask:
    query: SingleTSQuery
    first_dps_batch: DatapointsFromAPI


@dataclass(eq=False)
class UnlimitedRawNumericTask:
    query: SingleTSQuery
    first_dps_batch: DatapointsFromAPI


@dataclass(eq=False)
class LimitedRawNumericTask:
    query: SingleTSQuery
    first_dps_batch: DatapointsFromAPI


@dataclass(eq=False)
class UnlimitedAggNumericTask:
    query: SingleTSQuery
    first_dps_batch: DatapointsFromAPI


@dataclass(eq=False)
class LimitedAggNumericTask:
    query: SingleTSQuery
    first_dps_batch: DatapointsFromAPI
