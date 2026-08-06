import typing
from typing import Literal

from cognite.client.utils._text import to_snake_case

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
    "max_datapoint",
    "min",
    "min_datapoint",
    "state_count",
    "state_duration",
    "state_transitions",
    "step_interpolation",
    "sum",
    "total_variation",
]
_OBJECT_AGGREGATES_CAMEL: frozenset[Literal["maxDatapoint", "minDatapoint"]] = frozenset(
    {"maxDatapoint", "minDatapoint"}
)
OBJECT_AGGREGATES: frozenset[Literal["max_datapoint", "min_datapoint", "state_aggregates"]] = frozenset(
    {"max_datapoint", "min_datapoint", "state_aggregates"}
)
# Requested via aggregates=... but returned nested under ``stateAggregates`` on each aggregate datapoint:
STATE_AGGREGATES_CAMEL: frozenset[str] = frozenset({"stateCount", "stateTransitions", "stateDuration"})
AGGREGATE_REQUEST_ONLY_SNAKE = frozenset({"state_count", "state_transitions", "state_duration"})

# Assumption: All INT aggregates should adhere to the following logic: Missing values can be replace with 0.
#             Thus, if you add a new aggregate here, and this is no longer the case, a refactor is needed:
_INT_AGGREGATES_CAMEL = frozenset(
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
INT_AGGREGATES = frozenset(map(to_snake_case, _INT_AGGREGATES_CAMEL))
_ALL_AGGREGATE_LITERAL_ARGS = typing.get_args(Aggregate)
ALL_SORTED_DP_AGGS = sorted(a for a in _ALL_AGGREGATE_LITERAL_ARGS if a not in AGGREGATE_REQUEST_ONLY_SNAKE)
_ALL_AGGREGATES = frozenset(ALL_SORTED_DP_AGGS) | frozenset({"state_aggregates"})
ALL_SORTED_NUMERIC_DP_AGGS = [agg for agg in ALL_SORTED_DP_AGGS if agg not in ("min_datapoint", "max_datapoint")]

# When we add unit info to dataframe columns, we need to know if the physical unit should be included or not.
# Some, like variance, is technically the unit squared, but we still include them (as opposed to count):
_AGGREGATES_WITH_UNIT = frozenset(
    {
        "average",
        "continuous_variance",
        "discrete_variance",
        "interpolation",
        "max",
        "max_datapoint",
        "min",
        "min_datapoint",
        "step_interpolation",
        "sum",
        "total_variation",
    }
)
