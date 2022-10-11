# Used in FunctionsAPI
LIST_LIMIT_DEFAULT = 25
LIST_LIMIT_CEILING = 10_000  # variable used to guarantee all items are returned when list(limit) is None, inf or -1.

# Used in DatapointsAPI and SyntheticDatapointsAPI
DPS_LIMIT_AGG = 10_000
DPS_LIMIT = 100_000
POST_DPS_OBJECTS_LIMIT = 10_000
FETCH_TS_LIMIT = 100
RETRIEVE_LATEST_LIMIT = 100
ALL_SORTED_DP_AGGS = sorted(
    [
        "average",
        "continuous_variance",
        "count",
        "discrete_variance",
        "interpolation",
        "max",
        "min",
        "step_interpolation",
        "sum",
        "total_variation",
    ]
)
