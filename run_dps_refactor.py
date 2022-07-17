import math

from local_cog_client import setup_local_cog_client

# Specify the aggregates to return. Use default if null.
# If the default is a set of aggregates,
# specify an empty string to get raw data.
make_agg_dct = {"aggregates": ["average"], "granularity": "12h"}
START = None
END = None
LIMIT = 1000
AGGREGATES = None  # ["average"]
GRANULARITY = None  # "12h"
INCLUDE_OUTSIDE_POINTS = None
IGNORE_UNKNOWN_IDS = False
# ID = None
ID = [
    {"id": 226740051491},
    {"id": 2546012653669},  # string
    {"id": 1111111111111},  # missing...
    {"id": 2546012653669, "aggregates": ["max", "average"], "granularity": "1d"},  # string
]
EXTERNAL_ID = [
    # {"limit": None, "external_id": "ts-test-#01-daily-111/650"},
    {"limit": math.inf, "external_id": "ts-test-#01-daily-222/650", **make_agg_dct},
    {"limit": -1, "external_id": "ts-test-#01-daily-444/650"},
    # {"limit": None, "external_id": "8400074_destination"},  # string
    {"limit": 15, "external_id": "9624122_cargo_type"},  # string
    {"limit": None, "external_id": "ts-test-#01-daily-651/650", **make_agg_dct},  # missing
    {
        "include_outside_points": True,
        "limit": 1,
        "external_id": "ts-test-#04-ten-mill-dps-1/1",
        "start": 31536472487 - 1,
        "end": 31536698071 + 1,
        # To override "top level" settings (if given) or fails on include_outside not allowed for agg. q:
        "granularity": None,
        "aggregates": None,
    },
    {
        "include_outside_points": False,
        "limit": 100_000,
        "external_id": "8400074_destination",
        "start": 0,  # 1533945852000-1,
        "end": "now",  # 1533945852000+1,
    },
    {
        "include_outside_points": False,
        "limit": None,
        "external_id": "ts-test-#04-ten-mill-dps-1/1",
        "start": 31536472487 - 1,
        "end": 2 * 31536698071 + 1,
    },
]
# EXTERNAL_ID = [
#     f"ts-test-#01-daily-{i}/650" for i in range(1, 301)
# ]

max_workers = 10
client = setup_local_cog_client(max_workers, debug=False)

# query1 = DatapointsQuery(
res = client.datapoints.retrieve_new(
    start=START,
    end=END,
    id=ID,
    external_id=EXTERNAL_ID,
    aggregates=AGGREGATES,
    granularity=GRANULARITY,
    include_outside_points=INCLUDE_OUTSIDE_POINTS,
    limit=LIMIT,
    ignore_unknown_ids=IGNORE_UNKNOWN_IDS,
)
# query2 = DatapointsQuery(
#     start=START,
#     end=END,
#     id=None,
#     external_id=EXTERNAL_ID,
#     aggregates=AGGREGATES,
#     granularity=GRANULARITY,
#     include_outside_points=INCLUDE_OUTSIDE_POINTS,
#     limit=LIMIT,
#     ignore_unknown_ids=IGNORE_UNKNOWN_IDS,
# )
# res = client.datapoints.query_new([query1, query2])
