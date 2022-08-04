import math  # noqa
from timeit import default_timer as timer

from local_cog_client import setup_local_cog_client

make_agg_dct = {"aggregates": ["average"], "granularity": "12h"}
START = 0
END = 1  # 112548548397  # 31569600000
LIMIT = None
AGGREGATES = None  # ["average"]
GRANULARITY = None  # "12h"
INCLUDE_OUTSIDE_POINTS = True
IGNORE_UNKNOWN_IDS = False
# ID = None
ID = [
    # {"id": 226740051491},
    # {"id": 2546012653669, "start": 1031539300000},  # string, xid=9694359_cargo_type
    # {"id": 1111111111111},  # missing...
    # {"id": 2546012653669, "aggregates": ["max", "average"], "granularity": "1d"},  # string
]
EXTERNAL_ID = [
    # {"external_id": "ts-test-#01-daily-111/650"},
    # {"external_id": "ts-test-#01-daily-222/650"},
    # {"external_id": "ts-test-#04-ten-mill-dps-1/1"},
    {"external_id": "benchmark:11-1mill-blob-sec-after-1990-#1/10"},
    {"external_id": "benchmark:11-1mill-blob-sec-after-1990-#2/10"},
    {"external_id": "benchmark:11-1mill-blob-sec-after-1990-#3/10"},
    {"external_id": "benchmark:11-1mill-blob-sec-after-1990-#4/10"},
    {"external_id": "benchmark:11-1mill-blob-sec-after-1990-#5/10"},
    # {"external_id": "benchmark:10-1mill-blob-ms-after-1990-#1/10"},
    # {"external_id": "benchmark:10-1mill-blob-ms-after-1990-#2/10"},
    # {"limit": 99_999 + 3, "external_id": "benchmark:1-string-1h-gran-#3/50"},  # string
    # {"limit": -1, "external_id": "8400074_destination"},  # string
    # {"external_id": "benchmark:2-string-5m-gran-#1/1"},  # string
    # {"external_id": "benchmark:1-string-1h-gran-#1/50"},  # string
    # {"external_id": "benchmark:1-string-1h-gran-#8/50"},  # string
    # {"external_id": "benchmark:1-string-1h-gran-#3/50"},  # string
    # {"external_id": "benchmark:1-string-1h-gran-#4/50"},  # string
    # {"external_id": "benchmark:1-string-1h-gran-#10/50"},  # string
    # {"external_id": "benchmark:1-string-1h-gran-#6/50"},  # string
    # {"limit": None, "external_id": "ts-test-#01-daily-651/650"},  # missing
    # {"external_id": "benchmark:1-string-1h-gran-#9/50"},  # string
    # {"external_id": "benchmark:1-string-1h-gran-#9/50"},  # string
    # {"external_id": "benchmark:1-string-1h-gran-#9/50"},  # string
    # {"external_id": "benchmark:1-string-1h-gran-#2/50"},  # string
    # {"limit": None, "external_id": "ts-test-#01-daily-651/650"},  # missing
    # {"external_id": "benchmark:1-string-1h-gran-#7/50"},  # string
    # {"external_id": "benchmark:1-string-1h-gran-#5/50"},  # string
    # {"external_id": "benchmark:1-string-#4/50"},  # string
    # {"external_id": "benchmark:1-string-#5/50"},  # string
    # {"limit": math.inf, "external_id": "9694359_cargo_type", "end": 1534031491000},  # string
    # {
    #     "include_outside_points": True,
    #     "limit": 1,
    #     "external_id": "ts-test-#04-ten-mill-dps-1/1",
    #     "start": 31536472487 - 1,
    #     "end": 31536698071 + 1,
    #     # To override "top level" settings (if given) or fails on include_outside not allowed for agg. q:
    #     "granularity": None,
    #     "aggregates": None,
    # },
    # {
    #     "include_outside_points": False,
    #     "limit": 10,
    #     "external_id": "8400074_destination",
    #     "start": 0,  # 1533945852000-1,
    #     "end": "now",  # 1533945852000+1,
    # },
    # {
    #     "include_outside_points": False,
    #     "limit": None,
    #     "external_id": "ts-test-#04-ten-mill-dps-1/1",
    #     "start": 31536472487 - 1,
    #     "end": 2 * 31536698071 + 1,
    # },
]
# EXTERNAL_ID = [
#     f"ts-test-#01-daily-{i}/650" for i in range(1, 651)
# ]

max_workers = 10
client = setup_local_cog_client(max_workers, debug=False)

# query1 = DatapointsQuery(
t0 = timer()
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
t1 = timer()
df = res.to_pandas()
print(df.head())
if len(df) >= 10:
    print("...")
    print("\n".join(str(df.tail()).splitlines()[1:]))
print(f"{df.shape=}, {df.count().sum()=}")

tot_t = t1 - t0
n_dps_fetched = sum(map(len, res))
dps_ps = round(n_dps_fetched / tot_t, 2)
print(f"Dps/sec={dps_ps}, ~t: {round(tot_t, 4)} sec")

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
