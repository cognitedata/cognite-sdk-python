import math  # noqa
import random
from pprint import pprint
from timeit import default_timer as timer

import pandas as pd

from setup_client import setup_cog_client

which_client = "py-sdk-tests"
print(f"Running with {which_client=}")

xids = random.sample(
    [
        f"ts-test-#01-daily-{i}/650"
        for i in range(1, 651)
        # f"benchmark:1-weekly-gran-#{i}/10000" for i in range(1, 10001)
    ],
    k=6,
)
payload = {
    "start": 63072000000,
    "end": 1 + pd.Timestamp("1972-01-10").value // int(1e6),  # 1893456000000,
    "id": [],
    "external_id": [*xids, {"external_id": "ts-test-#01-daily-650/650", "limit": 0}],
    "aggregates": None,
    "limit": 0,
    "granularity": None,
    "ignore_unknown_ids": True,
    "include_outside_points": False,
}
payload = None

START = 0  # pd.Timestamp("1972-01-01 00:00:00").value // int(1e6)
END = "now"  # pd.Timestamp("2030-01-01 00:00:00").value // int(1e6)
LIMIT = None
AGGREGATES = None
# AGGREGATES = ["average", "interpolation", "stepInterpolation"]
# AGGREGATES = [
#     "average", "max", "min", "count", "sum", "interpolation", "stepInterpolation",
#     "continuousVariance", "discreteVariance", "totalVariation"
# ]
GRANULARITY = None
# GRANULARITY = "1d"
INCLUDE_OUTSIDE_POINTS = False
IGNORE_UNKNOWN_IDS = True
# ID = None
ID = [
    1581323881658399,
    42,
    # {"id": 226740051491},
    # {"id": 2546012653669, "start": 1031539300000},  # string, xid=9694359_cargo_type
    # {"id": 1111111111111},  # missing...
    # {"id": 1111111111112},  # missing...
    # {"id": 1111111111113},  # missing...
    # {"id": 1111111111114},  # missing...
    # {"id": 2546012653669},  # string
]
EXTERNAL_ID = [
    # {"external_id": "ts-test-#04-ten-mill-dps-1/1"},
    # {"external_id": "ts-test-#01-daily-111/650"},
    {"external_id": "ts-test-#01-daily-222/650"},
    # {"external_id": "benchmark:11-1mill-blob-sec-after-1990-#1/10"},
    # {"external_id": "benchmark:11-1mill-blob-sec-after-1990-#2/10"},
    # {"external_id": "benchmark:11-1mill-blob-sec-after-1990-#3/10"},
    # {"external_id": "benchmark:11-1mill-blob-sec-after-1990-#4/10"},
    # {"external_id": "benchmark:11-1mill-blob-sec-after-1990-#5/10"},
    # {"external_id": "benchmark:10-1mill-blob-ms-after-1990-#1/10"},
    # {"external_id": "benchmark:10-1mill-blob-ms-after-1990-#2/10"},
    # {"limit": 99_999 + 3, "external_id": "benchmark:1-string-1h-gran-#3/50"},  # string
    # {"limit": -1, "external_id": "8400074_destination"},  # string
    # {"external_id": "benchmark:2-string-5m-gran-#1/1"},  # string
    # {"external_id": "benchmark:1-string-1h-gran-#1/50"},  # string
    {"external_id": "benchmark:1-string-1h-gran-#8/50"},  # string
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
]
EXTERNAL_ID = [
    f"ts-test-#01-daily-{i}/650"
    for i in range(1, 651)
    # {"external_id": f"ts-test-#01-daily-{i}/650", "aggregates": ["average"], "granularity": "12h"}
    # for i in range(1, 651)
]
# EXTERNAL_ID = random.sample(EXTERNAL_ID, 650)
EXTERNAL_ID = "test__constant_0_with_noise"
# EXTERNAL_ID = None

max_workers = 20
client = setup_cog_client(which_client, max_workers, debug=False)

t0 = timer()
if payload is not None:
    print("Ran payload:")
    pprint(payload)
    res = client.datapoints.retrieve(**payload)
else:
    print("Did not use payload. Settings used:")
    settings = dict(
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
    pprint(settings)
    res = client.datapoints.retrieve(**settings)
t1 = timer()
df = res.to_pandas().rename(columns=lambda s: s.split("|")[-1])
print(df.head())
if len(df) >= 10:
    print("...")
    print("\n".join(str(df.tail()).splitlines()[1:]))
print(f"{df.shape=}, {df.count().sum()=}")

tot_t = t1 - t0
n_dps_fetched = sum(map(len, res))
dps_ps = round(n_dps_fetched / tot_t, 2)

tot_dps = df.count().sum()  # Ignores simultaneously fetched aggs
tot_dps_ps = round(tot_dps / tot_t, 2)
print(f"Dps/sec={dps_ps}, (double counting aggs: {tot_dps_ps}) ~t: {round(tot_t, 4)} sec")
print(f"Dps/sec, (counting all aggs.): {tot_dps_ps}")
print(df.count().sort_values(ascending=False).head(10))

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
