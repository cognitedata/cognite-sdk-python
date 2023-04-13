import time
from typing import List, Tuple

import numpy as np
import pandas as pd

from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client.data_classes import DatapointsList, TimeSeries, TimeSeriesList
from cognite.client.utils._time import MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS, UNIT_IN_MS

NAMES = [
    f"PYSDK integration test {s}"
    for s in [
        "001: outside points, numeric",
        "002: outside points, string",
        *[f"{i:03d}: weekly values, 1950-2000, numeric" for i in range(3, 54)],
        *[f"{i:03d}: weekly values, 1950-2000, string" for i in range(54, 104)],
        "104: daily values, 1965-1975, numeric",
        "105: hourly values, 1969-10-01 - 1970-03-01, numeric",
        "106: every minute, 1969-12-31 - 1970-01-02, numeric",
        "107: every second, 1969-12-31 23:30:00 - 1970-01-01 00:30:00, numeric",
        "108: every millisecond, 1969-12-31 23:59:58.500 - 1970-01-01 00:00:01.500, numeric",
        "109: daily values, is_step=True, 1965-1975, numeric",
        "110: hourly values, is_step=True, 1969-10-01 - 1970-03-01, numeric",
        "111: every minute, is_step=True, 1969-12-31 - 1970-01-02, numeric",
        "112: every second, is_step=True, 1969-12-31 23:30:00 - 1970-01-01 00:30:00, numeric",
        "113: every millisecond, is_step=True, 1969-12-31 23:59:58.500 - 1970-01-01 00:00:01.500, numeric",
        "114: 1mill dps, random distribution, 1950-2020, numeric",
        "115: 1mill dps, random distribution, 1950-2020, string",
        "116: 5mill dps, 2k dps (.1s res) burst per day, 2000-01-01 12:00:00 - 2013-09-08 12:03:19.900, numeric",
        "119: hourly normally distributed (0,1) data, 2020-2024 numeric",
        "120: minute normally distributed (0,1) data, 2023-01-01 00:00:00 - 2023-12-31 23:59:59, numeric",
    ]
]
SPARSE_NAMES = [
    "PYSDK integration test 117: single dp at 1900-01-01 00:00:00, numeric",
    "PYSDK integration test 118: single dp at 2099-12-31 23:59:59.999, numeric",
]


def create_dense_rand_dist_ts(xid, seed, n=1_000_000):
    np.random.seed(seed)
    idx = np.sort(
        np.random.randint(
            pd.Timestamp("1950-01-01").value // int(1e6),
            pd.Timestamp("2020-01-01").value // int(1e6),
            n,
            dtype=np.int64,
        )
    )
    dupe = idx[:-1] == idx[1:]
    idx[1:][dupe] += 1  # fingers crossed
    idx = pd.to_datetime(idx, unit="ms").sort_values()
    assert idx.is_unique, "not unique idx"
    return pd.DataFrame(
        {xid: np.arange(n) - 499_999},
        index=idx,
    )


def create_bursty_ts(xid, offset):
    idxs = [
        pd.date_range(start=day + pd.Timedelta("12h"), periods=2000, freq="100ms")
        for day in pd.date_range(start="2000", periods=5000, freq="D")
    ]
    idxs = np.concatenate(idxs)
    return pd.DataFrame({xid: idxs.astype(np.int64) // int(1e6) + offset}, index=idxs)


def delete_all_time_series(ts_api):
    ts_api.delete(external_id=NAMES, ignore_unknown_ids=True)
    print(f"Deleted ts: {len(NAMES)=}")
    time.sleep(10)


def _sparse_ts_and_dps():
    return TimeSeriesList(
        [
            TimeSeries(name=SPARSE_NAMES[0], external_id=SPARSE_NAMES[0], is_string=False),
            TimeSeries(name=SPARSE_NAMES[1], external_id=SPARSE_NAMES[1], is_string=False),
        ]
    ), DatapointsList(
        [
            {"externalId": SPARSE_NAMES[0], "datapoints": [(MIN_TIMESTAMP_MS, MIN_TIMESTAMP_MS)]},
            {"externalId": SPARSE_NAMES[1], "datapoints": [(MAX_TIMESTAMP_MS, MAX_TIMESTAMP_MS)]},
        ]
    )


def create_edge_case_time_series(ts_api):
    ts_lst, ts_dps = _sparse_ts_and_dps()

    ts_api.create(ts_lst)

    time.sleep(1)
    ts_api.data.insert_multiple(ts_dps)
    print(f"Created {len(ts_lst)} sparse ts with data")


def create_edge_case_if_not_exists(ts_api):
    ts_lst, ts_dps = _sparse_ts_and_dps()
    create_if_not_exists(ts_api, ts_lst, [ts_dps.to_pandas(column_names="external_id", include_aggregate_name=False)])


def create_dense_time_series() -> Tuple[List[TimeSeries], List[pd.DataFrame]]:
    ts_add = (ts_lst := []).append
    df_add = (df_lst := []).append
    ts_add(TimeSeries(name=NAMES[0], external_id=NAMES[0], is_string=False, metadata={"offset": 1, "delta": 10}))
    arr = np.arange(-100, 101, 10)
    df_add(pd.DataFrame({NAMES[0]: arr + 1}, index=pd.to_datetime(arr, unit="ms")))

    ts_add(TimeSeries(name=NAMES[1], external_id=NAMES[1], is_string=True, metadata={"offset": 2, "delta": 10}))
    df_add(pd.DataFrame({NAMES[1]: (arr + 2).astype(str)}, index=pd.to_datetime(arr, unit="ms")))

    weekly_idx = pd.date_range(start="1950", end="2000", freq="1w")
    arr = weekly_idx.to_numpy("datetime64[ms]").astype(np.int64)
    for i, name in enumerate(NAMES[2:53], 3):
        ts_add(
            TimeSeries(name=name, external_id=name, is_string=False, metadata={"offset": i, "delta": UNIT_IN_MS["w"]})
        )
        df_add(pd.DataFrame({name: arr + i}, index=weekly_idx))

    for i, name in enumerate(NAMES[53:103], i + 1):
        ts_add(
            TimeSeries(name=name, external_id=name, is_string=True, metadata={"offset": i, "delta": UNIT_IN_MS["w"]})
        )
        df_add(pd.DataFrame({name: (arr + i).astype(str)}, index=weekly_idx))

    i = 103
    for is_step in [False, True]:
        daily_idx = pd.date_range(start="1965", end="1975", freq="1d")
        arr = daily_idx.to_numpy("datetime64[ms]").astype(np.int64)
        ts_add(
            TimeSeries(
                name=NAMES[i],
                external_id=NAMES[i],
                is_string=False,
                is_step=is_step,
                metadata={"offset": i + 1, "delta": UNIT_IN_MS["d"]},
            )
        )
        df_add(pd.DataFrame({NAMES[i]: arr + i + 1}, index=daily_idx))

        hourly_idx = pd.date_range(start="1969-10-01", end="1970-03-01", freq="1h")
        arr = hourly_idx.to_numpy("datetime64[ms]").astype(np.int64)
        ts_add(
            TimeSeries(
                name=NAMES[i + 1],
                external_id=NAMES[i + 1],
                is_string=False,
                is_step=is_step,
                metadata={"offset": i + 2, "delta": UNIT_IN_MS["h"]},
            )
        )
        df_add(pd.DataFrame({NAMES[i + 1]: arr + i + 2}, index=hourly_idx))

        minute_idx = pd.date_range(start="1969-12-31", end="1970-01-02", freq="1T")
        arr = minute_idx.to_numpy("datetime64[ms]").astype(np.int64)
        ts_add(
            TimeSeries(
                name=NAMES[i + 2],
                external_id=NAMES[i + 2],
                is_string=False,
                is_step=is_step,
                metadata={"offset": i + 3, "delta": UNIT_IN_MS["m"]},
            )
        )
        df_add(pd.DataFrame({NAMES[i + 2]: arr + i + 3}, index=minute_idx))

        second_idx = pd.date_range(start="1969-12-31 23:30:00", end="1970-01-01 00:30:00", freq="1s")
        arr = second_idx.to_numpy("datetime64[ms]").astype(np.int64)
        ts_add(
            TimeSeries(
                name=NAMES[i + 3],
                external_id=NAMES[i + 3],
                is_string=False,
                is_step=is_step,
                metadata={"offset": i + 4, "delta": UNIT_IN_MS["s"]},
            )
        )
        df_add(pd.DataFrame({NAMES[i + 3]: arr + i + 4}, index=second_idx))

        millisec_idx = pd.date_range(start="1969-12-31 23:59:58.500", end="1970-01-01 00:00:01.500", freq="1ms")
        arr = millisec_idx.to_numpy("datetime64[ms]").astype(np.int64)
        ts_add(
            TimeSeries(
                name=NAMES[i + 4],
                external_id=NAMES[i + 4],
                is_string=False,
                is_step=is_step,
                metadata={"offset": i + 5, "delta": 1},
            )
        )
        df_add(pd.DataFrame({NAMES[i + 4]: arr + i + 5}, index=millisec_idx))
        i += 5

    ts_add(
        TimeSeries(
            name=NAMES[113],
            external_id=NAMES[113],
            is_string=False,
            metadata={"offset": "n/a", "delta": "uniform random"},
        )
    )
    df_add(create_dense_rand_dist_ts(NAMES[113], seed=42))

    ts_add(
        TimeSeries(
            name=NAMES[114],
            external_id=NAMES[114],
            is_string=True,
            metadata={"offset": "n/a", "delta": "uniform random"},
        )
    )
    df_add(create_dense_rand_dist_ts(NAMES[114], seed=43).astype(str))

    ts_add(
        TimeSeries(
            name=NAMES[115],
            external_id=NAMES[115],
            is_string=False,
            metadata={"offset": 116, "delta": "1 or 86200100000000"},
        )
    )
    df_add(create_bursty_ts(NAMES[115], 116))

    ts_add(
        TimeSeries(name=NAMES[116], external_id=NAMES[116], is_string=False, metadata={"offset": "n/a", "delta": "1H"})
    )
    hourly_idx = pd.date_range(start="2020-01-01", end="2024-12-31 23:59:59", freq="1H", tz="UTC")
    df_add(pd.DataFrame(index=hourly_idx, data=np.random.normal(0, 1, len(hourly_idx)), columns=[NAMES[116]]))

    ts_add(
        TimeSeries(name=NAMES[117], external_id=NAMES[117], is_string=False, metadata={"offset": "n/a", "delta": "1M"})
    )
    minutely_idx = pd.date_range(start="2023-01-01", end="2023-12-31 23:59:59", freq="1min", tz="UTC")
    df_add(pd.DataFrame(index=minutely_idx, data=np.random.normal(0, 1, len(minutely_idx)), columns=[NAMES[117]]))
    return ts_lst, df_lst


def create_if_not_exists(ts_api: TimeSeriesAPI, ts_list: List[TimeSeries], df_lst: List[pd.DataFrame]) -> None:
    existing = {
        t.external_id
        for t in ts_api.retrieve_multiple(external_ids=[t.external_id for t in ts_list], ignore_unknown_ids=True)
    }
    if to_create := [t for t in ts_list if t.external_id not in existing]:
        created = ts_api.create(to_create)
        print(f"Created {len(created)} ts")
        time.sleep(5)
    else:
        print("No timeseries to create")

    # Concat consumes too much RAM, loop through dfs:
    inserted = 0
    for df in df_lst:
        if df.columns[0] in existing:
            continue
        ts_api.data.insert_dataframe(
            df,
            external_id_headers=True,
            dropna=True,
        )
        inserted += 1
    print(f"Inserted {inserted} series of datapoints")


def create_time_series(ts_api, ts_lst: List[TimeSeries], df_lst: List[pd.DataFrame]):
    ts_api.create(ts_lst)
    print(f"Created {len(ts_lst)} ts")
    time.sleep(5)
    # Concat consumes too much RAM, loop through dfs:
    for df in df_lst:
        ts_api.data.insert_dataframe(
            df,
            external_id_headers=True,
            dropna=True,
        )
    print("Inserted loads of dps!")


if __name__ == "__main__":
    # # The code for getting a client is not committed, this is to avoid accidental runs.
    from scripts import local_client

    client = local_client.get_interactive()

    delete_all_time_series(client.time_series)
    ts_lst, df_lst = create_dense_time_series()
    create_if_not_exists(client.time_series, ts_lst, df_lst)
    create_edge_case_if_not_exists(client.time_series)
