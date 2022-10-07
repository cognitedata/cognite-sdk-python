"""
Note: If tests related to fetching datapoints are broken, all time series + their datapoints can be
      recreated easily by running the file linked below. You will need to provide a valid set of
      credentials to the `CogniteClient` for the Python SDK integration test CDF project:
>>> python scripts/create_ts_for_integration_tests.py
"""
import itertools
import random
import re
import time
from contextlib import nullcontext as does_not_raise
from datetime import datetime
from random import randint
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from cognite.client._constants import ALL_SORTED_DP_AGGS
from cognite.client.data_classes import Datapoints, DatapointsArray, DatapointsArrayList, DatapointsList, TimeSeries
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._auxiliary import to_camel_case, to_snake_case
from cognite.client.utils._time import (
    MAX_TIMESTAMP_MS,
    MIN_TIMESTAMP_MS,
    UNIT_IN_MS,
    align_start_and_end_for_granularity,
    granularity_to_ms,
    timestamp_to_ms,
)
from tests.utils import (
    random_aggregates,
    random_cognite_external_ids,
    random_cognite_ids,
    random_gamma_dist_integer,
    random_granularity,
    set_max_workers,
)

DATAPOINTS_API = "cognite.client._api.datapoints.{}"
WEEK_MS = UNIT_IN_MS["w"]
DAY_MS = UNIT_IN_MS["d"]
YEAR_MS = {
    1950: -631152000000,
    1965: -157766400000,
    1975: 157766400000,
    2000: 946684800000,
    2014: 1388534400000,
    2020: 1577836800000,
}
DPS_TYPES = (Datapoints, DatapointsArray)
DPS_LST_TYPES = (DatapointsList, DatapointsArrayList)

# To avoid the error "different tests were collected between...", we must make sure all parallel test-runners
# generate the same tests (randomized test data). We also want different random values over time (...thats the point),
# so we set seed based on time, but round to have some buffer:
random.seed(round(time.time(), -3))


@pytest.fixture(scope="session")
def all_test_time_series(cognite_client):
    prefix = "PYSDK integration test"
    return cognite_client.time_series.retrieve_multiple(
        external_ids=[
            f"{prefix} 001: outside points, numeric",
            f"{prefix} 002: outside points, string",
            *[f"{prefix} {i:03d}: weekly values, 1950-2000, numeric" for i in range(3, 54)],
            *[f"{prefix} {i:03d}: weekly values, 1950-2000, string" for i in range(54, 104)],
            f"{prefix} 104: daily values, 1965-1975, numeric",
            f"{prefix} 105: hourly values, 1969-10-01 - 1970-03-01, numeric",
            f"{prefix} 106: every minute, 1969-12-31 - 1970-01-02, numeric",
            f"{prefix} 107: every second, 1969-12-31 23:30:00 - 1970-01-01 00:30:00, numeric",
            f"{prefix} 108: every millisecond, 1969-12-31 23:59:58.500 - 1970-01-01 00:00:01.500, numeric",
            f"{prefix} 109: daily values, is_step=True, 1965-1975, numeric",
            f"{prefix} 110: hourly values, is_step=True, 1969-10-01 - 1970-03-01, numeric",
            f"{prefix} 111: every minute, is_step=True, 1969-12-31 - 1970-01-02, numeric",
            f"{prefix} 112: every second, is_step=True, 1969-12-31 23:30:00 - 1970-01-01 00:30:00, numeric",
            f"{prefix} 113: every millisecond, is_step=True, 1969-12-31 23:59:58.500 - 1970-01-01 00:00:01.500, numeric",
            f"{prefix} 114: 1mill dps, random distribution, 1950-2020, numeric",
            f"{prefix} 115: 1mill dps, random distribution, 1950-2020, string",
            f"{prefix} 116: 5mill dps, 2k dps (.1s res) burst per day, 2000-01-01 12:00:00 - 2013-09-08 12:03:19.900, numeric",
        ]
    )


@pytest.fixture
def outside_points_ts(all_test_time_series):
    return all_test_time_series[:2]


@pytest.fixture
def weekly_dps_ts(all_test_time_series):
    return all_test_time_series[2:53], all_test_time_series[53:103]


@pytest.fixture
def fixed_freq_dps_ts(all_test_time_series):
    return all_test_time_series[103:108], all_test_time_series[108:113]


@pytest.fixture
def one_mill_dps_ts(all_test_time_series):
    return all_test_time_series[113], all_test_time_series[114]


@pytest.fixture
def ms_bursty_ts(all_test_time_series):
    return all_test_time_series[115]


@pytest.fixture(scope="session")
def new_ts(cognite_client):
    ts = cognite_client.time_series.create(TimeSeries())
    yield ts
    cognite_client.time_series.delete(id=ts.id)
    assert cognite_client.time_series.retrieve(ts.id) is None


@pytest.fixture
def retrieve_endpoints(cognite_client):
    return [
        cognite_client.time_series.data.retrieve,
        cognite_client.time_series.data.retrieve_arrays,
    ]


def ts_to_ms(ts):
    assert isinstance(ts, str)
    return pd.Timestamp(ts).value // int(1e6)


def convert_any_ts_to_integer(ts):
    if isinstance(ts, int):
        return ts
    elif isinstance(ts, np.datetime64):
        return ts.astype("datetime64[ms]").astype(int)
    raise ValueError


def validate_raw_datapoints_lst(ts_lst, dps_lst, **kw):
    assert isinstance(dps_lst, DPS_LST_TYPES), "Datapoints(Array)List not given"
    for ts, dps in zip(ts_lst, dps_lst):
        validate_raw_datapoints(ts, dps, **kw)


def validate_raw_datapoints(ts, dps, check_offset=True, check_delta=True):
    assert isinstance(dps, DPS_TYPES), "Datapoints(Array) not given"
    # Convert both dps types to arrays for simple comparisons:
    # (also convert string datapoints - which are also integers)
    values = np.array(dps.value, dtype=np.int64)
    index = np.array(dps.timestamp, dtype=np.int64)
    if isinstance(dps, DatapointsArray):
        index = index // int(1e6)
    # Verify index is sorted:
    assert np.all(index[:-1] < index[1:])
    # Verify the actual datapoint values:
    if check_offset:
        offset = int(ts.metadata["offset"])
        assert np.all(index == values - offset)
    # Verify spacing between points:
    if check_delta:
        delta = int(ts.metadata["delta"])
        assert np.all(np.diff(values) == delta)

    return index, values


PARAMETRIZED_VALUES_OUTSIDE_POINTS = [
    (-100, 100, False, True),
    (-99, 100, True, True),
    (-99, 101, True, False),
    (-100, 101, False, False),
]


class TestRetrieveRawDatapointsAPI:
    """Note: Since `retrieve` and `retrieve_arrays` endspoints should give identical results,
    except for the data container types, all tests run both endpoints.
    """

    @pytest.mark.parametrize("start, end, has_before, has_after", PARAMETRIZED_VALUES_OUTSIDE_POINTS)
    def test_retrieve_outside_points_only(
        self, retrieve_endpoints, outside_points_ts, start, end, has_before, has_after
    ):
        # We have 10 ms resolution data between ts = -100 and +100
        for ts, endpoint in itertools.product(outside_points_ts, retrieve_endpoints):
            res = endpoint(id=ts.id, limit=0, start=start, end=end, include_outside_points=True)
            index, values = validate_raw_datapoints(ts, res, check_delta=False)
            assert len(res) == has_before + has_after

            if has_before or has_after:
                first_ts, last_ts = index[0].item(), index[-1].item()  # numpy bool != py bool
                assert (start > first_ts) is has_before
                assert (end <= last_ts) is has_after
                if has_before:
                    assert start > first_ts == -100
                if has_after:
                    assert end <= last_ts == 100

    @pytest.mark.parametrize("start, end, has_before, has_after", PARAMETRIZED_VALUES_OUTSIDE_POINTS)
    def test_retrieve_outside_points_nonzero_limit(
        self, retrieve_endpoints, outside_points_ts, start, end, has_before, has_after
    ):
        for ts, endpoint, limit in itertools.product(outside_points_ts, retrieve_endpoints, [3, None]):
            res = endpoint(id=ts.id, limit=limit, start=start, end=end, include_outside_points=True)
            index, values = validate_raw_datapoints(ts, res, check_delta=limit is None)
            if limit == 3:
                assert len(res) - 3 == has_before + has_after
            first_ts, last_ts = index[0].item(), index[-1].item()  # numpy bool != py bool
            assert (start > first_ts) is has_before
            assert (end <= last_ts) is has_after
            if has_before:
                assert start > first_ts == -100
            if has_after:
                assert end <= last_ts == 100

    @pytest.mark.parametrize("start, end, has_before, has_after", PARAMETRIZED_VALUES_OUTSIDE_POINTS)
    def test_retrieve_outside_points__query_limit_plusminus1_tests(
        self, retrieve_endpoints, outside_points_ts, start, end, has_before, has_after
    ):
        limit = 3
        for dps_limit in range(limit - 1, limit + 2):
            with patch(DATAPOINTS_API.format("DPS_LIMIT"), dps_limit):
                for ts, endpoint in itertools.product(outside_points_ts, retrieve_endpoints):
                    res = endpoint(id=ts.id, limit=limit, start=start, end=end, include_outside_points=True)
                    index, values = validate_raw_datapoints(ts, res, check_delta=False)
                    assert len(res) - 3 == has_before + has_after
                    first_ts, last_ts = index[0].item(), index[-1].item()  # numpy bool != py bool
                    assert (start > first_ts) is has_before
                    assert (end <= last_ts) is has_after
                    if has_before:
                        assert start > first_ts == -100
                    if has_after:
                        assert end <= last_ts == 100

    @pytest.mark.parametrize(
        "start, end, exp_first_ts, exp_last_ts",
        # fmt: off
        [
            (631670400000 + 1, 693964800000,     631670400000,           693964800000),  # noqa: E241
            (631670400000,     693964800000,     631670400000 - WEEK_MS, 693964800000),  # noqa: E241
            (631670400000,     693964800000 + 1, 631670400000 - WEEK_MS, 693964800000 + WEEK_MS),  # noqa: E241
            (631670400000 + 1, 693964800000 + 1, 631670400000,           693964800000 + WEEK_MS),  # noqa: E241
        ],
        # fmt: on
    )
    def test_retrieve_outside_points__query_chunking_mode(
        self, start, end, exp_first_ts, exp_last_ts, cognite_client, retrieve_endpoints, weekly_dps_ts
    ):
        ts_lst = weekly_dps_ts[0] + weekly_dps_ts[1]  # chain numeric & string
        limits = [0, 1, 50, int(1e9), None]  # None ~ 100 dps (max dps returned)
        with set_max_workers(cognite_client, 5), patch(DATAPOINTS_API.format("EagerDpsFetcher")):
            # `n_ts` is per identifier (id + xid). At least 3, since 3 x 2 > 5
            for n_ts, endpoint in itertools.product([3, 10, 50], retrieve_endpoints):
                id_ts_lst, xid_ts_lst = random.sample(ts_lst, k=n_ts), random.sample(ts_lst, k=n_ts)
                res_lst = endpoint(
                    external_id=[{"external_id": ts.external_id, "limit": random.choice(limits)} for ts in xid_ts_lst],
                    id=[{"id": ts.id, "limit": random.choice(limits)} for ts in id_ts_lst],
                    start=start,
                    end=end,
                    include_outside_points=True,
                )
                requested_ts = id_ts_lst + xid_ts_lst
                for ts, res in zip(requested_ts, res_lst):
                    index, values = validate_raw_datapoints(ts, res, check_delta=False)
                    assert exp_first_ts == index[0]
                    assert exp_last_ts == index[-1]

    @pytest.mark.parametrize(
        "n_ts, identifier",
        [
            (1, "id"),
            (1, "external_id"),
            (2, "id"),
            (2, "external_id"),
            (5, "id"),
            (5, "external_id"),
        ],
    )
    def test_retrieve_raw_eager_mode_single_identifier_type(
        self, cognite_client, n_ts, identifier, retrieve_endpoints, weekly_dps_ts
    ):
        # We patch out ChunkingDpsFetcher to make sure we fail if we're not in eager mode:
        with set_max_workers(cognite_client, 5), patch(DATAPOINTS_API.format("ChunkingDpsFetcher")):
            for ts_lst, endpoint, limit in itertools.product(weekly_dps_ts, retrieve_endpoints, [0, 50, None]):
                ts_lst = random.sample(ts_lst, n_ts)
                res_lst = endpoint(
                    **{identifier: [getattr(ts, identifier) for ts in ts_lst]},
                    limit=limit,
                    start=MIN_TIMESTAMP_MS,
                    end=MAX_TIMESTAMP_MS,
                    include_outside_points=False,
                )
                validate_raw_datapoints_lst(ts_lst, res_lst)
                exp_len = 2609 if limit is None else limit
                for res in res_lst:
                    assert len(res) == exp_len

    @pytest.mark.parametrize(
        "n_ts, identifier",
        [
            (3, "id"),
            (3, "external_id"),
            (10, "id"),
            (10, "external_id"),
            (50, "id"),
            (50, "external_id"),
        ],
    )
    def test_retrieve_raw_chunking_mode_single_identifier_type(
        self, cognite_client, n_ts, identifier, retrieve_endpoints, weekly_dps_ts
    ):
        # We patch out EagerDpsFetcher to make sure we fail if we're not in chunking mode:
        with set_max_workers(cognite_client, 2), patch(DATAPOINTS_API.format("EagerDpsFetcher")):
            for ts_lst, endpoint, limit in itertools.product(weekly_dps_ts, retrieve_endpoints, [0, 50, None]):
                ts_lst = random.sample(ts_lst, n_ts)
                res_lst = endpoint(
                    **{identifier: [getattr(ts, identifier) for ts in ts_lst]},
                    limit=limit,
                    start=MIN_TIMESTAMP_MS,
                    end=MAX_TIMESTAMP_MS,
                    include_outside_points=False,
                )
                validate_raw_datapoints_lst(ts_lst, res_lst)
                exp_len = 2609 if limit is None else limit
                for res in res_lst:
                    assert len(res) == exp_len

    @pytest.mark.parametrize(
        "n_ts, ignore_unknown_ids, mock_out_eager_or_chunk, expected_raise",
        [
            (1, True, "ChunkingDpsFetcher", does_not_raise()),
            (1, False, "ChunkingDpsFetcher", pytest.raises(CogniteNotFoundError, match=re.escape("Not found: ["))),
            (3, True, "ChunkingDpsFetcher", does_not_raise()),
            (3, False, "ChunkingDpsFetcher", pytest.raises(CogniteNotFoundError, match=re.escape("Not found: ["))),
            (10, True, "EagerDpsFetcher", does_not_raise()),
            (10, False, "EagerDpsFetcher", pytest.raises(CogniteNotFoundError, match=re.escape("Not found: ["))),
            (50, True, "EagerDpsFetcher", does_not_raise()),
            (50, False, "EagerDpsFetcher", pytest.raises(CogniteNotFoundError, match=re.escape("Not found: ["))),
        ],
    )
    def test_retrieve_unknown__check_raises_or_returns_existing_only(
        self,
        n_ts,
        ignore_unknown_ids,
        mock_out_eager_or_chunk,
        expected_raise,
        cognite_client,
        retrieve_endpoints,
        all_test_time_series,
    ):
        ts_exists = all_test_time_series[0]
        with set_max_workers(cognite_client, 9), patch(DATAPOINTS_API.format(mock_out_eager_or_chunk)):
            identifier = {
                "id": [ts_exists.id] + random_cognite_ids(n_ts),
                "external_id": [ts_exists.external_id] + random_cognite_external_ids(n_ts),
            }
            drop_id = random.choice(["id", "external_id", "keep"])
            if drop_id != "keep":
                identifier.pop(drop_id)
            for endpoint in retrieve_endpoints:
                with expected_raise:
                    res_lst = endpoint(
                        **identifier,
                        start=randint(MIN_TIMESTAMP_MS, 100),
                        end=randint(101, MAX_TIMESTAMP_MS),
                        ignore_unknown_ids=ignore_unknown_ids,
                        limit=5,
                    )
                    exp_len = 2 if drop_id == "keep" else 1
                    assert exp_len == len(res_lst)
                    validate_raw_datapoints_lst([ts_exists] * exp_len, res_lst)

    @pytest.mark.parametrize(
        "max_workers, mock_out_eager_or_chunk, ids, external_ids, exp_res_types",
        [
            # Single identifier given as base type (int/str) or as dict
            (1, "ChunkingDpsFetcher", random_cognite_ids(1)[0], None, [None, None]),
            (1, "ChunkingDpsFetcher", None, random_cognite_external_ids(1)[0], [None, None]),
            (1, "ChunkingDpsFetcher", {"id": random_cognite_ids(1)[0]}, None, [None, None]),
            (1, "ChunkingDpsFetcher", None, {"external_id": random_cognite_external_ids(1)[0]}, [None, None]),
            # Single identifier given as length-1 list:
            (1, "ChunkingDpsFetcher", random_cognite_ids(1), None, DPS_LST_TYPES),
            (1, "ChunkingDpsFetcher", None, random_cognite_external_ids(1), DPS_LST_TYPES),
            # Single identifier given by BOTH id and external id:
            (2, "ChunkingDpsFetcher", random_cognite_ids(1)[0], random_cognite_external_ids(1)[0], DPS_LST_TYPES),
            (
                2,
                "ChunkingDpsFetcher",
                {"id": random_cognite_ids(1)[0]},
                {"external_id": random_cognite_external_ids(1)[0]},
                DPS_LST_TYPES,
            ),
            (
                2,
                "ChunkingDpsFetcher",
                {"id": random_cognite_ids(1)[0]},
                random_cognite_external_ids(1)[0],
                DPS_LST_TYPES,
            ),
            (
                2,
                "ChunkingDpsFetcher",
                random_cognite_ids(1)[0],
                {"external_id": random_cognite_external_ids(1)[0]},
                DPS_LST_TYPES,
            ),
            (1, "EagerDpsFetcher", random_cognite_ids(1)[0], random_cognite_external_ids(1)[0], DPS_LST_TYPES),
            (
                1,
                "EagerDpsFetcher",
                {"id": random_cognite_ids(1)[0]},
                {"external_id": random_cognite_external_ids(1)[0]},
                DPS_LST_TYPES,
            ),
            (
                1,
                "EagerDpsFetcher",
                random_cognite_ids(1)[0],
                {"external_id": random_cognite_external_ids(1)[0]},
                DPS_LST_TYPES,
            ),
            (1, "EagerDpsFetcher", {"id": random_cognite_ids(1)[0]}, random_cognite_external_ids(1)[0], DPS_LST_TYPES),
            # Multiple identifiers given by single identifier:
            (4, "ChunkingDpsFetcher", random_cognite_ids(3), None, DPS_LST_TYPES),
            (4, "ChunkingDpsFetcher", None, random_cognite_external_ids(3), DPS_LST_TYPES),
            (2, "EagerDpsFetcher", random_cognite_ids(3), None, DPS_LST_TYPES),
            (2, "EagerDpsFetcher", None, random_cognite_external_ids(3), DPS_LST_TYPES),
            # Multiple identifiers given by BOTH identifiers:
            (5, "ChunkingDpsFetcher", random_cognite_ids(2), random_cognite_external_ids(2), DPS_LST_TYPES),
            (5, "ChunkingDpsFetcher", random_cognite_ids(2), random_cognite_external_ids(2), DPS_LST_TYPES),
            (3, "EagerDpsFetcher", random_cognite_ids(2), random_cognite_external_ids(2), DPS_LST_TYPES),
            (3, "EagerDpsFetcher", random_cognite_ids(2), random_cognite_external_ids(2), DPS_LST_TYPES),
            (
                5,
                "ChunkingDpsFetcher",
                [{"id": id} for id in random_cognite_ids(2)],
                [{"external_id": xid} for xid in random_cognite_external_ids(2)],
                DPS_LST_TYPES,
            ),
            (
                3,
                "EagerDpsFetcher",
                [{"id": id} for id in random_cognite_ids(2)],
                [{"external_id": xid} for xid in random_cognite_external_ids(2)],
                DPS_LST_TYPES,
            ),
        ],
    )
    def test_retrieve__all_unknown_single_multiple_given(
        self, max_workers, mock_out_eager_or_chunk, ids, external_ids, exp_res_types, cognite_client, retrieve_endpoints
    ):
        with set_max_workers(cognite_client, max_workers), patch(DATAPOINTS_API.format(mock_out_eager_or_chunk)):
            for endpoint, exp_res_type in zip(retrieve_endpoints, exp_res_types):
                res = endpoint(
                    id=ids,
                    external_id=external_ids,
                    ignore_unknown_ids=True,
                )
                if exp_res_type is None:
                    assert res is None
                else:
                    assert isinstance(res, exp_res_type)
                    assert len(res) == 0

    @pytest.mark.parametrize(
        "max_workers, mock_out_eager_or_chunk, identifier, exp_res_types",
        [
            (1, "ChunkingDpsFetcher", "id", DPS_TYPES),
            (1, "ChunkingDpsFetcher", "external_id", DPS_TYPES),
        ],
    )
    def test_retrieve_nothing__single(
        self,
        max_workers,
        mock_out_eager_or_chunk,
        identifier,
        exp_res_types,
        outside_points_ts,
        retrieve_endpoints,
        cognite_client,
    ):
        ts = outside_points_ts[0]
        with set_max_workers(cognite_client, max_workers), patch(DATAPOINTS_API.format(mock_out_eager_or_chunk)):
            for endpoint, exp_res_type in zip(retrieve_endpoints, exp_res_types):
                res = endpoint(**{identifier: getattr(ts, identifier)}, start=1, end=9)
                assert isinstance(res, exp_res_type)
                assert len(res) == 0
                assert isinstance(res.is_step, bool)
                assert isinstance(res.is_string, bool)

    @pytest.mark.parametrize(
        "max_workers, mock_out_eager_or_chunk, exp_res_types",
        [
            (1, "EagerDpsFetcher", DPS_LST_TYPES),
            (3, "ChunkingDpsFetcher", DPS_LST_TYPES),
        ],
    )
    def test_retrieve_nothing__multiple(
        self, max_workers, mock_out_eager_or_chunk, exp_res_types, outside_points_ts, retrieve_endpoints, cognite_client
    ):
        ts1, ts2 = outside_points_ts
        with set_max_workers(cognite_client, max_workers), patch(DATAPOINTS_API.format(mock_out_eager_or_chunk)):
            for endpoint, exp_res_type in zip(retrieve_endpoints, exp_res_types):
                res = endpoint(id=ts1.id, external_id=[ts2.external_id], start=1, end=9)
                assert isinstance(res, exp_res_type)
                assert len(res) == 2
                for r in res:
                    assert len(r) == 0
                    assert isinstance(r.is_step, bool)
                    assert isinstance(r.is_string, bool)


class TestRetrieveAggregateDatapointsAPI:
    @pytest.mark.parametrize(
        "aggregates",
        (
            ["step_interpolation"],
            ["stepInterpolation", "min"],
            ["continuous_variance", "discrete_variance", "step_interpolation", "total_variation"],
            ["continuous_variance", "discrete_variance", "step_interpolation", "total_variation", "min"],
            list(map(to_camel_case, ALL_SORTED_DP_AGGS)),
            list(map(to_snake_case, ALL_SORTED_DP_AGGS)),
            # Give a mix:
            ["continuousVariance", "discrete_variance", "step_interpolation", "totalVariation"],
            ["continuous_variance", "discreteVariance", "stepInterpolation", "total_variation", "min"],
        ),
    )
    def test_aggregates_in_camel_or_and_snake_case(self, aggregates, fixed_freq_dps_ts, retrieve_endpoints):
        dfs, ts = [], random.choice(fixed_freq_dps_ts[0])  # only pick from numeric ts
        granularity = random_granularity()
        for endpoint in retrieve_endpoints:
            res = endpoint(
                limit=5,
                id={"id": ts.id, "granularity": granularity, "aggregates": aggregates},
            )
            snake_aggs = sorted(map(to_snake_case, aggregates))
            for col_names in ["id", "external_id"]:
                res_df = res.to_pandas(column_names=col_names, include_aggregate_name=True)
                assert all(res_df.columns == [f"{getattr(ts, col_names)}|{agg}" for agg in snake_aggs])
                dfs.append(res_df)
        # Also make sure `Datapoints.to_pandas()` and `DatapointsArray.to_pandas()` give identical results:
        pd.testing.assert_frame_equal(dfs[0], dfs[2])
        pd.testing.assert_frame_equal(dfs[1], dfs[3])

    @pytest.mark.parametrize(
        "is_step, start, end, exp_start, exp_end, max_workers, mock_out_eager_or_chunk",
        (
            (True, MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS, YEAR_MS[1965], YEAR_MS[1975], 4, "ChunkingDpsFetcher"),
            (False, MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS, YEAR_MS[1965], YEAR_MS[1975], 4, "ChunkingDpsFetcher"),
            (True, MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS, YEAR_MS[1965], YEAR_MS[1975], 1, "EagerDpsFetcher"),
            (False, MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS, YEAR_MS[1965], YEAR_MS[1975], 1, "EagerDpsFetcher"),
            (True, YEAR_MS[1965], YEAR_MS[1975], YEAR_MS[1965], YEAR_MS[1975] - DAY_MS, 4, "ChunkingDpsFetcher"),
            (False, YEAR_MS[1965], YEAR_MS[1975], YEAR_MS[1965], YEAR_MS[1975] - DAY_MS, 4, "ChunkingDpsFetcher"),
            (True, YEAR_MS[1965], YEAR_MS[1975], YEAR_MS[1965], YEAR_MS[1975] - DAY_MS, 1, "EagerDpsFetcher"),
            (False, YEAR_MS[1965], YEAR_MS[1975], YEAR_MS[1965], YEAR_MS[1975] - DAY_MS, 1, "EagerDpsFetcher"),
            (True, -WEEK_MS, WEEK_MS + 1, -WEEK_MS, WEEK_MS, 4, "ChunkingDpsFetcher"),
            (False, -WEEK_MS, WEEK_MS + 1, -WEEK_MS, WEEK_MS, 4, "ChunkingDpsFetcher"),
            (True, -WEEK_MS, WEEK_MS + 1, -WEEK_MS, WEEK_MS, 1, "EagerDpsFetcher"),
            (False, -WEEK_MS, WEEK_MS + 1, -WEEK_MS, WEEK_MS, 1, "EagerDpsFetcher"),
            (True, -DAY_MS, DAY_MS + 1, -DAY_MS, DAY_MS, 4, "ChunkingDpsFetcher"),
            (False, -DAY_MS, DAY_MS + 1, -DAY_MS, DAY_MS, 4, "ChunkingDpsFetcher"),
            (True, -DAY_MS, DAY_MS + 1, -DAY_MS, DAY_MS, 1, "EagerDpsFetcher"),
            (False, -DAY_MS, DAY_MS + 1, -DAY_MS, DAY_MS, 1, "EagerDpsFetcher"),
        ),
    )
    def test_sparse_data__multiple_granularities_is_step_true_false(
        self,
        is_step,
        start,
        end,
        exp_start,
        exp_end,
        max_workers,
        mock_out_eager_or_chunk,
        cognite_client,
        retrieve_endpoints,
        fixed_freq_dps_ts,
    ):
        # Underlying time series has daily values, we ask for 1d, 1h, 1m and 1s and make sure all share
        # the exact same timestamps. Interpolation aggregates tested separately because they return data
        # also in empty regions... why:(
        (ts_daily, *_), (ts_daily_is_step, *_) = fixed_freq_dps_ts
        ts, exclude = ts_daily, {"step_interpolation"}
        if is_step:
            exclude.add("interpolation")
            ts = ts_daily_is_step

        assert ts.is_step is is_step

        with set_max_workers(cognite_client, max_workers), patch(DATAPOINTS_API.format(mock_out_eager_or_chunk)):
            for endpoint in retrieve_endpoints:
                # Give each "granularity" a random list of aggregates:
                aggs = [random_aggregates(exclude=exclude) for _ in range(4)]
                res = endpoint(
                    start=start,
                    end=end,
                    id=[
                        {"id": ts.id, "granularity": "1d", "aggregates": aggs[0]},
                        {"id": ts.id, "granularity": "1h", "aggregates": aggs[1]},
                    ],
                    external_id=[
                        {"external_id": ts.external_id, "granularity": "1m", "aggregates": aggs[2]},
                        {"external_id": ts.external_id, "granularity": "1s", "aggregates": aggs[3]},
                    ],
                )
                assert ((df := res.to_pandas()).isna().sum() == 0).all()
                assert df.index[0] == pd.Timestamp(exp_start, unit="ms")
                assert df.index[-1] == pd.Timestamp(exp_end, unit="ms")

    @pytest.mark.parametrize(
        "is_step, aggregates, empty, before_first_dp",
        (
            (False, ["interpolation"], True, False),
            (False, ["interpolation"], True, True),
            (False, ["step_interpolation"], False, False),
            (False, ["step_interpolation"], True, True),
            (True, ["interpolation"], False, False),
            (True, ["interpolation"], True, True),
            (True, ["step_interpolation"], False, False),
            (True, ["step_interpolation"], True, True),
        ),
    )
    def test_interpolation_returns_data_from_empty_periods_before_and_after_data(
        self,
        is_step,
        aggregates,
        empty,
        before_first_dp,
        retrieve_endpoints,
        fixed_freq_dps_ts,
    ):
        # Ts: has ms resolution data from:
        # 1969-12-31 23:59:58.500 (-1500 ms) to 1970-01-01 00:00:01.500 (1500 ms)
        (*_, ts_ms), (*_, ts_ms_is_step) = fixed_freq_dps_ts
        ts = ts_ms_is_step if is_step else ts_ms
        assert ts.is_step is is_step

        # Pick random start and end in an empty region:
        if before_first_dp:
            start = randint(MIN_TIMESTAMP_MS, -315619200000)  # 1900 -> 1960
            end = randint(start, -31536000000)  # start -> 1969
        else:  # after last dp
            start = randint(31536000000, 2524608000000)  # 1971 -> 2050
            end = randint(start, MAX_TIMESTAMP_MS)  # start -> (2051 minus 1ms)
        granularities = f"{randint(1, 15)}d", f"{randint(1, 50)}h", f"{randint(1, 120)}m", f"{randint(1, 120)}s"

        for endpoint in retrieve_endpoints:
            res_lst = endpoint(
                start=start,
                end=end,
                aggregates=aggregates,
                id=[
                    {"id": ts.id, "granularity": granularities[0]},
                    {"id": ts.id, "granularity": granularities[1]},
                ],
                external_id=[
                    {"external_id": ts.external_id, "granularity": granularities[2]},
                    {"external_id": ts.external_id, "granularity": granularities[3]},
                    # Verify empty with `count` (wont return any datapoints):
                    {"external_id": ts.external_id, "granularity": granularities[0], "aggregates": ["count"]},
                    # Verify empty with `count` and `step_interpolation`: will always return a datapoint (if not before_first_dp),
                    # with count agg missing, so make sure it is correctly "loaded" into the data object as a 0 (zero):
                    {
                        "external_id": ts.external_id,
                        "granularity": granularities[0],
                        "aggregates": ["count", "step_interpolation"],
                    },
                ],
            )
            *interp_res_lst, count_dps, count_step_interp_dps = res_lst
            assert len(count_dps.count) == 0
            if not before_first_dp:
                assert len(count_step_interp_dps.count) == 1 and count_step_interp_dps.count[0] == 0

            for dps, gran in zip(interp_res_lst, granularities):
                interp_dps = getattr(dps, aggregates[0])
                assert (len(interp_dps) == 0) is empty

                if not empty:
                    first_ts = convert_any_ts_to_integer(dps.timestamp[0])
                    aligned_start, _ = align_start_and_end_for_granularity(start, end, gran)
                    assert first_ts == aligned_start

    @pytest.mark.parametrize(
        "max_workers, n_ts, mock_out_eager_or_chunk",
        [
            (1, 1, "ChunkingDpsFetcher"),
            (5, 1, "ChunkingDpsFetcher"),
            (5, 5, "ChunkingDpsFetcher"),
            (1, 2, "EagerDpsFetcher"),
            (1, 10, "EagerDpsFetcher"),
            (9, 10, "EagerDpsFetcher"),
            (9, 50, "EagerDpsFetcher"),
        ],
    )
    def test_retrieve_aggregates__string_ts_raises(
        self, max_workers, n_ts, mock_out_eager_or_chunk, weekly_dps_ts, cognite_client, retrieve_endpoints
    ):
        _, string_ts = weekly_dps_ts
        with set_max_workers(cognite_client, max_workers), patch(DATAPOINTS_API.format(mock_out_eager_or_chunk)):
            ts_chunk = random.sample(string_ts, k=n_ts)
            for endpoint in retrieve_endpoints:
                with pytest.raises(CogniteAPIError) as exc:
                    endpoint(
                        granularity=random_granularity(),
                        aggregates=random_aggregates(),
                        id=[ts.id for ts in ts_chunk],
                        ignore_unknown_ids=random.choice((True, False)),
                    )
                assert exc.value.code == 400
                assert exc.value.message == "Aggregates are not supported for string time series"

    @pytest.mark.parametrize(
        "granularity, lower_lim, upper_lim",
        (
            ("h", 30, 1000),
            ("d", 1, 200),
        ),
    )
    def test_granularity_invariants(self, granularity, lower_lim, upper_lim, one_mill_dps_ts, retrieve_endpoints):
        # Sum of count and sum of sum is independent of granularity
        ts, _ = one_mill_dps_ts
        for endpoint in retrieve_endpoints:
            res = endpoint(
                start=MIN_TIMESTAMP_MS,
                end=MAX_TIMESTAMP_MS,
                id={"id": ts.id, "aggregates": ["count", "sum"]},
                granularity=random_granularity(granularity, lower_lim, upper_lim),
            )
            assert sum(res.count) == 1_000_000
            assert sum(res.sum) == 500_000

    @pytest.mark.parametrize(
        "first_gran, second_gran, start",
        (
            ("60s", "1m", 86572008555),
            ("120s", "2m", 27340402091),
            ("60m", "1h", -357464206106),
            ("120m", "2h", -150117679983),
            ("24h", "1d", 114399466017),
            ("48h", "2d", -170931071253),
            ("240h", "10d", 366850985031),
            ("4800h", "200d", -562661581583),
        ),
    )
    def test_can_be_equivalent_granularities(self, first_gran, second_gran, start, one_mill_dps_ts, retrieve_endpoints):
        ts, _ = one_mill_dps_ts  # data: 1950-2020
        gran_ms = granularity_to_ms(first_gran)
        for endpoint in retrieve_endpoints:
            end = start + gran_ms * random.randint(10, 1000)
            start_aligned, end_aligned = align_start_and_end_for_granularity(start, end, second_gran)
            res_lst = endpoint(
                aggregates=random_aggregates(),
                id=[
                    # These should return different results:
                    {"id": ts.id, "granularity": first_gran, "start": start, "end": end},
                    {"id": ts.id, "granularity": second_gran, "start": start, "end": end},
                ],
                external_id=[
                    # These should return identical results (up to float precision):
                    {
                        "external_id": ts.external_id,
                        "granularity": first_gran,
                        "start": start_aligned,
                        "end": end_aligned,
                    },
                    {
                        "external_id": ts.external_id,
                        "granularity": second_gran,
                        "start": start_aligned,
                        "end": end_aligned,
                    },
                ],
            )
            dps1, dps2, dps3, dps4 = res_lst
            pd.testing.assert_frame_equal(dps3.to_pandas(), dps4.to_pandas())
            with pytest.raises(AssertionError):
                pd.testing.assert_frame_equal(dps1.to_pandas(), dps2.to_pandas())

    @pytest.mark.parametrize(
        "max_workers, ts_idx, granularity, exp_len, start, end, exlude_step_interp",
        (
            (1, 105, "8m", 81, ts_to_ms("1969-12-31 14:14:14"), ts_to_ms("1970-01-01 01:01:01"), False),
            (1, 106, "7s", 386, ts_to_ms("1960"), ts_to_ms("1970-01-01 00:15:00"), False),
            (8, 106, "7s", 386, ts_to_ms("1960"), ts_to_ms("1970-01-01 00:15:00"), False),
            (2, 107, "1s", 4, ts_to_ms("1969-12-31 23:59:58.123"), ts_to_ms("2049-01-01 00:00:01.500"), True),
            (5, 113, "11h", 32_288, ts_to_ms("1960-01-02 03:04:05.060"), ts_to_ms("2000-07-08 09:10:11.121"), True),
            (3, 115, "1s", 200, ts_to_ms("2000-01-01"), ts_to_ms("2000-01-01 12:03:20"), False),
            (20, 115, "12h", 5_000, ts_to_ms("1990-01-01"), ts_to_ms("2013-09-09 00:00:00.001"), True),
        ),
    )
    def test_eager_fetcher_unlimited(
        self,
        max_workers,
        ts_idx,
        granularity,
        exp_len,
        start,
        end,
        exlude_step_interp,
        retrieve_endpoints,
        all_test_time_series,
        cognite_client,
    ):
        exclude = {"step_interpolation"} if exlude_step_interp else set()
        with set_max_workers(cognite_client, max_workers), patch(DATAPOINTS_API.format("ChunkingDpsFetcher")):
            for endpoint in retrieve_endpoints:
                res = endpoint(
                    id=all_test_time_series[ts_idx].id,
                    start=start,
                    end=end,
                    granularity=granularity,
                    aggregates=random_aggregates(exclude=exclude),
                    limit=None,
                )
                assert len(res) == exp_len
                assert len(set(np.diff(res.timestamp))) == 1

    def test_eager_chunking_unlimited(self, retrieve_endpoints, all_test_time_series, cognite_client):
        # We run all test from Eager (above) at the same time:
        test_setup_data = (
            (105, "8m", 81, ts_to_ms("1969-12-31 14:14:14"), ts_to_ms("1970-01-01 01:01:01"), False),
            (106, "7s", 386, ts_to_ms("1960"), ts_to_ms("1970-01-01 00:15:00"), False),
            (106, "7s", 386, ts_to_ms("1960"), ts_to_ms("1970-01-01 00:15:00"), False),
            (107, "1s", 4, ts_to_ms("1969-12-31 23:59:58.123"), ts_to_ms("2049-01-01 00:00:01.500"), True),
            (113, "11h", 32_288, ts_to_ms("1960-01-02 03:04:05.060"), ts_to_ms("2000-07-08 09:10:11.121"), True),
            (115, "1s", 200, ts_to_ms("2000-01-01"), ts_to_ms("2000-01-01 12:03:20"), False),
            (115, "12h", 5_000, ts_to_ms("1990-01-01"), ts_to_ms("2013-09-09 00:00:00.001"), True),
        )
        with set_max_workers(cognite_client, 2), patch(DATAPOINTS_API.format("EagerDpsFetcher")):
            ids = [
                {
                    "id": all_test_time_series[idx].id,
                    "start": start,
                    "end": end,
                    "granularity": gran,
                    "aggregates": random_aggregates(exclude={"step_interpolation"} if exclude else set()),
                }
                for idx, gran, _, start, end, exclude in test_setup_data
            ]
            for endpoint in retrieve_endpoints:
                res_lst = endpoint(id=ids, limit=None)
                for row, res in zip(test_setup_data, res_lst):
                    exp_len = row[2]
                    assert len(res) == exp_len
                    assert len(set(np.diff(res.timestamp))) == 1

    @pytest.mark.parametrize(
        "max_workers, n_ts, mock_out_eager_or_chunk, use_bursty",
        (
            (3, 1, "ChunkingDpsFetcher", True),
            (3, 1, "ChunkingDpsFetcher", False),
            (10, 10, "ChunkingDpsFetcher", True),
            (10, 10, "ChunkingDpsFetcher", False),
            (1, 2, "EagerDpsFetcher", True),
            (1, 2, "EagerDpsFetcher", False),
            (2, 10, "EagerDpsFetcher", True),
            (2, 10, "EagerDpsFetcher", False),
        ),
    )
    def test_finite_limit(
        self,
        max_workers,
        n_ts,
        mock_out_eager_or_chunk,
        use_bursty,
        cognite_client,
        ms_bursty_ts,
        one_mill_dps_ts,
        retrieve_endpoints,
    ):
        if use_bursty:
            ts = ms_bursty_ts  # data: 2000-01-01 12:00:00 - 2013-09-08 12:03:19.900
            start, end, gran_unit_upper = YEAR_MS[2000], YEAR_MS[2014], 60
        else:
            ts, _ = one_mill_dps_ts  # data: 1950-2020 ~25k days
            start, end, gran_unit_upper = YEAR_MS[1950], YEAR_MS[2020], 120
        with set_max_workers(cognite_client, max_workers), patch(DATAPOINTS_API.format(mock_out_eager_or_chunk)):
            for endpoint in retrieve_endpoints:
                limits = random.sample(range(2000), k=n_ts)
                res_lst = endpoint(
                    start=start,
                    end=end,
                    id=[
                        {
                            "id": ts.id,
                            "limit": lim,
                            "aggregates": random_aggregates(1),
                            "granularity": f"{random_gamma_dist_integer(gran_unit_upper)}{random.choice('smh')}",
                        }
                        for lim in limits
                    ],
                )
                for res, exp_lim in zip(res_lst, limits):
                    assert len(res) == exp_lim

    def test_edge_case_all_aggs_missing(self, one_mill_dps_ts, retrieve_endpoints):
        xid = one_mill_dps_ts[0].external_id
        for endpoint in retrieve_endpoints:
            res = endpoint(
                start=ts_to_ms("1970-09-05 12:00:06"),
                end=ts_to_ms("1970-09-05 19:49:40"),
                external_id=xid,
                granularity="1s",
                aggregates=["average", "count", "interpolation"],
            )
            # Each dp is more than 1h apart, leading to all-nans for interp. agg only:
            df = res.to_pandas(include_aggregate_name=True, column_names="external_id")
            assert df[f"{xid}|interpolation"].isna().all()  # SDK bug v<5, would be all None
            assert df[f"{xid}|average"].notna().all()
            assert df[f"{xid}|count"].dtype == np.int64
            assert df[f"{xid}|interpolation"].dtype == np.float64


class TestRetrieveMixedRawAndAgg:
    def test_multiple_settings_for_ignore_unknown_ids(
        self,
        ms_bursty_ts,
        one_mill_dps_ts,
        retrieve_endpoints,
    ):
        ts_num, ts_str = one_mill_dps_ts
        for endpoint, exp_res_lst_type in zip(retrieve_endpoints, DPS_LST_TYPES):
            random_xids = random_cognite_external_ids(3)
            res_lst = endpoint(
                ignore_unknown_ids=False,  # The key test ingredient
                external_id=[
                    ts_str.external_id,
                    *[
                        {"external_id": xid, "ignore_unknown_ids": True}  # Override ignore_unknown_ids default
                        for xid in random_xids
                    ],
                ],
                id=[
                    {"id": ms_bursty_ts.id, "include_outside_points": True, "limit": 20},
                    {"id": ts_num.id, "granularity": random_granularity("sm"), "aggregates": random_aggregates(2)},
                ],
                limit=5,
            )
            assert isinstance(res_lst, exp_res_lst_type)
            assert len(res_lst) == 3
            assert res_lst.get(external_id=random_xids[0]) is None
            dps_xid = res_lst.get(external_id=ts_str.external_id)
            assert isinstance(dps_xid, exp_res_lst_type._RESOURCE)
            dps_id = res_lst.get(id=ts_num.id)
            assert isinstance(dps_id, exp_res_lst_type._RESOURCE)

    def test_query_no_ts_exists(self, retrieve_endpoints):
        for endpoint, exp_res_lst_type in zip(retrieve_endpoints, DPS_LST_TYPES):
            ts_id = random_cognite_ids(1)  # list of len 1
            res_lst = endpoint(id=ts_id, ignore_unknown_ids=True)
            assert isinstance(res_lst, exp_res_lst_type)
            assert (
                res_lst.get(id=ts_id[0]) is None
            )  # SDK bug v<5, id mapping would not exist because no `.data` on res.lst.


class TestRetrieveDataFrameAPI:
    """The `retrieve_dataframe` endpoint uses `retrieve_arrays` under the hood, so lots of tests
    do not need to be repeated.
    """

    @pytest.mark.parametrize(
        "max_workers, n_ts, mock_out_eager_or_chunk, outside, exp_len",
        (
            (1, 1, "ChunkingDpsFetcher", True, 524),
            (1, 1, "ChunkingDpsFetcher", False, 524 - 2),
            (2, 5, "EagerDpsFetcher", True, 524),
            (2, 5, "EagerDpsFetcher", False, 524 - 2),
        ),
    )
    def test_raw_dps(self, max_workers, n_ts, mock_out_eager_or_chunk, outside, exp_len, cognite_client, weekly_dps_ts):
        ts_lst_numeric, ts_lst_string = weekly_dps_ts
        for exp_dtype, ts_lst in zip([np.float64, object], weekly_dps_ts):
            ts_sample = random.sample(ts_lst, k=n_ts)
            res_df = cognite_client.time_series.data.retrieve_dataframe(
                id=[ts.id for ts in ts_sample],
                start=YEAR_MS[1965],
                end=YEAR_MS[1975],
                include_outside_points=outside,
            )
            assert res_df.isna().sum().sum() == 0
            assert res_df.shape == (exp_len, n_ts)
            assert res_df.dtypes.nunique() == 1
            assert res_df.dtypes.iloc[0] == exp_dtype

    @pytest.mark.parametrize(
        "uniform, exp_n_ts_delta, exp_n_nans_step_interp",
        (
            (True, 1, 1),
            (False, 2, 0),
        ),
    )
    def test_agg_uniform_true_false(
        self, uniform, exp_n_ts_delta, exp_n_nans_step_interp, cognite_client, one_mill_dps_ts
    ):
        ts, _ = one_mill_dps_ts
        with set_max_workers(cognite_client, 1):
            res_df = cognite_client.time_series.data.retrieve_dataframe(
                id=ts.id,
                start=YEAR_MS[1965],
                end=YEAR_MS[1975],
                granularity="3h",
                aggregates=["step_interpolation", "average"],
                uniform_index=uniform,
            )
            assert len(set(np.diff(res_df.index))) == exp_n_ts_delta
            assert res_df[f"{ts.external_id}|step_interpolation"].isna().sum() == exp_n_nans_step_interp
            assert (res_df.count().values == [28994, 29215]).all()

    @pytest.mark.parametrize("limit", (0, 1, 2))
    def test_low_limits(self, limit, cognite_client, one_mill_dps_ts):
        ts_numeric, ts_string = one_mill_dps_ts
        res_df = cognite_client.time_series.data.retrieve_dataframe(
            # Raw dps:
            id=[
                ts_string.id,
                {"id": ts_string.id, "include_outside_points": True},
                ts_numeric.id,
                {"id": ts_numeric.id, "include_outside_points": True},
            ],
            # Agg dps:
            external_id={
                "external_id": ts_numeric.external_id,
                "granularity": random_granularity(upper_lim=120),
                # Exclude count (only non-float agg) and (step_)interpolation which might yield nans:
                "aggregates": random_aggregates(exclude={"count", "interpolation", "step_interpolation"}),
            },
            start=random.randint(YEAR_MS[1950], YEAR_MS[2000]),
            end=ts_to_ms("2019-12-01"),
            limit=limit,
        )
        # We have duplicates in df.columns, so to test specific columns, we reset first:
        res_df.columns = c1, c2, c3, c4, *cx = range(len(res_df.columns))
        assert res_df[[c1, c2]].dtypes.unique() == [object]
        assert res_df[[c3, c4, *cx]].dtypes.unique() == [np.float64]
        assert (res_df[[c1, c3, *cx]].count() == [limit] * (len(cx) + 2)).all()
        assert (res_df[[c2, c4]].count() == [limit + 2] * 2).all()

    @pytest.mark.parametrize(
        "granularity_lst, aggregates_lst, limits",
        (
            # Fail because of raw request:
            ([None, "1s"], [None, random_aggregates(1)], [None, None]),
            # Fail because of multiple granularities:
            (["1m", "60s"], [random_aggregates(1), random_aggregates(1)], [None, None]),
            # Fail because of finite limit:
            (["1d", "1d"], [random_aggregates(1), random_aggregates(1)], [123, None]),
        ),
    )
    def test_uniform_index_fails(self, granularity_lst, aggregates_lst, limits, cognite_client, one_mill_dps_ts):
        with pytest.raises(ValueError, match="Cannot return a uniform index"):
            cognite_client.time_series.data.retrieve_dataframe(
                uniform_index=True,
                id=[
                    {"id": one_mill_dps_ts[0].id, "granularity": gran, "aggregates": agg, "limit": lim}
                    for gran, agg, lim in zip(granularity_lst, aggregates_lst, limits)
                ],
            )

    @pytest.mark.parametrize(
        "include_aggregate_name, column_names",
        (
            (True, "id"),
            (True, "external_id"),
            (False, "id"),
            (False, "external_id"),
        ),
    )
    def test_include_aggregate_name_and_column_names_true_false(
        self, include_aggregate_name, column_names, cognite_client, one_mill_dps_ts
    ):
        ts = one_mill_dps_ts[0]
        random.shuffle(aggs := ALL_SORTED_DP_AGGS[:])

        res_df = cognite_client.time_series.data.retrieve_dataframe(
            id=ts.id,
            limit=5,
            granularity=random_granularity(),
            aggregates=aggs,
            include_aggregate_name=include_aggregate_name,
            column_names=column_names,
        )
        for col, agg in zip(res_df.columns, ALL_SORTED_DP_AGGS):
            name = str(getattr(ts, column_names))
            if include_aggregate_name:
                name += f"|{agg}"
            assert col == name

    def test_column_names_fails(self, cognite_client, one_mill_dps_ts):
        with pytest.raises(ValueError, match=re.escape("must be one of 'id' or 'external_id'")):
            cognite_client.time_series.data.retrieve_dataframe(
                id=one_mill_dps_ts[0].id, limit=5, column_names="bogus_id"
            )

    def test_include_aggregate_name_fails(self, cognite_client, one_mill_dps_ts):
        with pytest.raises(AssertionError):
            cognite_client.time_series.data.retrieve_dataframe(
                id=one_mill_dps_ts[0].id, limit=5, include_aggregate_name=None
            )


@pytest.fixture
def post_spy(cognite_client):
    dps_api = cognite_client.time_series.data
    with patch.object(dps_api, "_post", wraps=dps_api._post):
        yield


class TestRetrieveLatestDatapointsAPI:
    def test_retrieve_latest(self, cognite_client, all_test_time_series):
        ids = [all_test_time_series[0].id, all_test_time_series[1].id]
        res = cognite_client.time_series.data.retrieve_latest(id=ids)
        for dps in res:
            assert 1 == len(dps)

    def test_retrieve_latest_two_unknown(self, cognite_client, all_test_time_series):
        ids = [all_test_time_series[0].id, all_test_time_series[1].id, 42, 1337]
        res = cognite_client.time_series.data.retrieve_latest(id=ids, ignore_unknown_ids=True)
        assert 2 == len(res)
        for dps in res:
            assert 1 == len(dps)

    def test_retrieve_latest_many(self, cognite_client, post_spy):
        ids = [t.id for t in cognite_client.time_series.list(limit=12) if not t.security_categories]
        assert len(ids) > 10  # more than one page

        with patch(DATAPOINTS_API.format("RETRIEVE_LATEST_LIMIT"), 10):
            res = cognite_client.time_series.data.retrieve_latest(id=ids, ignore_unknown_ids=True)

        assert {dps.id for dps in res}.issubset(set(ids))
        assert 2 == cognite_client.time_series.data._post.call_count  # needs post_spy
        for dps in res:
            assert len(dps) <= 1  # could be empty

    def test_retrieve_latest_before(self, cognite_client, all_test_time_series):
        ts = all_test_time_series[0]
        res = cognite_client.time_series.data.retrieve_latest(id=ts.id, before="1h-ago")
        assert 1 == len(res)
        assert res[0].timestamp < timestamp_to_ms("1h-ago")


class TestInsertDatapointsAPI:
    def test_insert(self, cognite_client, new_ts, post_spy):
        datapoints = [(datetime(year=2018, month=1, day=1, hour=1, minute=i), i) for i in range(60)]
        with patch(DATAPOINTS_API.format("DPS_LIMIT"), 30), patch(DATAPOINTS_API.format("POST_DPS_OBJECTS_LIMIT"), 30):
            cognite_client.time_series.data.insert(datapoints, id=new_ts.id)
        assert 2 == cognite_client.time_series.data._post.call_count  # needs post_spy

    def test_insert_before_epoch(self, cognite_client, new_ts, post_spy):
        datapoints = [(datetime(year=1950, month=1, day=1, hour=1, minute=i), i) for i in range(60)]
        with patch(DATAPOINTS_API.format("DPS_LIMIT"), 30), patch(DATAPOINTS_API.format("POST_DPS_OBJECTS_LIMIT"), 30):
            cognite_client.time_series.data.insert(datapoints, id=new_ts.id)
        assert 2 == cognite_client.time_series.data._post.call_count  # needs post_spy

    def test_insert_copy(self, cognite_client, ms_bursty_ts, new_ts, post_spy):
        data = cognite_client.time_series.data.retrieve(id=ms_bursty_ts.id, start=0, end="now", limit=100)
        assert 100 == len(data)
        cognite_client.time_series.data.insert(data, id=new_ts.id)
        assert 2 == cognite_client.time_series.data._post.call_count  # needs post_spy

    def test_insert_copy_fails_at_aggregate(self, cognite_client, ms_bursty_ts, new_ts):
        data = cognite_client.time_series.data.retrieve(
            id=ms_bursty_ts.id, end="now", granularity="1m", aggregates=random_aggregates(1), limit=100
        )
        assert 100 == len(data)
        with pytest.raises(ValueError, match="only raw datapoints are supported"):
            cognite_client.time_series.data.insert(data, id=new_ts.id)

    def test_insert_pandas_dataframe(self, cognite_client, new_ts, post_spy):
        df = pd.DataFrame(
            {new_ts.id: np.random.normal(0, 1, 30)},
            index=pd.date_range(start="2018", freq="1D", periods=30),
        )
        with patch(DATAPOINTS_API.format("DPS_LIMIT"), 20), patch(DATAPOINTS_API.format("POST_DPS_OBJECTS_LIMIT"), 20):
            cognite_client.time_series.data.insert_dataframe(df, external_id_headers=False)
        assert 2 == cognite_client.time_series.data._post.call_count  # needs post_spy

    def test_delete_range(self, cognite_client, new_ts):
        cognite_client.time_series.data.delete_range(start="2d-ago", end="now", id=new_ts.id)

    def test_delete_range_before_epoch(self, cognite_client, new_ts):
        cognite_client.time_series.data.delete_range(start=MIN_TIMESTAMP_MS, end=0, id=new_ts.id)

    def test_delete_ranges(self, cognite_client, new_ts):
        cognite_client.time_series.data.delete_ranges([{"start": "2d-ago", "end": "now", "id": new_ts.id}])
