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
from random import randint
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from cognite.client._api.datapoint_constants import ALL_SORTED_DP_AGGS, DPS_LIMIT  # DPS_LIMIT_AGG,
from cognite.client.data_classes import (
    Datapoints,
    DatapointsArray,
    DatapointsArrayList,
    DatapointsList,
    DatapointsQuery,
    TimeSeries,
)
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._time import (
    MAX_TIMESTAMP_MS,
    MIN_TIMESTAMP_MS,
    UNIT_IN_MS,
    align_start_and_end_for_granularity,
)
from tests.utils import (
    random_cognite_external_ids,
    random_cognite_ids,
    random_valid_aggregates,
    random_valid_granularity,
    set_max_workers,
)

DATAPOINTS_API = "cognite.client._api.datapoints.{}"
WEEK_MS = UNIT_IN_MS["w"]
DAY_MS = UNIT_IN_MS["d"]
MS_1975 = 157766400000
MS_1965 = -MS_1975  # yes
DPS_TYPES = [Datapoints, DatapointsArray]
DPS_LST_TYPES = [DatapointsList, DatapointsArrayList]

# To avoid the error "different tests were collected between...", we must make sure all parallel test-runners
# generate the same tests random test data. We also want different random values over time (...thats the point),
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


def convert_any_ts_to_integer(ts):
    if isinstance(ts, int):
        return ts
    elif isinstance(ts, np.datetime64):
        return ts.astype("datetime64[ms]").astype(int)
    raise ValueError


def validate_raw_datapoints_lst(ts_lst, dps_lst, **kw):
    assert isinstance(dps_lst, (DatapointsList, DatapointsArrayList)), "Datapoints(Array)List not given"
    for ts, dps in zip(ts_lst, dps_lst):
        validate_raw_datapoints(ts, dps, **kw)


def validate_raw_datapoints(ts, dps, check_offset=True, check_delta=True):
    assert isinstance(dps, (Datapoints, DatapointsArray)), "Datapoints(Array) not given"
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


def validate_agg_datapoints(ts, dps, check_offset=True, check_delta=True):
    pass


PARAMETRIZED_VALUES_OUTSIDE_POINTS = [
    (-100, 100, False, True),
    (-99, 100, True, True),
    (-99, 101, True, False),
    (-100, 101, False, False),
]


# @pytest.fixture
# def post_spy(cognite_client):
#     with patch.object(cognite_client.time_series.data, "_post", wraps=cognite_client.time_series.data._post):
#         yield


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
        "is_step, start, end, exp_start, exp_end, max_workers, mock_out_eager_or_chunk",
        (
            (True, MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS, MS_1965, MS_1975, 4, "ChunkingDpsFetcher"),
            (False, MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS, MS_1965, MS_1975, 4, "ChunkingDpsFetcher"),
            (True, MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS, MS_1965, MS_1975, 1, "EagerDpsFetcher"),
            (False, MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS, MS_1965, MS_1975, 1, "EagerDpsFetcher"),
            (True, MS_1965, MS_1975, MS_1965, MS_1975 - DAY_MS, 4, "ChunkingDpsFetcher"),
            (False, MS_1965, MS_1975, MS_1965, MS_1975 - DAY_MS, 4, "ChunkingDpsFetcher"),
            (True, MS_1965, MS_1975, MS_1965, MS_1975 - DAY_MS, 1, "EagerDpsFetcher"),
            (False, MS_1965, MS_1975, MS_1965, MS_1975 - DAY_MS, 1, "EagerDpsFetcher"),
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
        # also in empty regions.
        (ts_daily, *_), (ts_daily_is_step, *_) = fixed_freq_dps_ts
        ts, exclude = ts_daily, {"step_interpolation"}
        if is_step:
            exclude.add("interpolation")
            ts = ts_daily_is_step

        assert ts.is_step is is_step

        with set_max_workers(cognite_client, max_workers), patch(DATAPOINTS_API.format(mock_out_eager_or_chunk)):
            for endpoint in retrieve_endpoints:
                # Give each "granularity" a random list of aggregates:
                aggs = [random_valid_aggregates(exclude=exclude) for _ in range(4)]
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
                    # Verify empty with `count`:
                    {"external_id": ts.external_id, "granularity": granularities[0], "aggregates": ["count"]},
                ],
            )
            *interp_res_lst, count_dps = res_lst
            assert sum(count_dps.count) == 0

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
                        granularity=random_valid_granularity(),
                        aggregates=random_valid_aggregates(),
                        id=[ts.id for ts in ts_chunk],
                        ignore_unknown_ids=random.choice((True, False)),
                    )
                assert exc.value.code == 400
                assert exc.value.message == "Aggregates are not supported for string time series"

    def test_sum_of_count_is_independant_of_granularity(self):
        # Sum of count is independent of granularity
        pass

    def test_sum_of_sum_is_independant_of_granularity(self):
        # Sum of sum is independent of granularity
        pass

    def test_equivalent_granularities(self):
        # - 60 sec == 1m
        # - 120 sec == 2m
        # - 60 min == 1h
        # - 120 min == 2h
        # - 24 hour == 1d
        # - 48 hour == 2d
        # - 240 hour == 2d
        # - 96000 hour == 4000d
        pass

    # @pytest.mark.parametrize(
    #     "is_step, start, end, is_empty",
    #     (
    #         (True, None, None, None),
    #         (False, None, None, None),
    #     )
    # )
    # def test_interpolation_and_step_interpolation__is_step_tf(self, is_step, start, end, is_empty, fixed_freq_dps_ts):
    #     print()
    #     for ts in fixed_freq_dps_ts:
    #         print(ts.name)
    # ts_h = "PYSDK integration test 105: hourly values, 1969-10-01 - 1970-03-01, numeric"


# def test_retrieve_aggregates(self):
#     # ALL_SORTED_DP_AGGS = [
#     #     "average",
#     #     "max",
#     #     "min",
#     #     "count",
#     #     "sum",
#     #     "interpolation",
#     #     "step_interpolation",
#     #     "continuous_variance",
#     #     "discrete_variance",
#     #     "total_variation",
#     # ]
#     pass

# def unpacking_failed
#     res = pysdk_client.time_series.data.retrieve(
#         external_id="PYSDK integration test 108: every millisecond, 1969-12-31 23:59:58.500 - 1970-01-01 00:00:01.500, numeric",
#         granularity="1s",
#         # aggregates=["interpolation", "stepInterpolation", "totalVariation"],
#         aggregates=["stepInterpolation"],
#         start=-5000,  # -10000,
#         end=5000,  # pd.Timestamp("1980").value // int(1e6),
#         limit=None,
#     )
#     df = res.to_pandas(include_aggregate_name=True)
#     df.index = df.index.to_numpy("datetime64[ms]").astype(np.int64)
#     df
#
#
#     res = pysdk_client.time_series.data.retrieve(
#         id=5823269796769815,
#         granularity="1h",
#         # aggregates=["interpolation", "stepInterpolation", "totalVariation"],
#         aggregates=["stepInterpolation"],
#         start=-10000,
#         end=105177600000,  #pd.Timestamp("1980").value // int(1e6),
#         limit=None,
#     )
#     df = res.to_pandas(include_aggregate_name=False)
#     df.index = df.index.to_numpy("datetime64[ms]").astype(np.int64)
#     df


# class TestRetrieveDataFrameAPI:
#     def test_retrieve_dataframe(self, cognite_client, test_time_series):
#         ts = test_time_series[0]
#         df = cognite_client.time_series.data.retrieve_dataframe(
#             id=ts.id, start="6h-ago", end="now", aggregates=["average"], granularity="1s"
#         )
#         assert df.shape[0] > 0
#         assert df.shape[1] == 1
#         assert has_expected_timestamp_spacing(df, "1s")
#
#     def test_retrieve_dataframe_one_missing(self, cognite_client, test_time_series):
#         ts = test_time_series[0]
#         df = cognite_client.time_series.data.retrieve_dataframe(
#             id=ts.id,
#             external_id="missing",
#             start="6h-ago",
#             end="now",
#             aggregates=["average"],
#             granularity="1s",
#             ignore_unknown_ids=True,
#         )
#         assert df.shape[0] > 0
#         assert df.shape[1] == 1
#
#
# class TestQueryDatapointsAPI:
#     def test_query(self, cognite_client, test_time_series):
#         dps_query1 = DatapointsQuery(id=test_time_series[0].id, start="6h-ago", end="5h-ago")
#         dps_query2 = DatapointsQuery(id=test_time_series[1].id, start="3h-ago", end="2h-ago")
#         dps_query3 = DatapointsQuery(
#             id=test_time_series[2].id, start="1d-ago", end="now", aggregates=["average"], granularity="1h"
#         )
#
#         res = cognite_client.time_series.data.query([dps_query1, dps_query2, dps_query3])
#         assert len(res) == 3
#         assert len(res[2]) < len(res[1]) < len(res[0])
#
#     def test_query_two_unknown(self, cognite_client, test_time_series):
#         dps_query1 = DatapointsQuery(id=test_time_series[0].id, start="6h-ago", end="5h-ago", ignore_unknown_ids=True)
#         dps_query2 = DatapointsQuery(id=123, start="3h-ago", end="2h-ago", ignore_unknown_ids=True)
#         dps_query3 = DatapointsQuery(
#             external_id="missing time series",
#             start="1d-ago",
#             end="now",
#             aggregates=["average"],
#             granularity="1h",
#             ignore_unknown_ids=True,
#         )
#         res = cognite_client.time_series.data.query([dps_query1, dps_query2, dps_query3])
#         assert len(res) == 1
#         assert len(res[0]) > 0


# class TestRetrieveLatestDatapointsAPI:
#
#     def test_retrieve_latest(self, cognite_client, test_time_series):
#         ids = [test_time_series[0].id, test_time_series[1].id]
#         res = cognite_client.time_series.data.retrieve_latest(id=ids)
#         for dps in res:
#             assert 1 == len(dps)
#
#     def test_retrieve_latest_two_unknown(self, cognite_client, test_time_series):
#         ids = [test_time_series[0].id, test_time_series[1].id, 42, 1337]
#         res = cognite_client.time_series.data.retrieve_latest(id=ids, ignore_unknown_ids=True)
#         assert 2 == len(res)
#         for dps in res:
#             assert 1 == len(dps)
#
#     def test_retrieve_latest_many(self, cognite_client, test_time_series, post_spy):
#         ids = [t.id for t in cognite_client.time_series.list(limit=12) if not t.security_categories]
#         assert len(ids) > 10  # more than one page
#
#         with patch(DATAPOINTS_API.format("RETRIEVE_LATEST_LIMIT"), 10):
#             res = cognite_client.time_series.data.retrieve_latest(id=ids, ignore_unknown_ids=True)
#
#         assert {dps.id for dps in res}.issubset(set(ids))
#         assert 2 == cognite_client.time_series.data._post.call_count
#         for dps in res:
#             assert len(dps) <= 1  # could be empty
#
#     def test_retrieve_latest_before(self, cognite_client, test_time_series):
#         ts = test_time_series[0]
#         res = cognite_client.time_series.data.retrieve_latest(id=ts.id, before="1h-ago")
#         assert 1 == len(res)
#         assert res[0].timestamp < timestamp_to_ms("1h-ago")
#
#
# class TestInsertDatapointsAPI:
#     def test_insert(self, cognite_client, new_ts, post_spy):
#         datapoints = [(datetime(year=2018, month=1, day=1, hour=1, minute=i), i) for i in range(60)]
#         with patch(DATAPOINTS_API.format("DPS_LIMIT"), 30), patch(DATAPOINTS_API.format("POST_DPS_OBJECTS_LIMIT"), 30):
#             cognite_client.time_series.data.insert(datapoints, id=new_ts.id)
#         assert 2 == cognite_client.time_series.data._post.call_count
#
#     def test_insert_before_epoch(self, cognite_client, new_ts, post_spy):
#         datapoints = [(datetime(year=1950, month=1, day=1, hour=1, minute=i), i) for i in range(60)]
#         with patch(DATAPOINTS_API.format("DPS_LIMIT"), 30), patch(DATAPOINTS_API.format("POST_DPS_OBJECTS_LIMIT"), 30):
#             cognite_client.time_series.data.insert(datapoints, id=new_ts.id)
#         assert 2 == cognite_client.time_series.data._post.call_count
#
#     def test_insert_copy(self, cognite_client, test_time_series, new_ts, post_spy):
#         data = cognite_client.time_series.data.retrieve(
#             id=test_time_series[0].id, start="600d-ago", end="now", limit=100
#         )
#         assert 100 == len(data)
#         cognite_client.time_series.data.insert(data, id=new_ts.id)
#         assert 2 == cognite_client.time_series.data._post.call_count
#
#     def test_insert_copy_fails_at_aggregate(self, cognite_client, test_time_series, new_ts, post_spy):
#         data = cognite_client.time_series.data.retrieve(
#             id=test_time_series[0].id, start="600d-ago", end="now", granularity="1m", aggregates=["average"], limit=100
#         )
#         assert 100 == len(data)
#         with pytest.raises(ValueError, match="only raw datapoints are supported"):
#             cognite_client.time_series.data.insert(data, id=new_ts.id)
#
#     def test_insert_pandas_dataframe(self, cognite_client, new_ts, post_spy):
#         df = pd.DataFrame(
#             {new_ts.id: np.random.normal(0, 1, 30)},
#             index=pd.date_range(start="2018", freq="1D", periods=30),
#         )
#         with patch(DATAPOINTS_API.format("DPS_LIMIT"), 20), patch(DATAPOINTS_API.format("POST_DPS_OBJECTS_LIMIT"), 20):
#             cognite_client.time_series.data.insert_dataframe(df, external_id_headers=False)
#         assert 2 == cognite_client.time_series.data._post.call_count
#
#     def test_delete_range(self, cognite_client, new_ts):
#         cognite_client.time_series.data.delete_range(start="2d-ago", end="now", id=new_ts.id)
#
#     def test_delete_range_before_epoch(self, cognite_client, new_ts):
#         cognite_client.time_series.data.delete_range(start=MIN_TIMESTAMP_MS, end="now", id=new_ts.id)
#
#     def test_delete_ranges(self, cognite_client, new_ts):
#         cognite_client.time_series.data.delete_ranges([{"start": "2d-ago", "end": "now", "id": new_ts.id}])
