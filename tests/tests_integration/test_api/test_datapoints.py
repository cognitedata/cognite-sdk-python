"""
Note: If tests related to fetching datapoints are broken, all time series + their datapoints can be
      recreated easily by running the file linked below. You will need to provide a valid set of
      credentials to the `CogniteClient` for the Python SDK integration test CDF project:
python scripts/create_ts_for_integration_tests.py
"""

from __future__ import annotations

import itertools
import math
import random
import re
import unittest
from collections import UserList
from collections.abc import Callable, Iterator
from contextlib import nullcontext as does_not_raise
from datetime import datetime, timezone
from typing import Literal
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_allclose, assert_equal

from cognite.client import CogniteClient
from cognite.client.data_classes import (
    Datapoint,
    Datapoints,
    DatapointsArray,
    DatapointsArrayList,
    DatapointsList,
    DatapointsQuery,
    LatestDatapointQuery,
    StatusCode,
    TimeSeries,
    TimeSeriesList,
)
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, Space
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteTimeSeries
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.data_modeling.instances import NodeApplyResult
from cognite.client.data_classes.data_modeling.spaces import SpaceApply
from cognite.client.data_classes.datapoints import ALL_SORTED_DP_AGGS
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._text import to_camel_case, to_snake_case
from cognite.client.utils._time import (
    MAX_TIMESTAMP_MS,
    MIN_TIMESTAMP_MS,
    UNIT_IN_MS,
    ZoneInfo,
    align_start_and_end_for_granularity,
    granularity_to_ms,
    timestamp_to_ms,
)
from tests.utils import (
    cdf_aggregate,
    random_aggregates,
    random_cognite_external_ids,
    random_cognite_ids,
    random_gamma_dist_integer,
    random_granularity,
    rng_context,
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
TEST_PREFIX = "PYSDK integration test"


@pytest.fixture(scope="session")
def all_test_time_series(cognite_client) -> TimeSeriesList:
    return cognite_client.time_series.retrieve_multiple(
        external_ids=[
            f"{TEST_PREFIX} 001: outside points, numeric",
            f"{TEST_PREFIX} 002: outside points, string",
            *[f"{TEST_PREFIX} {i:03d}: weekly values, 1950-2000, numeric" for i in range(3, 54)],
            *[f"{TEST_PREFIX} {i:03d}: weekly values, 1950-2000, string" for i in range(54, 104)],
            f"{TEST_PREFIX} 104: daily values, 1965-1975, numeric",
            f"{TEST_PREFIX} 105: hourly values, 1969-10-01 - 1970-03-01, numeric",
            f"{TEST_PREFIX} 106: every minute, 1969-12-31 - 1970-01-02, numeric",
            f"{TEST_PREFIX} 107: every second, 1969-12-31 23:30:00 - 1970-01-01 00:30:00, numeric",
            f"{TEST_PREFIX} 108: every millisecond, 1969-12-31 23:59:58.500 - 1970-01-01 00:00:01.500, numeric",
            f"{TEST_PREFIX} 109: daily values, is_step=True, 1965-1975, numeric",
            f"{TEST_PREFIX} 110: hourly values, is_step=True, 1969-10-01 - 1970-03-01, numeric",
            f"{TEST_PREFIX} 111: every minute, is_step=True, 1969-12-31 - 1970-01-02, numeric",
            f"{TEST_PREFIX} 112: every second, is_step=True, 1969-12-31 23:30:00 - 1970-01-01 00:30:00, numeric",
            f"{TEST_PREFIX} 113: every millisecond, is_step=True, 1969-12-31 23:59:58.500 - 1970-01-01 00:00:01.500, numeric",
            f"{TEST_PREFIX} 114: 1mill dps, random distribution, 1950-2020, numeric",
            f"{TEST_PREFIX} 115: 1mill dps, random distribution, 1950-2020, string",
            f"{TEST_PREFIX} 116: 5mill dps, 2k dps (.1s res) burst per day, 2000-01-01 12:00:00 - 2013-09-08 12:03:19.900, numeric",
            f"{TEST_PREFIX} 117: single dp at 1900-01-01 00:00:00, numeric",
            f"{TEST_PREFIX} 118: single dp at 2099-12-31 23:59:59.999, numeric",
            f"{TEST_PREFIX} 119: hourly normally distributed (0,1) data, 2020-2024 numeric",
            f"{TEST_PREFIX} 120: minute normally distributed (0,1) data, 2023-01-01 00:00:00 - 2023-12-31 23:59:59, numeric",
            f"{TEST_PREFIX} 121: mixed status codes, daily values, 2023-2024, numeric",
            f"{TEST_PREFIX} 122: mixed status codes, daily values, 2023-2024, string",
            f"{TEST_PREFIX} 123: only bad status codes, daily values, 2023-2024, numeric",
            f"{TEST_PREFIX} 124: only bad status codes, daily values, 2023-2024, string",
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


@pytest.fixture
def hourly_normal_dist(all_test_time_series) -> TimeSeries:
    return all_test_time_series[118]


@pytest.fixture
def minutely_normal_dist(all_test_time_series) -> TimeSeries:
    return all_test_time_series[119]


@pytest.fixture
def ts_status_codes(all_test_time_series) -> TimeSeriesList:
    return all_test_time_series[120:124]


@pytest.fixture(scope="session")
def new_ts(cognite_client):
    ts = cognite_client.time_series.create(TimeSeries(is_string=False))
    yield ts
    cognite_client.time_series.delete(id=ts.id)
    assert cognite_client.time_series.retrieve(ts.id) is None


@pytest.fixture(scope="session")
def new_ts_string(cognite_client):
    ts = cognite_client.time_series.create(TimeSeries(is_string=True))
    yield ts
    cognite_client.time_series.delete(id=ts.id)
    assert cognite_client.time_series.retrieve(ts.id) is None


@pytest.fixture
def retrieve_endpoints(cognite_client):
    return [
        cognite_client.time_series.data.retrieve,
        cognite_client.time_series.data.retrieve_arrays,
    ]


@pytest.fixture
def all_retrieve_endpoints(cognite_client, retrieve_endpoints):
    # retrieve_dataframe is just a wrapper around retrieve_arrays
    return [*retrieve_endpoints, cognite_client.time_series.data.retrieve_dataframe]


@pytest.fixture(scope="session")
def instance_ts_id(cognite_client: CogniteClient, instance_id_test_space: str, os_and_py_version: str) -> NodeId:
    my_ts = NodeApply(
        space=instance_id_test_space,
        external_id=f"ts_pysdk_instance_id_tests-{os_and_py_version}",
        sources=[
            NodeOrEdgeData(
                source=CogniteTimeSeries.get_source(),
                properties={
                    "name": "ts_python_sdk_instance_id_tests",
                    "isStep": False,
                    "type": "numeric",
                },
            )
        ],
    )
    created_ts = cognite_client.data_modeling.instances.apply(my_ts).nodes[0]
    return created_ts.as_id()


def ts_to_ms(ts, tz=None):
    assert isinstance(ts, str)
    return pd.Timestamp(ts, tz=tz).value // int(1e6)


def convert_any_ts_to_integer(ts):
    if isinstance(ts, int):
        return ts
    elif isinstance(ts, np.datetime64):
        return ts.astype("datetime64[ms]").astype(np.int64).item()
    raise ValueError


def validate_raw_datapoints_lst(ts_lst, dps_lst, **kw):
    assert isinstance(dps_lst, DPS_LST_TYPES), "Datapoints(Array)List not given"
    for ts, dps in zip(ts_lst, dps_lst):
        validate_raw_datapoints(ts, dps, **kw)


def validate_raw_datapoints(ts, dps, check_offset=True, check_delta=True):
    assert isinstance(dps, DPS_TYPES), "Datapoints(Array) not given"
    assert ts.id == dps.id
    assert ts.external_id == dps.external_id
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


@pytest.fixture(scope="module", autouse=True)
def make_dps_tests_reproducible(testrun_uid, request):
    # To avoid the `xdist` error "different tests were collected between...", we must make sure all parallel test-runners
    # generate the same tests (randomized test data) so we must set a fixed seed... but we also want different random
    # test data over time (...thats the whole point), so we set seed based on a unique run ID created by pytest-xdist:
    with rng_context(testrun_uid):  # Internal state of `random` will be reset after exiting contextmanager
        yield

    # To make this show up in the logs, it must be run here as part of teardown (post-yield):
    if request.session.testsfailed:
        print(  # noqa: T201
            f"Random seed used in datapoints integration tests: {testrun_uid}. If any datapoints "
            "test failed - and you weren't the cause, please create a new (GitHub) issue: "
            "https://github.com/cognitedata/cognite-sdk-python/issues"
        )


# We also have some test data that depend on random input:
@pytest.fixture
def parametrized_values_all_unknown_single_multiple_given(testrun_uid):
    with rng_context(testrun_uid + "42"):
        return (
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
        )


@pytest.fixture
def parametrized_values_uniform_index_fails(testrun_uid):
    with rng_context(testrun_uid + "1337"):
        return (
            # Fail because of raw request:
            ([None, "1s"], [None, random_aggregates(1)], [None, None]),
            # Fail because of multiple granularities:
            (["1m", "60s"], [random_aggregates(1), random_aggregates(1)], [None, None]),
            # Fail because of finite limit:
            (["1d", "1d"], [random_aggregates(1), random_aggregates(1)], [123, None]),
        )


@pytest.fixture(scope="module")
def timeseries_degree_c_minus40_0_100(cognite_client: CogniteClient) -> TimeSeries:
    ts = TimeSeries(
        external_id="test_retrieve_datapoints_in_target_unit",
        name="test_retrieve_datapoints_in_target_unit",
        is_string=False,
        unit_external_id="temperature:deg_c",
    )
    created_timeseries = cognite_client.time_series.upsert(ts, mode="patch")
    cognite_client.time_series.data.insert([(0, -40.0), (1, 0.0), (2, 100.0)], external_id=ts.external_id)
    return created_timeseries


@pytest.fixture
def dps_queries_dst_transitions(all_test_time_series):
    ts1, ts2 = all_test_time_series[113], all_test_time_series[119]
    oslo = "Europe/Oslo"
    return [
        # DST from winter to summer:
        DatapointsQuery(
            id=ts1.id,
            start=ts_to_ms("1991-03-31 00:20:05.912", tz=oslo),
            end=ts_to_ms("1991-03-31 03:28:51.903", tz=oslo),
            timezone=ZoneInfo(oslo),
        ),
        # DST from summer to winter:
        DatapointsQuery(
            id=ts1.id,
            start=ts_to_ms("1991-09-29 01:02:37.950", tz=oslo),
            end=ts_to_ms("1991-09-29 04:12:02.558", tz=oslo),
            timezone=ZoneInfo(oslo),
        ),
        DatapointsQuery(
            id=ts2.id,
            start=ts_to_ms("2023-03-26", tz=oslo),
            end=ts_to_ms("2023-03-26 05:00:00", tz=oslo),
            timezone=ZoneInfo(oslo),
        ),
        DatapointsQuery(
            id=ts2.id,
            start=ts_to_ms("2023-10-29 01:00:00", tz=oslo),
            end=ts_to_ms("2023-10-29 03:00:00.001", tz=oslo),
            timezone=ZoneInfo(oslo),
        ),
    ]


@pytest.fixture(scope="session")
def space_for_time_series(cognite_client) -> Iterator[Space]:
    name = "PySDK-DMS-time-series-integration-test"
    space = SpaceApply(name, name=name.replace("-", " "))
    yield cognite_client.data_modeling.spaces.apply(space)


@pytest.fixture(scope="session")
def ts_create_in_dms(cognite_client, space_for_time_series, os_and_py_version) -> NodeApplyResult:
    from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteTimeSeriesApply

    dms_ts = CogniteTimeSeriesApply(
        space=space_for_time_series.space,
        # One per test runner, per OS, to avoid conflicts:
        external_id=f"dms-time-series-{os_and_py_version}",
        is_step=True,
        time_series_type="numeric",
    )
    (dms_ts_node,) = cognite_client.data_modeling.instances.apply(dms_ts).nodes
    return dms_ts_node


class TestTimeSeriesCreatedInDMS:
    def test_insert_read_delete_dps(self, cognite_client, ts_create_in_dms):
        # Ensure the DMS time series is retrievable from normal TS API:
        inst_id = ts_create_in_dms.as_id()
        ts = cognite_client.time_series.retrieve(instance_id=inst_id)
        assert ts.instance_id == inst_id

        numbers = random_cognite_ids(3)
        datapoints = [
            (datetime(2018, 1, 1, tzinfo=timezone.utc), numbers[0]),
            (datetime(2018, 1, 2, tzinfo=timezone.utc), numbers[1], StatusCode.Good),
            (datetime(2018, 1, 3, tzinfo=timezone.utc), numbers[2], StatusCode.Uncertain),
        ]
        cognite_client.time_series.data.insert(datapoints, instance_id=inst_id)

        dps1 = cognite_client.time_series.data.retrieve(instance_id=inst_id, ignore_bad_datapoints=False)
        assert dps1.instance_id == inst_id
        dps2 = cognite_client.time_series.data.retrieve(id=ts.id, ignore_bad_datapoints=False)

        assert dps1.value == dps2.value == numbers


@pytest.fixture
def queries_for_iteration():
    # Mix of ids, external_ids, and instance_ids
    return [
        DatapointsQuery(
            external_id="PYSDK integration test 039: weekly values, 1950-2000, numeric",
            start=-39052800000,
            end=565142400000 + 1,
        ),
        DatapointsQuery(
            instance_id=NodeId(
                space="PySDK-DMS-time-series-integration-test", external_id="PYSDK integration test 125: clone of 109"
            ),
            start=132710400000,
        ),
        DatapointsQuery(
            id=1162585250935723,  # 'PYSDK integration test 089: weekly values, 1950-2000, string,
            start=-334195200000,
            end=270000000000 + 1,
        ),
        DatapointsQuery(
            external_id="PYSDK integration test 105: hourly values, 1969-10-01 - 1970-03-01, numeric",
            start=1166400000,
        ),
        DatapointsQuery(
            id=7134564432527017,  # 'PYSDK integration test 108: every millisecond, 1969-12-31 23:59:58.500 - 1970-01-01 00:00:01.500, numeric,
            start=-1118,
        ),
        DatapointsQuery(
            id=8856777097037888,  # 'PYSDK integration test 114: 1mill dps, random distribution, 1950-2020, numeric,
            start=-111360848336,
        ),
        DatapointsQuery(
            instance_id=NodeId(
                space="PySDK-DMS-time-series-integration-test", external_id="PYSDK integration test 126: clone of 114"
            ),
            start=89356795502,
            end=91618492299 + 1,
        ),
        DatapointsQuery(
            external_id="PYSDK integration test 116: 5mill dps, 2k dps (.1s res) burst per day, 2000-01-01 12:00:00 - 2013-09-08 12:03:19.900, numeric",
            start=947678498800,
        ),
        DatapointsQuery(
            instance_id=NodeId(
                space="PySDK-DMS-time-series-integration-test", external_id="PYSDK integration test 127: clone of 121"
            ),
            start=1702252800000,
        ),
        DatapointsQuery(
            id=6236123831652881,  # 'PYSDK integration test 121: mixed status codes, daily values, 2023-2024, numeric,
            start=1678924800000,
        ),
        DatapointsQuery(
            external_id="PYSDK integration test 124: only bad status codes, daily values, 2023-2024, string",
            start=1699747200000,
        ),
    ]


class TestIterateDatapoints:
    # This is for __call__, not subscriptions.
    @pytest.mark.parametrize(
        "chunk_size_datapoints, raises_ctx",
        (
            (10, does_not_raise()),
            (3125, does_not_raise()),
            (25_000, does_not_raise()),
            (40_000, pytest.raises(ValueError, match="evenly divides 100k OR an integer multiple")),
            (300_000, does_not_raise()),
        ),
    )
    def test_chunk_size_datapoints(self, cognite_client, chunk_size_datapoints, raises_ctx, queries_for_iteration):
        with raises_ctx:
            for dps_lst in cognite_client.time_series.data(
                queries_for_iteration[7:],
                chunk_size_datapoints=chunk_size_datapoints,
                chunk_size_time_series=1,
            ):
                assert len(dps_lst) == 1
                assert len(dps_lst[0]) == chunk_size_datapoints
                break

    @pytest.mark.parametrize(
        "chunk_size_time_series, n_exp_returned, raises_ctx",
        (
            (math.inf, 0, pytest.raises(ValueError, match="must be a positive integer or None")),
            (-1, 0, pytest.raises(ValueError, match="must be a positive integer or None")),
            (None, 5, does_not_raise()),
            (0, 0, pytest.raises(ValueError, match="must be a positive integer or None")),
            (1, 1, does_not_raise()),
            (4, 4, does_not_raise()),
        ),
    )
    def test_chunk_sizes_time_series(
        self, cognite_client, chunk_size_time_series, n_exp_returned, raises_ctx, queries_for_iteration
    ):
        with raises_ctx:
            for dps_lst in cognite_client.time_series.data(
                queries_for_iteration[:5],
                chunk_size_time_series=chunk_size_time_series,
            ):
                assert len(dps_lst) == n_exp_returned
                assert list(map(len, dps_lst)) == [1000, 291, 1000, 1093, 2619][:n_exp_returned]
                break

    def test_invalid_options(self, cognite_client, queries_for_iteration):
        _, q2, q3, q4, *_ = queries_for_iteration
        q2.limit = 100_000
        q3.limit = random.choice([None, -1, math.inf])
        q4.include_outside_points = True
        for query in q2, q3, q4:
            with pytest.raises(ValueError, match="options 'include_outside_points' and 'limit' are not supported"):
                next(cognite_client.time_series.data(query))

    def test_duplicates_not_allowed(self, cognite_client, queries_for_iteration):
        qs_with_duplicates = random.choices(queries_for_iteration * 2, k=len(queries_for_iteration) + 3)
        with pytest.raises(ValueError, match="identifiers must be unique! Duplicates found"):
            next(cognite_client.time_series.data(qs_with_duplicates))

    def test_hidden_duplicates_also_not_allowed(self, cognite_client, ts_create_in_dms):
        # We won't know that these are duplicates before runtime:
        instance_id = ts_create_in_dms.as_id()
        id_ = cognite_client.time_series.retrieve(instance_id=instance_id).id
        qs_with_hidden_duplicates = [DatapointsQuery(id=id_), DatapointsQuery(instance_id=instance_id)]

        with pytest.raises(RuntimeError, match="must be unique! You can not get around this"):
            next(cognite_client.time_series.data(qs_with_hidden_duplicates))

    @pytest.mark.parametrize(
        "is_single, use_numpy, exp_type",
        (
            (True, False, Datapoints),
            (False, False, DatapointsList),
            (True, True, DatapointsArray),
            (False, True, DatapointsArrayList),
        ),
    )
    def test_iterate_single_ts(self, cognite_client, is_single, use_numpy, exp_type, all_test_time_series):
        xid1 = all_test_time_series[0].external_id
        query = DatapointsQuery(external_id=xid1)
        if not is_single:
            xid2 = all_test_time_series[1].external_id
            query = [query, DatapointsQuery(external_id=xid2)]

        res = next(cognite_client.time_series.data(query, return_arrays=use_numpy, chunk_size_datapoints=5))
        assert isinstance(res, exp_type)
        assert 5 == len(res[0] if isinstance(res, UserList) else res)

    def test_no_data_is_noop(self, cognite_client, all_test_time_series):
        xid = all_test_time_series[0].external_id
        query = DatapointsQuery(external_id=xid, start=ts_to_ms("1970-01-01 00:00:00.100") + 1)
        for _ in cognite_client.time_series.data(query):
            assert False, "No iteration should happen"

    def test_no_data_due_to_missing_is_noop(self, cognite_client):
        xid = random_cognite_external_ids(1, str_len=80)[0]
        query = DatapointsQuery(external_id=xid, ignore_unknown_ids=True)
        for _ in cognite_client.time_series.data(query, ignore_unknown_ids=False):  # query should take precedence
            assert False, "No iteration should happen"

    def test_iterate_exhausted_but_queue_not_empty(self, cognite_client, weekly_dps_ts, all_test_time_series):
        # These two have exactly 100k dps left, and we chunk time series 1-by-1, so the first will be unknowningly
        # exhausted after the first iteration, then second iteration is not yielded because empty, but since the
        # queue is not empty, the third iteration yields the second and on the fourth iteration, we quit (queue empty):
        xid = all_test_time_series[115].external_id
        instance_id = NodeId(
            space="PySDK-DMS-time-series-integration-test",
            external_id="PYSDK integration test 126: clone of 114",
        )
        q1 = DatapointsQuery(external_id=xid, start=ts_to_ms("2013-07-21 12:00"))
        q2 = DatapointsQuery(instance_id=instance_id, start=1356395118928)
        dps_iterator = cognite_client.time_series.data(
            [q1, q2], chunk_size_datapoints=100_000, chunk_size_time_series=1
        )
        (dps,) = dps_lst = next(dps_iterator)
        assert len(dps_lst) == 1
        assert len(dps) == 100_000
        assert dps.external_id == xid

        (dps,) = dps_lst = next(dps_iterator)
        assert len(dps_lst) == 1
        assert len(dps) == 100_000
        assert dps.instance_id == instance_id

        assert next(dps_iterator, None) is None

    def test_iterate_datapoints(self, cognite_client, queries_for_iteration, all_test_time_series):
        # One external id to follow through all iterations and use for asserts:
        ts_xid = all_test_time_series[107].external_id
        dps_iterator = cognite_client.time_series.data(
            queries_for_iteration,
            ignore_bad_datapoints=False,
            start=MIN_TIMESTAMP_MS,
            end=MAX_TIMESTAMP_MS,
            chunk_size_datapoints=1_000,
            return_arrays=False,
        )
        dps_lst = next(dps_iterator)
        # First iteration, every time series is returned (all have data)
        exp_lengths = (1000, 291, 1000, 1000, 1000, 1000, 1000, 1000, 21, 291, 50)
        for query, dps, exp_len in zip(queries_for_iteration, dps_lst, exp_lengths):
            # Check that the order is preserved:
            assert query.identifier.as_primitive() == getattr(dps, query.identifier.name())
            assert len(dps) == exp_len
        dps = dps_lst.get(external_id=ts_xid)
        assert (dps.timestamp[0], dps.timestamp[-1]) == (-1118, -119)

        dps_lst = next(dps_iterator)
        assert len(dps_lst) == 4  # some few time series are exhausted
        assert list(map(len, dps_lst)) == [93, 1000, 1000, 1000]
        assert dps_lst[0].external_id == all_test_time_series[104].external_id
        dps = dps_lst.get(external_id=ts_xid)
        assert (dps.timestamp[0], dps.timestamp[-1]) == (-118, 881)

        dps_lst = next(dps_iterator)
        assert len(dps_lst) == 3  # another bites the dust
        assert list(map(len, dps_lst)) == [619, 1000, 1000]
        assert dps_lst[0].external_id == ts_xid

        dps = dps_lst.get(external_id=ts_xid)
        assert (dps.timestamp[0], dps.timestamp[-1]) == (882, 1500)


class TestRetrieveRawDatapointsAPI:
    """Note: Since `retrieve` and `retrieve_arrays` endpoints should give identical results,
    except for the data container types, all tests run both endpoints except those targeting a specific bug
    """

    def test_retrieve_chunking_mode_with_limit_ignores_dps_count_in_first_batch(
        self, cognite_client, all_test_time_series
    ):
        # From 6.33.2 to 7.41.0, when fetching in "chunking mode" with a finite limit and with more than
        # 100 time series per available worker - in some rare cases - the initial batch of datapoints would
        # not be counted towards the total limit requested.
        ids = [all_test_time_series[105].id] * 100
        with set_max_workers(cognite_client, 1), patch(DATAPOINTS_API.format("EagerDpsFetcher")):
            dps_lst = cognite_client.time_series.data.retrieve(id=ids, limit=1001)
        assert all(len(dps) == 1001 for dps in dps_lst)

    def test_retrieve_chunking_mode_outside_points_stopped_after_no_cursor(self, cognite_client, weekly_dps_ts):
        # From 7.45.0 to 7.48.0, when fetching in "chunking mode" with include_outside_points=True,
        # due to an added is-nextCursor-empty check, the queries would short-circuit after the first batch.
        ts_ids, ts_xids = weekly_dps_ts
        with set_max_workers(cognite_client, 1):
            dps_lst = cognite_client.time_series.data.retrieve(
                id=ts_ids.as_ids(),
                external_id=ts_xids.as_external_ids(),
                start=ts_to_ms("1951"),
                end=ts_to_ms("1999"),
                include_outside_points=True,
            )
            df = dps_lst.to_pandas()

            validate_raw_datapoints_lst(ts_ids + ts_xids, dps_lst)
            assert df.shape == (2506, 101)
            assert df.notna().any(axis=None)

    def test_retrieve_ignore_unknown_ids_true_passed_as_query_param_not_top_level(
        self, cognite_client, retrieve_endpoints
    ):
        # From v5 to 7.64.8, when requesting datapoints from a single time series that do not exist,
        # and passing ignore_unknown_ids=True as part of the datapoints-query (instead of the top-level),
        # an IndexError would be raised (no such issue for retrieve_latest which only allows top-level):

        for endpoint in retrieve_endpoints:
            query = DatapointsQuery(id=123, ignore_unknown_ids=True)
            res = cognite_client.time_series.data.retrieve(id=query, ignore_unknown_ids=False)
            assert res is None

    def test_retrieve_eager_mode_raises_single_error_with_all_missing_ts(self, cognite_client, outside_points_ts):
        # From v5 to 6.33.1, when fetching in "eager mode", only the first encountered missing
        # non-ignorable ts would be raised in a CogniteNotFoundError.
        ts_exists1, ts_exists2 = outside_points_ts
        missing_xid = "nope-doesnt-exist " * 3

        with set_max_workers(cognite_client, 6), patch(DATAPOINTS_API.format("ChunkingDpsFetcher")):
            with pytest.raises(CogniteNotFoundError, match=r"^Not found: \[{'") as err:
                cognite_client.time_series.data.retrieve(
                    id=[
                        ts_exists1.id,
                        # Only id=456 should be raised from 'id':
                        {"id": 456, "ignore_unknown_ids": False},
                        123,
                    ],
                    external_id=[
                        # Only xid on next line should be raised from 'xid':
                        {"external_id": f"{missing_xid}1", "ignore_unknown_ids": False},
                        ts_exists2.external_id,
                        f"{missing_xid}2",
                    ],
                    ignore_unknown_ids=True,
                )
            assert len(err.value.not_found) == 2
            unittest.TestCase().assertCountEqual(  # Asserts equal, but ignores ordering
                err.value.not_found,
                [{"id": 456}, {"external_id": f"{missing_xid}1"}],
            )

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
        self, retrieve_endpoints, outside_points_ts, start, end, has_before, has_after, cognite_client, monkeypatch
    ):
        limit = 3
        for dps_limit in range(limit - 1, limit + 2):
            monkeypatch.setattr(cognite_client.time_series.data, "_DPS_LIMIT_RAW", dps_limit)
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
        [
            (631670400000 + 1, 693964800000, 631670400000, 693964800000),
            (631670400000, 693964800000, 631670400000 - WEEK_MS, 693964800000),
            (631670400000, 693964800000 + 1, 631670400000 - WEEK_MS, 693964800000 + WEEK_MS),
            (631670400000 + 1, 693964800000 + 1, 631670400000, 693964800000 + WEEK_MS),
        ],
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
            (1, False, "ChunkingDpsFetcher", pytest.raises(CogniteNotFoundError, match=r"^Not found: \[{'")),
            (3, True, "ChunkingDpsFetcher", does_not_raise()),
            (3, False, "ChunkingDpsFetcher", pytest.raises(CogniteNotFoundError, match=r"^Not found: \[{'")),
            (10, True, "EagerDpsFetcher", does_not_raise()),
            (10, False, "EagerDpsFetcher", pytest.raises(CogniteNotFoundError, match=r"^Not found: \[{'")),
            (50, True, "EagerDpsFetcher", does_not_raise()),
            (50, False, "EagerDpsFetcher", pytest.raises(CogniteNotFoundError, match=r"^Not found: \[{'")),
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
                "id": [ts_exists.id, *random_cognite_ids(n_ts)],
                "external_id": [ts_exists.external_id, *random_cognite_external_ids(n_ts)],
            }
            drop_id = random.choice(["id", "external_id", "keep"])
            if drop_id != "keep":
                identifier.pop(drop_id)
            for endpoint in retrieve_endpoints:
                with expected_raise:
                    res_lst = endpoint(
                        **identifier,
                        start=random.randint(MIN_TIMESTAMP_MS, 100),
                        end=random.randint(101, MAX_TIMESTAMP_MS),
                        ignore_unknown_ids=ignore_unknown_ids,
                        limit=5,
                    )
                    exp_len = 2 if drop_id == "keep" else 1
                    assert exp_len == len(res_lst)
                    validate_raw_datapoints_lst([ts_exists] * exp_len, res_lst)

    @pytest.mark.parametrize("test_id", range(24))  # not populated here because test data depend on deterministic rng
    def test_retrieve__all_unknown_single_multiple_given(
        self, test_id, cognite_client, retrieve_endpoints, parametrized_values_all_unknown_single_multiple_given
    ):
        test_data = parametrized_values_all_unknown_single_multiple_given[test_id]
        max_workers, mock_out_eager_or_chunk, ids, external_ids, exp_res_types = test_data
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

    @pytest.mark.parametrize(
        "retrieve_method_name, kwargs",
        itertools.product(
            ["retrieve", "retrieve_arrays", "retrieve_dataframe"],
            [dict(target_unit="temperature:deg_f"), dict(target_unit_system="Imperial")],
        ),
    )
    def test_retrieve_methods_in_target_unit(
        self,
        retrieve_method_name: str,
        kwargs: dict,
        cognite_client: CogniteClient,
        timeseries_degree_c_minus40_0_100: TimeSeries,
    ) -> None:
        ts = timeseries_degree_c_minus40_0_100
        retrieve_method = getattr(cognite_client.time_series.data, retrieve_method_name)

        res = retrieve_method(external_id=ts.external_id, end=3, **kwargs)

        if isinstance(res, pd.DataFrame):
            res = DatapointsArray(value=res.values)

        assert math.isclose(res.value[0], -40)
        assert math.isclose(res.value[1], 32)
        assert math.isclose(res.value[2], 212)

    @pytest.mark.parametrize("retrieve_method_name", ("retrieve", "retrieve_arrays"))
    def test_unit_external_id__is_overridden_if_converted(
        self, cognite_client: CogniteClient, timeseries_degree_c_minus40_0_100: TimeSeries, retrieve_method_name: str
    ) -> None:
        ts = timeseries_degree_c_minus40_0_100
        assert ts.unit_external_id == "temperature:deg_c"

        retrieve_method = getattr(cognite_client.time_series.data, retrieve_method_name)
        res = retrieve_method(
            id=[
                {"id": ts.id},
                {"id": ts.id, "target_unit": "temperature:deg_f"},
                {"id": ts.id, "target_unit": "temperature:k"},
            ],
            end=3,
        )
        # Ensure unit_external_id is unchanged (Celsius):
        assert res[0].unit_external_id == ts.unit_external_id
        # ...and ensure it has changed for converted units (Fahrenheit or Kelvin):
        assert res[1].unit_external_id == "temperature:deg_f"
        assert res[2].unit_external_id == "temperature:k"

    def test_numpy_dtypes_conversions_for_string_and_numeric(self, cognite_client, all_test_time_series):
        # Bug prior to 7.32.4, several methods on DatapointsArray would fail due to a bad
        # conversion of numpy dtypes to native.
        str_ts = all_test_time_series[1]
        # We only test retrieve_array since that uses numpy arrays
        dps_arr = cognite_client.time_series.data.retrieve_arrays(id=str_ts.id, limit=3)
        # Test __iter__
        for dp in dps_arr:
            assert type(dp.timestamp) is int
            assert type(dp.value) is str
        # Test __getitem__ of non-slices
        dp = dps_arr[0]
        assert type(dp.timestamp) is int
        assert type(dp.value) is str
        # Test dump()
        dumped = dps_arr.dump(camel_case=False)
        dp_dumped = dumped["datapoints"][0]
        assert dumped["is_string"] is True
        assert dp_dumped == {"timestamp": 0, "value": "2"}
        assert type(dp_dumped["timestamp"]) is int
        assert type(dp_dumped["value"]) is str

    def test_getitem_and_iter_preserves_status_codes(self, cognite_client, ts_status_codes, retrieve_endpoints):
        mixed_ts, *_ = ts_status_codes
        for endpoint in retrieve_endpoints:
            dps_res = endpoint(
                id=mixed_ts.id, include_status=True, ignore_bad_datapoints=False, start=ts_to_ms("2023-02-11"), limit=5
            )
            # Test object itself, plus slice of object:
            for dps in [dps_res, dps_res[:5]]:
                for dp, code, symbol in zip(dps, dps.status_code, dps.status_symbol):
                    assert isinstance(dp, Datapoint)
                    assert code is not None and code == dp.status_code
                    assert symbol is not None and symbol == dp.status_symbol

                assert math.isclose(dps.value[0], dps[0].value)
                assert math.isclose(dps.value[4], dps[4].value)
                assert math.isclose(dps.value[0], 432.9514228031592)
                assert math.isclose(dps.value[4], 143.05065712951188)

                assert dps.value[1] == dps[1].value == math.inf
                assert math.isnan(dps.value[2]) and math.isnan(dps[2].value)

                if isinstance(dps, Datapoints):
                    assert dps.value[3] is None
                elif isinstance(dps, DatapointsArray):
                    assert math.isnan(dps.value[3])
                    bad_ts = dps.timestamp[3].item() // 1_000_000
                    assert dps.null_timestamps == {bad_ts}

                    # Test slicing a part without a missing value:
                    dps_slice = dps[:3]
                    assert not dps_slice.null_timestamps
                else:
                    assert False

    @pytest.mark.parametrize("test_is_string", (True, False))
    def test_n_dps_retrieved_with_without_uncertain_and_bad(self, retrieve_endpoints, ts_status_codes, test_is_string):
        if test_is_string:
            _, mixed_ts, _, bad_ts = ts_status_codes
        else:
            mixed_ts, _, bad_ts, _ = ts_status_codes

        q1 = DatapointsQuery(id=mixed_ts.id, treat_uncertain_as_bad=True, ignore_bad_datapoints=True)
        q2 = DatapointsQuery(external_id=bad_ts.external_id, treat_uncertain_as_bad=True, ignore_bad_datapoints=True)
        q3 = DatapointsQuery(id=mixed_ts.id, treat_uncertain_as_bad=False, ignore_bad_datapoints=True)
        q4 = DatapointsQuery(external_id=bad_ts.external_id, treat_uncertain_as_bad=False, ignore_bad_datapoints=True)
        q5 = DatapointsQuery(id=mixed_ts.id, treat_uncertain_as_bad=False, ignore_bad_datapoints=False)
        q6 = DatapointsQuery(external_id=bad_ts.external_id, treat_uncertain_as_bad=False, ignore_bad_datapoints=False)

        for endpoint in retrieve_endpoints:
            mix1, mix2, mix3, bad1, bad2, bad3 = dps_lst = endpoint(
                id=[q1, q3, q5], external_id=[q2, q4, q6], include_status=False
            )
            for dps in dps_lst:
                assert dps.is_string is test_is_string

            assert mix1.status_code is mix1.status_symbol is bad1.status_code is bad1.status_symbol is None
            assert len(mix1) == 117  # good
            assert len(mix2) == 239  # good + uncertain
            assert len(mix3) == 365  # good + uncertain + bad

            assert len(bad1) == 0
            assert len(bad2) == 0
            assert len(bad3) == 365

    @pytest.mark.parametrize("test_is_string", (True, False))
    def test_outside_points_with_bad_and_uncertain(self, retrieve_endpoints, ts_status_codes, test_is_string):
        if test_is_string:
            _, mixed_ts, _, bad_ts = ts_status_codes
        else:
            mixed_ts, _, bad_ts, _ = ts_status_codes

        for endpoint in retrieve_endpoints:
            mix1, mix2, bad1, bad2, mix3, bad3, mix4, bad4 = endpoint(
                id=[
                    DatapointsQuery(id=mixed_ts.id, include_outside_points=False),
                    DatapointsQuery(id=mixed_ts.id, include_outside_points=False, treat_uncertain_as_bad=False),
                    DatapointsQuery(id=bad_ts.id, include_outside_points=False),
                    DatapointsQuery(id=bad_ts.id, include_outside_points=False, ignore_bad_datapoints=False),
                ],
                external_id=[
                    mixed_ts.external_id,
                    bad_ts.external_id,
                    DatapointsQuery(external_id=mixed_ts.external_id, treat_uncertain_as_bad=False),
                    DatapointsQuery(external_id=bad_ts.external_id, ignore_bad_datapoints=False),
                ],
                include_outside_points=True,
                start=ts_to_ms("2023-01-10") + 1,  # first good dp
                end=ts_to_ms("2023-12-27"),  # last good dp
                include_status=False,
                treat_uncertain_as_bad=True,
                ignore_bad_datapoints=True,
            )
            assert len(mix1) == 115  # good only, no outside
            assert len(mix3) == 117  # good only, with outside
            assert len(mix2) == 233  # good+uncertain, no outside
            assert len(mix4) == 235  # good+uncertain, with outside

            assert len(bad1) == 0
            assert len(bad3) == 0
            assert len(bad2) == 350
            assert len(bad4) == 352

    def test_status_codes_and_symbols(self, retrieve_endpoints, ts_status_codes):
        mixed_ts, _, bad_ts, _ = ts_status_codes
        for endpoint, uses_numpy in zip(retrieve_endpoints, (False, True)):
            dps_lst = endpoint(
                id=[
                    DatapointsQuery(id=mixed_ts.id),
                    DatapointsQuery(id=mixed_ts.id, treat_uncertain_as_bad=False),
                    DatapointsQuery(id=mixed_ts.id, treat_uncertain_as_bad=False, ignore_bad_datapoints=False),
                    DatapointsQuery(id=bad_ts.id),
                    DatapointsQuery(id=bad_ts.id, treat_uncertain_as_bad=False),
                    DatapointsQuery(id=bad_ts.id, treat_uncertain_as_bad=False, ignore_bad_datapoints=False),
                ],
                start=ts_to_ms("2023-08-08"),
                include_status=True,
                limit=10,
            )
            for dps in dps_lst:
                # Some of these are empty, make sure they are still initiated to a container:
                assert dps.status_code is not None
                assert dps.status_symbol is not None

            m1, m2, m3, b1, b2, b3 = dps_lst
            assert math.isclose(m1.value[0], 686.4757694370811)
            assert math.isclose(m1.value[-1], 611.7573455502195)
            assert m1.status_code[0] == 10682368
            assert m1.status_code[-1] == m2.status_code[-1] == 3145728
            assert m1.status_symbol[-1] == m2.status_symbol[-1] == "GoodClamped"

            assert math.isclose(m2.value[0], -371525.6348704161)
            assert math.isclose(m2.value[-1], -538.1994761772598)
            assert m2.status_code[0] == 1083244544

            assert math.isclose(m3.value[0], 420)
            assert math.isnan(m3.value[2])  # actual nan returned
            if uses_numpy:
                assert math.isnan(m3.value[4])  # cant store 'missing' in numpy array
                assert convert_any_ts_to_integer(m3.timestamp[4]) in m3.null_timestamps
            else:
                assert m3.value[4] is None  # missing, nothing returned
            assert math.isclose(m3.value[-1], 1227.6332936465685)
            assert m3.status_code[0] == 2149122048
            assert m3.status_code[2] == 2152071168
            assert m3.status_code[4] == 2149777408
            assert m3.status_code[-1] == 67239936
            assert list(m3.status_symbol[5:8]) == [
                "BadWaitingForResponse",
                "GoodEntryReplaced",
                "UncertainReferenceOutOfServer",
            ]
            assert not b1.value and not b2.value
            assert not b1.status_code and not b2.status_code
            assert not b1.status_symbol and not b2.status_symbol

            assert b3.value[0] == -math.inf
            if uses_numpy:
                assert math.isnan(b3.value[1])
                assert convert_any_ts_to_integer(b3.timestamp[1]) in b3.null_timestamps
            else:
                assert b3.value[1] is None
            assert math.isclose(b3.value[-1], 2.71)
            assert b3.status_code[0] == 2148335616
            assert b3.status_code[1] == 2152267776
            assert b3.status_code[-1] == 2165309440
            assert b3.status_symbol[-1] == "BadLicenseNotAvailable"

    def test_query_no_ts_exists(self, retrieve_endpoints):
        for endpoint, exp_res_lst_type in zip(retrieve_endpoints, DPS_LST_TYPES):
            ts_id = random_cognite_ids(1)  # list of len 1
            res_lst = endpoint(id=ts_id, ignore_unknown_ids=True)
            assert isinstance(res_lst, exp_res_lst_type)
            # SDK bug v<5, id mapping would not exist because empty `.data` on res_lst:
            assert res_lst.get(id=ts_id[0]) is None

    def test_timezone_raw_query_dst_transitions(self, all_retrieve_endpoints, dps_queries_dst_transitions):
        expected_index = pd.to_datetime(
            [
                # to summer
                "1991-03-31 00:20:05.911+01:00",
                "1991-03-31 00:39:49.780+01:00",
                "1991-03-31 03:21:08.144+02:00",
                "1991-03-31 03:28:06.963+02:00",
                "1991-03-31 03:28:51.903+02:00",
                # to winter
                "1991-09-29 01:02:37.949+02:00",
                "1991-09-29 02:09:29.699+02:00",
                "1991-09-29 02:11:39.983+02:00",
                "1991-09-29 02:10:59.442+01:00",
                "1991-09-29 02:52:26.212+01:00",
                "1991-09-29 04:12:02.558+01:00",
            ],
            utc=True,  # pandas is not great at parameter names
        ).tz_convert("Europe/Oslo")
        expected_to_summer_index = expected_index[:5]
        expected_to_winter_index = expected_index[5:]
        for endpoint, convert in zip(all_retrieve_endpoints, (True, True, False)):
            to_summer, to_winter = dps_lst = endpoint(id=dps_queries_dst_transitions[:2], include_outside_points=True)
            if convert:
                dps_lst = dps_lst.to_pandas().astype("Int64")
                to_summer, to_winter = to_summer.to_pandas().astype(np.int64), to_winter.to_pandas().astype(np.int64)
            else:
                dps_lst = dps_lst.astype("Int64")
                to_summer, to_winter = dps_lst.iloc[:, 0], dps_lst.iloc[:, 1]

            if not convert:
                for dps in [to_summer, to_winter]:
                    pd.testing.assert_index_equal(expected_index, dps.index)
                to_summer = to_summer.dropna()
                to_winter = to_winter.dropna()

            assert list(range(89712, 89717)) == to_summer.squeeze().values.tolist()
            assert list(range(96821, 96827)) == to_winter.squeeze().values.tolist()
            pd.testing.assert_index_equal(expected_index, dps_lst.index)
            pd.testing.assert_index_equal(expected_to_winter_index, to_winter.index)
            pd.testing.assert_index_equal(expected_to_summer_index, to_summer.index)


class TestRetrieveAggregateDatapointsAPI:
    @pytest.mark.parametrize(
        "aggregates",
        (
            "min",
            "step_interpolation",
            "stepInterpolation",
            ["step_interpolation"],
            ["stepInterpolation"],
            ["continuous_variance", "discrete_variance", "step_interpolation", "total_variation"],
            ["continuous_variance", "discrete_variance", "step_interpolation", "total_variation", "min"],
            list(map(to_camel_case, ALL_SORTED_DP_AGGS)),
            list(map(to_snake_case, ALL_SORTED_DP_AGGS)),
            # Give a mix:
            ["continuousVariance", "discrete_variance", "step_interpolation", "totalVariation"],
            ["continuous_variance", "discreteVariance", "stepInterpolation", "total_variation", "min"],
        ),
    )
    def test_aggregates_single_and_multiple_in_snake_or_camel_case(
        self, aggregates, fixed_freq_dps_ts, retrieve_endpoints
    ):
        dfs, ts = [], random.choice(fixed_freq_dps_ts[0])  # only pick from numeric ts
        granularity = random_granularity()
        for endpoint in retrieve_endpoints:
            res = endpoint(
                limit=5,
                id={"id": ts.id, "granularity": granularity, "aggregates": aggregates},
            )
            snake_aggs = sorted(map(to_snake_case, [aggregates] if isinstance(aggregates, str) else aggregates))
            for col_names in ["id", "external_id"]:
                res_df = res.to_pandas(column_names=col_names, include_aggregate_name=True)
                assert all(res_df.columns == [f"{getattr(ts, col_names)}|{agg}" for agg in snake_aggs])
                dfs.append(res_df)
        # Also make sure `Datapoints.to_pandas()` and `DatapointsArray.to_pandas()` give identical results:
        pd.testing.assert_frame_equal(dfs[0], dfs[2])
        pd.testing.assert_frame_equal(dfs[1], dfs[3])

    def test_aggregates_bad_string(self, fixed_freq_dps_ts, retrieve_endpoints):
        ts = random.choice(fixed_freq_dps_ts[0])  # only pick from numeric ts
        granularity = random_granularity()
        for endpoint in retrieve_endpoints:
            with pytest.raises(
                CogniteAPIError, match=re.escape("Could not recognize aggregation value: min-max-lol | code: 400")
            ):
                endpoint(id=ts.id, granularity=granularity, aggregates="min-max-lol")

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
                df = res.to_pandas()
                assert (df.isna().sum() == 0).all()
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
        cognite_client,
    ):
        # Ts: has ms resolution data from:
        # 1969-12-31 23:59:58.500 (-1500 ms) to 1970-01-01 00:00:01.500 (1500 ms)
        (*_, ts_ms), (*_, ts_ms_is_step) = fixed_freq_dps_ts
        ts = ts_ms_is_step if is_step else ts_ms
        assert ts.is_step is is_step

        # Pick random start and end in an empty region:
        if before_first_dp:
            start = random.randint(MIN_TIMESTAMP_MS, -315619200000)  # 1900 -> 1960
            end = random.randint(start, -31536000000)  # start -> 1969
        else:  # after last dp
            start = random.randint(31536000000, 2524608000000)  # 1971 -> 2050
            end = random.randint(start, MAX_TIMESTAMP_MS)  # start -> (2100 minus 1ms)
        granularities = (
            f"{random.randint(1, 15)}d",
            f"{random.randint(1, 50)}h",
            f"{random.randint(1, 120)}m",
            f"{random.randint(1, 120)}s",
        )

        with set_max_workers(cognite_client, 8):
            for endpoint in retrieve_endpoints[:1]:
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

    @pytest.mark.parametrize("granularity, lower_lim, upper_lim", (("h", 30, 1000), ("d", 1, 200)))
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
        "max_workers, ts_idx, granularity, exp_len, start, end, exclude_aggregate",
        (
            (1, 105, "8m", 81, ts_to_ms("1969-12-31 14:14:14"), ts_to_ms("1970-01-01 01:01:01"), {}),
            (1, 106, "7s", 386, ts_to_ms("1960"), ts_to_ms("1970-01-01 00:15:00"), {}),
            (8, 106, "7s", 386, ts_to_ms("1960"), ts_to_ms("1970-01-01 00:15:00"), {}),
            (
                2,
                107,
                "1s",
                4,
                ts_to_ms("1969-12-31 23:59:58.123"),
                ts_to_ms("2049-01-01 00:00:01.500"),
                {"interpolation", "step_interpolation"},
            ),
            (
                5,
                113,
                "11h",
                32_288,
                ts_to_ms("1960-01-02 03:04:05.060"),
                ts_to_ms("2000-07-08 09:10:11.121"),
                {"interpolation"},
            ),
            (3, 115, "1s", 200, ts_to_ms("2000-01-01"), ts_to_ms("2000-01-01 12:03:20"), {}),
            (
                20,
                115,
                "12h",
                5_000,
                ts_to_ms("1990-01-01"),
                ts_to_ms("2013-09-09 00:00:00.001"),
                {"step_interpolation"},
            ),
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
        exclude_aggregate,
        retrieve_endpoints,
        all_test_time_series,
        cognite_client,
    ):
        with set_max_workers(cognite_client, max_workers), patch(DATAPOINTS_API.format("ChunkingDpsFetcher")):
            for endpoint in retrieve_endpoints:
                res = endpoint(
                    id=all_test_time_series[ts_idx].id,
                    start=start,
                    end=end,
                    granularity=granularity,
                    aggregates=random_aggregates(exclude=exclude_aggregate),
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
                            "aggregates": random_aggregates(1, exclude={"interpolation"}),
                            "granularity": f"{random_gamma_dist_integer(gran_unit_upper)}{random.choice('sm')}",
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

    @pytest.mark.parametrize("kwargs", (dict(target_unit="temperature:deg_f"), dict(target_unit_system="Imperial")))
    def test_retrieve_methods_in_target_unit(
        self,
        all_retrieve_endpoints: list[Callable],
        kwargs: dict,
        cognite_client: CogniteClient,
        timeseries_degree_c_minus40_0_100: TimeSeries,
    ) -> None:
        ts = timeseries_degree_c_minus40_0_100
        for retrieve_method in all_retrieve_endpoints:
            res = retrieve_method(external_id=ts.external_id, aggregates="max", granularity="1h", end=3, **kwargs)
            if isinstance(res, pd.DataFrame):
                res = DatapointsArray(max=res.values)
            assert math.isclose(res.max[0], 212)

    def test_status_codes_affect_aggregate_calculations(self, retrieve_endpoints, ts_status_codes):
        mixed_ts, _, bad_ts, _ = ts_status_codes  # No aggregates for string dps
        bad_xid = bad_ts.external_id
        for endpoint, uses_numpy in zip(retrieve_endpoints, (False, True)):
            dps_lst = endpoint(
                id=[
                    DatapointsQuery(id=mixed_ts.id, treat_uncertain_as_bad=True, ignore_bad_datapoints=True),
                    DatapointsQuery(id=mixed_ts.id, treat_uncertain_as_bad=False, ignore_bad_datapoints=True),
                    DatapointsQuery(id=mixed_ts.id, treat_uncertain_as_bad=True, ignore_bad_datapoints=False),
                    DatapointsQuery(id=mixed_ts.id, treat_uncertain_as_bad=False, ignore_bad_datapoints=False),
                ],
                external_id=[
                    DatapointsQuery(external_id=bad_xid, treat_uncertain_as_bad=True, ignore_bad_datapoints=True),
                    DatapointsQuery(external_id=bad_xid, treat_uncertain_as_bad=False, ignore_bad_datapoints=True),
                    DatapointsQuery(external_id=bad_xid, treat_uncertain_as_bad=True, ignore_bad_datapoints=False),
                    DatapointsQuery(external_id=bad_xid, treat_uncertain_as_bad=False, ignore_bad_datapoints=False),
                ],
                start=ts_to_ms("2023-01-01"),
                end=ts_to_ms("2024-01-01"),
                aggregates=ALL_SORTED_DP_AGGS,
                granularity="30d",
            )
            # Ensure all are not just empty, but not set:
            for dps in dps_lst:
                assert dps.status_code is None
                assert dps.status_symbol is None

            # Count and duration of specific status should be invariant:
            for dps in [dps_lst[:4], dps_lst[4:]]:
                for agg in "count", "duration":
                    for status in f"{agg}_good", f"{agg}_uncertain", f"{agg}_bad":
                        l1, l2, l3, l4 = (getattr(x, status) for x in dps)
                        assert list(l1) == list(l2) == list(l3) == list(l4)

                # Normal count may count uncertain depending on treat_uncertain_as_bad:
                assert list(dps[0].count) == list(dps[2].count)  # note: ignores treat_uncertain_as_bad
                assert list(dps[1].count) == list(dps[3].count)

            m1, m2, m3, m4, b1, b2, b3, b4 = dps_lst
            assert sum(m1.count) < sum(m2.count)
            assert sum(b1.count) == sum(b2.count) == sum(b3.count) == sum(b4.count) == 0  # bad is never counted

            for bad in b1, b2, b3, b4:
                assert np.isnan(bad.average).all()
                assert np.isnan(bad.interpolation).all()
                assert np.isnan(bad.total_variation).all()

            # Last aggregation period contains good only:
            assert_allclose(-543.501385, [m1.average[-1], m2.average[-1], m3.average[-1], m4.average[-1]])
            assert_allclose(
                -543.501385, [m1.interpolation[-1], m2.interpolation[-1], m3.interpolation[-1], m4.interpolation[-1]]
            )
            assert_allclose(
                0.0, [m1.total_variation[-1], m2.total_variation[-1], m3.total_variation[-1], m4.total_variation[-1]]
            )
            # The following aggregates do not care about the 'ignore_bad_datapoints' setting, only 'treat_uncertain_as_bad':
            for agg in ["min", "max", "sum", "interpolation", "discrete_variance", "total_variation"]:
                m1_agg, m2_agg, m3_agg, m4_agg = (getattr(m, agg) for m in (m1, m2, m3, m4))
                assert_allclose(m1_agg, m3_agg)  # both treat_uncertain_as_bad=True
                assert_allclose(m2_agg, m4_agg)  # both treat_uncertain_as_bad=False
                with pytest.raises(AssertionError, match=r"^\nNot equal to tolerance"):
                    assert_allclose(m1_agg, m2_agg)
            # The following aggregates varies with both settings, 'average', 'step_interpolation' and 'continuous_variance':
            assert_allclose(m1.average[:4], [-100.266803, 180.102903, 709.306477, -342.590187])
            assert_allclose(m2.average[:4], [-166855.089388, 113784.358276, 13372.243630, -65191.492671])
            assert_allclose(m3.average[:4], [-283.850509, 205.634247, 881.584739, -91.393828])
            assert_allclose(m4.average[:4], [-264471.559047, 117022.569448, -38772.339529, 47902.527674])

            assert_allclose(m1.step_interpolation[:4], [math.nan, -1138.57277, 274.866615, 651.621005])
            assert_allclose(
                m2.step_interpolation[:4], [math.nan, -1355647.5886087606, -110240.71105459922, 651.6210049165213]
            )
            assert_allclose(m3.step_interpolation[:4], [math.nan, math.nan, math.nan, 651.621005])
            assert_allclose(m4.step_interpolation[:4], [math.nan, math.nan, -110240.711055, 651.621005])

            assert_allclose(m1.continuous_variance[:4], [642800.685332, 426005.553231, 435043.668454, 622115.239423])
            assert_allclose(
                m2.continuous_variance[:4],
                [284755948510.1042, 375008672701.96277, 148584225325.00912, 375298973299.39276],
            )
            assert_allclose(
                m3.continuous_variance[:4], [625723.5820629946, 638941.7823122272, 675739.8119564621, 1583061.859119803]
            )
            assert_allclose(
                m4.continuous_variance[:4],
                [349079144838.96564, 512343998481.7162, 159180999248.7119, 529224146671.5178],
            )

    def test_timezone_agg_query_dst_transitions(self, all_retrieve_endpoints, dps_queries_dst_transitions):
        expected_values1 = [0.23625579717753353, 0.02829928231631262, -0.0673823850533647, -0.20908049925449418]
        expected_values2 = [-0.13218082741552517, -0.20824244773820486, 0.02566169899072951, 0.15040625644292185]
        expected_index = pd.to_datetime(
            [
                # to summer
                "2023-03-26 00:00:00+01:00",
                "2023-03-26 01:00:00+01:00",
                "2023-03-26 03:00:00+02:00",
                "2023-03-26 04:00:00+02:00",
                # to winter
                "2023-10-29 01:00:00+02:00",
                "2023-10-29 02:00:00+02:00",
                "2023-10-29 02:00:00+01:00",
                "2023-10-29 03:00:00+01:00",
            ],
            utc=True,  # pandas is still not great at parameter names
        ).tz_convert("Europe/Oslo")
        expected_to_summer_index = expected_index[:4]
        expected_to_winter_index = expected_index[4:]
        for endpoint, convert in zip(all_retrieve_endpoints, (True, True, False)):
            to_summer, to_winter = dps_lst = endpoint(
                id=dps_queries_dst_transitions[2:], aggregates="average", granularity="1hour"
            )
            if convert:
                dps_lst = dps_lst.to_pandas()
                to_summer, to_winter = to_summer.to_pandas(), to_winter.to_pandas()
            else:
                to_summer, to_winter = dps_lst.iloc[:, 0], dps_lst.iloc[:, 1]

            if not convert:
                for dps in [to_summer, to_winter]:
                    pd.testing.assert_index_equal(expected_index, dps.index)
                to_summer = to_summer.dropna()
                to_winter = to_winter.dropna()

            assert_allclose(expected_values1, to_summer.squeeze().to_numpy())
            assert_allclose(expected_values2, to_winter.squeeze().to_numpy())
            pd.testing.assert_index_equal(expected_index, dps_lst.index)
            pd.testing.assert_index_equal(expected_to_winter_index, to_winter.index)
            pd.testing.assert_index_equal(expected_to_summer_index, to_summer.index)

    def test_calendar_granularities_in_utc_and_timezone(self, retrieve_endpoints, all_test_time_series):
        daily_ts, oslo = all_test_time_series[108], ZoneInfo("Europe/Oslo")
        granularities = [
            "1" + random.choice(["mo", "month", "months"]),
            "1" + random.choice(["q", "quarter", "quarters"]),
            "1" + random.choice(["y", "year", "years"]),
        ]
        for endpoint in retrieve_endpoints:
            mo_utc, q_utc, y_utc, mo_oslo, q_oslo, y_oslo = endpoint(
                id=[DatapointsQuery(id=daily_ts.id, granularity=gran) for gran in granularities],
                external_id=[
                    DatapointsQuery(external_id=daily_ts.external_id, granularity=gran, timezone=oslo)
                    for gran in granularities
                ],
                start=ts_to_ms("1964-01-01"),
                end=ts_to_ms("1974-12-31"),
                aggregates="count",
            )
            assert_equal(mo_utc.count, mo_oslo.count)
            assert_equal(q_utc.count, q_oslo.count)
            assert_equal(y_utc.count, y_oslo.count)

            # Verify that the number of days per year/quarter/month follows the actual calendar:
            exp_days_per_year = [
                365, 365, 365, 366,
                365, 365, 365, 366,
                365, 365,  #   ^^^
            ]  # fmt: skip
            exp_days_per_quarter = [
                90, 91, 92, 92,
                90, 91, 92, 92,
                90, 91, 92, 92,
                91, 91, 92, 92,  # Look, I'm special
            ]  # fmt: skip
            exp_days_per_month = [
                31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
                31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
                31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
                31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,  # star of the show: Feb 29
            ]  # fmt: skip
            assert_equal(exp_days_per_year, y_utc.count)
            assert_equal(exp_days_per_quarter, q_utc.count[: 4 * 4])
            assert_equal(exp_days_per_month, mo_utc.count[: 12 * 4])


def retrieve_dataframe_in_tz_count_large_granularities_data():
    # "start, end, granularity, expected_df"
    oslo = ZoneInfo("Europe/Oslo")
    start = datetime(2023, 3, 21, tzinfo=oslo)
    end = datetime(2023, 4, 9, tzinfo=oslo)
    hours_in_week = 24 * 7
    index = pd.date_range("2023-03-20", "2023-04-09", freq="7D", tz="Europe/Oslo")

    yield pytest.param(
        start,
        end,
        "1week",
        pd.DataFrame(
            data=[hours_in_week - 1, hours_in_week, hours_in_week], index=index, columns=["count"], dtype="Int64"
        ),
        id="Weekly with first week DST transition",
    )
    start = datetime(2023, 3, 1, tzinfo=oslo)
    end = datetime(2023, 11, 1, tzinfo=oslo)
    index = pd.date_range("2023-03-01", "2023-10-31", freq="1D", tz="Europe/Oslo")
    index = index[index.is_month_start & (index.month % 2 == 1)]
    yield pytest.param(
        start,
        end,
        "2months",
        pd.DataFrame(
            data=[(31 + 30) * 24 - 1, (31 + 30) * 24, (31 + 31) * 24, (31 + 30) * 24 + 1],
            index=index,
            columns=["count"],
            dtype="Int64",
        ),
        id="Every other month from March to November 2023",
    )
    index = pd.date_range("2023-01-01", "2023-12-31", freq="1D", tz="Europe/Oslo")
    index = index[index.is_month_start & index.month.isin({1, 7})]
    yield pytest.param(
        start,
        end,
        "2quarters",
        pd.DataFrame(
            data=[(31 + 28 + 31 + 30 + 31 + 30) * 24 - 1, (31 + 31 + 30 + 31 + 30 + 31) * 24 + 1],
            index=index,
            columns=["count"],
            dtype="Int64",
        ),
        id="2023 in steps of 2 quarters",
    )
    start = datetime(2021, 7, 15, tzinfo=oslo)
    end = datetime(2021, 7, 16, tzinfo=oslo)
    yield pytest.param(
        start,
        end,
        "3years",
        pd.DataFrame(
            data=[3 * 365 * 24], index=[pd.Timestamp("2021-01-01", tz="Europe/Oslo")], columns=["count"], dtype="Int64"
        ),
        id="Aggregate from 2021 to 2023",
    )


def retrieve_dataframe_in_tz_count_small_granularities_data():
    # "106: every minute, 1969-12-31 - 1970-01-02, numeric",
    oslo = ZoneInfo("Europe/Oslo")
    yield pytest.param(
        106,
        datetime(1970, 1, 1, 0, 0, 0, tzinfo=oslo),
        datetime(1970, 1, 2, 0, 0, 0, tzinfo=oslo),
        "6hours",
        pd.DataFrame(
            [6 * 60] * 4,
            index=pd.date_range("1970-01-01 00:00:00", "1970-01-01 23:00:00", freq="6h", tz="Europe/Oslo"),
            columns=["count"],
            dtype="Int64",
        ),
        id="6 hour granularities on minute raw data",
    )
    yield pytest.param(
        106,
        datetime(1970, 1, 1, 0, 0, 0, tzinfo=oslo),
        datetime(1970, 1, 1, 0, 30, 0, tzinfo=oslo),
        "10minutes",
        pd.DataFrame(
            [10] * 3,
            index=pd.date_range("1970-01-01 00:00:00", "1970-01-01 00:29:00", freq="10min", tz="Europe/Oslo"),
            columns=["count"],
            dtype="Int64",
        ),
        id="10 minutes granularities on minute raw data",
    )
    yield pytest.param(
        106,
        datetime(1970, 1, 1, 0, 0, 0, tzinfo=oslo),
        datetime(1970, 1, 1, 0, 0, 2, tzinfo=oslo),
        "1second",
        pd.DataFrame(
            [1], index=[pd.Timestamp("1970-01-01 00:00:00", tz="Europe/Oslo")], columns=["count"], dtype="Int64"
        ),
        id="1 second granularity on minute raw data",
    )


def retrieve_dataframe_in_tz_uniform_data():
    oslo = ZoneInfo("Europe/Oslo")
    yield pytest.param(
        119,
        datetime(2019, 12, 23, tzinfo=oslo),
        datetime(2020, 1, 14, tzinfo=oslo),
        "1week",
        pd.DatetimeIndex(
            [pd.Timestamp(x) for x in ["2019-12-23", "2019-12-30", "2020-01-06", "2020-01-13"]],
            tz="Europe/Oslo",
        ),
        id="Uniform Weekly",
    )

    yield pytest.param(
        119,
        datetime(2019, 11, 23, tzinfo=oslo),
        datetime(2020, 1, 14, tzinfo=oslo),
        "2quarters",
        pd.DatetimeIndex([pd.Timestamp(x) for x in ["2019-10-01"]], tz="Europe/Oslo", freq="2QS-OCT"),
        id="Uniform 2nd Quarter",
    )


class TestRetrieveTimezoneDatapointsAPI:
    """
    Integration testing of all the functionality related to retrieving in the correct timezone
    """

    @staticmethod
    def test_retrieve_dataframe_in_tz_ambiguous_time(cognite_client, hourly_normal_dist):
        oslo = ZoneInfo("Europe/Oslo")
        df = cognite_client.time_series.data.retrieve_dataframe_in_tz(
            external_id=hourly_normal_dist.external_id,
            start=datetime(1901, 1, 1, tzinfo=oslo),
            end=datetime(2023, 1, 1, tzinfo=oslo),
            aggregates="average",
            granularity="1month",
        )
        assert not df.empty

    @staticmethod
    @pytest.mark.parametrize(
        "test_series_no, start, end, aggregation, granularity",
        (
            (119, "2023-01-01T00:00:00+00:00", "2023-01-02T00:00:01+00:00", "average", "2h"),
            (119, "2023-01-01T00:00:00+00:00", "2023-01-02T00:00:01+00:00", "average", "3h"),
            (119, "2023-01-01T00:00:00+00:00", "2023-01-02T00:00:01+00:00", "sum", "5h"),
            (119, "2023-01-01T00:00:00+00:00", "2023-01-02T00:00:01+00:00", "count", "5h"),
            (120, "2023-01-01T00:00:00+00:00", "2023-01-02T00:00:59+00:00", "average", "2m"),
            (120, "2023-01-01T00:00:00+00:00", "2023-01-02T00:00:01+00:00", "sum", "30m"),
            (120, "2023-01-01T00:00:00+00:00", "2023-01-01T23:59:01+00:00", "average", "15m"),
            (120, "2023-01-01T00:00:00+00:00", "2023-01-01T23:59:01+00:00", "average", "1h"),
            (120, "2023-01-01T00:00:00+00:00", "2023-01-01T23:59:01+00:00", "count", "38m"),
        ),
    )
    def test_cdf_aggregate_equal_to_cdf(
        test_series_no: str,
        start: str,
        end: str,
        aggregation: Literal["average", "sum", "count"],
        granularity: str,
        cognite_client: CogniteClient,
        all_test_time_series: TimeSeriesList,
    ):
        ts = all_test_time_series[test_series_no - 1]
        start, end = datetime.fromisoformat(start), datetime.fromisoformat(end)
        raw_df = cognite_client.time_series.data.retrieve_dataframe(external_id=ts.external_id, start=start, end=end)
        expected_aggregate = cognite_client.time_series.data.retrieve_dataframe(
            start=start,
            end=end,
            external_id=ts.external_id,
            aggregates=aggregation,
            granularity=granularity,
            include_aggregate_name=False,
            include_granularity_name=False,
        )
        actual_aggregate = cdf_aggregate(raw_df, aggregation, granularity, ts.is_step)

        # Pandas adds the correct frequency to the index, while the SDK does not when uniform is not True.
        # The last point is not compared as the raw data might be missing information to do the correct aggregate.
        pd.testing.assert_frame_equal(
            expected_aggregate.iloc[:-1], actual_aggregate.iloc[:-1], check_freq=False, check_exact=False
        )

    @staticmethod
    @pytest.mark.parametrize(
        "aggregation, granularity, tz_name",
        (
            ("average", "1d", "Europe/Oslo"),
            ("average", "5d", "Europe/Oslo"),
            ("sum", "31d", "Europe/Oslo"),
        ),
    )
    def test_retrieve_dataframe_in_tz_aggregate_raw_hourly(
        aggregation: Literal["average", "sum"],
        granularity: str,
        tz_name: str,
        cognite_client: CogniteClient,
        hourly_normal_dist: TimeSeries,
    ):
        tz = ZoneInfo(tz_name)
        start = datetime(2023, 1, 1, tzinfo=tz)
        end = datetime(2023, 12, 31, 23, 0, 0, tzinfo=tz)
        raw_df = (
            cognite_client.time_series.data.retrieve_dataframe(
                external_id=hourly_normal_dist.external_id, start=start, end=end
            )
            .tz_localize("UTC")
            .tz_convert(tz_name)
            .loc[str(start) : str(end)]
        )
        expected_aggregate = cdf_aggregate(raw_df, aggregation, granularity)

        actual_aggregate = cognite_client.time_series.data.retrieve_dataframe_in_tz(
            external_id=list(raw_df.columns),
            aggregates=aggregation,
            granularity=granularity,
            start=start,
            end=end,
            include_aggregate_name=False,
        )
        # When doing the aggregation in pandas frequency information is added to the
        # resulting dataframe which is not included when retrieving from CDF.
        # The last point is not compared as the raw data might be missing information to do the correct aggregate.
        pd.testing.assert_frame_equal(expected_aggregate.iloc[:-1], actual_aggregate.iloc[:-1], check_freq=False)

    @staticmethod
    @pytest.mark.parametrize(
        "start, end, granularity, expected_df", list(retrieve_dataframe_in_tz_count_large_granularities_data())
    )
    def test_retrieve_dataframe_in_tz_count_large_granularities(
        start: datetime, end: datetime, granularity: str, expected_df: pd.DataFrame, cognite_client, hourly_normal_dist
    ):
        actual_df = cognite_client.time_series.data.retrieve_dataframe_in_tz(
            external_id=hourly_normal_dist.external_id,
            start=start,
            end=end,
            aggregates="count",
            granularity=granularity,
        )
        actual_df.columns = ["count"]
        pd.testing.assert_frame_equal(actual_df, expected_df, check_freq=False)

    @staticmethod
    @pytest.mark.parametrize(
        "test_series_no, start, end, granularity, expected_df",
        list(retrieve_dataframe_in_tz_count_small_granularities_data()),
    )
    def test_retrieve_dataframe_in_tz_count_small_granularities(
        test_series_no: str,
        start: datetime,
        end: datetime,
        granularity: str,
        expected_df: pd.DataFrame,
        cognite_client,
        all_test_time_series,
    ):
        ts = all_test_time_series[test_series_no - 1]
        actual_df = cognite_client.time_series.data.retrieve_dataframe_in_tz(
            external_id=ts.external_id,
            start=start,
            end=end,
            aggregates="count",
            granularity=granularity,
        )
        actual_df.columns = ["count"]
        pd.testing.assert_frame_equal(actual_df, expected_df, check_freq=False)

    @staticmethod
    @pytest.mark.parametrize(
        "test_series_no, start, end, granularity, expected_index",
        list(retrieve_dataframe_in_tz_uniform_data()),
    )
    def test_retrieve_dataframe_in_tz_uniform(
        test_series_no: str,
        start: datetime,
        end: datetime,
        granularity: str,
        expected_index: pd.DatetimeIndex,
        cognite_client,
        all_test_time_series,
    ):
        ts = all_test_time_series[test_series_no - 1]
        actual_df = cognite_client.time_series.data.retrieve_dataframe_in_tz(
            external_id=ts.external_id,
            start=start,
            end=end,
            aggregates="count",
            granularity=granularity,
            uniform_index=True,
        )
        actual_df.columns = ["count"]
        pd.testing.assert_index_equal(actual_df.index, expected_index)

    @staticmethod
    @pytest.mark.parametrize(
        "test_series_no, start, end, tz_name",
        [
            (119, "2023-01-01", "2023-02-01", "Europe/Oslo"),
            (120, "2023-01-01", "2023-02-01", "Europe/Oslo"),
        ],
    )
    def test_retrieve_dataframe_in_tz_raw_data(
        test_series_no: str, start: str, end: str, tz_name: str, cognite_client, all_test_time_series
    ):
        ts = all_test_time_series[test_series_no - 1]
        start, end = pd.Timestamp(start).to_pydatetime(), pd.Timestamp(end).to_pydatetime()
        tz = ZoneInfo(tz_name)
        start, end = start.replace(tzinfo=tz), end.replace(tzinfo=tz)
        expected_df = (
            cognite_client.time_series.data.retrieve_dataframe(external_id=ts.external_id, start=start, end=end)
            .tz_localize("utc")
            .tz_convert(tz_name)
        )
        actual_df = cognite_client.time_series.data.retrieve_dataframe_in_tz(
            external_id=ts.external_id, start=start, end=end
        )
        pd.testing.assert_frame_equal(actual_df, expected_df)

    @staticmethod
    def test_retrieve_dataframe_in_tz_multiple_timeseries(cognite_client, hourly_normal_dist, minutely_normal_dist):
        oslo = ZoneInfo("Europe/Oslo")
        start, end = datetime(2023, 1, 2, tzinfo=oslo), datetime(2023, 1, 2, 23, 59, 59, tzinfo=oslo)
        expected_df = pd.DataFrame(
            [[24, 24 * 60]],
            index=[pd.Timestamp("2023-01-02", tz="Europe/Oslo")],
            columns=[hourly_normal_dist.external_id, minutely_normal_dist.external_id],
            dtype="Int64",
        )
        actual_df = cognite_client.time_series.data.retrieve_dataframe_in_tz(
            external_id=[hourly_normal_dist.external_id, minutely_normal_dist.external_id],
            start=start,
            end=end,
            aggregates="count",
            granularity="1day",
            include_aggregate_name=False,
        )
        pd.testing.assert_frame_equal(actual_df[sorted(actual_df)], expected_df[sorted(expected_df)])

    @staticmethod
    def test_retrieve_dataframe_in_tz_empty_timeseries(cognite_client, hourly_normal_dist, minutely_normal_dist):
        oslo = ZoneInfo("Europe/Oslo")
        start, end = datetime(2010, 1, 1, tzinfo=oslo), datetime(2022, 1, 1, tzinfo=oslo)
        index = pd.date_range("2020-01-01", "2021-12-31", tz="Europe/Oslo", freq="AS")
        expected_df = pd.DataFrame(
            [[366 * 24 - 1, pd.NA], [365 * 24, pd.NA]],
            index=index,
            columns=[hourly_normal_dist.external_id, minutely_normal_dist.external_id],
            dtype="Int64",
        )
        actual_df = cognite_client.time_series.data.retrieve_dataframe_in_tz(
            external_id=[hourly_normal_dist.external_id, minutely_normal_dist.external_id],
            start=start,
            end=end,
            aggregates="count",
            granularity="1year",
            include_aggregate_name=False,
        )
        pd.testing.assert_frame_equal(actual_df[sorted(actual_df)], expected_df[sorted(expected_df)], check_freq=False)

    @staticmethod
    @pytest.mark.parametrize(
        "start, end, aggregates, granularity",
        [
            pytest.param(
                pd.Timestamp("2023-01-01", tz="Europe/Oslo"),
                pd.Timestamp("2023-02-01", tz="Europe/Oslo"),
                None,
                None,
                id="RAW query with pandas.Timestamp",
            ),
            pytest.param(
                pd.Timestamp("2023-01-01", tz="Europe/Oslo"),
                pd.Timestamp("2023-02-01", tz="Europe/Oslo"),
                "average",
                "1day",
                id="Aggregate query with pandas.Timestamp",
            ),
        ],
    )
    def test_retrieve_dataframe_in_tz_datetime_formats(
        start: datetime, end: datetime, cognite_client, aggregates: str, granularity: str, hourly_normal_dist
    ):
        df = cognite_client.time_series.data.retrieve_dataframe_in_tz(
            external_id=hourly_normal_dist.external_id,
            start=start,
            end=end,
            aggregates=aggregates,
            granularity=granularity,
        )
        assert not df.empty


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

    def test_query_with_duplicates(self, retrieve_endpoints, one_mill_dps_ts, ms_bursty_ts):
        ts_numeric, ts_string = one_mill_dps_ts
        for endpoint, exp_res_lst_type in zip(retrieve_endpoints, DPS_LST_TYPES):
            res_lst = endpoint(
                id=[
                    ms_bursty_ts.id,  # This is the only non-duplicated
                    ts_string.id,
                    {"id": ts_numeric.id, "granularity": "1d", "aggregates": "average"},
                ],
                external_id=[
                    ts_string.external_id,
                    ts_numeric.external_id,
                    {"external_id": ts_numeric.external_id, "granularity": "1d", "aggregates": "average"},
                ],
                limit=5,
            )
            assert isinstance(res_lst, exp_res_lst_type)
            # Check non-duplicated in result:
            assert isinstance(res_lst.get(id=ms_bursty_ts.id), exp_res_lst_type._RESOURCE)
            assert isinstance(res_lst.get(external_id=ms_bursty_ts.external_id), exp_res_lst_type._RESOURCE)
            # Check duplicated in result:
            assert isinstance(res_lst.get(id=ts_numeric.id), list)
            assert isinstance(res_lst.get(id=ts_string.id), list)
            assert isinstance(res_lst.get(external_id=ts_numeric.external_id), list)
            assert isinstance(res_lst.get(external_id=ts_string.external_id), list)
            assert len(res_lst.get(id=ts_numeric.id)) == 3
            assert len(res_lst.get(id=ts_string.id)) == 2
            assert len(res_lst.get(external_id=ts_numeric.external_id)) == 3
            assert len(res_lst.get(external_id=ts_string.external_id)) == 2


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

    @pytest.mark.parametrize("uniform, exp_n_ts_delta, exp_n_nans_step_interp", ((True, 1, 1), (False, 2, 0)))
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
                "aggregates": random_aggregates(
                    exclude={"interpolation", "step_interpolation"},
                    exclude_integer_aggregates=True,
                ),
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

    @pytest.mark.parametrize("test_id", range(3))
    def test_uniform_index_fails(
        self, test_id, parametrized_values_uniform_index_fails, cognite_client, one_mill_dps_ts
    ):
        granularity_lst, aggregates_lst, limits = parametrized_values_uniform_index_fails[test_id]
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
        with pytest.warns(UserWarning, match=re.escape("must be either 'instance_id', 'external_id' or 'id'")):
            cognite_client.time_series.data.retrieve_dataframe(
                id=one_mill_dps_ts[0].id, limit=5, column_names="bogus_id"
            )

    def test_include_aggregate_name_fails(self, cognite_client, one_mill_dps_ts):
        with pytest.raises(TypeError, match="can't multiply sequence by non-int of type 'NoneType"):
            cognite_client.time_series.data.retrieve_dataframe(
                id=one_mill_dps_ts[0].id, limit=5, granularity="1d", aggregates="min", include_aggregate_name=None
            )

    def test_include_granularity_name_fails(self, cognite_client, one_mill_dps_ts):
        with pytest.raises(TypeError, match="can't multiply sequence by non-int of type 'NoneType"):
            cognite_client.time_series.data.retrieve_dataframe(
                id=one_mill_dps_ts[0].id, limit=5, granularity="1d", aggregates="min", include_granularity_name=None
            )


@pytest.fixture
def post_spy(cognite_client):
    dps_api = cognite_client.time_series.data
    with patch.object(dps_api, "_post", wraps=dps_api._post):
        yield


@pytest.fixture
def do_request_spy(cognite_client):
    dps_api = cognite_client.time_series.data
    with patch.object(dps_api, "_do_request", wraps=dps_api._do_request):
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

    @pytest.mark.usefixtures("post_spy")
    def test_retrieve_latest_many(self, cognite_client, monkeypatch):
        ids = [t.id for t in cognite_client.time_series.list(limit=12) if not t.security_categories]
        assert len(ids) > 10  # more than one page

        monkeypatch.setattr(cognite_client.time_series.data, "_RETRIEVE_LATEST_LIMIT", 10)
        res = cognite_client.time_series.data.retrieve_latest(id=ids, ignore_unknown_ids=True)

        assert {dps.id for dps in res}.issubset(set(ids))
        assert 2 == cognite_client.time_series.data._post.call_count
        for dps in res:
            assert len(dps) <= 1  # could be empty

    def test_retrieve_latest_before(self, cognite_client, all_test_time_series):
        ts = all_test_time_series[0]
        res = cognite_client.time_series.data.retrieve_latest(id=ts.id, before="1h-ago")
        assert 1 == len(res)
        assert res[0].timestamp < timestamp_to_ms("1h-ago")

    @pytest.mark.parametrize(
        "kwargs",
        [dict(target_unit="temperature:deg_f"), dict(target_unit_system="Imperial")],
    )
    def test_retrieve_latest_in_target_unit(
        self,
        kwargs: dict,
        cognite_client: CogniteClient,
        timeseries_degree_c_minus40_0_100: TimeSeries,
    ) -> None:
        ts = timeseries_degree_c_minus40_0_100

        res = cognite_client.time_series.data.retrieve_latest(external_id=ts.external_id, before="now", **kwargs)
        assert math.isclose(res.value[0], 212)
        assert res.unit_external_id == "temperature:deg_f"

    @pytest.mark.parametrize(
        "kwargs",
        [dict(target_unit="temperature:deg_f"), dict(target_unit_system="Imperial")],
    )
    def test_retrieve_latest_query_in_target_unit(
        self,
        kwargs: dict,
        cognite_client: CogniteClient,
        timeseries_degree_c_minus40_0_100: TimeSeries,
    ) -> None:
        ts = timeseries_degree_c_minus40_0_100

        res = cognite_client.time_series.data.retrieve_latest(
            external_id=LatestDatapointQuery(external_id=ts.external_id, before="now", **kwargs)
        )
        assert math.isclose(res.value[0], 212)
        assert res.unit_external_id == "temperature:deg_f"

    def test_error_when_both_target_unit_and_system_in_latest(self, cognite_client, all_test_time_series):
        ts = all_test_time_series[0]
        with pytest.raises(ValueError, match="You must use either 'target_unit' or 'target_unit_system', not both."):
            cognite_client.time_series.data.retrieve_latest(
                id=ts.id, before="1h-ago", target_unit="temperature:deg_f", target_unit_system="imperial"
            )
        with pytest.raises(ValueError, match="You must use either 'target_unit' or 'target_unit_system', not both."):
            cognite_client.time_series.data.retrieve_latest(
                id=LatestDatapointQuery(
                    id=ts.id, before="1h-ago", target_unit="temperature:deg_f", target_unit_system="imperial"
                )
            )

    @pytest.mark.parametrize(
        "attr, multiple",
        (
            ("id", False),
            ("id", True),
            ("external_id", False),
            ("external_id", True),
        ),
    )
    def test_using_latest_datapoint_query(self, cognite_client, all_test_time_series, attr, multiple):
        identifier = getattr(all_test_time_series[0], attr)
        ldq = LatestDatapointQuery(**{attr: identifier}, before="1d-ago")
        if multiple:
            # Package inside list with other "primitive" identifiers:
            ldq = [identifier, ldq, ldq]
        res = cognite_client.time_series.data.retrieve_latest(**{attr: ldq})
        if multiple:
            assert isinstance(res, DatapointsList)
            assert len(ldq) == len(res)
        else:
            assert isinstance(res, Datapoints)
            assert 1 == len(res)

    @pytest.mark.usefixtures("post_spy")
    @pytest.mark.parametrize("test_is_string", (True, False))
    def test_ignore_unknown_ids_true_good_status_codes_are_populated(
        self, cognite_client, ts_status_codes, test_is_string, monkeypatch
    ):
        # We test result ordering by ensuring multiple splits of identifiers:
        monkeypatch.setattr(cognite_client.time_series.data, "_RETRIEVE_LATEST_LIMIT", 4)

        if test_is_string:
            _, mixed_ts, _, bad_ts = ts_status_codes
        else:
            mixed_ts, _, bad_ts, _ = ts_status_codes

        kwargs = dict(
            id=[mixed_ts.id, *random_cognite_ids(3), bad_ts.id, *random_cognite_ids(4)],
            external_id=[mixed_ts.external_id, *random_cognite_external_ids(4), bad_ts.external_id],
            before=1698537600000 + 1,  # 2023-10-29
            include_status=True,
            ignore_bad_datapoints=False,
        )
        with pytest.raises(CogniteNotFoundError, match=r"^Not found: \[\{"):
            cognite_client.time_series.data.retrieve_latest(**kwargs, ignore_unknown_ids=False)

        assert 4 == cognite_client.time_series.data._post.call_count

        res = cognite_client.time_series.data.retrieve_latest(**kwargs, ignore_unknown_ids=True)
        assert len(res) == 4  # Only 2 real identifiers (duplicated twice)

        m1, b1, m2, b2 = res
        assert m1.id == m2.id == mixed_ts.id
        assert b1.id == b2.id == bad_ts.id
        assert m1.is_string is test_is_string
        assert b1.is_string is test_is_string

        assert m1.timestamp == [1698537600000]
        assert m1.status_code == [0]  # This is empty in the JSON response
        assert m1.status_symbol == ["Good"]  # This is empty in the JSON response

        assert b1.timestamp == [1698537600000]
        assert b1.status_code == [2154168320]
        assert b1.status_symbol == ["BadDuplicateReferenceNotAllowed"]

    @pytest.mark.parametrize("test_is_string", (True, False))
    def test_effect_of_uncertain_and_bad_settings_using_same_before_setting(
        self, cognite_client, ts_status_codes, test_is_string
    ):
        if test_is_string:
            _, mixed_ts, _, bad_ts = ts_status_codes
        else:
            mixed_ts, _, bad_ts, _ = ts_status_codes

        m1, m2, m3, b1, b2, b3 = cognite_client.time_series.data.retrieve_latest(
            id=[
                mixed_ts.id,
                LatestDatapointQuery(id=mixed_ts.id, treat_uncertain_as_bad=False),
                LatestDatapointQuery(id=mixed_ts.id, ignore_bad_datapoints=False),
            ],
            external_id=[
                bad_ts.external_id,
                LatestDatapointQuery(external_id=bad_ts.external_id, treat_uncertain_as_bad=False),
                LatestDatapointQuery(external_id=bad_ts.external_id, ignore_bad_datapoints=False),
            ],
            include_status=False,
            ignore_bad_datapoints=True,
            treat_uncertain_as_bad=True,
            before=ts_to_ms("2023-08-05 12:00:00"),
        )
        assert m1.timestamp == [1691020800000]  # 2023-08-03
        assert m2.timestamp == [1691107200000]  # 2023-08-04 newer because uncertain is treated as good
        assert m3.timestamp == [1691193600000]  # 2023-08-05 even newer because bad is not ignored
        assert b3.timestamp == [1691193600000]
        assert not b1.timestamp and not b1.value
        assert not b2.timestamp and not b2.value

        if not test_is_string:
            assert math.isclose(m1.value[0], -443.7838445173604)
            assert math.isclose(m2.value[0], 792804.310084)
            assert math.isclose(m3.value[0], 1e100)
            assert math.isclose(b3.value[0], -1e100)

    @pytest.mark.parametrize("test_is_string", (True, False))
    def test_json_float_translation(self, cognite_client, ts_status_codes, test_is_string):
        if test_is_string:
            _, mixed_ts, _, bad_ts = ts_status_codes
        else:
            mixed_ts, _, bad_ts, _ = ts_status_codes

        exp_timestamps = [1691625600000, 1691798400000, 1691884800000, 1692403200000, 1692576000000, 1692835200000]
        dps_lst = cognite_client.time_series.data.retrieve_latest(
            id=[LatestDatapointQuery(id=bad_ts.id, before=ts + 1) for ts in exp_timestamps],
            ignore_bad_datapoints=False,
        )
        for dps, exp_ts in zip(dps_lst, exp_timestamps):
            assert dps.timestamp == [exp_ts]

        assert dps_lst[3].value[0] is None
        if test_is_string:
            for i, dps in enumerate(dps_lst):
                if i == 3:
                    continue  # None aka missing, checked above
                assert isinstance(dps.value[0], str)
        else:
            assert math.isclose(dps_lst[0].value[0], 2.71)
            assert math.isclose(dps_lst[2].value[0], -5e-324)
            assert math.isnan(dps_lst[4].value[0])
            assert dps_lst[1].value[0] == -math.inf
            assert dps_lst[5].value[0] == math.inf


class TestInsertDatapointsAPI:
    @pytest.mark.usefixtures("post_spy")
    def test_insert(self, cognite_client, new_ts, monkeypatch):
        datapoints = [(datetime(year=2018, month=1, day=1, hour=1, minute=i), i) for i in range(60)]
        monkeypatch.setattr(cognite_client.time_series.data, "_DPS_INSERT_LIMIT", 30)
        monkeypatch.setattr(cognite_client.time_series.data, "_POST_DPS_OBJECTS_LIMIT", 30)
        cognite_client.time_series.data.insert(datapoints, id=new_ts.id)
        assert 2 == cognite_client.time_series.data._post.call_count

    @pytest.mark.usefixtures("post_spy")
    def test_insert_before_epoch(self, cognite_client, new_ts, monkeypatch):
        datapoints = [
            (datetime(year=1950, month=1, day=1, hour=1, minute=i, tzinfo=timezone.utc), i) for i in range(60)
        ]
        monkeypatch.setattr(cognite_client.time_series.data, "_DPS_INSERT_LIMIT", 30)
        monkeypatch.setattr(cognite_client.time_series.data, "_POST_DPS_OBJECTS_LIMIT", 30)
        cognite_client.time_series.data.insert(datapoints, id=new_ts.id)
        assert 2 == cognite_client.time_series.data._post.call_count

    @pytest.mark.parametrize("endpoint_attr", ("retrieve", "retrieve_arrays"))
    @pytest.mark.usefixtures("post_spy")
    def test_insert_copy(self, cognite_client, endpoint_attr, ms_bursty_ts, new_ts, do_request_spy):
        endpoint = getattr(cognite_client.time_series.data, endpoint_attr)
        data = endpoint(id=ms_bursty_ts.id, start=0, end="now", limit=100)
        assert 100 == len(data)
        assert 1 == cognite_client.time_series.data._do_request.call_count  # needs do_request_spy
        cognite_client.time_series.data.insert(data, id=new_ts.id)
        assert 1 == cognite_client.time_series.data._post.call_count

    @pytest.mark.parametrize("endpoint_attr", ("retrieve", "retrieve_arrays"))
    def test_insert_copy_fails_at_aggregate(self, cognite_client, endpoint_attr, ms_bursty_ts, new_ts):
        endpoint = getattr(cognite_client.time_series.data, endpoint_attr)
        data = endpoint(id=ms_bursty_ts.id, end="now", granularity="1m", aggregates=random_aggregates(1), limit=100)
        assert 100 == len(data)
        with pytest.raises(ValueError, match="Only raw datapoints are supported when inserting data from"):
            cognite_client.time_series.data.insert(data, id=new_ts.id)

    def test_insert_not_found_ts(self, cognite_client, new_ts, monkeypatch):
        # From 7.35.0 to 7.37.1, 'CogniteNotFoundError.[failed, successful]' was not reported correctly:
        xid = random_cognite_external_ids(1)[0]
        dps = [
            {"id": new_ts.id, "datapoints": [(123456789, 1111111)]},
            {"external_id": xid, "datapoints": [(123456789, 6666666)]},
        ]
        # Let's make sure these two go in separate requests:
        monkeypatch.setattr(cognite_client.time_series.data, "_POST_DPS_OBJECTS_LIMIT", 1)
        with pytest.raises(CogniteNotFoundError, match=r"^Not found: \[{") as err:
            cognite_client.time_series.data.insert_multiple(dps)

        assert isinstance(err.value, CogniteNotFoundError)
        assert err.value.successful == [{"id": new_ts.id}]
        assert err.value.not_found == [{"externalId": xid}]
        assert err.value.failed == [{"externalId": xid}]

    @pytest.mark.usefixtures("post_spy")
    def test_insert_pandas_dataframe(self, cognite_client, new_ts, post_spy, monkeypatch):
        df = pd.DataFrame(
            {new_ts.id: np.random.normal(0, 1, 30)},
            index=pd.date_range(start="2018", freq="1D", periods=30),
        )
        monkeypatch.setattr(cognite_client.time_series.data, "_DPS_INSERT_LIMIT", 20)
        monkeypatch.setattr(cognite_client.time_series.data, "_POST_DPS_OBJECTS_LIMIT", 20)
        cognite_client.time_series.data.insert_dataframe(df, external_id_headers=False)
        assert 2 == cognite_client.time_series.data._post.call_count

    def test_delete_range(self, cognite_client, new_ts):
        cognite_client.time_series.data.delete_range(start="2d-ago", end="now", id=new_ts.id)

    def test_delete_range_before_epoch(self, cognite_client, new_ts):
        cognite_client.time_series.data.delete_range(start=MIN_TIMESTAMP_MS, end=0, id=new_ts.id)

    def test_delete_ranges(self, cognite_client, new_ts):
        cognite_client.time_series.data.delete_ranges([{"start": "2d-ago", "end": "now", "id": new_ts.id}])

    def test_invalid_status_code(self, cognite_client, new_ts):
        with pytest.raises(CogniteAPIError, match="^Invalid status code"):
            # code=1 is not allowed: When info type is 00, all info bits must be 0
            cognite_client.time_series.data.insert(datapoints=[(1, 3.1, 1)], id=new_ts.id)

    def test_invalid_status_symbol(self, cognite_client, new_ts):
        symbol = random.choice(("good", "uncertain", "bad"))  # should be PascalCased
        with pytest.raises(CogniteAPIError, match="^Invalid status code symbol"):
            cognite_client.time_series.data.insert(
                datapoints=[{"timestamp": 0, "value": 2.3, "status": {"symbol": symbol}}], id=new_ts.id
            )

    def test_tuples_and_dps_objects_with_status_codes__numeric_ts(self, cognite_client, new_ts):
        ts_kwargs = dict(id=new_ts.id, start=-123, limit=50, include_status=True, ignore_bad_datapoints=False)
        cognite_client.time_series.data.delete_range(id=new_ts.id, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS)
        empty_dps = cognite_client.time_series.data.retrieve(**ts_kwargs)
        assert empty_dps.timestamp == empty_dps.value == empty_dps.status_code == []

        actual_timestamp = [0, 123, 1234, 12345, 123456, 1234567, 12345678, 123456789, 1234567890]
        accepted_insert_values = [0, None, "NaN", math.nan, "-Infinity", -math.inf, "Infinity", math.inf, 1]
        actual_value = [0, None, math.nan, math.nan, -math.inf, -math.inf, math.inf, math.inf, 1]
        actual_status_codes = [
            0, 2147483648, 2147483648, 2147483648, 2147483648, 2147483648, 2147483648, 2147483648, StatusCode.Uncertain
        ]  # fmt: skip

        cognite_client.time_series.data.insert(
            id=new_ts.id,
            datapoints=[
                (-123, -1),  # no status code
                *zip(actual_timestamp, accepted_insert_values, actual_status_codes),
            ],
        )

        def assert_correct_data(to_check):
            assert to_check.value[0] == -1
            assert to_check.value[1] == actual_value[0]
            assert math.isnan(to_check.value[3]) and math.isnan(to_check.value[4])
            if isinstance(to_check, Datapoints):
                assert to_check.value[2] is None
            else:
                bad_ts = to_check.timestamp[2].item() // 1_000_000
                assert math.isnan(to_check.value[2]) and to_check.null_timestamps == {bad_ts}
                to_check.timestamp = to_check.timestamp.astype("datetime64[ms]").astype(np.int64).tolist()
            assert list(to_check.value[5:]) == actual_value[4:]
            assert list(to_check.timestamp[1:]) == actual_timestamp
            exp_status_symbols = ["Good", "Good", "Bad", "Bad", "Bad", "Bad", "Bad", "Bad", "Bad", "Uncertain"]
            assert list(to_check.status_symbol) == exp_status_symbols

        dps1 = cognite_client.time_series.data.retrieve(**ts_kwargs)
        assert_correct_data(dps1)

        cognite_client.time_series.data.delete_range(id=new_ts.id, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS)
        empty_dps = cognite_client.time_series.data.retrieve(**ts_kwargs)
        assert empty_dps.timestamp == empty_dps.value == empty_dps.status_code == []

        # Test insert Datapoints object:
        cognite_client.time_series.data.insert(id=new_ts.id, datapoints=dps1)
        dps2 = cognite_client.time_series.data.retrieve(**ts_kwargs)
        assert_correct_data(dps2)

        dps_array1 = cognite_client.time_series.data.retrieve_arrays(**ts_kwargs)
        cognite_client.time_series.data.delete_range(id=new_ts.id, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS)
        empty_dps = cognite_client.time_series.data.retrieve(**ts_kwargs)
        assert empty_dps.timestamp == empty_dps.value == empty_dps.status_code == []

        # Test insert DatapointsArray object:
        cognite_client.time_series.data.insert(id=new_ts.id, datapoints=dps_array1)
        dps_array2 = cognite_client.time_series.data.retrieve_arrays(**ts_kwargs)
        assert_correct_data(dps_array2)

    def test_tuples_and_dps_objects_with_status_codes__string_ts(self, cognite_client, new_ts_string):
        ts_kwargs = dict(id=new_ts_string.id, start=-123, limit=50, include_status=True, ignore_bad_datapoints=False)
        cognite_client.time_series.data.delete_range(id=new_ts_string.id, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS)
        empty_dps = cognite_client.time_series.data.retrieve(**ts_kwargs)
        assert empty_dps.timestamp == empty_dps.value == empty_dps.status_code == []

        sassy = "Negative, really? Where's my status code, huh"
        actual_timestamp = [0, 123, 1234, 12345, 123456, 1234567, 12345678]
        actual_value = ["0", None, "NaN", "Infinity", "-Infinity", "good-yes?", "uncertain-yes?"]
        actual_status_codes = [0, 2147483648, StatusCode.Bad, 2147483648, 2147483648, 1024, 1073741824]  # fmt: skip

        cognite_client.time_series.data.insert(
            id=new_ts_string.id,
            datapoints=[
                (-123, sassy),  # no status code
                *zip(actual_timestamp, actual_value, actual_status_codes),
            ],
        )

        def assert_correct_data(to_check):
            assert to_check.value[0] == sassy
            if isinstance(to_check, DatapointsArray):
                to_check.timestamp = to_check.timestamp.astype("datetime64[ms]").astype(np.int64).tolist()
            assert list(to_check.timestamp[1:]) == actual_timestamp
            assert list(to_check.value[1:]) == actual_value
            assert list(to_check.status_symbol) == ["Good", "Good", "Bad", "Bad", "Bad", "Bad", "Good", "Uncertain"]

        dps1 = cognite_client.time_series.data.retrieve(**ts_kwargs)
        assert_correct_data(dps1)

        cognite_client.time_series.data.delete_range(id=new_ts_string.id, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS)
        empty_dps = cognite_client.time_series.data.retrieve(**ts_kwargs)
        assert empty_dps.timestamp == empty_dps.value == empty_dps.status_code == []

        # Test insert Datapoints object:
        cognite_client.time_series.data.insert(id=new_ts_string.id, datapoints=dps1)
        dps2 = cognite_client.time_series.data.retrieve(**ts_kwargs)
        assert_correct_data(dps2)

        dps_array1 = cognite_client.time_series.data.retrieve_arrays(**ts_kwargs)
        cognite_client.time_series.data.delete_range(id=new_ts_string.id, start=MIN_TIMESTAMP_MS, end=MAX_TIMESTAMP_MS)
        empty_dps = cognite_client.time_series.data.retrieve(**ts_kwargs)
        assert empty_dps.timestamp == empty_dps.value == empty_dps.status_code == []

        # Test insert DatapointsArray object:
        cognite_client.time_series.data.insert(id=new_ts_string.id, datapoints=dps_array1)
        dps_array2 = cognite_client.time_series.data.retrieve_arrays(**ts_kwargs)
        assert_correct_data(dps_array2)

    def test_dict_format_with_status_codes_using_insert_multiple(self, cognite_client, new_ts, new_ts_string):
        cognite_client.time_series.data.delete_ranges(
            [{"id": new_ts.id, "start": 0, "end": 20}, {"id": new_ts_string.id, "start": 0, "end": 20}]
        )
        cognite_client.time_series.data.insert_multiple(
            [
                {
                    "id": new_ts.id,
                    "datapoints": [
                        {"timestamp": 0, "value": 0},
                        {"timestamp": 1, "value": 1, "status": {}},
                        {"timestamp": 2, "value": 2, "status": {"code": StatusCode.Good}},
                        {"timestamp": 3, "value": 3, "status": {"symbol": "Good"}},
                        {"timestamp": 4, "value": 4, "status": {"code": 0, "symbol": "Good"}},
                        {"timestamp": 5, "value": 5, "status": {"code": 1073741824}},
                        {"timestamp": 6, "value": 6, "status": {"symbol": "Uncertain"}},
                        {"timestamp": 7, "value": 7, "status": {"code": StatusCode.Uncertain, "symbol": "Uncertain"}},
                        {"timestamp": 8, "value": 8, "status": {"symbol": "Bad"}},
                        {"timestamp": 9, "value": 9, "status": {"code": StatusCode.Bad, "symbol": "Bad"}},
                        {"timestamp": 10, "value": None, "status": {"code": 2147483648}},
                    ],
                },
                {
                    "id": new_ts_string.id,
                    "datapoints": [
                        {"timestamp": 0, "value": "s0"},
                        {"timestamp": 1, "value": "s1", "status": {}},
                        {"timestamp": 2, "value": "s2", "status": {"code": StatusCode.Good}},
                        {"timestamp": 3, "value": "s3", "status": {"symbol": "Good"}},
                        {"timestamp": 4, "value": "s4", "status": {"code": 0, "symbol": "Good"}},
                        {"timestamp": 5, "value": "s5", "status": {"code": StatusCode.Uncertain}},
                        {"timestamp": 6, "value": "s6", "status": {"symbol": "Uncertain"}},
                        {"timestamp": 7, "value": "s7", "status": {"code": 1073741824, "symbol": "Uncertain"}},
                        {"timestamp": 8, "value": "s9", "status": {"symbol": "Bad"}},
                        {"timestamp": 9, "value": "s10", "status": {"code": 2147483648, "symbol": "Bad"}},
                        {"timestamp": 10, "value": None, "status": {"code": StatusCode.Bad}},
                    ],
                },
            ]
        )
        dps_numeric, dps_str = cognite_client.time_series.data.retrieve(
            id=[new_ts.id, new_ts_string.id], end=20, include_status=True, ignore_bad_datapoints=False
        )
        # Superficial tests here; well covered elsewhere:
        assert dps_numeric.timestamp == dps_str.timestamp == list(range(11))
        assert dps_numeric.value and dps_str.value
        assert None in dps_numeric.value and None in dps_str.value
        assert dps_numeric.status_code == dps_str.status_code
        assert dps_numeric.status_symbol == dps_str.status_symbol
        assert set(dps_numeric.status_symbol) == {"Good", "Uncertain", "Bad"}

    def test_insert_retrieve_delete_datapoints_with_instance_id(
        self, cognite_client: CogniteClient, instance_ts_id: NodeId
    ) -> None:
        v1, v2 = random_cognite_ids(2)
        cognite_client.time_series.data.insert([(0, v1), (1.0, v2)], instance_id=instance_ts_id)
        retrieved = cognite_client.time_series.data.retrieve(instance_id=instance_ts_id, start=0, end=2)

        assert retrieved.timestamp == [0, 1]
        assert retrieved.value == [v1, v2]

        cognite_client.time_series.data.delete_range(0, 2, instance_id=instance_ts_id)
        retrieved = cognite_client.time_series.data.retrieve(instance_id=instance_ts_id, start=0, end=2)

        assert retrieved.timestamp == []

    def test_insert_multiple_with_instance_id(self, cognite_client: CogniteClient, instance_ts_id: NodeId) -> None:
        ts, value = random.choices(range(MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS + 1), k=2)
        cognite_client.time_series.data.insert_multiple(
            [{"instance_id": instance_ts_id, "datapoints": [{"timestamp": ts, "value": value}]}]
        )
        retrieved = cognite_client.time_series.data.retrieve(instance_id=instance_ts_id, start=ts, end=ts + 1)

        assert retrieved.timestamp == [ts]
        assert retrieved.value == [value]
