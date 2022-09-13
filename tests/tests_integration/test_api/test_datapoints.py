"""
Note: If tests related to fetching datapoints are broken, all time series + their datapoints can be
      recreated easily by running the file linked below. You will need to provide a valid set of
      credentials to the `CogniteClient` for the Python SDK integration test CDF project:
>>> python scripts/create_ts_for_integration_tests.py
"""
import itertools
import random
from unittest.mock import patch

import numpy as np
import pytest

from cognite.client._api.datapoint_constants import DPS_LIMIT  # DPS_LIMIT_AGG,
from cognite.client.data_classes import (
    Datapoints,
    DatapointsArray,
    DatapointsArrayList,
    DatapointsList,
    DatapointsQuery,
    TimeSeries,
)
from cognite.client.utils._time import MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS
from tests.utils import set_max_workers

DATAPOINTS_API = "cognite.client._api.datapoints.{}"


@pytest.fixture(scope="session")
def all_test_time_series(cognite_client):
    return cognite_client.time_series.retrieve_multiple(
        external_ids=[
            "PYSDK integration test 001: outside points, numeric",
            "PYSDK integration test 002: outside points, string",
            *[f"PYSDK integration test {i:03d}: weekly values, 1950-2000, numeric" for i in range(3, 54)],
            *[f"PYSDK integration test {i:03d}: weekly values, 1950-2000, string" for i in range(54, 104)],
        ]
    )


@pytest.fixture
def outside_points_ts(all_test_time_series):
    return all_test_time_series[:2]


@pytest.fixture
def weekly_dps_ts(all_test_time_series):
    return all_test_time_series[2:53], all_test_time_series[53:103]


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


PARAMETRIZED_VALUES_OUTSIDE_POINTS = [
    (-100, 100, False, True),
    (-99, 100, True, True),
    (-99, 101, True, False),
    (-100, 101, False, False),
]


class TestRetrieveDatapoints:
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

    def test_a(self):
        pass

    def test_b(self):
        pass

    def test_c(self):
        pass

    def test_d(self):
        pass

    def test_e(self):
        pass


# def has_expected_timestamp_spacing(df: pd.DataFrame, granularity: str):
#     timestamps = df.index.values.astype("datetime64[ms]").astype(np.int64)
#     deltas = np.diff(timestamps)
#     granularity_ms = granularity_to_ms(granularity)
#     return np.all(deltas != 0) and np.all(deltas % granularity_ms == 0)
#
#
# @pytest.fixture
# def post_spy(cognite_client):
#     with patch.object(cognite_client.time_series.data, "_post", wraps=cognite_client.time_series.data._post):
#         yield


# class TestRetrieveDatapointsAPI:
#     @pytest.mark.parametrize(
#         "endpoint_attr, res_exp_type", [("retrieve", Datapoints), ("retrieve_arrays", DatapointsArray)]
#     )
#     def test_retrieve(self, cognite_client, test_time_series, endpoint_attr, res_exp_type):
#         ts = test_time_series[0]
#         endpoint = getattr(cognite_client.time_series.data, endpoint_attr)
#         dps = endpoint(id=ts.id, start=0, end="now", limit=50)
#         assert isinstance(dps, res_exp_type)
#         assert len(dps) > 0
#
#     @pytest.mark.parametrize("endpoint_attr", ["retrieve", "retrieve_arrays"])
#     def test_retrieve_before_epoch(self, cognite_client, test_time_series, endpoint_attr):
#         ts = test_time_series[0]
#         endpoint = getattr(cognite_client.time_series.data, endpoint_attr)
#         dps = endpoint(id=ts.id, start=MIN_TIMESTAMP_MS, end="now", limit=50)
#         assert len(dps) > 0
#
#     @pytest.mark.parametrize(
#         "endpoint_attr, res_exp_type", [("retrieve", DatapointsList), ("retrieve_arrays", DatapointsArrayList)]
#     )
#     def test_retrieve_unknown(self, cognite_client, test_time_series, endpoint_attr, res_exp_type):
#         ts = test_time_series[0]
#         endpoint = getattr(cognite_client.time_series.data, endpoint_attr)
#         dps_lst = endpoint(id=[ts.id, 42], start=0, end="now", ignore_unknown_ids=True, limit=50)
#         assert isinstance(dps_lst, res_exp_type)
#         assert 1 == len(dps_lst)
#         assert 50 == len(dps_lst[0])
#
#     @pytest.mark.parametrize(
#         "endpoint_attr, res_exp_type", [("retrieve", DatapointsList), ("retrieve_arrays", DatapointsArrayList)]
#     )
#     def test_retrieve_all_unknown(self, cognite_client, test_time_series, endpoint_attr, res_exp_type):
#         endpoint = getattr(cognite_client.time_series.data, endpoint_attr)
#         dps_lst = endpoint(id=[42], external_id="missing", start="1d-ago", end="now", ignore_unknown_ids=True)
#         assert isinstance(dps_lst, res_exp_type)
#         assert 0 == len(dps_lst)
#
#     @pytest.mark.parametrize("endpoint_attr", ["retrieve", "retrieve_arrays"])
#     def test_retrieve_all_unknown_single(self, cognite_client, test_time_series, endpoint_attr):
#         endpoint = getattr(cognite_client.time_series.data, endpoint_attr)
#         res = endpoint(external_id="missing", start="1d-ago", end="now", ignore_unknown_ids=True)
#         assert res is None
#
#     @pytest.mark.parametrize("endpoint_attr", ["retrieve", "retrieve_arrays"])
#     def test_retrieve_multiple_aggregates(self, cognite_client, test_time_series, endpoint_attr):
#         id1, id2, id3 = [test_time_series[i].id for i in range(3)]
#         ids = [id1, id2, {"id": id3, "aggregates": ["max"]}]
#
#         endpoint = getattr(cognite_client.time_series.data, endpoint_attr)
#         dps = endpoint(id=ids, start="6h-ago", end="now", aggregates=["min"], granularity="1s")
#         df = dps.to_pandas(column_names="id")
#         assert (df.columns == [f"{id1}|min", f"{id2}|min", f"{id3}|max"]).all()
#         assert not df.empty
#         assert has_expected_timestamp_spacing(df, "1s")
#         assert all(dp.is_step is not None for dp in dps)
#         assert all(dp.is_string is not None for dp in dps)
#
#     @pytest.mark.parametrize("endpoint_attr", ["retrieve", "retrieve_arrays"])
#     def test_retrieve_nothing(self, cognite_client, test_time_series, endpoint_attr):
#         endpoint = getattr(cognite_client.time_series.data, endpoint_attr)
#         dpl = endpoint(id=test_time_series[0].id, start=0, end=1)
#         assert 0 == len(dpl)
#         assert dpl.is_step is not None
#         assert dpl.is_string is not None
#
#     @pytest.mark.parametrize("endpoint_attr", ["retrieve", "retrieve_arrays"])
#     def test_retrieve_multiple_raise_exception_invalid_id(self, cognite_client, test_time_series, endpoint_attr):
#         with pytest.raises(CogniteAPIError, match="Exactly one of 'id' or 'externalId' must be set"):
#             endpoint = getattr(cognite_client.time_series.data, endpoint_attr)
#             endpoint(
#                 id=[test_time_series[0].id, test_time_series[1].id, 0],
#                 start="1m-ago",
#                 end="now",
#                 aggregates=["min"],
#                 granularity="1s",
#             )
#
#     @pytest.mark.parametrize("endpoint_attr", ["retrieve", "retrieve_arrays"])
#     def test_retrieve_include_outside_points(self, cognite_client, test_time_series, endpoint_attr):
#         endpoint = getattr(cognite_client.time_series.data, endpoint_attr)
#         kwargs = dict(id=test_time_series[0].id, start=timestamp_to_ms("360m-ago"), end=timestamp_to_ms("359m-ago"))
#         outside_yes = endpoint(include_outside_points=True, **kwargs)
#         outside_noo = endpoint(include_outside_points=False, **kwargs)
#
#         assert len(outside_yes.timestamp) == len(set(outside_yes.timestamp))
#         # TODO: fix check after making test non-relative in time:
#         assert len(outside_noo) + 1 <= len(outside_yes) <= len(outside_noo) + 2
#
#     def test_retrieve_include_outside_points_with_limit(self, cognite_client, test_time_series, post_spy):
#         kwargs = dict(
#             id=test_time_series[0].id,
#             start=timestamp_to_ms("156w-ago"),
#             end=timestamp_to_ms("1d-ago"),
#             limit=1234,
#         )
#         dps = cognite_client.time_series.data.retrieve(**kwargs)
#
#         with set_request_limit(cognite_client.time_series.data, limit=250):
#             outside_included = cognite_client.time_series.data.retrieve(include_outside_points=True, **kwargs)
#
#         assert len(outside_included) == len(dps) + 1
#         assert outside_included.to_pandas().duplicated().sum() == 0
#         outside_timestamps = set(outside_included.timestamp) - set(dps.timestamp)
#         assert len(outside_timestamps) == 1
#
#     def test_retrieve_include_outside_points_paginate_outside_exists(self, cognite_client, test_time_series, post_spy):
#         # Arrange
#         start, end = timestamp_to_ms("720m-ago"), timestamp_to_ms("710m-ago")
#         kwargs = dict(id=test_time_series[0].id, start=start, end=end)
#         dps = cognite_client.time_series.data.retrieve(**kwargs)
#
#         # Act
#         with set_request_limit(cognite_client.time_series.data, limit=2_500):
#             outside_included = cognite_client.time_series.data.retrieve(include_outside_points=True, **kwargs)
#
#         outside_timestamps = sorted(set(outside_included.timestamp) - set(dps.timestamp))
#         assert len(outside_timestamps) == 2
#         assert outside_timestamps[0] < start
#         assert outside_timestamps[1] > end
#         assert set(outside_included.timestamp) - set(outside_timestamps) == set(dps.timestamp)
#
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
#     def test_retrieve_string(self, cognite_client):
#         dps = cognite_client.time_series.data.retrieve(external_id="test__string_b", start="48h-ago", end="45h-ago")
#         assert len(dps) > 10_000
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
