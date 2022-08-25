import re
from datetime import datetime, timedelta
from unittest import mock

import numpy
import pandas
import pytest

from cognite.client.data_classes import Datapoints, DatapointsList, DatapointsQuery, TimeSeries
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._time import MIN_TIMESTAMP_MS, granularity_to_ms, timestamp_to_ms
from tests.utils import set_request_limit


@pytest.fixture(scope="session")
def test_time_series(cognite_client):
    eids = ["test__constant_%d_with_noise" % i for i in range(10)]
    ts = cognite_client.time_series.retrieve_multiple(external_ids=eids, ignore_unknown_ids=True)
    yield {int(re.match(r"test__constant_(\d+)_with_noise", t.name).group(1)): t for t in ts}


@pytest.fixture(scope="session")
def new_ts(cognite_client):
    ts = cognite_client.time_series.create(TimeSeries())
    yield ts
    cognite_client.time_series.delete(id=ts.id)
    assert cognite_client.time_series.retrieve(ts.id) is None


def has_expected_timestamp_spacing(df: pandas.DataFrame, granularity: str):
    timestamps = df.index.values.astype("datetime64[ms]").astype("int64")
    deltas = numpy.diff(timestamps, 1)
    granularity_ms = granularity_to_ms(granularity)
    return (deltas != 0).all() and (deltas % granularity_ms == 0).all()


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.datapoints, "_post", wraps=cognite_client.datapoints._post) as _:
        yield


class TestDatapointsAPI:
    def test_retrieve(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        dps = cognite_client.datapoints.retrieve(id=ts.id, start="1d-ago", end="now")
        assert isinstance(dps, Datapoints)
        assert len(dps) > 0

    def test_retrieve_before_epoch(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        dps = cognite_client.datapoints.retrieve(id=ts.id, start=MIN_TIMESTAMP_MS, end="now")
        assert len(dps) > 0

    def test_retrieve_unknown(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        dps = cognite_client.datapoints.retrieve(id=[ts.id] + [42], start="1d-ago", end="now", ignore_unknown_ids=True)
        assert isinstance(dps, DatapointsList)
        assert 1 == len(dps)

    def test_retrieve_all_unknown(self, cognite_client, test_time_series):
        dps = cognite_client.datapoints.retrieve(
            id=[42], external_id="missing", start="1d-ago", end="now", ignore_unknown_ids=True
        )
        assert isinstance(dps, DatapointsList)
        assert 0 == len(dps)

    def test_retrieve_all_unknown_single(self, cognite_client, test_time_series):
        dps = cognite_client.datapoints.retrieve(
            external_id="missing", start="1d-ago", end="now", ignore_unknown_ids=True
        )
        assert dps is None

    def test_retrieve_multiple_aggregates(self, cognite_client, test_time_series):
        id1, id2, id3 = [test_time_series[i].id for i in range(3)]
        ids = [id1, id2, {"id": id3, "aggregates": ["max"]}]

        dps = cognite_client.datapoints.retrieve(
            id=ids, start="6h-ago", end="now", aggregates=["min"], granularity="1s"
        )
        df = dps.to_pandas(column_names="id")
        assert sorted(df.columns) == sorted([f"{id1}|min", f"{id2}|min", f"{id3}|max"])
        assert len(df) > 0
        assert has_expected_timestamp_spacing(df, "1s")
        assert all(dp.is_step is not None for dp in dps)
        assert all(dp.is_string is not None for dp in dps)

    def test_retrieve_nothing(self, cognite_client, test_time_series):
        dpl = cognite_client.datapoints.retrieve(id=test_time_series[0].id, start=0, end=1)
        assert 0 == len(dpl)
        assert dpl.is_step is not None
        assert dpl.is_string is not None

    def test_retrieve_multiple_raise_exception_invalid_id(self, cognite_client, test_time_series):
        with pytest.raises(CogniteAPIError):
            cognite_client.datapoints.retrieve(
                id=[test_time_series[0].id, test_time_series[1].id, 0],
                start="1m-ago",
                end="now",
                aggregates=["min"],
                granularity="1s",
            )

    def test_stop_pagination_in_time(self, cognite_client, test_time_series, post_spy):
        limit = 152_225
        ts = test_time_series[0]
        dps = cognite_client.datapoints.retrieve(id=ts.id, start=0, end="now", limit=limit)
        assert limit == len(dps)
        # Todo Adjust lower limit with new implementation
        # first page 100k, counts 1, paginate 2 windows (+1 potential for 1st day uncertainty)
        assert 3 <= cognite_client.datapoints._post.call_count <= 4

    def test_retrieve_include_outside_points(self, cognite_client, test_time_series):
        kwargs = dict(id=test_time_series[0].id, start=timestamp_to_ms("6h-ago"), end=timestamp_to_ms("1h-ago"))
        dps = cognite_client.datapoints.retrieve(include_outside_points=False, **kwargs)

        outside_included = cognite_client.datapoints.retrieve(include_outside_points=True, **kwargs).to_pandas()

        assert outside_included.duplicated().sum() == 0
        assert len(dps) + 1 <= len(outside_included) <= len(dps) + 2

    def test_retrieve_include_outside_points_with_limit(self, cognite_client, test_time_series, post_spy):
        # Todo Remove tests to get consistent behavior with API
        kwargs = dict(
            id=test_time_series[0].id,
            start=timestamp_to_ms("156w-ago"),
            end=timestamp_to_ms("1h-ago"),
            limit=1234,
        )
        dps = cognite_client.datapoints.retrieve(**kwargs)

        with set_request_limit(cognite_client.datapoints, limit=250):
            outside_included = cognite_client.datapoints.retrieve(include_outside_points=True, **kwargs)

        assert len(outside_included) == len(dps)
        assert outside_included.to_pandas().duplicated().sum() == 0
        outside_timestamps = set(outside_included.timestamp) - set(dps.timestamp)
        assert len(outside_timestamps) == 0
        assert set(outside_included.timestamp) == set(dps.timestamp)

    def test_retrieve_include_outside_points_paginate_outside_exists(self, cognite_client, test_time_series, post_spy):
        # Arrange
        start, end = timestamp_to_ms("12h-ago"), timestamp_to_ms("1h-ago")
        kwargs = dict(id=test_time_series[0].id, start=start, end=end)
        dps = cognite_client.datapoints.retrieve(**kwargs)

        # Act
        with set_request_limit(cognite_client.datapoints, limit=2_500):
            outside_included = cognite_client.datapoints.retrieve(include_outside_points=True, **kwargs)

        # Assert
        outside_timestamps = sorted(set(outside_included.timestamp) - set(dps.timestamp))
        assert len(outside_timestamps) == 2
        assert outside_timestamps[0] < start
        assert outside_timestamps[1] > end
        assert set(outside_included.timestamp) - set(outside_timestamps) == set(dps.timestamp)

    def test_retrieve_dataframe(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        df = cognite_client.datapoints.retrieve_dataframe(
            id=ts.id, start="6h-ago", end="now", aggregates=["average"], granularity="1s"
        )
        assert df.shape[0] > 0
        assert df.shape[1] == 1
        assert has_expected_timestamp_spacing(df, "1s")

    def test_retrieve_dataframe_one_missing(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        df = cognite_client.datapoints.retrieve_dataframe(
            id=ts.id,
            external_id="missing",
            start="6h-ago",
            end="now",
            aggregates=["average"],
            granularity="1s",
            ignore_unknown_ids=True,
        )
        assert df.shape[0] > 0
        assert df.shape[1] == 1

    def test_retrieve_string(self, cognite_client):
        dps = cognite_client.datapoints.retrieve(external_id="test__string_b", start=1563000000000, end=1564100000000)
        assert len(dps) > 100_000

    def test_query(self, cognite_client, test_time_series):
        dps_query1 = DatapointsQuery(id=test_time_series[0].id, start="6h-ago", end="now")
        dps_query2 = DatapointsQuery(id=test_time_series[1].id, start="3h-ago", end="now")
        dps_query3 = DatapointsQuery(
            id=test_time_series[2].id, start="1d-ago", end="now", aggregates=["average"], granularity="1h"
        )

        res = cognite_client.datapoints.query([dps_query1, dps_query2, dps_query3])
        assert len(res) == 3
        assert len(res[2][0]) < len(res[1][0]) < len(res[0][0])

    def test_query_two_unknown(self, cognite_client, test_time_series):
        dps_query1 = DatapointsQuery(id=test_time_series[0].id, start="6h-ago", end="now", ignore_unknown_ids=True)
        dps_query2 = DatapointsQuery(id=123, start="3h-ago", end="now", ignore_unknown_ids=True)
        dps_query3 = DatapointsQuery(
            external_id="missing time series",
            start="1d-ago",
            end="now",
            aggregates=["average"],
            granularity="1h",
            ignore_unknown_ids=True,
        )
        res = cognite_client.datapoints.query([dps_query1, dps_query2, dps_query3])
        assert len(res) == 3
        assert len(res[0]) == 1
        assert len(res[0][0]) > 0
        assert len(res[1]) == 0
        assert len(res[2]) == 0

    def test_retrieve_latest(self, cognite_client, test_time_series):
        ids = [test_time_series[0].id, test_time_series[1].id]
        res = cognite_client.datapoints.retrieve_latest(id=ids)
        for dps in res:
            assert 1 == len(dps)

    def test_retrieve_latest_two_unknown(self, cognite_client, test_time_series):
        ids = [test_time_series[0].id, test_time_series[1].id, 42, 1337]
        res = cognite_client.datapoints.retrieve_latest(id=ids, ignore_unknown_ids=True)
        assert 2 == len(res)
        for dps in res:
            assert 1 == len(dps)

    def test_retrieve_latest_many(self, cognite_client, test_time_series, post_spy):
        ids = [
            t.id for t in cognite_client.time_series.list(limit=12) if not t.security_categories
        ]  # more than one page
        assert len(ids) > 10

        with set_request_limit(cognite_client.datapoints, limit=10):
            res = cognite_client.datapoints.retrieve_latest(id=ids, ignore_unknown_ids=True)

        assert {dps.id for dps in res}.issubset(set(ids))
        assert 2 == cognite_client.datapoints._post.call_count
        for dps in res:
            assert len(dps) <= 1  # could be empty

    def test_retrieve_latest_before(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        res = cognite_client.datapoints.retrieve_latest(id=ts.id, before="1h-ago")
        assert 1 == len(res)
        assert res[0].timestamp < timestamp_to_ms("1h-ago")

    def test_insert(self, cognite_client, new_ts, post_spy):
        datapoints = [(datetime(year=2018, month=1, day=1, hour=1, minute=i), i) for i in range(60)]
        with set_request_limit(cognite_client.datapoints, 30):
            cognite_client.datapoints.insert(datapoints, id=new_ts.id)
        assert 2 == cognite_client.datapoints._post.call_count

    def test_insert_before_epoch(self, cognite_client, new_ts, post_spy):
        datapoints = [(datetime(year=1950, month=1, day=1, hour=1, minute=i), i) for i in range(60)]
        with set_request_limit(cognite_client.datapoints, 30):
            cognite_client.datapoints.insert(datapoints, id=new_ts.id)
        assert 2 == cognite_client.datapoints._post.call_count

    def test_insert_copy(self, cognite_client, test_time_series, new_ts, post_spy):
        data = cognite_client.datapoints.retrieve(id=test_time_series[0].id, start="600d-ago", end="now", limit=100)
        assert 100 == len(data)
        cognite_client.datapoints.insert(data, id=new_ts.id)
        assert 2 == cognite_client.datapoints._post.call_count

    def test_insert_pandas_dataframe(self, cognite_client, new_ts, post_spy):
        start = datetime(2018, 1, 1)
        x = pandas.DatetimeIndex([start + timedelta(days=d) for d in range(100)])
        y = numpy.random.normal(0, 1, 100)
        df = pandas.DataFrame({new_ts.id: y}, index=x)
        with set_request_limit(cognite_client.datapoints, 50):
            cognite_client.datapoints.insert_dataframe(df)
        assert 2 == cognite_client.datapoints._post.call_count

    def test_delete_range(self, cognite_client, new_ts):
        cognite_client.datapoints.delete_range(start="2d-ago", end="now", id=new_ts.id)

    def test_delete_range_before_epoch(self, cognite_client, new_ts):
        cognite_client.datapoints.delete_range(start=MIN_TIMESTAMP_MS, end="now", id=new_ts.id)

    def test_delete_ranges(self, cognite_client, new_ts):
        cognite_client.datapoints.delete_ranges([{"start": "2d-ago", "end": "now", "id": new_ts.id}])

    def test_retrieve_dataframe_dict(self, cognite_client, test_time_series):
        dfd = cognite_client.datapoints.retrieve_dataframe_dict(
            id=[test_time_series[0].id, 42],
            external_id=["missing time series", test_time_series[1].external_id],
            aggregates=["count", "interpolation"],
            start=0,
            end="now",
            limit=100,
            granularity="1m",
            ignore_unknown_ids=True,
        )
        assert isinstance(dfd, dict)
        assert 2 == len(dfd.keys())
        assert dfd["interpolation"].shape[0] > 0
        assert dfd["interpolation"].shape[1] == 2

        assert dfd["count"].shape[0] > 0
        assert dfd["count"].shape[1] == 2
