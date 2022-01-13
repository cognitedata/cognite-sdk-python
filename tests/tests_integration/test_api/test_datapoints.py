import re
from datetime import datetime, timedelta
from unittest import mock

import numpy
import pandas
import pytest

from cognite.client import utils
from cognite.client.data_classes import DatapointsList, DatapointsQuery, TimeSeries
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._time import timestamp_to_ms
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


def has_duplicates(df: pandas.DataFrame):
    return df.duplicated().any()


def has_correct_timestamp_spacing(df: pandas.DataFrame, granularity: str):
    timestamps = df.index.values.astype("datetime64[ms]").astype("int64")
    deltas = numpy.diff(timestamps, 1)
    granularity_ms = utils._time.granularity_to_ms(granularity)
    return (deltas != 0).all() and (deltas % granularity_ms == 0).all()


@pytest.fixture
def post_spy(cognite_client):
    with mock.patch.object(cognite_client.datapoints, "_post", wraps=cognite_client.datapoints._post) as _:
        yield


class TestDatapointsAPI:
    def test_retrieve(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        dps = cognite_client.datapoints.retrieve(id=ts.id, start="1d-ago", end="now")
        assert len(dps) > 0

    def test_retrieve_unknown(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        dps = cognite_client.datapoints.retrieve(id=[ts.id] + [42], start="1d-ago", end="now", ignore_unknown_ids=True)
        assert 1 == len(dps)

    def test_retrieve_all_unknown(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        dps = cognite_client.datapoints.retrieve(
            id=[42], external_id="missing", start="1d-ago", end="now", ignore_unknown_ids=True
        )
        assert isinstance(dps, DatapointsList)
        assert 0 == len(dps)

    def test_retrieve_all_unknown_single(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        dps = cognite_client.datapoints.retrieve(
            external_id="missing", start="1d-ago", end="now", ignore_unknown_ids=True
        )
        assert dps is None

    def test_retrieve_multiple(self, cognite_client, test_time_series):
        ids = [test_time_series[0].id, test_time_series[1].id, {"id": test_time_series[2].id, "aggregates": ["max"]}]

        dps = cognite_client.datapoints.retrieve(
            id=ids, start="6h-ago", end="now", aggregates=["min"], granularity="1s"
        )
        df = dps.to_pandas(column_names="id")
        assert "{}|min".format(test_time_series[0].id) in df.columns
        assert "{}|min".format(test_time_series[1].id) in df.columns
        assert "{}|max".format(test_time_series[2].id) in df.columns
        assert 0 < df.shape[0]
        assert 3 == df.shape[1]
        assert has_correct_timestamp_spacing(df, "1s")
        for dpl in dps:
            assert dpl.is_step is not None
            assert dpl.is_string is not None

    def test_retrieve_nothing(self, cognite_client, test_time_series):
        dpl = cognite_client.datapoints.retrieve(id=test_time_series[0].id, start=0, end=1)
        assert 0 == len(dpl)
        assert dpl.is_step is not None
        assert dpl.is_string is not None

    def test_retrieve_multiple_with_exception(self, cognite_client, test_time_series):
        with pytest.raises(CogniteAPIError):
            cognite_client.datapoints.retrieve(
                id=[test_time_series[0].id, test_time_series[1].id, 0],
                start="1m-ago",
                end="now",
                aggregates=["min"],
                granularity="1s",
            )

    def test_stop_pagination_in_time(self, cognite_client, test_time_series, post_spy):
        lim = 152225
        ts = test_time_series[0]
        dps = cognite_client.datapoints.retrieve(id=ts.id, start=0, end="now", limit=lim)
        assert lim == len(dps)
        # first page 100k, counts 1, paginate 2 windows (+1 potential for 1st day uncertainty)
        assert 3 <= cognite_client.datapoints._post.call_count <= 4

    def test_retrieve_include_outside_points(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        start = utils._time.timestamp_to_ms("6h-ago")
        end = utils._time.timestamp_to_ms("1h-ago")
        dps_wo_outside = cognite_client.datapoints.retrieve(
            id=ts.id, start=start, end=end, include_outside_points=False
        )
        dps_w_outside = cognite_client.datapoints.retrieve(id=ts.id, start=start, end=end, include_outside_points=True)
        assert not has_duplicates(dps_w_outside.to_pandas())
        assert len(dps_wo_outside) + 1 <= len(dps_w_outside) <= len(dps_wo_outside) + 2

    def test_retrieve_include_outside_points_paginate_no_outside(self, cognite_client, test_time_series, post_spy):
        ts = test_time_series[0]
        start = utils._time.timestamp_to_ms("156w-ago")
        end = utils._time.timestamp_to_ms("1h-ago")
        test_lim = 250
        dps_non_outside = cognite_client.datapoints.retrieve(id=ts.id, start=start, end=end, limit=1234)

        tmp = cognite_client.datapoints._DPS_LIMIT
        cognite_client.datapoints._DPS_LIMIT = test_lim
        dps = cognite_client.datapoints.retrieve(
            id=ts.id, start=start, end=end, include_outside_points=True, limit=1234
        )
        cognite_client.datapoints._DPS_LIMIT = tmp
        assert len(dps) == len(dps_non_outside)
        assert not has_duplicates(dps.to_pandas())
        ts_outside = set(dps.timestamp) - set(dps_non_outside.timestamp)
        assert 0 == len(ts_outside)
        assert set(dps.timestamp) == set(dps_non_outside.timestamp)

    def test_retrieve_include_outside_points_paginate_outside_exists(self, cognite_client, test_time_series, post_spy):
        ts = test_time_series[0]
        start = utils._time.timestamp_to_ms("12h-ago")
        end = utils._time.timestamp_to_ms("1h-ago")
        test_lim = 2500
        dps_non_outside = cognite_client.datapoints.retrieve(id=ts.id, start=start, end=end)
        tmp = cognite_client.datapoints._DPS_LIMIT
        cognite_client.datapoints._DPS_LIMIT = test_lim
        dps = cognite_client.datapoints.retrieve(id=ts.id, start=start, end=end, include_outside_points=True)
        cognite_client.datapoints._DPS_LIMIT = tmp
        ts_outside = set(dps.timestamp) - set(dps_non_outside.timestamp)
        assert 2 == len(ts_outside)
        for ts in ts_outside:
            assert ts < timestamp_to_ms(start) or ts >= timestamp_to_ms(end)
        assert set(dps.timestamp) - ts_outside == set(dps_non_outside.timestamp)

    def test_retrieve_dataframe(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        df = cognite_client.datapoints.retrieve_dataframe(
            id=ts.id, start="6h-ago", end="now", aggregates=["average"], granularity="1s"
        )
        assert df.shape[0] > 0
        assert df.shape[1] == 1
        assert has_correct_timestamp_spacing(df, "1s")

    def test_retrieve_dataframe_missing(self, cognite_client, test_time_series):
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
        assert len(dps) > 100000

    def test_query(self, cognite_client, test_time_series):
        dps_query1 = DatapointsQuery(id=test_time_series[0].id, start="6h-ago", end="now")
        dps_query2 = DatapointsQuery(id=test_time_series[1].id, start="3h-ago", end="now")
        dps_query3 = DatapointsQuery(
            id=test_time_series[2].id, start="1d-ago", end="now", aggregates=["average"], granularity="1h"
        )

        res = cognite_client.datapoints.query([dps_query1, dps_query2, dps_query3])
        assert len(res) == 3
        assert len(res[2][0]) < len(res[1][0]) < len(res[0][0])

    def test_query_unknown(self, cognite_client, test_time_series):
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

    def test_retrieve_latest_unknown(self, cognite_client, test_time_series):
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
        tmp = cognite_client.datapoints._RETRIEVE_LATEST_LIMIT
        cognite_client.datapoints._RETRIEVE_LATEST_LIMIT = 10
        res = cognite_client.datapoints.retrieve_latest(id=ids, ignore_unknown_ids=True)
        cognite_client.datapoints._RETRIEVE_LATEST_LIMIT = tmp
        assert set(dps.id for dps in res).issubset(set(ids))
        assert 2 == cognite_client.datapoints._post.call_count
        for dps in res:
            assert len(dps) <= 1  # could be empty

    def test_retrieve_latest_before(self, cognite_client, test_time_series):
        ts = test_time_series[0]
        res = cognite_client.datapoints.retrieve_latest(id=ts.id, before="1h-ago")
        assert 1 == len(res)
        assert res[0].timestamp < utils._time.timestamp_to_ms("1h-ago")

    def test_insert(self, cognite_client, new_ts, post_spy):
        datapoints = [(datetime(year=2018, month=1, day=1, hour=1, minute=i), i) for i in range(60)]
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
