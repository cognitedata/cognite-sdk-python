import math
import re
from datetime import datetime, timedelta
from unittest import mock

import numpy
import pandas
import pytest

from cognite.client import CogniteClient, utils
from cognite.client.data_classes import DatapointsList, DatapointsQuery, TimeSeries
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._time import timestamp_to_ms
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="session")
def test_time_series():
    eids = ["test__constant_%d_with_noise" % i for i in range(10)]
    ts = COGNITE_CLIENT.time_series.retrieve_multiple(external_ids=eids, ignore_unknown_ids=True)
    yield {int(re.match(r"test__constant_(\d+)_with_noise", t.name).group(1)): t for t in ts}


@pytest.fixture(scope="session")
def new_ts():
    ts = COGNITE_CLIENT.time_series.create(TimeSeries())
    yield ts
    COGNITE_CLIENT.time_series.delete(id=ts.id)
    assert COGNITE_CLIENT.time_series.retrieve(ts.id) is None


def has_duplicates(df: pandas.DataFrame):
    return df.duplicated().any()


def has_correct_timestamp_spacing(df: pandas.DataFrame, granularity: str):
    timestamps = df.index.values.astype("datetime64[ms]").astype("int64")
    deltas = numpy.diff(timestamps, 1)
    granularity_ms = utils._time.granularity_to_ms(granularity)
    return (deltas != 0).all() and (deltas % granularity_ms == 0).all()


@pytest.fixture
def post_spy():
    with mock.patch.object(COGNITE_CLIENT.datapoints, "_post", wraps=COGNITE_CLIENT.datapoints._post) as _:
        yield


class TestDatapointsAPI:
    def test_retrieve(self, test_time_series):
        ts = test_time_series[0]
        dps = COGNITE_CLIENT.datapoints.retrieve(id=ts.id, start="1d-ago", end="now")
        assert len(dps) > 0

    def test_retrieve_unknown(self, test_time_series):
        ts = test_time_series[0]
        dps = COGNITE_CLIENT.datapoints.retrieve(id=[ts.id] + [42], start="1d-ago", end="now", ignore_unknown_ids=True)
        assert 1 == len(dps)

    def test_retrieve_all_unknown(self, test_time_series):
        ts = test_time_series[0]
        dps = COGNITE_CLIENT.datapoints.retrieve(
            id=[42], external_id="missing", start="1d-ago", end="now", ignore_unknown_ids=True
        )
        assert isinstance(dps, DatapointsList)
        assert 0 == len(dps)

    def test_retrieve_all_unknown_single(self, test_time_series):
        ts = test_time_series[0]
        dps = COGNITE_CLIENT.datapoints.retrieve(
            external_id="missing", start="1d-ago", end="now", ignore_unknown_ids=True
        )
        assert dps is None

    def test_retrieve_multiple(self, test_time_series):
        ids = [test_time_series[0].id, test_time_series[1].id, {"id": test_time_series[2].id, "aggregates": ["max"]}]

        dps = COGNITE_CLIENT.datapoints.retrieve(
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

    def test_retrieve_nothing(self, test_time_series):
        dpl = COGNITE_CLIENT.datapoints.retrieve(id=test_time_series[0].id, start=0, end=1)
        assert 0 == len(dpl)
        assert dpl.is_step is not None
        assert dpl.is_string is not None

    def test_retrieve_multiple_with_exception(self, test_time_series):
        with pytest.raises(CogniteAPIError):
            COGNITE_CLIENT.datapoints.retrieve(
                id=[test_time_series[0].id, test_time_series[1].id, 0],
                start="1m-ago",
                end="now",
                aggregates=["min"],
                granularity="1s",
            )

    def test_stop_pagination_in_time(self, test_time_series, post_spy):
        ts = test_time_series[0]
        dps = COGNITE_CLIENT.datapoints.retrieve(id=ts.id, start=0, end="now", limit=223456)
        assert 223456 == len(dps)
        # first page 100k, counts 1, paginate 2 windows (+1 potential for 1st day uncertainty)
        assert 4 <= COGNITE_CLIENT.datapoints._post.call_count <= 5

    def test_retrieve_include_outside_points(self, test_time_series):
        ts = test_time_series[0]
        start = utils._time.timestamp_to_ms("6h-ago")
        end = utils._time.timestamp_to_ms("1h-ago")
        dps_wo_outside = COGNITE_CLIENT.datapoints.retrieve(
            id=ts.id, start=start, end=end, include_outside_points=False
        )
        dps_w_outside = COGNITE_CLIENT.datapoints.retrieve(id=ts.id, start=start, end=end, include_outside_points=True)
        assert not has_duplicates(dps_w_outside.to_pandas())
        assert len(dps_wo_outside) + 1 <= len(dps_w_outside) <= len(dps_wo_outside) + 2

    def test_retrieve_include_outside_points_paginate_no_outside(self, test_time_series, post_spy):
        ts = test_time_series[0]
        start = datetime(2019, 1, 1)
        end = datetime(2019, 6, 29)
        test_lim = 250
        dps_non_outside = COGNITE_CLIENT.datapoints.retrieve(id=ts.id, start=start, end=end, limit=1234)
        count_first = COGNITE_CLIENT.datapoints._post.call_count

        tmp = COGNITE_CLIENT.datapoints._DPS_LIMIT
        COGNITE_CLIENT.datapoints._DPS_LIMIT = test_lim
        dps = COGNITE_CLIENT.datapoints.retrieve(
            id=ts.id, start=start, end=end, include_outside_points=True, limit=1234
        )
        COGNITE_CLIENT.datapoints._DPS_LIMIT = tmp
        assert len(dps) == len(dps_non_outside)
        assert math.ceil(len(dps) / test_lim) + 1 == COGNITE_CLIENT.datapoints._post.call_count - count_first
        assert not has_duplicates(dps.to_pandas())
        ts_outside = set(dps.timestamp) - set(dps_non_outside.timestamp)
        assert 0 == len(ts_outside)
        assert set(dps.timestamp) == set(dps_non_outside.timestamp)

    def test_retrieve_include_outside_points_paginate_outside_exists(self, test_time_series, post_spy):
        ts = test_time_series[0]
        start = datetime(2019, 6, 29)
        end = datetime(2019, 6, 29, 5)
        test_lim = 2500
        dps_non_outside = COGNITE_CLIENT.datapoints.retrieve(id=ts.id, start=start, end=end)
        tmp = COGNITE_CLIENT.datapoints._DPS_LIMIT
        COGNITE_CLIENT.datapoints._DPS_LIMIT = test_lim
        dps = COGNITE_CLIENT.datapoints.retrieve(id=ts.id, start=start, end=end, include_outside_points=True)
        COGNITE_CLIENT.datapoints._DPS_LIMIT = tmp
        ts_outside = set(dps.timestamp) - set(dps_non_outside.timestamp)
        assert 2 == len(ts_outside)
        for ts in ts_outside:
            assert ts < timestamp_to_ms(start) or ts >= timestamp_to_ms(end)
        assert set(dps.timestamp) - ts_outside == set(dps_non_outside.timestamp)

    def test_retrieve_dataframe(self, test_time_series):
        ts = test_time_series[0]
        df = COGNITE_CLIENT.datapoints.retrieve_dataframe(
            id=ts.id, start="6h-ago", end="now", aggregates=["average"], granularity="1s"
        )
        assert df.shape[0] > 0
        assert df.shape[1] == 1
        assert has_correct_timestamp_spacing(df, "1s")

    def test_retrieve_dataframe_missing(self, test_time_series):
        ts = test_time_series[0]
        df = COGNITE_CLIENT.datapoints.retrieve_dataframe(
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

    def test_retrieve_string(self):
        dps = COGNITE_CLIENT.datapoints.retrieve(external_id="test__string_b", start=1563000000000, end=1564100000000)
        assert len(dps) > 100000

    def test_query(self, test_time_series):
        dps_query1 = DatapointsQuery(id=test_time_series[0].id, start="6h-ago", end="now")
        dps_query2 = DatapointsQuery(id=test_time_series[1].id, start="3h-ago", end="now")
        dps_query3 = DatapointsQuery(
            id=test_time_series[2].id, start="1d-ago", end="now", aggregates=["average"], granularity="1h"
        )

        res = COGNITE_CLIENT.datapoints.query([dps_query1, dps_query2, dps_query3])
        assert len(res) == 3
        assert len(res[2][0]) < len(res[1][0]) < len(res[0][0])

    def test_query_unknown(self, test_time_series):
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
        res = COGNITE_CLIENT.datapoints.query([dps_query1, dps_query2, dps_query3])
        assert len(res) == 3
        assert len(res[0]) == 1
        assert len(res[0][0]) > 0
        assert len(res[1]) == 0
        assert len(res[2]) == 0

    def test_retrieve_latest(self, test_time_series):
        ids = [test_time_series[0].id, test_time_series[1].id]
        res = COGNITE_CLIENT.datapoints.retrieve_latest(id=ids)
        for dps in res:
            assert 1 == len(dps)

    def test_retrieve_latest_unknown(self, test_time_series):
        ids = [test_time_series[0].id, test_time_series[1].id, 42, 1337]
        res = COGNITE_CLIENT.datapoints.retrieve_latest(id=ids, ignore_unknown_ids=True)
        assert 2 == len(res)
        for dps in res:
            assert 1 == len(dps)

    def test_retrieve_latest_many(self, test_time_series, post_spy):
        ids = [
            t.id for t in COGNITE_CLIENT.time_series.list(limit=12) if not t.security_categories
        ]  # more than one page
        assert len(ids) > 10
        tmp = COGNITE_CLIENT.datapoints._RETRIEVE_LATEST_LIMIT
        COGNITE_CLIENT.datapoints._RETRIEVE_LATEST_LIMIT = 10
        res = COGNITE_CLIENT.datapoints.retrieve_latest(id=ids)
        COGNITE_CLIENT.datapoints._RETRIEVE_LATEST_LIMIT = tmp
        assert len(ids) == len(res)
        assert 2 == COGNITE_CLIENT.datapoints._post.call_count
        for i, dps in enumerate(res):
            assert len(dps) <= 1  # could be empty
            assert ids[i] == res[i].id

    def test_retrieve_latest_before(self, test_time_series):
        ts = test_time_series[0]
        res = COGNITE_CLIENT.datapoints.retrieve_latest(id=ts.id, before="1h-ago")
        assert 1 == len(res)
        assert res[0].timestamp < utils._time.timestamp_to_ms("1h-ago")

    def test_insert(self, new_ts, post_spy):
        datapoints = [(datetime(year=2018, month=1, day=1, hour=1, minute=i), i) for i in range(60)]
        with set_request_limit(COGNITE_CLIENT.datapoints, 30):
            COGNITE_CLIENT.datapoints.insert(datapoints, id=new_ts.id)
        assert 2 == COGNITE_CLIENT.datapoints._post.call_count

    def test_insert_copy(self, test_time_series, new_ts, post_spy):
        data = COGNITE_CLIENT.datapoints.retrieve(id=test_time_series[0].id, start="600d-ago", end="now", limit=100)
        assert 100 == len(data)
        COGNITE_CLIENT.datapoints.insert(data, id=new_ts.id)
        assert 2 == COGNITE_CLIENT.datapoints._post.call_count

    def test_insert_pandas_dataframe(self, new_ts, post_spy):
        start = datetime(2018, 1, 1)
        x = pandas.DatetimeIndex([start + timedelta(days=d) for d in range(100)])
        y = numpy.random.normal(0, 1, 100)
        df = pandas.DataFrame({new_ts.id: y}, index=x)
        with set_request_limit(COGNITE_CLIENT.datapoints, 50):
            COGNITE_CLIENT.datapoints.insert_dataframe(df)
        assert 2 == COGNITE_CLIENT.datapoints._post.call_count

    def test_delete_range(self, new_ts):
        COGNITE_CLIENT.datapoints.delete_range(start="2d-ago", end="now", id=new_ts.id)

    def test_retrieve_dataframe_dict(self, test_time_series):

        dfd = COGNITE_CLIENT.datapoints.retrieve_dataframe_dict(
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
