import re
from datetime import datetime, timedelta

import numpy
import pandas
import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Datapoint, DatapointsQuery, TimeSeries
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils import _utils
from tests.utils import set_request_limit

COGNITE_CLIENT = CogniteClient()


@pytest.fixture(scope="session")
def test_time_series():
    time_series = {}
    for ts in COGNITE_CLIENT.time_series.list(limit=150):
        if ts.name in ["test__constant_{}_with_noise".format(i) for i in range(0, 10)]:
            value = int(re.match("test__constant_(\d+)_with_noise", ts.name).group(1))
            time_series[value] = ts
    yield time_series


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
    granularity_ms = _utils.granularity_to_ms(granularity)
    return (deltas != 0).all() and (deltas % granularity_ms == 0).all()


class TestDatapointsAPI:
    def test_retrieve(self, test_time_series):
        ts = test_time_series[0]
        dps = COGNITE_CLIENT.datapoints.retrieve(id=ts.id, start="1d-ago", end="now")
        assert len(dps) > 0

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

    def test_retrieve_include_outside_points(self, test_time_series):
        ts = test_time_series[0]
        start = _utils.timestamp_to_ms("6h-ago")
        end = _utils.timestamp_to_ms("1h-ago")
        dps_wo_outside = COGNITE_CLIENT.datapoints.retrieve(
            id=ts.id, start=start, end=end, include_outside_points=False
        )
        dps_w_outside = COGNITE_CLIENT.datapoints.retrieve(id=ts.id, start=start, end=end, include_outside_points=True)
        assert not has_duplicates(dps_w_outside.to_pandas())
        assert len(dps_wo_outside) + 1 <= len(dps_w_outside) <= len(dps_wo_outside) + 2

    def test_retrieve_dataframe(self, test_time_series):
        ts = test_time_series[0]
        df = COGNITE_CLIENT.datapoints.retrieve_dataframe(
            id=ts.id, start="6h-ago", end="now", aggregates=["average"], granularity="1s"
        )
        assert df.shape[0] > 0
        assert df.shape[1] == 1
        assert has_correct_timestamp_spacing(df, "1s")

    def test_query(self, test_time_series):
        dps_query1 = DatapointsQuery(id=test_time_series[0].id, start="6h-ago", end="now")
        dps_query2 = DatapointsQuery(id=test_time_series[1].id, start="3h-ago", end="now")
        dps_query3 = DatapointsQuery(
            id=test_time_series[2].id, start="1d-ago", end="now", aggregates=["average"], granularity="1h"
        )

        res = COGNITE_CLIENT.datapoints.query([dps_query1, dps_query2, dps_query3])

        assert len(res) == 3

        for dps in res:
            if dps.id == test_time_series[0].id:
                assert 19000 < len(dps.to_pandas())
            if dps.id == test_time_series[1].id:
                assert 9000 < len(dps.to_pandas())
            if dps.id == test_time_series[2].id:
                assert 24 == len(dps.to_pandas())

    def test_retrieve_latest(self, test_time_series):
        ids = [test_time_series[0].id, test_time_series[1].id]
        res = COGNITE_CLIENT.datapoints.retrieve_latest(id=ids)
        for dps in res:
            assert 1 == len(dps)

    def test_retrieve_latest_before(self, test_time_series):
        ts = test_time_series[0]
        res = COGNITE_CLIENT.datapoints.retrieve_latest(id=ts.id, before="1h-ago")
        assert 1 == len(res)
        assert res[0].timestamp < _utils.timestamp_to_ms("1h-ago")

    def test_insert(self, new_ts, mocker):
        datapoints = [(datetime(year=2018, month=1, day=1, hour=1, minute=i), i) for i in range(60)]
        mocker.spy(COGNITE_CLIENT.datapoints, "_post")
        with set_request_limit(COGNITE_CLIENT.datapoints, 30):
            COGNITE_CLIENT.datapoints.insert(datapoints, id=new_ts.id)
        assert 2 == COGNITE_CLIENT.datapoints._post.call_count

    def test_insert_pandas_dataframe(self, new_ts, mocker):
        start = datetime(2018, 1, 1)
        x = pandas.DatetimeIndex([start + timedelta(days=d) for d in range(100)])
        y = numpy.random.normal(0, 1, 100)
        df = pandas.DataFrame({new_ts.id: y}, index=x)
        mocker.spy(COGNITE_CLIENT.datapoints, "_post")
        with set_request_limit(COGNITE_CLIENT.datapoints, 50):
            COGNITE_CLIENT.datapoints.insert_dataframe(df)
        assert 2 == COGNITE_CLIENT.datapoints._post.call_count

    def test_delete_range(self, new_ts):
        COGNITE_CLIENT.datapoints.delete_range(start="2d-ago", end="now", id=new_ts.id)
