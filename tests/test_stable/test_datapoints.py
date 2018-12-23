from datetime import datetime
from random import randint
from typing import List
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from cognite import CogniteClient
from cognite.client._api_client import APIClient
from cognite.client.stable.datapoints import (
    Datapoint,
    DatapointsQuery,
    DatapointsResponse,
    LatestDatapointResponse,
    TimeseriesWithDatapoints,
)
from cognite.client.stable.time_series import TimeSeries
from tests.conftest import (
    TEST_TS_1_NAME,
    TEST_TS_2_NAME,
    TEST_TS_REASONABLE_INTERVAL,
    TEST_TS_REASONABLE_INTERVAL_DATETIME,
)

client = CogniteClient()

TS_NAME = None


@pytest.fixture(autouse=True, scope="class")
def ts_name():
    global TS_NAME
    TS_NAME = "test_ts_{}".format(randint(1, 2 ** 53 - 1))


@pytest.fixture(scope="class")
def datapoints_fixture():
    tso = TimeSeries(TS_NAME)
    client.time_series.post_time_series([tso])
    yield
    client.time_series.delete_time_series(TS_NAME)


dps_params = [
    TEST_TS_REASONABLE_INTERVAL,
    TEST_TS_REASONABLE_INTERVAL_DATETIME,
    {
        "start": TEST_TS_REASONABLE_INTERVAL_DATETIME["start"],
        "end": TEST_TS_REASONABLE_INTERVAL_DATETIME["end"],
        "protobuf": True,
    },
]


@pytest.mark.usefixtures("datapoints_fixture")
class TestDatapoints:
    @pytest.fixture(scope="class", params=dps_params)
    def get_dps_response_obj(self, request):
        res = client.datapoints.get_datapoints(
            name=TEST_TS_1_NAME,
            start=request.param["start"],
            end=request.param["end"],
            include_outside_points=True,
            protobuf=request.param.get("protobuf", False),
        )
        yield res

    def test_post_datapoints(self):
        dps = [Datapoint(i, i * 100) for i in range(10)]
        res = client.datapoints.post_datapoints(TS_NAME, datapoints=dps)
        assert res == {}

    def test_post_datapoints_frame(self):
        data = pd.DataFrame()
        data["timestamp"] = [int(1537208777557 + 1000 * i) for i in range(0, 100)]
        X = data["timestamp"].values.astype(float)
        data["X"] = X ** 2
        data["Y"] = 1.0 / (1 + X)

        for name in data.drop(["timestamp"], axis=1).columns:
            ts = TimeSeries(name=name, description="To be deleted")
            try:
                client.time_series.post_time_series([ts])
            except:
                pass

        res = client.datapoints.post_datapoints_frame(data)
        assert res == {}

    def test_get_datapoints(self, get_dps_response_obj):
        assert isinstance(get_dps_response_obj, DatapointsResponse)

    def test_get_dps_output_formats(self, get_dps_response_obj):
        assert isinstance(get_dps_response_obj.to_pandas(), pd.DataFrame)
        assert isinstance(get_dps_response_obj.to_json(), dict)

    def test_get_dps_correctly_spaced(self, get_dps_response_obj):
        timestamps = get_dps_response_obj.to_pandas().timestamp.values
        deltas = np.diff(timestamps, 1)
        assert (deltas != 0).all()
        assert (deltas % 10000 == 0).all()

    def test_get_dps_with_limit(self):
        res = client.datapoints.get_datapoints(name=TEST_TS_1_NAME, start=0, limit=1)
        assert len(res.to_json().get("datapoints")) == 1

    def test_get_dps_with_end_now(self):
        res = client.datapoints.get_datapoints(name=TEST_TS_1_NAME, start=0, end="now", limit=100)
        assert len(res.to_json().get("datapoints")) == 100


class TestLatest:
    def test_get_latest(self):
        response = client.datapoints.get_latest(TEST_TS_1_NAME)
        assert list(response.to_json().keys()) == ["timestamp", "value"]
        assert isinstance(response, LatestDatapointResponse)
        assert isinstance(response.to_pandas(), pd.DataFrame)
        assert isinstance(response.to_json(), dict)
        timestamp = response.to_json()["timestamp"]
        response = client.datapoints.get_latest(TEST_TS_1_NAME, before=timestamp)
        assert response.to_json()["timestamp"] < timestamp


class TestDatapointsFrame:
    @pytest.fixture(scope="class", params=dps_params[:2])
    def get_datapoints_frame_response_obj(self, request):
        yield client.datapoints.get_datapoints_frame(
            time_series=[TEST_TS_1_NAME],
            start=request.param["start"],
            end=request.param["end"],
            aggregates=["avg"],
            granularity="1m",
        )

    def test_get_dps_frame_output_format(self, get_datapoints_frame_response_obj):
        assert isinstance(get_datapoints_frame_response_obj, pd.DataFrame)

    def test_get_dps_frame_correctly_spaced(self, get_datapoints_frame_response_obj):
        timestamps = get_datapoints_frame_response_obj.timestamp.values
        deltas = np.diff(timestamps, 1)
        assert (deltas != 0).all()
        assert (deltas % 60000 == 0).all()

    def test_get_dps_frame_with_limit(self):
        df = client.datapoints.get_datapoints_frame(
            time_series=[TEST_TS_1_NAME], aggregates=["avg"], granularity="1m", start=0, limit=1
        )
        assert df.shape[0] == 1


class TestMultiTimeseriesDatapoints:
    @pytest.fixture(scope="class", params=dps_params[:1])
    def get_multi_time_series_dps_response_obj(self, request):
        dq1 = DatapointsQuery(TEST_TS_1_NAME)
        dq2 = DatapointsQuery(TEST_TS_2_NAME, aggregates=["avg"], granularity="30s")
        yield list(
            client.datapoints.get_multi_time_series_datapoints(
                datapoints_queries=[dq1, dq2],
                start=request.param["start"],
                end=request.param["end"],
                aggregates=["avg"],
                granularity="60s",
            )
        )

    def test_post_multitag_datapoints(self):
        timeseries_with_too_many_datapoints = TimeseriesWithDatapoints(
            name="test", datapoints=[Datapoint(x, x) for x in range(100001)]
        )
        timeseries_with_99999_datapoints = TimeseriesWithDatapoints(
            name="test", datapoints=[Datapoint(x, x) for x in range(99999)]
        )

        with mock.patch.object(APIClient, "_post") as post_request_mock:
            post_request_mock: mock.MagicMock = post_request_mock

            client.datapoints.post_multi_time_series_datapoints([timeseries_with_too_many_datapoints])
            assert post_request_mock.call_count == 2

        with mock.patch.object(APIClient, "_post") as post_request_mock:
            post_request_mock: mock.MagicMock = post_request_mock

            client.datapoints.post_multi_time_series_datapoints(
                [timeseries_with_99999_datapoints, timeseries_with_too_many_datapoints]
            )
            assert post_request_mock.call_count == 2

    def test_get_multi_time_series_dps_output_format(self, get_multi_time_series_dps_response_obj):
        assert isinstance(get_multi_time_series_dps_response_obj, list)

        for dpr in get_multi_time_series_dps_response_obj:
            assert isinstance(dpr, DatapointsResponse)

    def test_get_multi_time_series_dps_response_length(self, get_multi_time_series_dps_response_obj):
        assert len(list(get_multi_time_series_dps_response_obj)) == 2

    def test_get_multi_timeseries_dps_correctly_spaced(self, get_multi_time_series_dps_response_obj):
        m = list(get_multi_time_series_dps_response_obj)
        timestamps = m[0].to_pandas().timestamp.values
        deltas = np.diff(timestamps, 1)
        assert (deltas != 0).all()
        assert (deltas % 60000 == 0).all()
        timestamps = m[1].to_pandas().timestamp.values
        deltas = np.diff(timestamps, 1)
        assert (deltas != 0).all()
        assert (deltas % 30000 == 0).all()

    def test_split_TimeseriesWithDatapoints_if_over_limit(self):
        timeseries_with_datapoints_over_limit: TimeseriesWithDatapoints = TimeseriesWithDatapoints(
            name="test", datapoints=[Datapoint(x, x) for x in range(1000)]
        )

        result: List[TimeseriesWithDatapoints] = client.datapoints._split_TimeseriesWithDatapoints_if_over_limit(
            timeseries_with_datapoints_over_limit, 100
        )

        assert isinstance(result[0], TimeseriesWithDatapoints)
        assert len(result) == 10

        result = client.datapoints._split_TimeseriesWithDatapoints_if_over_limit(
            timeseries_with_datapoints_over_limit, 1000
        )

        assert isinstance(result[0], TimeseriesWithDatapoints)
        assert len(result) == 1
