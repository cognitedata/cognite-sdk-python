from random import randint

import numpy as np
import pandas as pd
import pytest

from cognite.client import CogniteClient
from cognite.client.experimental.datapoints import DatapointsResponse
from cognite.client.resources.time_series import TimeSeries
from tests.tests_old.conftest import TEST_TS_REASONABLE_INTERVAL, TEST_TS_REASONABLE_INTERVAL_DATETIME

cognite_client = CogniteClient()
datapoints = cognite_client.experimental.datapoints

TS_NAME = None

dps_params = [TEST_TS_REASONABLE_INTERVAL, TEST_TS_REASONABLE_INTERVAL_DATETIME]


@pytest.fixture(autouse=True, scope="class")
def ts_name():
    global TS_NAME
    TS_NAME = "test_ts_{}".format(randint(1, 2 ** 53 - 1))


@pytest.fixture(scope="class")
def datapoints_fixture():
    tso = TimeSeries(TS_NAME)
    cognite_client.time_series.post_time_series([tso])
    yield
    cognite_client.time_series.delete_time_series(TS_NAME)


@pytest.mark.usefixtures("datapoints_fixture")
class TestDatapoints:
    @pytest.fixture(scope="class", params=dps_params)
    def get_dps_response_obj(self, request, time_series_in_cdp):
        yield datapoints.get_datapoints(
            id=time_series_in_cdp[0],
            start=request.param["start"],
            end=request.param["end"],
            include_outside_points=True,
        )

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

    def test_get_dps_with_limit(self, time_series_in_cdp):
        res = datapoints.get_datapoints(id=time_series_in_cdp[0], start=0, limit=1)
        assert len(res.to_json().get("datapoints")) == 1

    def test_get_dps_with_end_now(self, time_series_in_cdp):
        res = datapoints.get_datapoints(id=time_series_in_cdp[0], start=0, end="now", limit=100)
        assert len(res.to_json().get("datapoints")) == 100
