from datetime import datetime
from random import randint

import numpy as np
import pandas as pd
import pytest

from cognite.v05 import dto
from cognite.v05.timeseries import delete_time_series as delete_time_series_v05
from cognite.v05.timeseries import post_time_series as post_time_series_v05
from cognite.v06 import datapoints

TS_NAME = None

dps_params = [
    {"start": 1522188000000, "end": 1522620000000},
    {"start": datetime(2018, 4, 1), "end": datetime(2018, 4, 2)},
]


@pytest.fixture(autouse=True, scope="class")
def ts_name():
    global TS_NAME
    TS_NAME = "test_ts_{}".format(randint(1, 2 ** 53 - 1))


@pytest.fixture(scope="class")
def datapoints_fixture():
    tso = dto.TimeSeries(TS_NAME)
    post_time_series_v05([tso])
    yield
    delete_time_series_v05(TS_NAME)


@pytest.mark.usefixtures("datapoints_fixture")
class TestDatapoints:
    @pytest.fixture(scope="class", params=dps_params)
    def get_dps_response_obj(self, request):
        yield datapoints.get_datapoints(
            id=4536445397018257, start=request.param["start"], end=request.param["end"], include_outside_points=True
        )

    def test_get_datapoints(self, get_dps_response_obj):
        from cognite.v05.dto import DatapointsResponse

        assert isinstance(get_dps_response_obj, DatapointsResponse)

    def test_get_dps_output_formats(self, get_dps_response_obj):
        assert isinstance(get_dps_response_obj.to_ndarray(), np.ndarray)
        assert isinstance(get_dps_response_obj.to_pandas(), pd.DataFrame)
        assert isinstance(get_dps_response_obj.to_json(), dict)

    def test_get_dps_correctly_spaced(self, get_dps_response_obj):
        timestamps = get_dps_response_obj.to_pandas().timestamp.values
        deltas = np.diff(timestamps, 1)
        assert (deltas != 0).all()
        assert (deltas % 10000 == 0).all()

    def test_get_dps_with_limit(self):
        res = datapoints.get_datapoints(id=4536445397018257, start=0, limit=1)
        assert len(res.to_json().get("datapoints")) == 1

    def test_get_dps_with_end_now(self):
        res = datapoints.get_datapoints(id=4536445397018257, start=0, end="now", limit=100)
        assert len(res.to_json().get("datapoints")) == 100

    def test_get_dps_with_limit_with_config_variables_from_argument(self, unset_config_variables):
        res = datapoints.get_datapoints(
            id=4536445397018257, start=0, limit=1, api_key=unset_config_variables[0], project=unset_config_variables[1]
        )
        assert len(res.to_json().get("datapoints")) == 1

    def test_get_dps_with_config_variables_from_argument(self, unset_config_variables):
        res = datapoints.get_datapoints(
            id=4536445397018257,
            start=1522188000000,
            end=1522620000000,
            api_key=unset_config_variables[0],
            project=unset_config_variables[1],
        )
        assert res
