from datetime import datetime
from random import randint

import numpy as np
import pandas as pd
import pytest

from cognite.v05 import dto
from cognite.v05.dto import TimeSeriesResponse
from cognite.v06 import time_series

TS_NAME = None

dps_params = [
    {"start": 1522188000000, "end": 1522620000000},
    {"start": datetime(2018, 4, 1), "end": datetime(2018, 4, 2)},
    {"start": datetime(2018, 4, 1), "end": datetime(2018, 4, 2), "protobuf": True},
]


@pytest.fixture(autouse=True, scope="class")
def ts_name():
    global TS_NAME
    TS_NAME = "test_ts_{}".format(randint(1, 2 ** 53 - 1))


class TestTimeseries:
    @pytest.fixture(scope="class", params=[True, False])
    def get_time_series_response_obj(self, request):
        yield time_series.get_time_series(prefix=TS_NAME, limit=1, include_metadata=request.param)

    def test_post_time_series(self):
        tso = dto.TimeSeries(TS_NAME)
        res = time_series.post_time_series([tso])
        assert isinstance(res, TimeSeriesResponse)
        assert res.to_json()[0]["name"] == TS_NAME

    def test_update_time_series(self):
        tso = dto.TimeSeries(TS_NAME, unit="celsius")
        res = time_series.update_time_series([tso])
        assert res == {}

    def test_time_series_unit_correct(self, get_time_series_response_obj):
        if "unit" in get_time_series_response_obj.to_json()[0]:
            assert get_time_series_response_obj.to_json()[0]["unit"] == "celsius"

    def test_get_time_series_output_format(self, get_time_series_response_obj):
        print(get_time_series_response_obj.to_pandas())
        from cognite.v05.dto import TimeSeriesResponse

        assert isinstance(get_time_series_response_obj, TimeSeriesResponse)
        assert isinstance(get_time_series_response_obj.to_ndarray(), np.ndarray)
        assert isinstance(get_time_series_response_obj.to_pandas(), pd.DataFrame)
        assert isinstance(get_time_series_response_obj.to_json()[0], dict)

    def test_get_time_series_no_results(self):
        result = time_series.get_time_series(name="not_a_time_series")
        assert result.to_pandas().empty
        assert not result.to_json()

    def test_delete_time_series(self):
        res = time_series.delete_time_series(TS_NAME)
        assert res == {}

    def test_get_time_series_with_config_variables_from_argument(self, unset_config_variables):
        ts = time_series.get_time_series(
            prefix=TS_NAME, limit=1, api_key=unset_config_variables[0], project=unset_config_variables[1]
        )
        assert ts
