from datetime import datetime
from random import randint

import pytest

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
    def get_time_series_by_id_response_obj(self, request):
        yield time_series.get_time_series_by_id(id=4536445397018257, include_metadata=request.param)

    @pytest.fixture(scope="class")
    def get_multiple_time_series_by_id_response_obj(self):
        yield time_series.get_multiple_time_series_by_id(ids=[4536445397018257])

    def test_get_time_series_by_id(self, get_time_series_by_id_response_obj):
        assert isinstance(get_time_series_by_id_response_obj, TimeSeriesResponse)
        assert get_time_series_by_id_response_obj.to_json()[0]["name"] == "constant"

    def test_get_multiple_time_series_by_id(self, get_multiple_time_series_by_id_response_obj):
        assert isinstance(get_multiple_time_series_by_id_response_obj, TimeSeriesResponse)
        assert get_multiple_time_series_by_id_response_obj.to_json()[0]["name"] == "constant"
