from random import randint

import pytest

from cognite import CogniteClient
from cognite.client.stable.time_series import TimeSeries
from cognite.client.experimental.time_series import TimeSeriesClient, TimeSeriesResponse
from tests.conftest import TEST_TS_1_NAME

stable_time_series = CogniteClient().time_series
time_series = CogniteClient().experimental.time_series

TS_NAME = None


@pytest.fixture(autouse=True, scope="class")
def ts_name():
    global TS_NAME
    TS_NAME = "test_ts_{}".format(randint(1, 2 ** 53 - 1))


class TestTimeseries:
    @pytest.fixture(scope="class")
    def created_ts_id(self):
        tso = TimeSeries(TS_NAME)
        res = stable_time_series.post_time_series([tso])
        yield time_series.search_for_time_series().to_json()[0]["id"]

    def test_delete_time_series_by_id(self, created_ts_id):
        res = time_series.delete_time_series_by_id([created_ts_id])
        assert res is None

    @pytest.fixture(scope="class")
    def get_time_series_by_id_response_obj(self, time_series_in_cdp):
        yield time_series.get_time_series_by_id(id=time_series_in_cdp[0])

    @pytest.fixture(scope="class")
    def get_multiple_time_series_by_id_response_obj(self, time_series_in_cdp):
        yield time_series.get_multiple_time_series_by_id(ids=[time_series_in_cdp[0]])

    def test_get_time_series_by_id(self, get_time_series_by_id_response_obj):
        assert isinstance(get_time_series_by_id_response_obj, TimeSeriesResponse)
        assert get_time_series_by_id_response_obj.to_json()[0]["name"] == TEST_TS_1_NAME

    def test_get_multiple_time_series_by_id(self, get_multiple_time_series_by_id_response_obj):
        assert isinstance(get_multiple_time_series_by_id_response_obj, TimeSeriesResponse)
        assert get_multiple_time_series_by_id_response_obj.to_json()[0]["name"] == TEST_TS_1_NAME
