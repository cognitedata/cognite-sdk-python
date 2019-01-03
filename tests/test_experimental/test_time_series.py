from random import randint

import pytest

from cognite import CogniteClient
from cognite.client.experimental.time_series import TimeSeriesClient, TimeSeriesResponse
from tests.conftest import TEST_TS_1_NAME

time_series = CogniteClient().experimental.time_series


TS_NAME = None


@pytest.fixture(autouse=True, scope="class")
def ts_name():
    global TS_NAME
    TS_NAME = "test_ts_{}".format(randint(1, 2 ** 53 - 1))


class TestTimeseries:
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
