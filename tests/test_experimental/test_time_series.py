from random import randint

import pytest

from cognite import CogniteClient
from cognite.client.stable.time_series import TimeSeries
from cognite.client.experimental.time_series import TimeSeriesClient, TimeSeriesResponse
from tests.conftest import TEST_TS_1_NAME

stable_time_series = CogniteClient().time_series
time_series = CogniteClient().experimental.time_series


@pytest.fixture
def new_ts_id():
    name = "test_ts_{}".format(randint(1, 2 ** 53 - 1))
    stable_time_series.post_time_series([TimeSeries(name)])
    yield stable_time_series.get_time_series(prefix=name).to_json()[0]["id"]


class TestTimeseries:
    def test_delete_time_series_by_id(self, new_ts_id):
        res = time_series.delete_time_series_by_id([new_ts_id])
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
