from random import randint
from time import sleep

import pandas as pd
import pytest

from cognite.client import CogniteClient
from cognite.client.stable.time_series import TimeSeries, TimeSeriesListResponse, TimeSeriesResponse

timeseries = CogniteClient().time_series


@pytest.fixture(autouse=True, scope="class")
def ts_name():
    global TS_NAME
    TS_NAME = "test_ts_{}".format(randint(1, 2 ** 53 - 1))


class TestTimeseries:
    def test_post_timeseries(self):
        tso = TimeSeries(TS_NAME)
        res = timeseries.post_time_series([tso])
        assert res is None

    def test_update_timeseries(self):
        tso = TimeSeries(TS_NAME, unit="celsius")
        res = timeseries.update_time_series([tso])
        assert res is None

    @pytest.fixture(scope="class", params=[True, False])
    def get_timeseries_response_obj(self, request):
        res = timeseries.get_time_series(prefix=TS_NAME, limit=1, include_metadata=request.param)
        while len(res) == 0:
            res = timeseries.get_time_series(prefix=TS_NAME, limit=1, include_metadata=request.param)
            sleep(0.5)
        yield res

    def test_timeseries_unit_correct(self, get_timeseries_response_obj):
        assert get_timeseries_response_obj[0].unit == "celsius"

    def test_get_timeseries_output_format(self, get_timeseries_response_obj):
        assert isinstance(get_timeseries_response_obj, TimeSeriesListResponse)
        assert isinstance(get_timeseries_response_obj.to_pandas(), pd.DataFrame)
        assert isinstance(get_timeseries_response_obj.to_json()[0], dict)
        assert isinstance(get_timeseries_response_obj[0], TimeSeriesResponse)
        assert isinstance(get_timeseries_response_obj[:1], TimeSeriesListResponse)
        assert len(get_timeseries_response_obj[:1]) == 1

        for ts in get_timeseries_response_obj:
            assert isinstance(ts, TimeSeriesResponse)
            assert isinstance(ts.to_pandas(), pd.DataFrame)
            assert isinstance(ts.to_json(), dict)
            for key, val in ts.__dict__.items():
                if key == "metadata":
                    assert val is None
                else:
                    assert val is not None

    def test_get_timeseries_no_results(self):
        result = timeseries.get_time_series(prefix="not_a_timeseries_prefix")
        assert result.to_pandas().empty
        assert not result.to_json()

    def test_delete_timeseries(self):
        res = timeseries.delete_time_series(TS_NAME)
        assert res is None
