from datetime import datetime
from random import randint

import numpy as np
import pandas as pd
import pytest

from cognite.v05 import depthseries, timeseries, dto

DS_NAME = None


@pytest.fixture(autouse=True, scope='class')
def ts_name():
    global DS_NAME
    DS_NAME = 'test_ds_{}'.format(randint(1, 2 ** 53 - 1))


class TestDepthseries:
    @pytest.fixture(scope='class', params=[True, False])
    def get_depthseries_response_obj(self ):
        yield depthseries.get_depthseries(prefix=DS_NAME, limit=1)

    def test_post_depthseries(self):
        tso = dto.TimeSeries(DS_NAME)
        res = depthseries.post_depth_series([tso])
        assert res == {}

    def test_post_datapoints(self):
        dps = [dto.DatapointDepth(i, i * 100) for i in range(10)]
        res = depthseries.post_datapoints(DS_NAME, depthdatapoints=dps)
        assert res == {}

    def test_get_latest(self):
        from cognite.v05.dto import LatestDatapointResponse
        response = depthseries.get_latest(DS_NAME)
        assert isinstance(response, LatestDatapointResponse)
        assert isinstance(response.to_ndarray(), np.ndarray)
        assert isinstance(response.to_pandas(), pd.DataFrame)
        assert isinstance(response.to_json(), dict)

    def test_update_timeseries(self):
        tso = dto.TimeSeries(DS_NAME, unit='celsius')
        res = depthseries.update_depth_series([tso])
        assert res == {}

    def test_depthseries_unit_correct(self, get_depthseries_response_obj):
        assert get_depthseries_response_obj.to_json()[0]['unit'] == 'celsius'

    def test_get_depthseries_output_format(self, get_depthseries_response_obj):
        print(get_depthseries_response_obj.to_pandas())
        from cognite.v05.dto import TimeSeriesResponse
        assert isinstance(get_depthseries_response_obj, TimeSeriesResponse)
        assert isinstance(get_depthseries_response_obj.to_ndarray(), np.ndarray)
        assert isinstance(get_depthseries_response_obj.to_pandas(), pd.DataFrame)
        assert isinstance(get_depthseries_response_obj.to_json()[0], dict)

    def test_get_depthseries_no_results(self):
        result = depthseries.get_depthseries(prefix='not_a_depthseries_prefix')
        assert result.to_pandas().empty
        assert len(result.to_json()) == 0

    def test_reset_depthseries(self):
        res = depthseries.reset_depth_series(DS_NAME)
        assert res == {}

    def test_delete_depthseries(self):
        res = depthseries.delete_depth_series(DS_NAME)
        assert res == {}

