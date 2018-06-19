from datetime import datetime
from random import randint

import numpy as np
import pandas as pd
import pytest

from cognite.v05 import depthseries, timeseries, dto

DS_NAME = None
DS2_NAME = None


@pytest.fixture(autouse=True, scope='class')
def ts_name():
    global DS_NAME
    global DS2_NAME
    DS_NAME = 'test_ds_{}'.format(randint(1, 2 ** 53 - 1))
    DS2_NAME = "2" + DS_NAME


class TestDepthseries:
    def test_post_depthseries(self):
        tso = dto.TimeSeries(DS_NAME)
        res = depthseries.post_depth_series([tso])
        assert res == {}

    @pytest.fixture(scope='class', autouse=True)
    def create_depthseries(self):
        tso = dto.TimeSeries(DS_NAME)
        tso2 = dto.TimeSeries(DS2_NAME)
        try:
            res = depthseries.post_depth_series([tso2, tso])
        except:
            pass
        yield depthseries.get_depthseries(prefix=DS_NAME)
        try:
            depthseries.delete_depth_series(DS_NAME)
            depthseries.delete_depth_series(DS2_NAME)
        except:
            pass

    def test_post_datapoints(self):
        dps = [dto.DatapointDepth(i, i * 100) for i in range(10)]
        res = depthseries.post_datapoints(DS_NAME, depthdatapoints=dps)
        assert res == {}

    def test_post_multitagdatapoints(self):
        dps = [dto.DatapointDepth(i, i * 100) for i in range(10)]
        dps2 = [dto.DatapointDepth(i, i * 200) for i in range(10)]
        ts1 = dto.TimeseriesWithDatapoints(DS_NAME, dps)
        ts2 = dto.TimeseriesWithDatapoints(DS2_NAME, dps2)
        res = depthseries.post_multitag_datapoints([ts1, ts2])
        assert res == {}

    def test_get_latest(self):
        dps = [dto.DatapointDepth(i, i * 100) for i in range(10)]
        depthseries.post_datapoints(DS_NAME, depthdatapoints=dps)
        response = depthseries.get_latest(DS_NAME)
        assert isinstance(response, dto.LatestDatapointResponse)
        assert isinstance(response.to_ndarray(), np.ndarray)
        assert isinstance(response.to_pandas(), pd.DataFrame)
        assert isinstance(response.to_json(), dict)
        assert response.to_json()['timestamp'] == 9000
        assert response.to_json()['value'] == 900

    def test_update_timeseries(self):
        tso = dto.TimeSeries(DS_NAME, unit='celsius')
        res = depthseries.update_depth_series([tso])
        assert res == {}

    def test_depthseries_unit_correct(self):
        tso = dto.TimeSeries(DS_NAME, unit='celsius')
        res = depthseries.update_depth_series([tso])
        series = depthseries.get_depthseries(prefix=DS_NAME)
        assert series.to_json()[0]['unit'] == 'celsius'
        assert series.to_json()[1]['unit'] == 'm'

    def test_get_depthseries_output_format(self):
        from cognite.v05.dto import TimeSeriesResponse
        series = depthseries.get_depthseries(prefix=DS_NAME)
        assert isinstance(series, TimeSeriesResponse)
        assert isinstance(series.to_ndarray(), np.ndarray)
        assert isinstance(series.to_pandas(), pd.DataFrame)
        assert isinstance(series.to_json()[0], dict)

    def test_get_depthseries_confirm_names(self):
        df = depthseries.get_depthseries(prefix=DS_NAME).to_pandas()
        assert df.loc[df.index[0], 'name'] == DS_NAME
        assert df.loc[df.index[1], 'name'] == DS_NAME + "_DepthIndex"

    def test_get_depthseries_no_results(self):
        result = depthseries.get_depthseries(prefix='not_a_depthseries_prefix')
        assert result.to_pandas().empty
        assert len(result.to_json()) == 0

    def test_reset_depthseries(self):
        res = depthseries.reset_depth_series(DS_NAME)
        assert res == {}


class TestMultiDepthseriesPost:
    def test_exception_parsing(self):
        typoException = "Some metrics ready exist: "
        preText = "Some metrics already exist: "
        series = ["series1", "dsf-sfasdf;asdf-fd", "asdf/sdf/-df-"]
        singleException = preText + series[0]
        doubleException = preText + ",".join([series[0], series[1]])
        tripleException = preText + ",".join(series)
        exceptionWithMoreText = preText + ",".join(series) + " caused by..."
        empty = depthseries._parse_exists_exception(typoException)
        single = depthseries._parse_exists_exception(singleException)
        double = depthseries._parse_exists_exception(doubleException)
        triple = depthseries._parse_exists_exception(tripleException)
        more = depthseries._parse_exists_exception(exceptionWithMoreText)
        assert len(empty) == 0
        assert len(single) == 1
        assert len(double) == 2
        assert len(triple) == 3
        assert len(more) == 3
        assert triple == more
        assert series[0] in single
        assert series[0] in double
        assert series[0] in triple
        assert series[1] in double
        assert series[1] in triple
        assert series[2] in triple

    def test_create_series(self):
        prefix = ["one", "two", "three"]
        stage = ["stageone", "stagetwo"]
        stage1 = []
        stage2 = []
        for pre in prefix:
            stage1.append(";".join([pre, stage[0], DS_NAME]))
            stage2.append(";".join([pre, stage[1], DS_NAME]))

        stage1tso = [dto.TimeSeries(t) for t in stage1]
        stage2tso = [dto.TimeSeries(t) for t in stage2]

        res = depthseries.post_depth_series(stage1tso)
        assert res == {}
        res = depthseries.post_depth_series(stage1tso+stage2tso)
        assert res == {}
        try:
            for ds in stage1+stage2:
                depthseries.delete_depth_series(ds)
        except:
            pass
