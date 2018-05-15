from datetime import datetime
from random import randint

import numpy as np
import pandas as pd
import pytest

from cognite.v04 import timeseries

TS_NAME = None

dps_params = [
    {'start': 1522188000000, 'end': 1522620000000},
    {'start': datetime(2018, 4, 1), 'end': datetime(2018, 4, 2)},
    {'start': datetime(2018, 4, 1), 'end': datetime(2018, 4, 2), 'protobuf': True}]


@pytest.fixture(scope='module', params=dps_params)
def get_dps_response_obj(request):
    yield timeseries.get_datapoints(tag_id='constant', start=request.param['start'], end=request.param['end'],
                                    protobuf=request.param.get('protobuf', False))


def test_get_datapoints(get_dps_response_obj):
    from cognite.v04.dto import DatapointsResponse
    assert isinstance(get_dps_response_obj, DatapointsResponse)


def test_get_dps_output_formats(get_dps_response_obj):
    assert isinstance(get_dps_response_obj.to_ndarray(), np.ndarray)
    assert isinstance(get_dps_response_obj.to_pandas(), pd.DataFrame)
    assert isinstance(get_dps_response_obj.to_json(), dict)


def test_get_dps_correctly_spaced(get_dps_response_obj):
    timestamps = get_dps_response_obj.to_pandas().timestamp.values
    deltas = np.diff(timestamps, 1)
    assert (deltas != 0).all()
    assert (deltas % 10000 == 0).all()


def test_get_latest():
    from cognite.v04.dto import LatestDatapointResponse
    response = timeseries.get_latest('constant')
    assert isinstance(response, LatestDatapointResponse)
    assert isinstance(response.to_ndarray(), np.ndarray)
    assert isinstance(response.to_pandas(), pd.DataFrame)
    assert isinstance(response.to_json(), dict)


@pytest.fixture(scope='module', params=dps_params[:2])
def get_datapoints_frame_response_obj(request):
    yield timeseries.get_datapoints_frame(tag_ids=['constant'], start=request.param['start'], end=request.param['end'],
                                          aggregates=['avg'], granularity='1m')


def test_get_dps_frame_output_format(get_datapoints_frame_response_obj):
    assert isinstance(get_datapoints_frame_response_obj, pd.DataFrame)


def test_get_dps_frame_correctly_spaced(get_datapoints_frame_response_obj):
    timestamps = get_datapoints_frame_response_obj.timestamp.values
    deltas = np.diff(timestamps, 1)
    assert (deltas != 0).all()
    assert (deltas % 60000 == 0).all()


@pytest.fixture(scope='module', params=dps_params[:2])
def get_multitag_dps_response_obj(request):
    from cognite.v04.dto import DatapointsQuery
    dq1 = DatapointsQuery('constant')
    dq2 = DatapointsQuery('sinus', aggregates=['avg'], granularity='30s')
    yield list(timeseries.get_multi_tag_datapoints(datapoints_queries=[dq1, dq2], start=request.param['start'],
                                                   end=request.param['end'], aggregates=['avg'], granularity='60s'))


def test_get_multitag_dps_output_format(get_multitag_dps_response_obj):
    from cognite.v04.dto import DatapointsResponse
    assert isinstance(get_multitag_dps_response_obj, list)
    for dpr in get_multitag_dps_response_obj:
        assert isinstance(dpr, DatapointsResponse)


def test_get_multitag_dps_response_length(get_multitag_dps_response_obj):
    assert len(list(get_multitag_dps_response_obj)) == 2


def test_get_multitag_dps_correctly_spaced(get_multitag_dps_response_obj):
    m = list(get_multitag_dps_response_obj)
    timestamps = m[0].to_pandas().timestamp.values
    deltas = np.diff(timestamps, 1)
    assert (deltas != 0).all()
    assert (deltas % 60000 == 0).all()
    timestamps = m[1].to_pandas().timestamp.values
    deltas = np.diff(timestamps, 1)
    assert (deltas != 0).all()
    assert (deltas % 30000 == 0).all()


@pytest.fixture(autouse=True, scope='class')
def ts_name():
    global TS_NAME
    TS_NAME = 'test_ts_{}'.format(randint(1, 2 ** 53 - 1))


@pytest.fixture(scope='class')
def timeseries_fixture():
    from cognite.v05 import timeseries
    yield
    timeseries.delete_time_series(TS_NAME)


@pytest.mark.usefixtures('timeseries_fixture')
class TestTimeseries:
    @pytest.fixture(scope='class', params=[True, False])
    def get_timeseries_response_obj(self, request):
        yield timeseries.get_timeseries(prefix=TS_NAME, limit=1, include_metadata=request.param)

    def test_post_timeseries(self):
        from cognite.v04 import dto
        tso = dto.TimeSeries(TS_NAME)
        res = timeseries.post_time_series([tso])
        assert res == {}

    @staticmethod
    def test_post_multitag_datapoints():
        from cognite.v04 import dto
        from cognite.v04.dto import TimeseriesWithDatapoints
        from unittest import mock
        import cognite._utils as utils

        timeseries_with_too_many_datapoints: TimeseriesWithDatapoints = TimeseriesWithDatapoints(
            tagId="test",
            datapoints=[dto.Datapoint(x, x) for x in range(100001)]
        )
        timeseries_with_99999_datapoints: TimeseriesWithDatapoints = TimeseriesWithDatapoints(
            tagId="test",
            datapoints=[dto.Datapoint(x, x) for x in range(99999)]
        )

        with mock.patch.object(utils, 'post_request') as post_request_mock:
            post_request_mock: mock.MagicMock = post_request_mock

            timeseries.post_multi_tag_datapoints([timeseries_with_too_many_datapoints])
            assert post_request_mock.call_count == 2

        with mock.patch.object(utils, 'post_request') as post_request_mock:
            post_request_mock: mock.MagicMock = post_request_mock

            timeseries.post_multi_tag_datapoints([
                timeseries_with_99999_datapoints,
                timeseries_with_too_many_datapoints
            ])
            assert post_request_mock.call_count == 2

    def test_update_timeseries(self):
        from cognite.v04 import dto
        tso = dto.TimeSeries(TS_NAME, unit='celsius')
        res = timeseries.update_time_series([tso])
        assert res == {}

    def test_timeseries_unit_correct(self, get_timeseries_response_obj):
        assert get_timeseries_response_obj.to_json()[0]['unit'] == 'celsius'

    def test_get_timeseries_output_format(self, get_timeseries_response_obj):
        from cognite.v04.dto import TimeSeriesResponse
        assert isinstance(get_timeseries_response_obj, TimeSeriesResponse)
        assert isinstance(get_timeseries_response_obj.to_ndarray(), np.ndarray)
        assert isinstance(get_timeseries_response_obj.to_pandas(), pd.DataFrame)
        assert isinstance(get_timeseries_response_obj.to_json()[0], dict)

    def test_get_timeseries_no_results(self):
        result = timeseries.get_timeseries(prefix='not_a_timeseries_prefix')
        assert result.to_pandas().empty
        assert len(result.to_json()) == 0


def test_split_TimeseriesWithDatapoints_if_over_limit():
    from cognite.v04.dto import TimeseriesWithDatapoints
    from cognite.v04.dto import Datapoint
    from cognite.v04.timeseries import _split_TimeseriesWithDatapoints_if_over_limit
    from typing import List

    timeseries_with_datapoints_over_limit: TimeseriesWithDatapoints = TimeseriesWithDatapoints(
        tagId="test",
        datapoints=[Datapoint(x, x) for x in range(1000)]
    )

    result: List[TimeseriesWithDatapoints] = _split_TimeseriesWithDatapoints_if_over_limit(
        timeseries_with_datapoints_over_limit,
        100
    )

    assert isinstance(result[0], TimeseriesWithDatapoints)
    assert len(result) == 10

    result = _split_TimeseriesWithDatapoints_if_over_limit(
        timeseries_with_datapoints_over_limit,
        1000
    )

    assert isinstance(result[0], TimeseriesWithDatapoints)
    assert len(result) == 1
