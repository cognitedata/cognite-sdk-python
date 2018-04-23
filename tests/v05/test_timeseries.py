from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from cognite.v05 import timeseries

dps_params = [
    {'start': 1522188000000, 'end': 1522620000000},
    {'start': datetime(2018, 4, 1), 'end': datetime(2018, 4, 2)},
    {'start': datetime(2018, 4, 1), 'end': datetime(2018, 4, 2), 'protobuf': True}]


@pytest.fixture(scope='module', params=dps_params)
def get_dps_response_obj(request):
    yield timeseries.get_datapoints(timeseries='constant', start=request.param['start'], end=request.param['end'],
                                    protobuf=request.param.get('protobuf', False))


def test_get_datapoints(get_dps_response_obj):
    from cognite.v05.data_objects import DatapointsResponse
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
    from cognite.v05.data_objects import LatestDatapointResponse
    response = timeseries.get_latest('constant')
    assert isinstance(response, LatestDatapointResponse)
    assert isinstance(response.to_ndarray(), np.ndarray)
    assert isinstance(response.to_pandas(), pd.DataFrame)
    assert isinstance(response.to_json(), dict)


@pytest.fixture(scope='module', params=dps_params[:2])
def get_datapoints_frame_response_obj(request):
    yield timeseries.get_datapoints_frame(timeseries=['constant'], start=request.param['start'],
                                          end=request.param['end'],
                                          aggregates=['avg'], granularity='1m')


def test_get_dps_frame_output_format(get_datapoints_frame_response_obj):
    assert isinstance(get_datapoints_frame_response_obj, pd.DataFrame)


def test_get_dps_frame_correctly_spaced(get_datapoints_frame_response_obj):
    timestamps = get_datapoints_frame_response_obj.timestamp.values
    deltas = np.diff(timestamps, 1)
    assert (deltas != 0).all()
    assert (deltas % 60000 == 0).all()


@pytest.fixture(scope='module', params=dps_params[:2])
def get_multi_time_series_dps_response_obj(request):
    from cognite.v05.data_objects import DatapointsQuery
    dq1 = DatapointsQuery('constant')
    dq2 = DatapointsQuery('sinus', aggregates=['avg'], granularity='30s')
    yield list(timeseries.get_multi_time_series_datapoints(datapoints_queries=[dq1, dq2], start=request.param['start'],
                                                           end=request.param['end'], aggregates=['avg'],
                                                           granularity='60s'))


def test_get_multi_time_series_dps_output_format(get_multi_time_series_dps_response_obj):
    from cognite.v05.data_objects import DatapointsResponse
    assert isinstance(get_multi_time_series_dps_response_obj, list)
    for dpr in get_multi_time_series_dps_response_obj:
        assert isinstance(dpr, DatapointsResponse)


def test_get_multi_time_series_dps_response_length(get_multi_time_series_dps_response_obj):
    assert len(list(get_multi_time_series_dps_response_obj)) == 2


def test_get_multi_timeseries_dps_correctly_spaced(get_multi_time_series_dps_response_obj):
    m = list(get_multi_time_series_dps_response_obj)
    timestamps = m[0].to_pandas().timestamp.values
    deltas = np.diff(timestamps, 1)
    assert (deltas != 0).all()
    assert (deltas % 60000 == 0).all()
    timestamps = m[1].to_pandas().timestamp.values
    deltas = np.diff(timestamps, 1)
    assert (deltas != 0).all()
    assert (deltas % 30000 == 0).all()


@pytest.fixture(scope='module', params=[True, False])
def get_timeseries_response_obj(request):
    yield timeseries.get_timeseries(prefix='constant', limit=1, include_metadata=request.param)


def test_get_timeseries_output_format(get_timeseries_response_obj):
    from cognite.v05.data_objects import TimeseriesResponse
    assert isinstance(get_timeseries_response_obj, TimeseriesResponse)
    assert isinstance(get_timeseries_response_obj.to_ndarray(), np.ndarray)
    assert isinstance(get_timeseries_response_obj.to_pandas(), pd.DataFrame)
    assert isinstance(get_timeseries_response_obj.to_json()[0], dict)


def test_get_timeseries_no_results():
    result = timeseries.get_timeseries(prefix='not_a_timeseries_prefix')
    assert result.to_pandas().empty
    assert len(result.to_json()) == 0
