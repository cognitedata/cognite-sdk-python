from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from cognite import timeseries

dps_params = [
    {'start': 1522188000000, 'end': 1522620000000},
    {'start': '3d-ago', 'end': '1d-ago'},
    {'start': datetime(2018, 4, 1), 'end': datetime(2018, 4, 2)},
    {'start': datetime(2018, 4, 1), 'end': datetime(2018, 4, 2), 'protobuf': True}]


@pytest.fixture(scope='module', params=dps_params)
def get_dps_response_obj(request):
    yield timeseries.get_datapoints(tag_id='constant', start=request.param['start'], end=request.param['end'],
                                    protobuf=request.param.get('protobuf', False))


def test_get_datapoints(get_dps_response_obj):
    from cognite.data_objects import DatapointsObject
    assert isinstance(get_dps_response_obj, DatapointsObject)


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
    from cognite.data_objects import LatestDatapointObject
    response = timeseries.get_latest('constant')
    assert isinstance(response, LatestDatapointObject)
    assert isinstance(response.to_ndarray(), np.ndarray)
    assert isinstance(response.to_pandas(), pd.DataFrame)
    assert isinstance(response.to_json(), dict)


@pytest.fixture(scope='module', params=dps_params[:3])
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


@pytest.fixture(scope='module', params=dps_params[:3])
def get_multitag_dps_response_obj(request):
    yield timeseries.get_multi_tag_datapoints(tag_ids=['constant', 'sinus'], start=request.param['start'],
                                              end=request.param['end'])


def test_get_multitag_dps_output_format(get_multitag_dps_response_obj):
    from cognite.data_objects import DatapointsObject
    assert isinstance(get_multitag_dps_response_obj, list)
    assert isinstance(get_multitag_dps_response_obj[0], DatapointsObject)


def test_get_multitag_dps_correctly_spaced(get_multitag_dps_response_obj):
    timestamps = get_multitag_dps_response_obj[0].to_pandas().timestamp.values
    deltas = np.diff(timestamps, 1)
    assert (deltas != 0).all()
    assert (deltas % 10000 == 0).all()


@pytest.fixture(scope='module', params=[True, False])
def get_timeseries_response_obj(request):
    yield timeseries.get_timeseries(prefix='constant', limit=1, include_metadata=request.param)


def test_get_timeseries_output_format(get_timeseries_response_obj):
    from cognite.data_objects import TimeseriesObject
    assert isinstance(get_timeseries_response_obj, TimeseriesObject)
    assert isinstance(get_timeseries_response_obj.to_ndarray(), np.ndarray)
    assert isinstance(get_timeseries_response_obj.to_pandas(), pd.DataFrame)
    assert isinstance(get_timeseries_response_obj.to_json()[0], dict)
