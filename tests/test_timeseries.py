# TODO: Temporary mock setup while waiting for a better way to do integration tests
from datetime import datetime
from unittest.mock import patch

import pytest

from cognite.timeseries import get_datapoints


@patch('requests.get')
@pytest.fixture(scope='module', params=[
    {'start': 0, 'end': 1},
    {'start': '1w-ago', 'end': '1d-ago'},
    {'start': datetime(2018, 3, 26), 'end': datetime(2018, 3, 27)}])
def get_datapoints_response_obj(mock_get, request):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        'data': {
            'items': [
                {'tagId': 'a_tag',
                 'datapoints': [
                     {'timestamp': 0, 'value': 10},
                     {'timestamp': 1, 'value': 20},
                     {'timestamp': 2, 'value': 30},
                     {'timestamp': 3, 'value': 40},
                     {'timestamp': 4, 'value': 50},
                 ]
                 }
            ]
        }
    }

    return get_datapoints(tag_id='SKAP_18PI2317/Y/10sSAMP', start=request.param['start'], end=request.param['end'],
                          protobuf=False)


def test_get_datapoints(get_datapoints_response_obj):
    from cognite.data_objects import DatapointsObject
    assert isinstance(get_datapoints_response_obj, DatapointsObject)


def test_multi_tag_element_json(get_datapoints_response_obj):
    assert isinstance(get_datapoints_response_obj.to_json(), dict)


def test_pandas(get_datapoints_response_obj):
    import pandas as pd
    assert isinstance(get_datapoints_response_obj.to_pandas(), pd.DataFrame)


def test_ndarray(get_datapoints_response_obj):
    import numpy as np
    assert isinstance(get_datapoints_response_obj.to_ndarray(), np.ndarray)
