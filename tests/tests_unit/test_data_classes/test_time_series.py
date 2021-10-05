import pytest

from cognite.client import utils
from cognite.client.data_classes import Asset, Datapoint
from tests.utils import jsgz_load


@pytest.fixture
def mock_ts_by_ids_response(rsps, cognite_client):
    res = {
        "items": [
            {
                "id": 1,
                "externalId": "1",
                "name": "stringname",
                "isString": True,
                "metadata": {"metadata-key": "metadata-value"},
                "unit": "string",
                "assetId": 1,
                "isStep": True,
                "description": "string",
                "securityCategories": [0],
                "createdTime": 0,
                "lastUpdatedTime": 0,
            }
        ]
    }
    rsps.add(
        rsps.POST, cognite_client.time_series._get_base_url_with_base_path() + "/timeseries/byids", status=200, json=res
    )
    yield rsps


@pytest.fixture
def mock_asset_by_ids_response(rsps, cognite_client):
    res = {"items": [{"id": 1, "externalId": "1", "name": "assetname"}]}
    rsps.add(
        rsps.POST, cognite_client.time_series._get_base_url_with_base_path() + "/assets/byids", status=200, json=res
    )
    yield rsps


@pytest.fixture
def mock_count_dps_in_ts(mock_ts_by_ids_response, cognite_client):
    mock_ts_by_ids_response.add(
        mock_ts_by_ids_response.POST,
        cognite_client.time_series._get_base_url_with_base_path() + "/timeseries/data/list",
        status=200,
        json={
            "items": [
                {
                    "id": 1,
                    "externalId": "1",
                    "isString": False,
                    "isStep": False,
                    "datapoints": [{"timestamp": 1, "count": 10}, {"timestamp": 2, "count": 5}],
                }
            ]
        },
    )
    yield mock_ts_by_ids_response


@pytest.fixture
def mock_get_latest_dp_in_ts(mock_ts_by_ids_response, cognite_client):
    mock_ts_by_ids_response.add(
        mock_ts_by_ids_response.POST,
        cognite_client.time_series._get_base_url_with_base_path() + "/timeseries/data/latest",
        status=200,
        json={
            "items": [
                {
                    "id": 1,
                    "externalId": "1",
                    "isString": False,
                    "isStep": False,
                    "datapoints": [{"timestamp": 1, "value": 10}],
                }
            ]
        },
    )
    yield mock_ts_by_ids_response


@pytest.fixture
def mock_get_first_dp_in_ts(mock_ts_by_ids_response, cognite_client):
    mock_ts_by_ids_response.add(
        mock_ts_by_ids_response.POST,
        cognite_client.time_series._get_base_url_with_base_path() + "/timeseries/data/list",
        status=200,
        json={
            "items": [
                {
                    "id": 1,
                    "externalId": "1",
                    "isString": False,
                    "isStep": False,
                    "datapoints": [{"timestamp": 1, "value": 10}],
                }
            ]
        },
    )
    yield mock_ts_by_ids_response


class TestTimeSeries:
    def test_get_count(self, cognite_client, mock_count_dps_in_ts):
        now = utils.timestamp_to_ms("now")
        assert 15 == cognite_client.time_series.retrieve(id=1).count()
        assert "count" == jsgz_load(mock_count_dps_in_ts.calls[1].request.body)["aggregates"][0]
        assert 0 == jsgz_load(mock_count_dps_in_ts.calls[1].request.body)["start"]
        assert now <= jsgz_load(mock_count_dps_in_ts.calls[1].request.body)["end"]

    def test_get_latest(self, cognite_client, mock_get_latest_dp_in_ts):
        res = cognite_client.time_series.retrieve(id=1).latest()
        assert isinstance(res, Datapoint)
        assert Datapoint(timestamp=1, value=10) == res

    def test_get_first_datapoint(self, cognite_client, mock_get_first_dp_in_ts):
        now = utils.timestamp_to_ms("now")
        res = cognite_client.time_series.retrieve(id=1).first()
        assert isinstance(res, Datapoint)
        assert Datapoint(timestamp=1, value=10) == res
        assert 0 == jsgz_load(mock_get_first_dp_in_ts.calls[1].request.body)["start"]
        assert now <= jsgz_load(mock_get_first_dp_in_ts.calls[1].request.body)["end"]
        assert 1 == jsgz_load(mock_get_first_dp_in_ts.calls[1].request.body)["limit"]

    def test_asset(self, cognite_client, mock_ts_by_ids_response, mock_asset_by_ids_response):
        now = utils.timestamp_to_ms("now")
        asset = cognite_client.time_series.retrieve(id=1).asset()
        assert isinstance(asset, Asset)
        assert "assetname" == asset.name
