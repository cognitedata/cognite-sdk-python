from collections.abc import Iterator

import pytest
from responses import RequestsMock

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, Datapoint


@pytest.fixture
def mock_ts_by_ids_response(rsps: RequestsMock, cognite_client: CogniteClient) -> Iterator[RequestsMock]:
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
def mock_asset_by_ids_response(rsps: RequestsMock, cognite_client: CogniteClient) -> Iterator[RequestsMock]:
    res = {"items": [{"id": 1, "externalId": "1", "name": "assetname", "createdTime": 0, "lastUpdatedTime": 0}]}
    rsps.add(
        rsps.POST, cognite_client.time_series._get_base_url_with_base_path() + "/assets/byids", status=200, json=res
    )
    yield rsps


@pytest.fixture
def mock_count_dps_in_ts(
    mock_ts_by_ids_response: RequestsMock, cognite_client: CogniteClient
) -> Iterator[RequestsMock]:
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
def mock_get_latest_dp_in_ts(
    mock_ts_by_ids_response: RequestsMock, cognite_client: CogniteClient
) -> Iterator[RequestsMock]:
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
def mock_get_first_dp_in_ts(
    mock_ts_by_ids_response: RequestsMock, cognite_client: CogniteClient
) -> Iterator[RequestsMock]:
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
    def test_get_count__string_raises(
        self, cognite_client: CogniteClient, mock_ts_by_ids_response: RequestsMock
    ) -> None:
        with pytest.raises(RuntimeError, match="String time series does not support count aggregate"):
            ts = cognite_client.time_series.retrieve(id=1)
            assert ts
            ts.count()

    def test_get_latest(self, cognite_client: CogniteClient, mock_get_latest_dp_in_ts: RequestsMock) -> None:
        ts = cognite_client.time_series.retrieve(id=1)
        assert ts
        res = ts.latest()
        assert isinstance(res, Datapoint)
        assert Datapoint(timestamp=1, value=10) == res

    def test_asset(
        self,
        cognite_client: CogniteClient,
        mock_ts_by_ids_response: RequestsMock,
        mock_asset_by_ids_response: RequestsMock,
    ) -> None:
        ts = cognite_client.time_series.retrieve(id=1)
        assert ts
        asset = ts.asset()
        assert isinstance(asset, Asset)
        assert "assetname" == asset.name
