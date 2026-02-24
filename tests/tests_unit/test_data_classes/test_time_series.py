from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, Datapoint
from tests.utils import get_or_raise, get_url

if TYPE_CHECKING:
    from pytest_httpx import HTTPXMock

    from cognite.client import AsyncCogniteClient, CogniteClient


@pytest.fixture
def mock_ts_by_ids_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[HTTPXMock]:
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
    httpx_mock.add_response(
        method="POST", url=get_url(async_client.time_series) + "/timeseries/byids", status_code=200, json=res
    )
    yield httpx_mock


@pytest.fixture
def mock_asset_by_ids_response(
    httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[HTTPXMock]:
    res = {
        "items": [
            {"id": 1, "externalId": "1", "name": "assetname", "rootId": 1, "createdTime": 0, "lastUpdatedTime": 0}
        ]
    }
    httpx_mock.add_response(
        method="POST", url=get_url(async_client.time_series) + "/assets/byids", status_code=200, json=res
    )
    yield httpx_mock


@pytest.fixture
def mock_count_dps_in_ts(
    mock_ts_by_ids_response: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[HTTPXMock]:
    mock_ts_by_ids_response.add_response(
        method="POST",
        url=get_url(async_client.time_series) + "/timeseries/data/list",
        status_code=200,
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
    mock_ts_by_ids_response: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[HTTPXMock]:
    mock_ts_by_ids_response.add_response(
        method="POST",
        url=get_url(async_client.time_series) + "/timeseries/data/latest",
        status_code=200,
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
    mock_ts_by_ids_response: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
) -> Iterator[HTTPXMock]:
    mock_ts_by_ids_response.add_response(
        method="POST",
        url=get_url(async_client.time_series) + "/timeseries/data/list",
        status_code=200,
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
    def test_get_count__string_raises(self, cognite_client: CogniteClient, mock_ts_by_ids_response: HTTPXMock) -> None:
        with pytest.raises(RuntimeError, match="String time series does not support count aggregate"):
            get_or_raise(cognite_client.time_series.retrieve(id=1)).count()

    def test_get_latest(self, cognite_client: CogniteClient, mock_get_latest_dp_in_ts: HTTPXMock) -> None:
        res = get_or_raise(cognite_client.time_series.retrieve(id=1)).latest()
        assert isinstance(res, Datapoint)
        assert Datapoint(timestamp=1, value=10) == res

    async def test_asset(
        self, cognite_client: CogniteClient, mock_ts_by_ids_response: HTTPXMock, mock_asset_by_ids_response: HTTPXMock
    ) -> None:
        ts = get_or_raise(cognite_client.time_series.retrieve(id=1))
        asset = await ts.asset_async()
        assert isinstance(asset, Asset)
        assert "assetname" == asset.name
