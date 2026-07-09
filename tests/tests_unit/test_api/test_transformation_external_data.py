from __future__ import annotations

from collections.abc import Iterator

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.transformations.external_data import (
    ExternalDataSource,
    ExternalDataSourceList,
    ExternalDataSourceUsability,
    ExternalDataSourceWrite,
)
from tests.utils import get_url, jsgz_load


@pytest.fixture
def external_data_url(async_client: AsyncCogniteClient) -> str:
    return get_url(
        async_client.transformations.external_data_sources,
        async_client.transformations.external_data_sources._RESOURCE_PATH,
    )


@pytest.fixture
def source_response_body() -> dict:
    return {
        "items": [
            {
                "externalId": "x",
                "format": "one_lake",
                "settings": {
                    "credentials": {"clientId": "cid", "tenantId": "tid"},
                    "locationDescription": {"workspaceName": "ws", "containerName": "cn"},
                },
            }
        ]
    }


@pytest.fixture
def mock_list_response(
    httpx_mock: HTTPXMock, external_data_url: str, source_response_body: dict
) -> Iterator[HTTPXMock]:
    httpx_mock.add_response(
        method="GET",
        url=external_data_url + "?limit=1000",
        status_code=200,
        json=source_response_body,
    )
    yield httpx_mock


@pytest.fixture
def mock_upsert_response(
    httpx_mock: HTTPXMock, external_data_url: str, source_response_body: dict
) -> Iterator[HTTPXMock]:
    httpx_mock.add_response(method="POST", url=external_data_url, status_code=201, json=source_response_body)
    yield httpx_mock


@pytest.fixture
def mock_delete_response(httpx_mock: HTTPXMock, external_data_url: str) -> Iterator[HTTPXMock]:
    httpx_mock.add_response(method="POST", url=external_data_url + "/delete", status_code=200, json={})
    yield httpx_mock


@pytest.fixture
def mock_verify_usability_response(httpx_mock: HTTPXMock, external_data_url: str) -> Iterator[HTTPXMock]:
    httpx_mock.add_response(
        method="POST",
        url=external_data_url + "/usability",
        status_code=200,
        json={"externalId": "x", "usableVersion": "abc-uuid"},
    )
    yield httpx_mock


class TestTransformationExternalDataAPI:
    @pytest.mark.usefixtures("mock_list_response")
    def test_list(self, cognite_client: CogniteClient) -> None:
        result = cognite_client.transformations.external_data_sources.list()

        assert isinstance(result, ExternalDataSourceList)
        assert len(result) == 1
        assert result[0].external_id == "x"
        assert result[0].format == "one_lake"

    def test_upsert_single(self, cognite_client: CogniteClient, mock_upsert_response: HTTPXMock) -> None:
        source = ExternalDataSourceWrite.onelake(
            external_id="x",
            client_id="cid",
            tenant_id="tid",
            client_secret="sec",
            workspace_name="ws",
            container_name="cn",
        )
        result = cognite_client.transformations.external_data_sources.upsert(source)

        assert isinstance(result, ExternalDataSource)
        assert result.external_id == "x"
        # Verify the POST body contained "format": "one_lake"
        request_body = jsgz_load(mock_upsert_response.get_requests()[-1].content)
        assert request_body["items"][0]["format"] == "one_lake"

    def test_delete(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        mock_delete_response: HTTPXMock,
    ) -> None:
        cognite_client.transformations.external_data_sources.delete("x")

        last_request = mock_delete_response.get_requests()[-1]
        url = str(last_request.url)
        assert url.endswith(async_client.transformations.external_data_sources._RESOURCE_PATH + "/delete")
        request_body = jsgz_load(last_request.content)
        assert request_body["items"] == [{"externalId": "x"}]

    def test_verify_usability(self, cognite_client: CogniteClient, mock_verify_usability_response: HTTPXMock) -> None:
        result = cognite_client.transformations.external_data_sources.verify_usability("x")

        assert isinstance(result, ExternalDataSourceUsability)
        assert result.usable_version == "abc-uuid"
        assert result.external_id == "x"
