from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Stream, StreamList, StreamWrite, StreamWriteSettings
from tests.utils import get_url

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient

STREAM_RESPONSE_BODY = {
    "externalId": "test_stream",
    "createdTime": 1761319720908,
    "createdFromTemplate": "ImmutableTestStream",
    "type": "Immutable",
    "settings": {
        "lifecycle": {
            "retainedAfterSoftDelete": "PT24H",
            "dataDeletedAfter": "PT168H",
        },
        "limits": {
            "maxFilteringInterval": "PT168H",
            "maxRecordsTotal": {"provisioned": 500000000},
            "maxGigaBytesTotal": {"provisioned": 500},
        },
    },
}


class TestStreams:
    @pytest.fixture
    def mock_streams_create_response(
        self, httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> HTTPXMock:
        url_pattern = re.compile(re.escape(get_url(async_client.data_modeling.streams)) + "/streams$")
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=201, json={"items": [STREAM_RESPONSE_BODY]})
        return httpx_mock

    @pytest.fixture
    def mock_streams_delete_response(
        self, httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> HTTPXMock:
        url_pattern = re.compile(re.escape(get_url(async_client.data_modeling.streams)) + "/streams/delete$")
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=200, json={})
        return httpx_mock

    @pytest.fixture
    def mock_streams_retrieve_response(
        self, httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> HTTPXMock:
        url_pattern = re.compile(
            re.escape(get_url(async_client.data_modeling.streams)) + "/streams/test_stream(\\?.*)?$"
        )
        httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=STREAM_RESPONSE_BODY)
        return httpx_mock

    @pytest.fixture
    def mock_streams_retrieve_not_found(
        self, httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> HTTPXMock:
        url_pattern = re.compile(
            re.escape(get_url(async_client.data_modeling.streams)) + "/streams/nonexistent(\\?.*)?$"
        )
        httpx_mock.add_response(
            method="GET",
            url=url_pattern,
            status_code=404,
            json={"error": {"code": 404, "message": "Not Found."}},
        )
        return httpx_mock

    @pytest.fixture
    def mock_streams_list_response(
        self, httpx_mock: HTTPXMock, cognite_client: CogniteClient, async_client: AsyncCogniteClient
    ) -> HTTPXMock:
        url_pattern = re.compile(re.escape(get_url(async_client.data_modeling.streams)) + "/streams(\\?.*)?$")
        httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json={"items": [STREAM_RESPONSE_BODY]})
        return httpx_mock

    @pytest.mark.usefixtures("disable_gzip")
    def test_create_stream(self, cognite_client: CogniteClient, mock_streams_create_response: HTTPXMock) -> None:
        stream = StreamWrite(
            external_id="test_stream",
            settings=StreamWriteSettings(template_name="ImmutableTestStream"),
        )
        result = cognite_client.data_modeling.streams.create(stream)
        assert isinstance(result, Stream)
        assert result.external_id == "test_stream"
        assert result.type == "Immutable"

        requests = mock_streams_create_response.get_requests()
        assert len(requests) == 1
        sent_payload = json.loads(requests[0].content)
        assert sent_payload["items"][0] == {
            "externalId": "test_stream",
            "settings": {"template": {"name": "ImmutableTestStream"}},
        }

    @pytest.mark.usefixtures("disable_gzip")
    def test_delete_stream(self, cognite_client: CogniteClient, mock_streams_delete_response: HTTPXMock) -> None:
        cognite_client.data_modeling.streams.delete(external_id="test_stream")

        requests = mock_streams_delete_response.get_requests()
        assert len(requests) == 1
        sent_payload = json.loads(requests[0].content)
        assert sent_payload == {"items": [{"externalId": "test_stream"}]}

    def test_retrieve_stream(self, cognite_client: CogniteClient, mock_streams_retrieve_response: HTTPXMock) -> None:
        result = cognite_client.data_modeling.streams.retrieve(external_id="test_stream")
        assert isinstance(result, Stream)
        assert result.external_id == "test_stream"
        assert result.type == "Immutable"
        assert result.created_from_template == "ImmutableTestStream"

    def test_retrieve_stream_not_found(
        self, cognite_client: CogniteClient, mock_streams_retrieve_not_found: HTTPXMock
    ) -> None:
        result = cognite_client.data_modeling.streams.retrieve(external_id="nonexistent")
        assert result is None

    def test_list_streams(self, cognite_client: CogniteClient, mock_streams_list_response: HTTPXMock) -> None:
        result = cognite_client.data_modeling.streams.list()
        assert isinstance(result, StreamList)
        assert len(result) == 1
        assert result[0].external_id == "test_stream"
