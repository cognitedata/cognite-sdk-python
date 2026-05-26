from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.data_modeling.records import RecordId
from tests.utils import jsgz_load


@pytest.fixture
def stream_id() -> str:
    return "my-stream"


@pytest.fixture
def records_base_url(async_client: AsyncCogniteClient, stream_id: str) -> str:
    return async_client.data_modeling.records._base_url_with_base_path + f"/streams/{stream_id}/records"


@pytest.fixture
def delete_url_pattern(records_base_url: str) -> re.Pattern:
    return re.compile(re.escape(records_base_url) + r"/delete$")


@pytest.fixture
def mock_delete(self, httpx_mock: HTTPXMock, delete_url_pattern: re.Pattern) -> None:
    httpx_mock.add_response(method="POST", url=delete_url_pattern, status_code=200)


class TestRecordsAPIDelete:
    def test_delete_posts_space_external_id_pairs(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_delete: None,
        stream_id: str,
    ) -> None:
        cognite_client.data_modeling.records.delete(RecordId(space="sp", external_id="rec-1"), stream_id=stream_id)
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        body = jsgz_load(requests[0].content)
        assert body == {"items": [{"space": "sp", "externalId": "rec-1"}]}

    def test_delete_accepts_sequence(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_delete: None,
        stream_id: str,
    ) -> None:
        items = [RecordId(space="sp", external_id="rec-1"), RecordId(space="sp", external_id="rec-2")]
        cognite_client.data_modeling.records.delete(items, stream_id=stream_id)
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {
            "items": [
                {"space": "sp", "externalId": "rec-1"},
                {"space": "sp", "externalId": "rec-2"},
            ]
        }

    def test_delete_chunks(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        delete_url_pattern: re.Pattern,
        stream_id: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(async_client.data_modeling.records, "_DELETE_LIMIT", 42)
        httpx_mock.add_response(method="POST", url=delete_url_pattern, status_code=200)
        httpx_mock.add_response(method="POST", url=delete_url_pattern, status_code=200)
        items = [RecordId(space="sp", external_id=f"r-{i}") for i in range(43)]
        cognite_client.data_modeling.records.delete(items, stream_id=stream_id)
        assert len(httpx_mock.get_requests()) == 2
