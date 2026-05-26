from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.data_modeling.records import RecordId
from tests.utils import jsgz_load


@pytest.fixture
def records_base_url(async_client: AsyncCogniteClient) -> str:
    return async_client.data_modeling.records._base_url_with_base_path + "/streams/my-stream/records"


class TestRecordsAPIDelete:
    @pytest.fixture
    def mock_delete(self, httpx_mock: HTTPXMock, records_base_url: str) -> None:
        httpx_mock.add_response(
            method="POST", url=re.compile(re.escape(records_base_url) + r"/delete$"), status_code=200
        )

    def test_delete_posts_space_external_id_pairs(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_delete: None,
    ) -> None:
        cognite_client.data_modeling.records.delete(RecordId(space="sp", external_id="rec-1"), stream_id="my-stream")
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        body = jsgz_load(requests[0].content)
        assert body == {"items": [{"space": "sp", "externalId": "rec-1"}]}

    def test_delete_accepts_sequence(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_delete: None,
    ) -> None:
        items = [RecordId(space="sp", external_id="rec-1"), RecordId(space="sp", external_id="rec-2")]
        cognite_client.data_modeling.records.delete(items, stream_id="my-stream")
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
        records_base_url: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(async_client.data_modeling.records, "_DELETE_LIMIT", 42)
        url_pattern = re.compile(re.escape(records_base_url) + r"/delete$")
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=200)
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=200)
        items = [RecordId(space="sp", external_id=f"r-{i}") for i in range(43)]
        cognite_client.data_modeling.records.delete(items, stream_id="my-stream")
        assert len(httpx_mock.get_requests()) == 2
