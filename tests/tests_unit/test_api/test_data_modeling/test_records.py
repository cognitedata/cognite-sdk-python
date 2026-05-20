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
    def test_delete_posts_space_external_id_pairs(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=re.compile(re.escape(records_base_url) + r"/delete$"), status_code=200
        )
        cognite_client.data_modeling.records.delete("my-stream", RecordId(space="sp", external_id="rec-1"))
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        body = jsgz_load(requests[0].content)
        assert body == {"items": [{"space": "sp", "externalId": "rec-1"}]}

    def test_delete_accepts_sequence(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=re.compile(re.escape(records_base_url) + r"/delete$"), status_code=200
        )
        items = [RecordId(space="sp", external_id="rec-1"), RecordId(space="sp", external_id="rec-2")]
        cognite_client.data_modeling.records.delete("my-stream", items)
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {
            "items": [
                {"space": "sp", "externalId": "rec-1"},
                {"space": "sp", "externalId": "rec-2"},
            ]
        }

    def test_delete_chunks_over_1000(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
    ) -> None:
        url_pattern = re.compile(re.escape(records_base_url) + r"/delete$")
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=200)
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=200)
        items = [RecordId(space="sp", external_id=f"r-{i}") for i in range(1001)]
        cognite_client.data_modeling.records.delete("my-stream", items)
        assert len(httpx_mock.get_requests()) == 2
