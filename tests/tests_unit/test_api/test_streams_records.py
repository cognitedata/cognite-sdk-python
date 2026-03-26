"""Unit tests for ILA :class:`~cognite.client._api.streams.records.StreamsRecordsAPI` (via sync client + httpx)."""

from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.streams import (
    RecordsAggregateResponse,
    RecordsDeleteResponse,
    RecordsFilterResponse,
    RecordsIngestResponse,
    RecordsSyncResponse,
)


@pytest.fixture
def streams_base_url(async_client: AsyncCogniteClient) -> str:
    return async_client.streams._base_url_with_base_path + "/streams"


class TestIngest:
    def test_posts_body_and_returns_ingest_response(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(streams_base_url) + r"/my-stream/records$"),
            json={},
        )
        out = cognite_client.streams.records.ingest("my-stream", {"items": []})
        assert isinstance(out, RecordsIngestResponse)


class TestIngestItems:
    def test_wraps_sequence_as_items_key(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(streams_base_url) + r"/s1/records$"),
            json={},
        )
        row = {"space": "sp", "externalId": "r1", "sources": []}
        cognite_client.streams.records.ingest_items("s1", [row])
        req = httpx_mock.get_requests()[0]
        import json as _json

        body = _json.loads(req.content.decode())
        assert body == {"items": [{"space": "sp", "externalId": "r1", "sources": []}]}

    def test_empty_items_raises(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="at least one record"):
            cognite_client.streams.records.ingest_items("s1", [])


class TestUpsertAndUpsertItems:
    def test_upsert_posts_to_upsert_path(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(streams_base_url) + r"/st/records/upsert$"),
            json={},
        )
        cognite_client.streams.records.upsert("st", {"items": [{"x": 1}]})

    def test_upsert_items_requires_non_empty(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="at least one record"):
            cognite_client.streams.records.upsert_items("st", [])


class TestDeleteAndDeleteItems:
    def test_delete_posts_body(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(streams_base_url) + r"/st/records/delete$"),
            json={},
        )
        out = cognite_client.streams.records.delete("st", {"items": [{"space": "sp", "externalId": "a"}]})
        assert isinstance(out, RecordsDeleteResponse)

    def test_delete_items_wraps_identifiers(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(streams_base_url) + r"/st/records/delete$"),
            json={},
        )
        cognite_client.streams.records.delete_items("st", [{"space": "sp", "externalId": "a"}])

    def test_delete_items_empty_raises(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="at least one"):
            cognite_client.streams.records.delete_items("st", [])


class TestFilter:
    def test_parses_filter_response(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        payload = {
            "items": [
                {
                    "space": "sp",
                    "externalId": "r1",
                    "createdTime": 1,
                    "lastUpdatedTime": 2,
                    "properties": {},
                }
            ]
        }
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(streams_base_url) + r"/st/records/filter$"),
            json=payload,
        )
        out = cognite_client.streams.records.filter("st", {"filter": {"matchAll": {}}})
        assert isinstance(out, RecordsFilterResponse)
        assert len(out.items) == 1
        assert out.items[0].external_id == "r1"


class TestAggregate:
    def test_parses_aggregates(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(streams_base_url) + r"/st/records/aggregate$"),
            json={"aggregates": {"cnt": 3}, "typing": {}},
        )
        out = cognite_client.streams.records.aggregate("st", {"aggregate": []})
        assert isinstance(out, RecordsAggregateResponse)
        assert out.aggregates == {"cnt": 3}


class TestSync:
    def test_parses_sync_page(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        payload = {
            "items": [
                {
                    "space": "sp",
                    "externalId": "r1",
                    "createdTime": 1,
                    "lastUpdatedTime": 2,
                    "status": "created",
                }
            ],
            "nextCursor": "next",
            "hasNext": False,
        }
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(streams_base_url) + r"/st/records/sync$"),
            json=payload,
        )
        out = cognite_client.streams.records.sync("st", {"sources": [], "limit": 10})
        assert isinstance(out, RecordsSyncResponse)
        assert out.next_cursor == "next"
        assert not out.has_next
        assert out.items[0].status == "created"
