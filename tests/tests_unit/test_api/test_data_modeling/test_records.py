from __future__ import annotations

import re
from collections.abc import Callable

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.data_modeling.records import (
    AvgAggregate,
    CountAggregate,
    Record,
    RecordList,
    RecordSource,
    RecordSourceReference,
    RecordWrite,
    SyncRecord,
    TimeRange,
)
from tests.utils import jsgz_load


@pytest.fixture
def records_base_url(async_client: AsyncCogniteClient) -> str:
    return async_client.data_modeling.records._base_url_with_base_path + "/streams/my-stream/records"


@pytest.fixture
def make_record_response() -> Callable[..., dict]:
    def _make(external_id: str = "rec-1", space: str = "sp") -> dict:
        return {
            "space": space,
            "externalId": external_id,
            "createdTime": 1000,
            "lastUpdatedTime": 2000,
            "properties": {"sp": {"container-x": {"temp": 22.5}}},
        }

    return _make


@pytest.fixture
def record_response(make_record_response: Callable[..., dict]) -> dict:
    return make_record_response()


@pytest.fixture
def record_list_response(record_response: dict) -> dict:
    return {"items": [record_response]}


@pytest.fixture
def write_item() -> RecordWrite:
    return RecordWrite(
        space="sp",
        external_id="rec-1",
        sources=[
            RecordSource(
                source=RecordSourceReference(space="sp", external_id="container-x"),
                properties={"temp": 22.5},
            )
        ],
    )


class TestRecordsAPIIngest:
    def test_ingest_single_posts_correct_body(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
        write_item: RecordWrite,
    ) -> None:
        httpx_mock.add_response(method="POST", url=re.compile(re.escape(records_base_url) + r"$"), status_code=202)
        cognite_client.data_modeling.records.ingest("my-stream", write_item)
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        body = jsgz_load(requests[0].content)
        assert body == {
            "items": [
                {
                    "space": "sp",
                    "externalId": "rec-1",
                    "sources": [
                        {
                            "source": {"type": "container", "space": "sp", "externalId": "container-x"},
                            "properties": {"temp": 22.5},
                        }
                    ],
                }
            ]
        }

    def test_ingest_chunks_over_1000(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
    ) -> None:
        url_pattern = re.compile(re.escape(records_base_url) + r"$")
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=202)
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=202)
        items = [RecordWrite(space="sp", external_id=f"r-{i}", sources=[]) for i in range(1001)]
        cognite_client.data_modeling.records.ingest("my-stream", items)
        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        assert len(jsgz_load(requests[0].content)["items"]) == 1000
        assert len(jsgz_load(requests[1].content)["items"]) == 1


class TestRecordsAPIUpsert:
    def test_upsert_posts_to_upsert_path(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
        write_item: RecordWrite,
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=re.compile(re.escape(records_base_url) + r"/upsert$"), status_code=202
        )
        cognite_client.data_modeling.records.upsert("my-stream", write_item)
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].url.path.endswith("/records/upsert")


class TestRecordsAPIDelete:
    def test_delete_posts_space_external_id_pairs(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
        write_item: RecordWrite,
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=re.compile(re.escape(records_base_url) + r"/delete$"), status_code=200
        )
        cognite_client.data_modeling.records.delete("my-stream", write_item)
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        body = jsgz_load(requests[0].content)
        assert body == {"items": [{"space": "sp", "externalId": "rec-1"}]}

    def test_delete_accepts_record_objects(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
        record_response: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=re.compile(re.escape(records_base_url) + r"/delete$"), status_code=200
        )
        record = Record._load(record_response)
        cognite_client.data_modeling.records.delete("my-stream", record)
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {"items": [{"space": "sp", "externalId": "rec-1"}]}

    def test_delete_chunks_over_1000(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
    ) -> None:
        url_pattern = re.compile(re.escape(records_base_url) + r"/delete$")
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=200)
        httpx_mock.add_response(method="POST", url=url_pattern, status_code=200)
        items = [RecordWrite(space="sp", external_id=f"r-{i}", sources=[]) for i in range(1001)]
        cognite_client.data_modeling.records.delete("my-stream", items)
        assert len(httpx_mock.get_requests()) == 2


class TestRecordsAPIList:
    def test_list_returns_record_list(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
        record_list_response: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=re.compile(re.escape(records_base_url) + r"/filter$"), json=record_list_response
        )
        out = cognite_client.data_modeling.records.list("my-stream")
        assert isinstance(out, RecordList)
        assert out[0].external_id == "rec-1"
        assert out[0].properties == {"sp": {"container-x": {"temp": 22.5}}}

    def test_list_sends_last_updated_time(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
        record_list_response: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=re.compile(re.escape(records_base_url) + r"/filter$"), json=record_list_response
        )
        cognite_client.data_modeling.records.list(
            "my-stream",
            last_updated_time=TimeRange(gte=1_000_000),
            limit=50,
        )
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["lastUpdatedTime"] == {"gte": 1_000_000}
        assert body["limit"] == 50


class TestRecordsAPISync:
    def test_sync_yields_records_and_follows_has_next(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
    ) -> None:
        url = re.compile(re.escape(records_base_url) + r"/sync$")
        page1 = {
            "items": [{"space": "sp", "externalId": "a", "createdTime": 1, "lastUpdatedTime": 2, "status": "created"}],
            "nextCursor": "cursor-2",
            "hasNext": True,
        }
        page2 = {
            "items": [{"space": "sp", "externalId": "b", "createdTime": 3, "lastUpdatedTime": 4, "status": "updated"}],
            "nextCursor": "cursor-end",
            "hasNext": False,
        }
        httpx_mock.add_response(method="POST", url=url, json=page1)
        httpx_mock.add_response(method="POST", url=url, json=page2)

        out = list(cognite_client.data_modeling.records.sync("my-stream", initialize_cursor="1d-ago"))
        assert len(out) == 2
        assert isinstance(out[0], SyncRecord)
        assert out[0].external_id == "a"
        assert out[1].external_id == "b"
        assert len(httpx_mock.get_requests()) == 2

    def test_sync_deleted_tombstone_has_no_properties(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(records_base_url) + r"/sync$"),
            json={
                "items": [
                    {"space": "sp", "externalId": "gone", "createdTime": 1, "lastUpdatedTime": 5, "status": "deleted"}
                ],
                "nextCursor": "c",
                "hasNext": False,
            },
        )
        out = list(cognite_client.data_modeling.records.sync("my-stream", cursor="some-cursor"))
        assert len(out) == 1
        assert out[0].status == "deleted"
        assert out[0].properties is None


class TestRecordsAPIAggregate:
    def test_aggregate_posts_correct_body(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        records_base_url: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(records_base_url) + r"/aggregate$"),
            json={"aggregates": {"total": {"count": 42}, "avg_t": {"avg": 21.5}}},
        )
        prop = ["sp", "container-x", "temp"]
        res = cognite_client.data_modeling.records.aggregate(
            "my-stream",
            aggregates={
                "total": CountAggregate(property=prop),
                "avg_t": AvgAggregate(property=prop),
            },
            last_updated_time=TimeRange(gte=0),
        )
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["aggregates"] == {
            "total": {"count": {"property": prop}},
            "avg_t": {"avg": {"property": prop}},
        }
        assert body["lastUpdatedTime"] == {"gte": 0}
        assert res == {"total": {"count": 42}, "avg_t": {"avg": 21.5}}


class TestRecordDTOs:
    def test_record_load_round_trip(self, record_response: dict) -> None:
        r = Record._load(record_response)
        assert r.space == "sp"
        assert r.external_id == "rec-1"
        assert r.created_time == 1000
        assert r.last_updated_time == 2000
        assert r.properties == {"sp": {"container-x": {"temp": 22.5}}}

    def test_record_as_write_reconstructs_sources(self, record_response: dict) -> None:
        r = Record._load(record_response)
        w = r.as_write()
        assert isinstance(w, RecordWrite)
        assert w.space == "sp"
        assert w.external_id == "rec-1"
        assert len(w.sources) == 1
        assert w.sources[0].source.space == "sp"
        assert w.sources[0].source.external_id == "container-x"
        assert w.sources[0].properties == {"temp": 22.5}

    def test_sync_record_deleted_tombstone(self) -> None:
        r = SyncRecord._load(
            {"space": "sp", "externalId": "x", "createdTime": 1, "lastUpdatedTime": 2, "status": "deleted"}
        )
        assert r.status == "deleted"
        assert r.properties is None
