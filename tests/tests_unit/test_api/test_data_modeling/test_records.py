from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.data_modeling.data_types import UnitReference
from cognite.client.data_classes.data_modeling.instances import TypeInformation
from cognite.client.data_classes.data_modeling.records import (
    RecordContainerId,
    RecordId,
    RecordSource,
    RecordSourceSelector,
    RecordTargetUnit,
    RecordTargetUnits,
    RecordWrite,
    SyncRecord,
    SyncRecordList,
)
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
def mock_delete(httpx_mock: HTTPXMock, delete_url_pattern: re.Pattern) -> None:
    httpx_mock.add_response(method="POST", url=delete_url_pattern, status_code=200)


@pytest.fixture
def ingest_url_pattern(records_base_url: str) -> re.Pattern:
    return re.compile(re.escape(records_base_url) + r"$")


@pytest.fixture
def mock_ingest(httpx_mock: HTTPXMock, ingest_url_pattern: re.Pattern) -> None:
    httpx_mock.add_response(method="POST", url=ingest_url_pattern, status_code=202)


@pytest.fixture
def upsert_url_pattern(records_base_url: str) -> re.Pattern:
    return re.compile(re.escape(records_base_url) + r"/upsert$")


@pytest.fixture
def mock_upsert(httpx_mock: HTTPXMock, upsert_url_pattern: re.Pattern) -> None:
    httpx_mock.add_response(method="POST", url=upsert_url_pattern, status_code=202)


@pytest.fixture
def record_response() -> dict:
    return {
        "space": "sp",
        "externalId": "rec-1",
        "createdTime": 100,
        "lastUpdatedTime": 200,
        "properties": {"sp": {"container-x": {"temp": 22.5}}},
    }


@pytest.fixture
def sync_url_pattern(records_base_url: str) -> re.Pattern:
    return re.compile(re.escape(records_base_url) + r"/sync$")


@pytest.fixture
def write_item() -> RecordWrite:
    return RecordWrite(
        space="sp",
        external_id="rec-1",
        sources=[
            RecordSource(
                source=RecordContainerId(space="sp", external_id="container-x"),
                properties={"temp": 22.5},
            )
        ],
    )


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


class TestRecordsAPIIngest:
    def test_ingest_single_posts_correct_body(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_ingest: None,
        stream_id: str,
        write_item: RecordWrite,
    ) -> None:
        cognite_client.data_modeling.records.ingest(write_item, stream_id=stream_id)
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
        ingest_url_pattern: re.Pattern,
        stream_id: str,
    ) -> None:
        httpx_mock.add_response(method="POST", url=ingest_url_pattern, status_code=202)
        httpx_mock.add_response(method="POST", url=ingest_url_pattern, status_code=202)
        items = [RecordWrite(space="sp", external_id=f"r-{i}", sources=[]) for i in range(1001)]
        cognite_client.data_modeling.records.ingest(items, stream_id=stream_id)
        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        assert len(jsgz_load(requests[0].content)["items"]) == 1000
        assert len(jsgz_load(requests[1].content)["items"]) == 1


class TestRecordsAPIUpsert:
    def test_upsert_single_posts_correct_body(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_upsert: None,
        stream_id: str,
        write_item: RecordWrite,
    ) -> None:
        cognite_client.data_modeling.records.upsert(write_item, stream_id=stream_id)
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].url.path.endswith(f"/streams/{stream_id}/records/upsert")
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

    def test_upsert_accepts_sequence(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_upsert: None,
        stream_id: str,
    ) -> None:
        items = [
            RecordWrite(space="sp", external_id="rec-1", sources=[]),
            RecordWrite(space="sp", external_id="rec-2", sources=[]),
        ]
        cognite_client.data_modeling.records.upsert(items, stream_id=stream_id)
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert [item["externalId"] for item in body["items"]] == ["rec-1", "rec-2"]

    def test_upsert_chunks(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        upsert_url_pattern: re.Pattern,
        stream_id: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(async_client.data_modeling.records, "_CREATE_LIMIT", 10)
        httpx_mock.add_response(method="POST", url=upsert_url_pattern, status_code=202)
        httpx_mock.add_response(method="POST", url=upsert_url_pattern, status_code=202)
        items = [RecordWrite(space="sp", external_id=f"r-{i}", sources=[]) for i in range(11)]
        cognite_client.data_modeling.records.upsert(items, stream_id=stream_id)
        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        assert len(jsgz_load(requests[0].content)["items"]) == 10
        assert len(jsgz_load(requests[1].content)["items"]) == 1


class TestRecordsAPISync:
    def test_sync_returns_page_with_cursor(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
    ) -> None:
        items = [{**record_response, "externalId": f"rec-{i}", "status": "created"} for i in range(10)]
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={"items": items, "nextCursor": "abc", "hasNext": False},
        )
        page = cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="7d-ago")
        assert isinstance(page, SyncRecordList)
        assert page.cursor == "abc"
        assert page.has_next is False
        assert page[0].status == "created"
        request = httpx_mock.get_requests()[0]
        assert request.url.path.endswith(f"/streams/{stream_id}/records/sync")
        assert jsgz_load(request.content) == {"initializeCursor": "7d-ago", "limit": 10}

    def test_sync_resume_sends_cursor(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        stream_id: str,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={
                "items": [
                    {"space": "sp", "externalId": "rec-1", "createdTime": 1, "lastUpdatedTime": 2, "status": "created"}
                ],
                "nextCursor": "p2",
                "hasNext": True,
            },
        )
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={
                "items": [
                    {"space": "sp", "externalId": "rec-2", "createdTime": 3, "lastUpdatedTime": 4, "status": "updated"}
                ],
                "nextCursor": "p3",
                "hasNext": False,
            },
        )
        first = cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="2d-ago", limit=1)
        assert first.has_next is True
        assert first.cursor is not None
        second = cognite_client.data_modeling.records.sync_resume(stream_id=stream_id, cursor=first.cursor, limit=1)
        assert second.cursor == "p3"
        body2 = jsgz_load(httpx_mock.get_requests()[1].content)
        assert body2 == {"cursor": "p2", "limit": 1}

    def test_sync_deleted_tombstone_has_no_properties(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        stream_id: str,
    ) -> None:
        item = {"space": "sp", "externalId": "rec-1", "createdTime": 1, "lastUpdatedTime": 2, "status": "deleted"}
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={"items": [item], "nextCursor": "z", "hasNext": False},
        )
        page = cognite_client.data_modeling.records.sync(stream_id=stream_id, initialize_cursor="c", limit=1)
        assert page[0].status == "deleted"
        assert page[0].properties is None

    def test_sync_include_typing(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
    ) -> None:
        item = {**record_response, "status": "updated"}
        typing = {"sp": {"container-x": {"temp": {"type": {"type": "float64", "list": False}, "nullable": True}}}}
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={"items": [item], "nextCursor": "z", "hasNext": False, "typing": typing},
        )
        page = cognite_client.data_modeling.records.sync(
            stream_id=stream_id, initialize_cursor="c", include_typing=True, limit=1
        )
        assert jsgz_load(httpx_mock.get_requests()[0].content)["includeTyping"] is True
        assert isinstance(page.typing, TypeInformation)

    def test_sync_target_units_body_shape(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        sync_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
    ) -> None:
        item = {**record_response, "status": "updated"}
        httpx_mock.add_response(
            method="POST",
            url=sync_url_pattern,
            status_code=200,
            json={"items": [item], "nextCursor": "z", "hasNext": False},
        )
        cognite_client.data_modeling.records.sync(
            stream_id=stream_id,
            initialize_cursor="c",
            target_units=RecordTargetUnits(unit_system_name="Imperial"),
            limit=1,
        )
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["targetUnits"] == {"unitSystemName": "Imperial"}


class TestRecordDTOs:
    def test_record_write_as_id(self, write_item: RecordWrite) -> None:
        rid = write_item.as_id()
        assert isinstance(rid, RecordId)
        assert rid.space == "sp"
        assert rid.external_id == "rec-1"

    def test_record_write_round_trip(self, write_item: RecordWrite) -> None:
        dumped = write_item.dump()
        loaded = RecordWrite._load(dumped)
        assert loaded.space == write_item.space
        assert loaded.external_id == write_item.external_id
        assert len(loaded.sources) == 1
        assert loaded.sources[0].source.space == "sp"
        assert loaded.sources[0].source.external_id == "container-x"
        assert loaded.sources[0].properties == {"temp": 22.5}

    def test_record_source_reference_dump(self) -> None:
        ref = RecordContainerId(space="s", external_id="c")
        d = ref.dump()
        assert d == {"type": "container", "space": "s", "externalId": "c"}

    def test_record_source_dump(self) -> None:
        src = RecordSource(
            source=RecordContainerId(space="s", external_id="c"),
            properties={"x": 1},
        )
        d = src.dump()
        assert d["source"]["type"] == "container"
        assert d["properties"] == {"x": 1}

    def test_record_source_selector_dump(self) -> None:
        selector = RecordSourceSelector(RecordContainerId(space="sp", external_id="c"), ["temp", "pressure"])
        assert selector.dump() == {
            "source": {"type": "container", "space": "sp", "externalId": "c"},
            "properties": ["temp", "pressure"],
        }

    def test_sync_record_as_write_reconstructs_sources(self) -> None:
        record = SyncRecord(
            space="sp",
            external_id="rec-1",
            created_time=1,
            last_updated_time=2,
            status="updated",
            properties={"sp": {"c": {"temp": 22.5}}},
        )
        write = record.as_write()
        assert isinstance(write, RecordWrite)
        assert write.dump()["sources"] == [
            {"source": {"type": "container", "space": "sp", "externalId": "c"}, "properties": {"temp": 22.5}}
        ]

    def test_record_target_units_dump(self) -> None:
        target_units = RecordTargetUnits(
            properties=[RecordTargetUnit(["sp", "c", "pressure"], UnitReference("pressure:pa"))]
        )
        assert target_units.dump() == {
            "properties": [{"property": ["sp", "c", "pressure"], "unit": {"externalId": "pressure:pa"}}]
        }

    def test_record_target_units_rejects_empty_request_mode(
        self, cognite_client: CogniteClient, stream_id: str
    ) -> None:
        expected_err = "Provide exactly one of 'properties' or 'unit_system_name'."
        with pytest.raises(ValueError, match=expected_err):
            cognite_client.data_modeling.records.sync(
                stream_id=stream_id,
                initialize_cursor="c",
                target_units=RecordTargetUnits(),
                limit=1,
            )

    def test_record_target_units_rejects_multiple_request_modes(
        self, cognite_client: CogniteClient, stream_id: str
    ) -> None:
        expected_err = "Provide exactly one of 'properties' or 'unit_system_name'."
        with pytest.raises(ValueError, match=expected_err):
            cognite_client.data_modeling.records.sync(
                stream_id=stream_id,
                initialize_cursor="c",
                target_units=RecordTargetUnits(properties=[], unit_system_name="Imperial"),
                limit=1,
            )

    def test_sync_record_load_dump_round_trip(self) -> None:
        payload = {
            "space": "sp",
            "externalId": "rec-1",
            "createdTime": 100,
            "lastUpdatedTime": 200,
            "status": "updated",
            "properties": {"sp": {"c": {"temp": 22.5}}},
        }
        record = SyncRecord._load(payload)
        assert isinstance(record, SyncRecord)
        assert record.status == "updated"
        assert record.dump() == payload

    def test_sync_record_deleted_tombstone(self) -> None:
        record = SyncRecord._load(
            {"space": "sp", "externalId": "rec-1", "createdTime": 1, "lastUpdatedTime": 2, "status": "deleted"}
        )
        assert record.status == "deleted"
        assert record.properties is None
        assert "properties" not in record.dump()
