from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.data_modeling.instances import InstanceSort, TypeInformation
from cognite.client.data_classes.data_modeling.records import (
    Record,
    RecordContainerId,
    RecordId,
    RecordList,
    RecordSource,
    RecordSourceSelector,
    RecordWrite,
    TimeRange,
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
def filter_url_pattern(records_base_url: str) -> re.Pattern:
    return re.compile(re.escape(records_base_url) + r"/filter$")


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
def mock_filter(httpx_mock: HTTPXMock, filter_url_pattern: re.Pattern, record_response: dict) -> None:
    httpx_mock.add_response(method="POST", url=filter_url_pattern, status_code=200, json={"items": [record_response]})


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


class TestRecordsAPIList:
    def test_list_returns_record_list(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_filter: None,
        stream_id: str,
    ) -> None:
        result = cognite_client.data_modeling.records.list(stream_id=stream_id)
        assert isinstance(result, RecordList)
        assert len(result) == 1
        assert result[0].external_id == "rec-1"
        assert result[0].properties == {"sp": {"container-x": {"temp": 22.5}}}
        request = httpx_mock.get_requests()[0]
        assert request.url.path.endswith(f"/streams/{stream_id}/records/filter")

    def test_list_default_limit_is_10(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_filter: None,
        stream_id: str,
    ) -> None:
        cognite_client.data_modeling.records.list(stream_id=stream_id)
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body == {"limit": 10}

    def test_list_sends_last_updated_time_and_limit(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_filter: None,
        stream_id: str,
    ) -> None:
        cognite_client.data_modeling.records.list(
            stream_id=stream_id, last_updated_time=TimeRange(gte=1_000_000), limit=50
        )
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["lastUpdatedTime"] == {"gte": 1_000_000}
        assert body["limit"] == 50

    def test_list_sources_body_shape(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_filter: None,
        stream_id: str,
    ) -> None:
        cognite_client.data_modeling.records.list(
            stream_id=stream_id,
            sources=[RecordSourceSelector(RecordContainerId(space="sp", external_id="container-x"), ["*"])],
        )
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["sources"] == [
            {"source": {"type": "container", "space": "sp", "externalId": "container-x"}, "properties": ["*"]}
        ]

    def test_list_sort_body_shape(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        mock_filter: None,
        stream_id: str,
    ) -> None:
        cognite_client.data_modeling.records.list(
            stream_id=stream_id, sort=InstanceSort(property=["sp", "container-x", "temp"], direction="descending")
        )
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["sort"] == [{"property": ["sp", "container-x", "temp"], "direction": "descending"}]

    def test_list_include_typing(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        filter_url_pattern: re.Pattern,
        record_response: dict,
        stream_id: str,
    ) -> None:
        typing = {"sp": {"container-x": {"temp": {"type": {"type": "float64", "list": False}, "nullable": True}}}}
        httpx_mock.add_response(
            method="POST", url=filter_url_pattern, status_code=200, json={"items": [record_response], "typing": typing}
        )
        result = cognite_client.data_modeling.records.list(stream_id=stream_id, include_typing=True)
        body = jsgz_load(httpx_mock.get_requests()[0].content)
        assert body["includeTyping"] is True
        assert isinstance(result.typing, TypeInformation)


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

    def test_record_load_dump_round_trip(self) -> None:
        payload = {
            "space": "sp",
            "externalId": "rec-1",
            "createdTime": 100,
            "lastUpdatedTime": 200,
            "properties": {"sp": {"c": {"temp": 22.5}}},
        }
        record = Record._load(payload)
        assert record.created_time == 100
        assert record.last_updated_time == 200
        assert record.dump() == payload

    def test_record_as_id(self) -> None:
        record = Record(space="sp", external_id="rec-1", created_time=1, last_updated_time=2)
        rid = record.as_id()
        assert isinstance(rid, RecordId)
        assert (rid.space, rid.external_id) == ("sp", "rec-1")

    def test_record_as_write_reconstructs_sources(self) -> None:
        record = Record(
            space="sp",
            external_id="rec-1",
            created_time=1,
            last_updated_time=2,
            properties={"sp": {"c": {"temp": 22.5}}},
        )
        write = record.as_write()
        assert isinstance(write, RecordWrite)
        assert write.dump()["sources"] == [
            {"source": {"type": "container", "space": "sp", "externalId": "c"}, "properties": {"temp": 22.5}}
        ]

    def test_record_list_as_ids_and_as_write(self) -> None:
        records = RecordList(
            [
                Record(space="sp", external_id="rec-1", created_time=1, last_updated_time=2),
                Record(space="sp", external_id="rec-2", created_time=1, last_updated_time=2),
            ]
        )
        assert records.as_ids() == [RecordId("sp", "rec-1"), RecordId("sp", "rec-2")]
        assert [w.external_id for w in records.as_write()] == ["rec-1", "rec-2"]

    def test_time_range_dump_omits_none(self) -> None:
        assert TimeRange(gte=1, lt=5).dump() == {"gte": 1, "lt": 5}
        assert TimeRange().dump() == {}

    def test_record_source_selector_dump(self) -> None:
        selector = RecordSourceSelector(RecordContainerId(space="sp", external_id="c"), ["temp", "pressure"])
        assert selector.dump() == {
            "source": {"type": "container", "space": "sp", "externalId": "c"},
            "properties": ["temp", "pressure"],
        }
