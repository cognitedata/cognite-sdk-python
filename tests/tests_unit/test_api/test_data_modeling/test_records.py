from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.data_modeling.records import (
    RecordId,
    RecordSource,
    RecordSourceReference,
    RecordWrite,
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
        cognite_client.data_modeling.records.ingest(stream_id, write_item)
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
        cognite_client.data_modeling.records.ingest(stream_id, items)
        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        assert len(jsgz_load(requests[0].content)["items"]) == 1000
        assert len(jsgz_load(requests[1].content)["items"]) == 1


class TestRecordDTOs:
    def test_record_write_to_identifier(self, write_item: RecordWrite) -> None:
        from cognite.client.utils._identifier import RecordId

        rid = write_item.to_identifier()
        assert isinstance(rid, RecordId)
        assert rid.space == "sp"
        assert rid.external_id == "rec-1"

    def test_record_source_reference_to_identifier(self) -> None:
        from cognite.client.utils._identifier import RecordId

        ref = RecordSourceReference(space="s", external_id="c")
        rid = ref.to_identifier()
        assert isinstance(rid, RecordId)
        assert rid.space == "s"
        assert rid.external_id == "c"

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
        ref = RecordSourceReference(space="s", external_id="c")
        d = ref.dump()
        assert d == {"type": "container", "space": "s", "externalId": "c"}

    def test_record_source_dump(self) -> None:
        src = RecordSource(
            source=RecordSourceReference(space="s", external_id="c"),
            properties={"x": 1},
        )
        d = src.dump()
        assert d["source"]["type"] == "container"
        assert d["properties"] == {"x": 1}
