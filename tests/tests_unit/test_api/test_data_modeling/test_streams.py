from __future__ import annotations

import re
from collections.abc import Callable

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.data_modeling.streams import (
    Stream,
    StreamList,
    StreamTemplate,
    StreamTemplateWriteSettings,
    StreamWrite,
)
from tests.utils import jsgz_load


@pytest.fixture
def streams_base_url(async_client: AsyncCogniteClient) -> str:
    return async_client.data_modeling.streams._base_url_with_base_path + "/streams"


@pytest.fixture
def make_stream_response() -> Callable[..., dict]:
    def _make(external_id: str = "st1", created_time: int = 10) -> dict:
        return {
            "externalId": external_id,
            "createdTime": created_time,
            "createdFromTemplate": "ImmutableTestStream",
            "type": "Immutable",
            "settings": {
                "lifecycle": {"retainedAfterSoftDelete": "P1D"},
                "limits": {
                    "maxRecordsTotal": {"provisioned": 1000.0},
                    "maxGigaBytesTotal": {"provisioned": 1.0},
                },
            },
        }

    return _make


@pytest.fixture
def stream_response(make_stream_response: Callable[..., dict]) -> dict:
    return make_stream_response()


@pytest.fixture
def stream_list_response(stream_response: dict) -> dict:
    return {"items": [stream_response]}


class TestStreamsAPI:
    def test_list_parses_items(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
        stream_list_response: dict,
    ) -> None:
        httpx_mock.add_response(
            method="GET", url=re.compile(re.escape(streams_base_url) + r"(?:\?.+)?$"), json=stream_list_response
        )
        out = cognite_client.data_modeling.streams.list()
        assert isinstance(out, StreamList)
        assert out[0].external_id == "st1"

    def test_retrieve_include_statistics_query(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
        stream_response: dict,
    ) -> None:
        httpx_mock.add_response(
            method="GET",
            url=re.compile(re.escape(streams_base_url) + r"/st1(?:\?.+)?$"),
            json=stream_response,
        )
        cognite_client.data_modeling.streams.retrieve("st1", include_statistics=True)
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].url.params["includeStatistics"].lower() == "true"

    def test_create_posts_single_item(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
        stream_list_response: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST", url=re.compile(re.escape(streams_base_url) + r"$"), json=stream_list_response
        )
        w = StreamWrite(
            "st1",
            StreamTemplateWriteSettings(StreamTemplate("ImmutableTestStream")),
        )
        out = cognite_client.data_modeling.streams.create(w)
        requests = httpx_mock.get_requests()
        assert isinstance(out, Stream)
        assert len(requests) == 1
        assert requests[0].url.path.endswith("/streams")
        assert jsgz_load(requests[0].content) == {
            "items": [{"externalId": "st1", "settings": {"template": {"name": "ImmutableTestStream"}}}]
        }

    def test_create_chunks_multiple_items(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
        make_stream_response: Callable[..., dict],
    ) -> None:
        url_pattern = re.compile(re.escape(streams_base_url) + r"$")
        httpx_mock.add_response(method="POST", url=url_pattern, json={"items": [make_stream_response("a", 1)]})
        httpx_mock.add_response(method="POST", url=url_pattern, json={"items": [make_stream_response("b", 2)]})
        tpl = StreamTemplateWriteSettings(StreamTemplate("ImmutableTestStream"))
        a = StreamWrite("a", tpl)
        b = StreamWrite("b", tpl)
        out = cognite_client.data_modeling.streams.create([a, b])
        requests = httpx_mock.get_requests()
        assert isinstance(out, StreamList)
        assert [stream.external_id for stream in out] == ["a", "b"]
        assert len(requests) == 2
        assert [jsgz_load(request.content) for request in requests] == [
            {"items": [{"externalId": "a", "settings": {"template": {"name": "ImmutableTestStream"}}}]},
            {"items": [{"externalId": "b", "settings": {"template": {"name": "ImmutableTestStream"}}}]},
        ]

    def test_delete_chunks_multiple_items(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        url_pattern = re.compile(re.escape(streams_base_url) + r"/delete$")
        httpx_mock.add_response(method="POST", url=url_pattern, json={})
        httpx_mock.add_response(method="POST", url=url_pattern, json={})
        cognite_client.data_modeling.streams.delete(["a", "b"])
        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        assert [jsgz_load(request.content) for request in requests] == [
            {"items": [{"externalId": "a"}]},
            {"items": [{"externalId": "b"}]},
        ]
