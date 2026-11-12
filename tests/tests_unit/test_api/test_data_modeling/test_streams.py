from __future__ import annotations

import re
from collections.abc import Callable

import pytest
from pytest_httpx2 import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.data_modeling.streams import (
    Stream,
    StreamList,
    StreamTemplate,
    StreamTemplateInfoList,
    StreamWrite,
    StreamWriteSettings,
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
        httpx2_mock: HTTPXMock,
        streams_base_url: str,
        stream_list_response: dict,
    ) -> None:
        httpx2_mock.add_response(
            method="GET", url=re.compile(re.escape(streams_base_url) + r"(?:\?.+)?$"), json=stream_list_response
        )
        out = cognite_client.data_modeling.streams.list()
        assert isinstance(out, StreamList)
        assert out[0].external_id == "st1"
        assert out[0].settings.limits.max_unique_properties_with_data is None
        assert out.dump() == stream_list_response["items"]
        (request,) = httpx2_mock.get_requests()
        assert request.headers["cdf-version"] == "20230101"

    def test_templates_list_parses_items(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        response = {
            "items": [
                {
                    "name": "ImmutableTestStream",
                    "type": "Immutable",
                    "maxStreams": {"provisioned": 2},
                    "settings": {
                        "limits": {
                            "maxRecordsTotal": {
                                "type": "fixed",
                                "name": "Maximum total number of records",
                                "value": 1000,
                            },
                        },
                        "lifecycle": {
                            "retainedAfterSoftDelete": {"type": "fixed", "name": "Soft delete", "value": "PT168H"},
                            "dataDeletedAfter": {"type": "fixed", "name": "Data retention", "value": "infinity"},
                        },
                    },
                }
            ]
        }
        httpx2_mock.add_response(
            method="GET", url=re.compile(re.escape(streams_base_url + "/templates") + r"(?:\?.+)?$"), json=response
        )
        out = cognite_client.data_modeling.streams.templates.list()
        assert isinstance(out, StreamTemplateInfoList)
        assert out[0].name == "ImmutableTestStream"
        assert out[0].max_streams == 2
        assert out[0].settings.limits["maxRecordsTotal"].value == 1000
        assert out[0].settings.lifecycle["retainedAfterSoftDelete"].value == "PT168H"
        assert out.dump() == response["items"]
        (request,) = httpx2_mock.get_requests()
        assert request.headers["cdf-version"] == "20230101"

    def test_retrieve_include_statistics_query(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        streams_base_url: str,
        stream_response: dict,
    ) -> None:
        httpx2_mock.add_response(
            method="GET",
            url=re.compile(re.escape(streams_base_url) + r"/st1(?:\?.+)?$"),
            json=stream_response,
        )
        cognite_client.data_modeling.streams.retrieve("st1", include_statistics=True)
        requests = httpx2_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].url.params["includeStatistics"].lower() == "true"
        assert requests[0].headers["cdf-version"] == "20230101"

    def test_retrieve_parses_max_unique_properties_with_data(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        streams_base_url: str,
        stream_response: dict,
    ) -> None:
        stream_response["settings"]["limits"]["maxUniquePropertiesWithData"] = {"provisioned": 1000, "consumed": 42}
        httpx2_mock.add_response(
            method="GET",
            url=re.compile(re.escape(streams_base_url) + r"/st1(?:\?.+)?$"),
            json=stream_response,
        )
        out = cognite_client.data_modeling.streams.retrieve("st1", include_statistics=True)
        assert isinstance(out, Stream)
        unique_properties = out.settings.limits.max_unique_properties_with_data
        assert unique_properties is not None
        assert unique_properties.provisioned == 1000
        assert unique_properties.consumed == 42
        assert out.dump() == stream_response

    def test_create_posts_single_item(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        streams_base_url: str,
        stream_list_response: dict,
    ) -> None:
        httpx2_mock.add_response(
            method="POST", url=re.compile(re.escape(streams_base_url) + r"$"), json=stream_list_response
        )
        w = StreamWrite(
            "st1",
            StreamWriteSettings(StreamTemplate("ImmutableTestStream")),
        )
        out = cognite_client.data_modeling.streams.create(w)
        requests = httpx2_mock.get_requests()
        assert isinstance(out, Stream)
        assert len(requests) == 1
        assert requests[0].url.path.endswith("/streams")
        assert requests[0].headers["cdf-version"] == "20230101"
        assert jsgz_load(requests[0].content) == {
            "items": [{"externalId": "st1", "settings": {"template": {"name": "ImmutableTestStream"}}}]
        }

    @pytest.mark.parametrize(
        "allowed_record_spaces",
        [
            pytest.param(["space-a", "space-b"], id="spaces"),
            pytest.param([], id="empty list is sent as-is"),
        ],
    )
    def test_create_posts_allowed_record_spaces(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        streams_base_url: str,
        stream_list_response: dict,
        allowed_record_spaces: list[str],
    ) -> None:
        httpx2_mock.add_response(
            method="POST", url=re.compile(re.escape(streams_base_url) + r"$"), json=stream_list_response
        )
        w = StreamWrite(
            "st1",
            StreamWriteSettings(StreamTemplate("ImmutableTestStream"), allowed_record_spaces=allowed_record_spaces),
        )
        cognite_client.data_modeling.streams.create(w)
        (request,) = httpx2_mock.get_requests()
        assert jsgz_load(request.content) == {
            "items": [
                {
                    "externalId": "st1",
                    "settings": {
                        "template": {"name": "ImmutableTestStream"},
                        "allowedRecordSpaces": allowed_record_spaces,
                    },
                }
            ]
        }

    def test_retrieve_loads_allowed_record_spaces_and_as_write_carries_them_over(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        streams_base_url: str,
        stream_response: dict,
    ) -> None:
        stream_response["settings"]["allowedRecordSpaces"] = ["space-a", "space-b"]
        httpx2_mock.add_response(
            method="GET", url=re.compile(re.escape(streams_base_url) + r"/st1(?:\?.+)?$"), json=stream_response
        )
        out = cognite_client.data_modeling.streams.retrieve("st1")
        assert out is not None
        assert out.settings.allowed_record_spaces == ["space-a", "space-b"]
        assert out.dump() == stream_response
        assert out.as_write().dump() == {
            "externalId": "st1",
            "settings": {"template": {"name": "ImmutableTestStream"}, "allowedRecordSpaces": ["space-a", "space-b"]},
        }

    def test_retrieve_without_allowed_record_spaces_omits_them(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        streams_base_url: str,
        stream_response: dict,
    ) -> None:
        httpx2_mock.add_response(
            method="GET", url=re.compile(re.escape(streams_base_url) + r"/st1(?:\?.+)?$"), json=stream_response
        )
        out = cognite_client.data_modeling.streams.retrieve("st1")
        assert out is not None
        assert out.settings.allowed_record_spaces is None
        assert out.dump() == stream_response
        assert out.as_write().dump() == {"externalId": "st1", "settings": {"template": {"name": "ImmutableTestStream"}}}

    def test_create_chunks_multiple_items(
        self,
        cognite_client: CogniteClient,
        httpx2_mock: HTTPXMock,
        streams_base_url: str,
        make_stream_response: Callable[..., dict],
    ) -> None:
        url_pattern = re.compile(re.escape(streams_base_url) + r"$")
        httpx2_mock.add_response(method="POST", url=url_pattern, json={"items": [make_stream_response("a", 1)]})
        httpx2_mock.add_response(method="POST", url=url_pattern, json={"items": [make_stream_response("b", 2)]})
        tpl = StreamWriteSettings(StreamTemplate("ImmutableTestStream"))
        a = StreamWrite("a", tpl)
        b = StreamWrite("b", tpl)
        out = cognite_client.data_modeling.streams.create([a, b])
        requests = httpx2_mock.get_requests()
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
        httpx2_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        url_pattern = re.compile(re.escape(streams_base_url) + r"/delete$")
        httpx2_mock.add_response(method="POST", url=url_pattern, json={})
        httpx2_mock.add_response(method="POST", url=url_pattern, json={})
        cognite_client.data_modeling.streams.delete(["a", "b"])
        requests = httpx2_mock.get_requests()
        assert len(requests) == 2
        assert [jsgz_load(request.content) for request in requests] == [
            {"items": [{"externalId": "a"}]},
            {"items": [{"externalId": "b"}]},
        ]
