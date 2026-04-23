from __future__ import annotations

import re

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


class TestStreamsAPI:
    def test_list_parses_items(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        sample = {
            "items": [
                {
                    "externalId": "st1",
                    "createdTime": 10,
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
            ]
        }
        httpx_mock.add_response(method="GET", url=re.compile(re.escape(streams_base_url) + r"$"), json=sample)
        out = cognite_client.data_modeling.streams.list()
        assert isinstance(out, StreamList)
        assert out[0].external_id == "st1"

    def test_retrieve_include_statistics_query(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        streams_base_url: str,
    ) -> None:
        sample = {
            "externalId": "st1",
            "createdTime": 10,
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
        httpx_mock.add_response(
            method="GET",
            url=re.compile(re.escape(streams_base_url) + r"/st1(?:\?.+)?$"),
            json=sample,
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
    ) -> None:
        sample = {
            "items": [
                {
                    "externalId": "st1",
                    "createdTime": 10,
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
            ]
        }
        httpx_mock.add_response(method="POST", url=re.compile(re.escape(streams_base_url) + r"$"), json=sample)
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
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(streams_base_url) + r"$"),
            json={
                "items": [
                    {
                        "externalId": "a",
                        "createdTime": 1,
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
                ]
            },
        )
        httpx_mock.add_response(
            method="POST",
            url=re.compile(re.escape(streams_base_url) + r"$"),
            json={
                "items": [
                    {
                        "externalId": "b",
                        "createdTime": 2,
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
                ]
            },
        )
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
        httpx_mock.add_response(method="POST", url=re.compile(re.escape(streams_base_url) + r"/delete$"), json={})
        httpx_mock.add_response(method="POST", url=re.compile(re.escape(streams_base_url) + r"/delete$"), json={})
        cognite_client.data_modeling.streams.delete(["a", "b"])
        requests = httpx_mock.get_requests()
        assert len(requests) == 2
        assert [jsgz_load(request.content) for request in requests] == [
            {"items": [{"externalId": "a"}]},
            {"items": [{"externalId": "b"}]},
        ]
