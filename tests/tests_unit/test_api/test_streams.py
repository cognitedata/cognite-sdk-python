from __future__ import annotations

import re

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.streams import (
    StreamList,
    StreamTemplate,
    StreamTemplateWriteSettings,
    StreamWrite,
)


@pytest.fixture
def streams_base_url(async_client: AsyncCogniteClient) -> str:
    return async_client.streams._base_url_with_base_path + "/streams"


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
        out = cognite_client.streams.list()
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
            url=re.compile(re.escape(streams_base_url) + r"/st1\?includeStatistics=true$"),
            json=sample,
        )
        cognite_client.streams.retrieve("st1", include_statistics=True)

    def test_create_posts_items(
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
        cognite_client.streams.create([w])
        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].url.path.endswith("/streams")

    def test_create_rejects_multiple_items(self, cognite_client: CogniteClient) -> None:
        tpl = StreamTemplateWriteSettings(StreamTemplate("ImmutableTestStream"))
        a = StreamWrite("a", tpl)
        b = StreamWrite("b", tpl)
        with pytest.raises(ValueError, match="exactly one"):
            cognite_client.streams.create([a, b])

    def test_delete_rejects_multiple_items(self, cognite_client: CogniteClient) -> None:
        from cognite.client.data_classes.streams import StreamDeleteItem

        with pytest.raises(ValueError, match="exactly one"):
            cognite_client.streams.delete([StreamDeleteItem("a"), StreamDeleteItem("b")])
