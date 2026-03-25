from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import Token
from cognite.client.data_classes.streams import StreamList, StreamWrite


@pytest.fixture
def client() -> CogniteClient:
    return CogniteClient(
        ClientConfig(
            client_name="unit-streams",
            project="test-proj",
            credentials=Token("token"),
            base_url="https://greenfield.cognitedata.com",
        )
    )


def test_streams_list_parses_items(client: CogniteClient) -> None:
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
    client.streams._get = MagicMock(return_value=MagicMock(json=lambda: sample))
    out = client.streams.list()
    assert isinstance(out, StreamList)
    assert out[0].external_id == "st1"
    client.streams._get.assert_called_once_with("/streams")


def test_streams_retrieve_include_statistics_query(client: CogniteClient) -> None:
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
    client.streams._get = MagicMock(return_value=MagicMock(json=lambda: sample))
    client.streams.retrieve("st1", include_statistics=True)
    client.streams._get.assert_called_once_with(
        "/streams/st1", params={"includeStatistics": "true"}
    )


def test_streams_create_posts_items(client: CogniteClient) -> None:
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
    client.streams._post = MagicMock(return_value=MagicMock(json=lambda: sample))
    w = StreamWrite("st1", {"template": {"name": "ImmutableTestStream"}})
    client.streams.create([w])
    client.streams._post.assert_called_once()
    call_kw = client.streams._post.call_args
    assert call_kw[0][0] == "/streams"
    assert call_kw[1]["json"]["items"][0]["externalId"] == "st1"


def test_records_ingest_delegates(client: CogniteClient) -> None:
    client.streams.records.ingest = MagicMock()
    client.streams.ingest_records("my-stream", {"items": []})
    client.streams.records.ingest.assert_called_once_with("my-stream", {"items": []})
