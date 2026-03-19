import re
from collections.abc import Iterator

import pytest
from responses import RequestsMock

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Stream, StreamWrite, StreamWriteSettings
from tests.utils import jsgz_load


class TestStreams:
    @pytest.fixture
    def mock_streams_create_response(self, rsps: RequestsMock, cognite_client: CogniteClient) -> Iterator[RequestsMock]:
        response_body = {
            "items": [
                {
                    "externalId": "test_stream",
                    "createdTime": 1761319720908,
                    "createdFromTemplate": "ImmutableTestStream",
                    "type": "Immutable",
                    "settings": {
                        "lifecycle": {
                            "retainedAfterSoftDelete": "PT24H",
                            "dataDeletedAfter": "PT168H",
                        },
                        "limits": {
                            "maxFilteringInterval": "PT168H",
                            "maxRecordsTotal": {"provisioned": 500000000},
                            "maxGigaBytesTotal": {"provisioned": 500},
                        },
                    },
                }
            ]
        }
        url_pattern = re.compile(
            re.escape(cognite_client.data_modeling.streams._get_base_url_with_base_path()) + "/streams"
        )
        rsps.add(rsps.POST, url_pattern, status=201, json=response_body)
        yield rsps

    @pytest.fixture
    def mock_streams_delete_response(self, rsps: RequestsMock, cognite_client: CogniteClient) -> Iterator[RequestsMock]:
        url_pattern = re.compile(
            re.escape(cognite_client.data_modeling.streams._get_base_url_with_base_path()) + "/streams/delete"
        )
        rsps.add(rsps.POST, url_pattern, status=200, json={})
        yield rsps

    def test_create_stream(self, cognite_client: CogniteClient, mock_streams_create_response: RequestsMock) -> None:
        stream = StreamWrite(
            external_id="test_stream",
            settings=StreamWriteSettings(template_name="ImmutableTestStream"),
        )
        result = cognite_client.data_modeling.streams.create(stream)
        assert isinstance(result, Stream)
        assert result.external_id == "test_stream"
        assert result.type == "Immutable"

        assert len(mock_streams_create_response.calls) == 1
        sent_payload = jsgz_load(mock_streams_create_response.calls[0].request.body)
        assert sent_payload["items"][0] == {
            "externalId": "test_stream",
            "settings": {"template": {"name": "ImmutableTestStream"}},
        }

    def test_delete_stream(self, cognite_client: CogniteClient, mock_streams_delete_response: RequestsMock) -> None:
        cognite_client.data_modeling.streams.delete(external_id="test_stream")

        assert len(mock_streams_delete_response.calls) == 1
        sent_payload = jsgz_load(mock_streams_delete_response.calls[0].request.body)
        assert sent_payload == {"items": [{"externalId": "test_stream"}]}
