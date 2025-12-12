import re
from collections.abc import Iterator

import pytest
from responses import RequestsMock

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Stream, StreamWrite, StreamWriteSettings
from cognite.client.exceptions import CogniteAPIError


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
    def mock_streams_list_response(self, rsps: RequestsMock, cognite_client: CogniteClient) -> Iterator[RequestsMock]:
        response_body = {
            "items": [
                {
                    "externalId": "test_stream_1",
                    "createdTime": 1760201842000,
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
                },
                {
                    "externalId": "test_stream_2",
                    "createdTime": 1760201817980,
                    "createdFromTemplate": "MutableTestStream",
                    "type": "Mutable",
                    "settings": {
                        "lifecycle": {"retainedAfterSoftDelete": "PT24H"},
                        "limits": {
                            "maxRecordsTotal": {"provisioned": 10000000},
                            "maxGigaBytesTotal": {"provisioned": 20},
                        },
                    },
                },
            ]
        }
        url_pattern = re.compile(
            re.escape(cognite_client.data_modeling.streams._get_base_url_with_base_path()) + "/streams"
        )
        rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
        yield rsps

    @pytest.fixture
    def mock_streams_retrieve_response(
        self, rsps: RequestsMock, cognite_client: CogniteClient
    ) -> Iterator[RequestsMock]:
        response_body = {
            "externalId": "test_stream",
            "createdTime": 1760201842000,
            "createdFromTemplate": "ImmutableTestStream",
            "type": "Immutable",
            "settings": {
                "lifecycle": {
                    "retainedAfterSoftDelete": "PT24H",
                    "hotPhaseDuration": "PT72H",
                    "dataDeletedAfter": "PT168H",
                },
                "limits": {
                    "maxFilteringInterval": "PT168H",
                    "maxRecordsTotal": {"provisioned": 500000000},
                    "maxGigaBytesTotal": {"provisioned": 500},
                },
            },
        }
        url_pattern = re.compile(
            re.escape(cognite_client.data_modeling.streams._get_base_url_with_base_path())
            + r"/streams/test_stream(\?.*)?$"
        )
        rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
        yield rsps

    @pytest.fixture
    def mock_streams_retrieve_with_stats_response(
        self, rsps: RequestsMock, cognite_client: CogniteClient
    ) -> Iterator[RequestsMock]:
        response_body = {
            "externalId": "test_stream",
            "createdTime": 1760201842000,
            "createdFromTemplate": "ImmutableTestStream",
            "type": "Immutable",
            "settings": {
                "lifecycle": {
                    "retainedAfterSoftDelete": "PT24H",
                    "hotPhaseDuration": "PT72H",
                    "dataDeletedAfter": "PT168H",
                },
                "limits": {
                    "maxFilteringInterval": "PT168H",
                    "maxRecordsTotal": {"provisioned": 500000000, "consumed": 25000000},
                    "maxGigaBytesTotal": {"provisioned": 500, "consumed": 100},
                },
            },
        }
        url_pattern = re.compile(
            re.escape(cognite_client.data_modeling.streams._get_base_url_with_base_path())
            + r"/streams/test_stream\?includeStatistics=true$"
        )
        rsps.add(rsps.GET, url_pattern, status=200, json=response_body)
        yield rsps

    @pytest.fixture
    def mock_streams_delete_response(self, rsps: RequestsMock, cognite_client: CogniteClient) -> Iterator[RequestsMock]:
        url_pattern = re.compile(
            re.escape(cognite_client.data_modeling.streams._get_base_url_with_base_path()) + "/streams/delete"
        )
        rsps.add(rsps.POST, url_pattern, status=200, json={})
        yield rsps

    @pytest.fixture
    def mock_streams_delete_raise_error(
        self, rsps: RequestsMock, cognite_client: CogniteClient
    ) -> Iterator[RequestsMock]:
        response_body = {"error": {"message": "Stream not found", "code": 404}}
        url_pattern = re.compile(
            re.escape(cognite_client.data_modeling.streams._get_base_url_with_base_path()) + "/streams/delete"
        )
        rsps.add(rsps.POST, url_pattern, status=404, json=response_body)
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

    def test_list_streams(self, cognite_client: CogniteClient, mock_streams_list_response: RequestsMock) -> None:
        result = cognite_client.data_modeling.streams.list()
        assert len(result) == 2
        assert result[0].external_id == "test_stream_1"
        assert result[1].external_id == "test_stream_2"

    def test_retrieve_stream(self, cognite_client: CogniteClient, mock_streams_retrieve_response: RequestsMock) -> None:
        result = cognite_client.data_modeling.streams.retrieve(external_id="test_stream")
        assert result is not None
        assert result.external_id == "test_stream"
        assert result.type == "Immutable"

    def test_retrieve_stream_with_statistics(
        self, cognite_client: CogniteClient, mock_streams_retrieve_with_stats_response: RequestsMock
    ) -> None:
        result = cognite_client.data_modeling.streams.retrieve(external_id="test_stream", include_statistics=True)
        assert result is not None
        assert result.external_id == "test_stream"
        assert result.type == "Immutable"
        assert result.settings.limits.max_filtering_interval == "PT168H"
        assert result.settings.limits.max_records_total.provisioned == 500000000
        assert result.settings.limits.max_records_total.consumed == 25000000
        assert result.settings.limits.max_giga_bytes_total.provisioned == 500
        assert result.settings.limits.max_giga_bytes_total.consumed == 100

    def test_delete_stream(self, cognite_client: CogniteClient, mock_streams_delete_response: RequestsMock) -> None:
        cognite_client.data_modeling.streams.delete(external_id="test_stream")

    def test_failed_delete_stream(
        self, cognite_client: CogniteClient, mock_streams_delete_raise_error: RequestsMock
    ) -> None:
        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.streams.delete(external_id="i-dont-actually-exist")
        assert error.value.code == 404

    def test_stream_as_write(self, cognite_client: CogniteClient, mock_streams_retrieve_response: RequestsMock) -> None:
        stream = cognite_client.data_modeling.streams.retrieve(external_id="test_stream")
        assert stream is not None

        # Convert to write object
        stream_write = stream.as_write()
        assert stream_write.external_id == stream.external_id
        assert stream_write.settings.template_name == stream.created_from_template
        assert stream_write.settings.template_name == "ImmutableTestStream"

    def test_retrieve_non_existent_stream_returns_none(self, rsps: RequestsMock, cognite_client: CogniteClient) -> None:
        response_body = {"error": {"message": "Stream not found", "code": 404}}
        url_pattern = re.compile(
            re.escape(cognite_client.data_modeling.streams._get_base_url_with_base_path())
            + r"/streams/non_existent_stream(\\?.*)?$"
        )
        rsps.add(rsps.GET, url_pattern, status=404, json=response_body)

        result = cognite_client.data_modeling.streams.retrieve(external_id="non_existent_stream")
        assert result is None
