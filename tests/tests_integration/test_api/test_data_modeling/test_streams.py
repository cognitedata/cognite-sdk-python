import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Stream, StreamList, StreamWrite


class TestStreams:
    @pytest.mark.usefixtures("immutable_test_stream", "mutable_test_stream")
    def test_list_streams(self, cognite_client: CogniteClient) -> None:
        streams = cognite_client.data_modeling.streams.list()
        assert isinstance(streams, StreamList)
        assert len(streams) > 0

        stream_ids = {s.external_id for s in streams}
        assert "sdk_test_immutable_stream" in stream_ids
        assert "sdk_test_mutable_stream" in stream_ids

    def test_retrieve_stream(self, cognite_client: CogniteClient, immutable_test_stream: Stream) -> None:
        retrieved = cognite_client.data_modeling.streams.retrieve(immutable_test_stream.external_id)

        assert retrieved is not None
        assert isinstance(retrieved, Stream)
        assert retrieved.external_id == immutable_test_stream.external_id
        assert retrieved.created_time > 0
        assert retrieved.type == "Immutable"
        assert retrieved.settings.limits.max_records_total.provisioned >= 0
        assert retrieved.settings.limits.max_records_total.consumed is None

    def test_retrieve_stream_with_statistics(self, cognite_client: CogniteClient, mutable_test_stream: Stream) -> None:
        retrieved = cognite_client.data_modeling.streams.retrieve(
            mutable_test_stream.external_id, include_statistics=True
        )

        assert retrieved is not None
        assert isinstance(retrieved, Stream)
        assert retrieved.external_id == mutable_test_stream.external_id
        assert retrieved.type == "Mutable"
        assert retrieved.settings.limits.max_records_total.provisioned >= 0
        assert retrieved.settings.limits.max_records_total.consumed is not None

    def test_retrieve_non_existent_stream(self, cognite_client: CogniteClient) -> None:
        result = cognite_client.data_modeling.streams.retrieve("non_existent_stream_id")

        assert result is None

    def test_delete_non_existent_stream(self, cognite_client: CogniteClient) -> None:
        cognite_client.data_modeling.streams.delete("non_existent_stream_id")

    def test_stream_as_write(self, cognite_client: CogniteClient, immutable_test_stream: Stream) -> None:
        write = immutable_test_stream.as_write()

        assert isinstance(write, StreamWrite)
        assert write.external_id == immutable_test_stream.external_id
        assert write.settings.template_name == immutable_test_stream.created_from_template
