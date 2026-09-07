from __future__ import annotations

from collections.abc import Iterator

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    Boolean,
    ContainerApply,
    ContainerPropertyApply,
    Float64,
    Space,
    Text,
)
from cognite.client.data_classes.data_modeling.records import RecordContainerId, RecordSourceSelector
from cognite.client.data_classes.data_modeling.streams import Stream
from tests.records_helpers import (
    RecordBatch,
    assert_eventually,
    assert_filtered_records,
    assert_record_aggregates,
    assert_sync_records,
    get_or_create_mutable_stream,
    record_batch,
    upsert_and_assert_record,
)

CONTAINER_EXTERNAL_ID = "PythonSdkIntegrationTestRecords"


@pytest.fixture(scope="session")
def record_container(cognite_client: CogniteClient, integration_test_space: Space) -> ContainerApply:
    container = ContainerApply(
        space=integration_test_space.space,
        external_id=CONTAINER_EXTERNAL_ID,
        name="Python SDK Records tests",
        description="Container used by the Records integration tests.",
        used_for="record",
        properties={
            "name": ContainerPropertyApply(type=Text(is_list=False), nullable=False),
            "value": ContainerPropertyApply(type=Float64(is_list=False), nullable=False),
            "processed": ContainerPropertyApply(type=Boolean(is_list=False), nullable=False),
        },
    )
    cognite_client.data_modeling.containers.apply(container)
    return container


@pytest.fixture(scope="session")
def mutable_stream(cognite_client: CogniteClient) -> Stream:
    return get_or_create_mutable_stream(cognite_client)


@pytest.fixture(scope="session")
def container_ref(record_container: ContainerApply) -> RecordContainerId:
    return RecordContainerId(space=record_container.space, external_id=record_container.external_id)


@pytest.fixture(scope="session")
def sources(container_ref: RecordContainerId) -> list[RecordSourceSelector]:
    return [RecordSourceSelector(source=container_ref, properties=["*"])]


@pytest.fixture
def ingested_records(
    cognite_client: CogniteClient,
    mutable_stream: Stream,
    container_ref: RecordContainerId,
) -> Iterator[RecordBatch]:
    with record_batch(cognite_client, mutable_stream, container_ref) as batch:
        yield batch


class TestRecordsIntegration:
    def test_filter_returns_ingested_records(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        container_ref: RecordContainerId,
        sources: list[RecordSourceSelector],
        ingested_records: RecordBatch,
    ) -> None:
        assert_filtered_records(cognite_client, mutable_stream, container_ref, sources, ingested_records)

    def test_aggregate_over_ingested_records(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        container_ref: RecordContainerId,
        ingested_records: RecordBatch,
    ) -> None:
        assert_record_aggregates(cognite_client, mutable_stream, container_ref, ingested_records)

    @pytest.mark.parametrize("chunk_size", [1000, 2])
    def test_sync_iterates_until_feed_exhausted(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        sources: list[RecordSourceSelector],
        ingested_records: RecordBatch,
        chunk_size: int,
    ) -> None:
        assert_sync_records(cognite_client, mutable_stream, sources, ingested_records, chunk_size)

    def test_upsert_replaces_record(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        container_ref: RecordContainerId,
        sources: list[RecordSourceSelector],
        ingested_records: RecordBatch,
    ) -> None:
        upsert_and_assert_record(cognite_client, mutable_stream, container_ref, sources, ingested_records)


class TestStreamsIntegration:
    def test_retrieve_and_list_stream(self, cognite_client: CogniteClient, mutable_stream: Stream) -> None:
        def stream_is_visible() -> None:
            retrieved = cognite_client.data_modeling.streams.retrieve(mutable_stream.external_id)
            assert retrieved is not None
            assert retrieved.external_id == mutable_stream.external_id
            assert retrieved.type == "Mutable"

            listed = cognite_client.data_modeling.streams.list()
            assert mutable_stream.external_id in {stream.external_id for stream in listed}

        assert_eventually(stream_is_visible)

    def test_retrieve_unknown_stream_returns_none(self, cognite_client: CogniteClient) -> None:
        assert cognite_client.data_modeling.streams.retrieve("this-stream-does-not-exist-12345") is None
