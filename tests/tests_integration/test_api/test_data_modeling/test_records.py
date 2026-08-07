from __future__ import annotations

import time
import uuid
from collections.abc import Iterator

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import filters
from cognite.client.data_classes.data_modeling import (
    Boolean,
    ContainerApply,
    ContainerPropertyApply,
    Float64,
    Space,
    Text,
)
from cognite.client.data_classes.data_modeling.aggregates import Average, Count, MetricResult
from cognite.client.data_classes.data_modeling.records import (
    RecordContainerId,
    RecordSource,
    RecordSourceSelector,
    RecordWrite,
    TimeRange,
)
from cognite.client.data_classes.data_modeling.streams import Stream

# Streams cannot be hard deleted and their external IDs can never be reused, so these tests reuse
# a stream that already exists in the project, plus one container, for every run.
STREAM_EXTERNAL_ID = "sdk_test_mutable_stream"
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
    """Reuse the shared mutable test stream that already exists in the project.

    These tests never create a stream: a project may only hold a few active streams, and a stream
    can neither be deleted nor have its external ID reused - so creating one per suite would
    permanently burn a quota slot and eventually make every run fail on the quota. Fall back to
    any other mutable stream in case the shared one is ever replaced.
    """
    stream = cognite_client.data_modeling.streams.retrieve(STREAM_EXTERNAL_ID)
    if stream is not None:
        return stream
    for stream in sorted(cognite_client.data_modeling.streams.list(), key=lambda s: s.external_id):
        if stream.type == "Mutable":
            return stream
    pytest.skip(f"No mutable stream in the project (looked for {STREAM_EXTERNAL_ID!r}) to ingest records into.")


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
) -> Iterator[list[RecordWrite]]:
    """Ingest a small, uniquely tagged batch and clean it up afterwards."""
    tag = uuid.uuid4().hex
    records = [
        RecordWrite(
            space=container_ref.space,
            external_id=f"{tag}-{i}",
            sources=[
                RecordSource(
                    source=container_ref,
                    properties={"name": tag, "value": float(i), "processed": i % 2 == 0},
                )
            ],
        )
        for i in range(3)
    ]
    cognite_client.data_modeling.records.ingest(records, stream_id=mutable_stream.external_id)
    # Records are not queryable the instant ingest returns.
    time.sleep(2)
    yield records
    cognite_client.data_modeling.records.delete(
        [record.as_id() for record in records], stream_id=mutable_stream.external_id
    )


class TestRecordsIntegration:
    def test_filter_returns_ingested_records(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        container_ref: RecordContainerId,
        sources: list[RecordSourceSelector],
        ingested_records: list[RecordWrite],
    ) -> None:
        tag = ingested_records[0].sources[0].properties["name"]
        result = cognite_client.data_modeling.records.filter(
            stream_id=mutable_stream.external_id,
            last_updated_time=TimeRange(gt="1h-ago"),
            sources=sources,
            filter=filters.Equals(property=[container_ref.space, container_ref.external_id, "name"], value=tag),
            limit=10,
        )
        assert len(result) == len(ingested_records)
        assert {record.external_id for record in result} == {r.external_id for r in ingested_records}

    def test_aggregate_over_ingested_records(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        container_ref: RecordContainerId,
        ingested_records: list[RecordWrite],
    ) -> None:
        tag = ingested_records[0].sources[0].properties["name"]
        value = [container_ref.space, container_ref.external_id, "value"]
        result = cognite_client.data_modeling.records.aggregate(
            {"total": Count(), "avg_value": Average(value)},
            stream_id=mutable_stream.external_id,
            last_updated_time=TimeRange(gt="1h-ago"),
            filter=filters.Equals(property=[container_ref.space, container_ref.external_id, "name"], value=tag),
        )
        total, avg_value = result["total"], result["avg_value"]
        assert isinstance(total, MetricResult) and isinstance(avg_value, MetricResult)
        assert total.value == len(ingested_records)
        assert avg_value.value == pytest.approx(1.0)  # mean of 0.0, 1.0, 2.0

    def test_sync_returns_partial_page_without_hanging(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        container_ref: RecordContainerId,
        sources: list[RecordSourceSelector],
        ingested_records: list[RecordWrite],
    ) -> None:
        """Regression test: sync must return one page, even when it holds fewer than 'limit' records.

        The endpoint always returns a 'nextCursor' - that is what makes the feed resumable - so
        paging until the cursor runs out never terminates. Before this was fixed, asking for more
        records than the feed can deliver made the SDK request in a loop until the test timed out.
        """
        tag = ingested_records[0].sources[0].properties["name"]
        page = cognite_client.data_modeling.records.sync(
            stream_id=mutable_stream.external_id,
            initialize_cursor="1m-ago",
            sources=sources,
            filter=filters.Equals(property=[container_ref.space, container_ref.external_id, "name"], value=tag),
            limit=1000,
        )
        assert len(page) == len(ingested_records)
        assert page.cursor is not None
        assert page.has_next is False

    def test_sync_resume_drains_the_feed(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        container_ref: RecordContainerId,
        sources: list[RecordSourceSelector],
        ingested_records: list[RecordWrite],
    ) -> None:
        """Walk the change feed one small page at a time until it is empty.

        This is the cursor-based pipeline pattern: read a page, process it, and only persist the
        cursor once processing succeeded. The final page is always partial, so this loop is what
        the runaway-paging bug used to hang on.
        """
        tag = ingested_records[0].sources[0].properties["name"]
        tagged = filters.Equals(property=[container_ref.space, container_ref.external_id, "name"], value=tag)
        page = cognite_client.data_modeling.records.sync(
            stream_id=mutable_stream.external_id,
            initialize_cursor="1m-ago",
            sources=sources,
            filter=tagged,
            limit=2,
        )
        seen = [record.external_id for record in page]
        pages = 1
        while page.has_next:
            assert page.cursor is not None
            page = cognite_client.data_modeling.records.sync_resume(
                stream_id=mutable_stream.external_id,
                cursor=page.cursor,
                sources=sources,
                filter=tagged,
                limit=2,
            )
            seen.extend(record.external_id for record in page)
            pages += 1
            assert pages < 20, "sync_resume did not drain the feed"

        assert pages > 1, "expected the 3 ingested records to span more than one page of size 2"
        assert set(seen) == {record.external_id for record in ingested_records}

    def test_upsert_replaces_record(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        container_ref: RecordContainerId,
        sources: list[RecordSourceSelector],
        ingested_records: list[RecordWrite],
    ) -> None:
        target = ingested_records[0]
        properties = {**target.sources[0].properties, "value": 99.0}
        cognite_client.data_modeling.records.upsert(
            RecordWrite(
                space=target.space,
                external_id=target.external_id,
                sources=[RecordSource(source=container_ref, properties=properties)],
            ),
            stream_id=mutable_stream.external_id,
        )
        time.sleep(2)

        result = cognite_client.data_modeling.records.filter(
            stream_id=mutable_stream.external_id,
            last_updated_time=TimeRange(gt="1h-ago"),
            sources=sources,
            filter=filters.Equals(property=[container_ref.space, container_ref.external_id, "value"], value=99.0),
            limit=10,
        )
        assert [record.external_id for record in result] == [target.external_id]


class TestStreamsIntegration:
    def test_retrieve_and_list_stream(self, cognite_client: CogniteClient, mutable_stream: Stream) -> None:
        retrieved = cognite_client.data_modeling.streams.retrieve(mutable_stream.external_id)
        assert retrieved is not None
        assert retrieved.external_id == mutable_stream.external_id
        assert retrieved.type == "Mutable"

        listed = cognite_client.data_modeling.streams.list()
        assert mutable_stream.external_id in {stream.external_id for stream in listed}

    def test_retrieve_unknown_stream_returns_none(self, cognite_client: CogniteClient) -> None:
        assert cognite_client.data_modeling.streams.retrieve("this-stream-does-not-exist-12345") is None
