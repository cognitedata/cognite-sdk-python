from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone

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
from cognite.client.data_classes.data_modeling.streams import (
    Stream,
    StreamTemplate,
    StreamWrite,
    StreamWriteSettings,
)
from cognite.client.utils._retry import Backoff

# Deleted streams are soft deleted, and their external IDs stay reserved for a couple of weeks
# before they can be reused. Combined with the low quota on active streams per project, these
# tests use one fixed stream (created only if missing) rather than one per run.
STREAM_EXTERNAL_ID = "sdk_test_mutable_stream"
CONTAINER_EXTERNAL_ID = "PythonSdkIntegrationTestRecords"


def an_hour_ago() -> str:
    """A lower bound for 'lastUpdatedTime'.

    TimeRange forwards its bounds to the API untouched, and the API only accepts ISO-8601 there -
    not the "1h-ago" shorthand that the rest of the SDK understands.
    """
    return (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


def assert_eventually(assertion: Callable[[], None], retries: int = 5) -> None:
    """Retry an assertion about eventually-consistent backend state instead of a fixed sleep."""
    wait = Backoff(max_wait=4, min_wait=0.25)
    for _ in range(retries):
        try:
            assertion()
            return
        except AssertionError:
            time.sleep(next(wait))
    assertion()


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
    stream = cognite_client.data_modeling.streams.retrieve(STREAM_EXTERNAL_ID)
    if stream is not None:
        return stream
    return cognite_client.data_modeling.streams.create(
        StreamWrite(
            external_id=STREAM_EXTERNAL_ID,
            settings=StreamWriteSettings(template=StreamTemplate(name="BasicLiveData")),
        )
    )


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

    def all_records_are_queryable() -> None:
        result = cognite_client.data_modeling.records.filter(
            stream_id=mutable_stream.external_id,
            last_updated_time=TimeRange(gt=an_hour_ago()),
            filter=filters.Equals(property=[container_ref.space, container_ref.external_id, "name"], value=tag),
            limit=len(records) + 1,
        )
        assert len(result) == len(records)

    # Records are not queryable the instant ingest returns.
    assert_eventually(all_records_are_queryable)
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
            last_updated_time=TimeRange(gt=an_hour_ago()),
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
            last_updated_time=TimeRange(gt=an_hour_ago()),
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
        """Regression test: sync must yield a chunk, even when it holds fewer than 'chunk_size' records."""
        tag = ingested_records[0].sources[0].properties["name"]
        page = next(
            cognite_client.data_modeling.records.sync(
                stream_id=mutable_stream.external_id,
                initialize_cursor="1m-ago",
                sources=sources,
                filter=filters.Equals(property=[container_ref.space, container_ref.external_id, "name"], value=tag),
            )
        )
        assert len(page) == len(ingested_records)
        assert page.cursor is not None
        assert page.has_next is False

    def test_sync_iterates_until_feed_exhausted(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        container_ref: RecordContainerId,
        sources: list[RecordSourceSelector],
        ingested_records: list[RecordWrite],
    ) -> None:
        """Walk the change feed one small chunk at a time until it is empty.

        Each yielded chunk carries the cursor to persist once processing succeeded. The final
        chunk is always partial.
        """
        tag = ingested_records[0].sources[0].properties["name"]
        tagged = filters.Equals(property=[container_ref.space, container_ref.external_id, "name"], value=tag)
        seen: list[str] = []
        pages = 0
        for page in cognite_client.data_modeling.records.sync(
            stream_id=mutable_stream.external_id,
            initialize_cursor="1m-ago",
            sources=sources,
            filter=tagged,
            chunk_size=2,
        ):
            assert page.cursor is not None
            seen.extend(record.external_id for record in page)
            pages += 1
            assert pages < 20, "sync did not exhaust the feed"

        assert pages > 1, "expected the 3 ingested records to span more than one chunk of size 2"
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

        def replacement_is_queryable() -> None:
            result = cognite_client.data_modeling.records.filter(
                stream_id=mutable_stream.external_id,
                last_updated_time=TimeRange(gt=an_hour_ago()),
                sources=sources,
                filter=filters.Equals(property=[container_ref.space, container_ref.external_id, "value"], value=99.0),
                limit=10,
            )
            assert [record.external_id for record in result] == [target.external_id]

        assert_eventually(replacement_is_queryable)


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
