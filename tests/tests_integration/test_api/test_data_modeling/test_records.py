from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
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
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._retry import Backoff

STREAM_EXTERNAL_ID = "sdk_test_mutable_stream"
CONTAINER_EXTERNAL_ID = "PythonSdkIntegrationTestRecords"
CONSISTENCY_TIMEOUT = 60.0


def an_hour_ago() -> str:
    """A lower bound for 'lastUpdatedTime'.

    TimeRange forwards its bounds to the API untouched, and the API only accepts ISO-8601 there -
    not the "1h-ago" shorthand that the rest of the SDK understands.
    """
    return (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


def assert_eventually(assertion: Callable[[], None]) -> None:
    """Retry an assertion about eventually-consistent backend state instead of a fixed sleep."""
    deadline = time.monotonic() + CONSISTENCY_TIMEOUT
    wait = Backoff(max_wait=4, min_wait=0.25)
    while True:
        try:
            assertion()
            return
        except AssertionError as error:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                pytest.fail(f"Timed out after {CONSISTENCY_TIMEOUT}s: {error}")
            time.sleep(min(next(wait), remaining))


@dataclass
class RecordBatch:
    records: list[RecordWrite]
    filter: filters.Equals  # Scopes every query to this batch, so parallel runs don't see each other
    cursor: str  # Sync position captured before ingestion


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
    try:
        return cognite_client.data_modeling.streams.create(
            StreamWrite(
                external_id=STREAM_EXTERNAL_ID,
                settings=StreamWriteSettings(template=StreamTemplate(name="BasicLiveData")),
            )
        )
    except CogniteAPIError as error:
        if error.code != 409:
            raise

    def stream_is_visible() -> None:
        nonlocal stream
        stream = cognite_client.data_modeling.streams.retrieve(STREAM_EXTERNAL_ID)
        assert stream is not None

    assert_eventually(stream_is_visible)
    assert stream is not None
    return stream


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
    tagged = filters.Equals(property=[container_ref.space, container_ref.external_id, "name"], value=tag)

    # Capture the sync position before writing
    cursor = next(
        cognite_client.data_modeling.records.sync(
            stream_id=mutable_stream.external_id, initialize_cursor="1m-ago", filter=tagged
        )
    ).cursor
    assert cursor is not None

    def all_records_are_queryable() -> None:
        result = cognite_client.data_modeling.records.filter(
            stream_id=mutable_stream.external_id,
            last_updated_time=TimeRange(gt=an_hour_ago()),
            filter=tagged,
            limit=len(records) + 1,
        )
        assert {record.as_id() for record in result} == {record.as_id() for record in records}

    try:
        cognite_client.data_modeling.records.ingest(records, stream_id=mutable_stream.external_id)
        # Records are not immediatly queryable after ingestion
        assert_eventually(all_records_are_queryable)
        yield RecordBatch(records, tagged, cursor)
    finally:
        cognite_client.data_modeling.records.delete(
            [record.as_id() for record in records], stream_id=mutable_stream.external_id
        )


class TestRecordsIntegration:
    def test_filter_returns_ingested_records(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        sources: list[RecordSourceSelector],
        ingested_records: RecordBatch,
    ) -> None:
        result = cognite_client.data_modeling.records.filter(
            stream_id=mutable_stream.external_id,
            last_updated_time=TimeRange(gt=an_hour_ago()),
            sources=sources,
            filter=ingested_records.filter,
            limit=10,
        )
        assert len(result) == len(ingested_records.records)
        assert {record.external_id for record in result} == {r.external_id for r in ingested_records.records}

    def test_aggregate_over_ingested_records(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        container_ref: RecordContainerId,
        ingested_records: RecordBatch,
    ) -> None:
        value = [container_ref.space, container_ref.external_id, "value"]

        def aggregates_are_visible() -> None:
            result = cognite_client.data_modeling.records.aggregate(
                {"total": Count(), "avg_value": Average(value)},
                stream_id=mutable_stream.external_id,
                last_updated_time=TimeRange(gt=an_hour_ago()),
                filter=ingested_records.filter,
            )
            total, avg_value = result["total"], result["avg_value"]
            assert isinstance(total, MetricResult) and isinstance(avg_value, MetricResult)
            assert total.value == len(ingested_records.records)
            assert avg_value.value == pytest.approx(1.0)  # mean of 0.0, 1.0, 2.0

        # Aggregates could lag behind filter queries
        assert_eventually(aggregates_are_visible)

    @pytest.mark.parametrize("chunk_size", [1000, 2])
    def test_sync_iterates_until_feed_exhausted(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        sources: list[RecordSourceSelector],
        ingested_records: RecordBatch,
        chunk_size: int,
    ) -> None:
        """Walk the sync feed from the pre-ingestion cursor until 'has_next' is False.

        Covers both a single partial page (chunk_size 1000) and several small chunks
        (chunk_size 2). The service decides page boundaries, so records are accumulated
        across pages rather than expected in any particular one.
        """

        def feed_yields_every_record() -> None:
            seen: list[str] = []
            pages = 0
            for page in cognite_client.data_modeling.records.sync(
                stream_id=mutable_stream.external_id,
                cursor=ingested_records.cursor,
                sources=sources,
                filter=ingested_records.filter,
                chunk_size=chunk_size,
            ):
                assert page.cursor is not None
                assert len(page) <= chunk_size
                seen.extend(record.external_id for record in page)
                pages += 1
                assert pages < 20, "sync did not exhaust the feed"
            assert set(seen) == {record.external_id for record in ingested_records.records}

        # The sync feed might lag behind filter queries, so the records may not all be in it yet
        assert_eventually(feed_yields_every_record)

    def test_upsert_replaces_record(
        self,
        cognite_client: CogniteClient,
        mutable_stream: Stream,
        container_ref: RecordContainerId,
        sources: list[RecordSourceSelector],
        ingested_records: RecordBatch,
    ) -> None:
        target = ingested_records.records[0]
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
                # Scoped to this run: other jobs might upsert the same value concurrently
                filter=filters.And(
                    ingested_records.filter,
                    filters.Equals(property=[container_ref.space, container_ref.external_id, "value"], value=99.0),
                ),
                limit=10,
            )
            assert [record.external_id for record in result] == [target.external_id]
            assert result[0].properties is not None
            assert result[0].properties[container_ref.space][container_ref.external_id] == properties

        assert_eventually(replacement_is_queryable)


class TestStreamsIntegration:
    def test_retrieve_and_list_stream(self, cognite_client: CogniteClient, mutable_stream: Stream) -> None:
        def stream_is_listed() -> None:
            retrieved = cognite_client.data_modeling.streams.retrieve(mutable_stream.external_id)
            assert retrieved is not None
            assert retrieved.external_id == mutable_stream.external_id
            assert retrieved.type == "Mutable"

            listed = cognite_client.data_modeling.streams.list()
            assert mutable_stream.external_id in {stream.external_id for stream in listed}

        # A stream created moments ago by the fixture may not be listed yet.
        assert_eventually(stream_is_listed)

    def test_retrieve_unknown_stream_returns_none(self, cognite_client: CogniteClient) -> None:
        assert cognite_client.data_modeling.streams.retrieve("this-stream-does-not-exist-12345") is None
