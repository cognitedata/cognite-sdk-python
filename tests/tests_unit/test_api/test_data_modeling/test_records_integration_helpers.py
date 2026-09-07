from __future__ import annotations

import time
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite.client.data_classes import filters
from cognite.client.data_classes.data_modeling.aggregates import MetricResult
from cognite.client.data_classes.data_modeling.records import Record, RecordContainerId, RecordSource, RecordWrite
from cognite.client.exceptions import CogniteAPIError
from tests.tests_integration.test_api.test_data_modeling import test_records as integration


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    now = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    monkeypatch.setattr(time, "sleep", lambda delay: now.__setitem__(0, now[0] + delay))
    return now


@pytest.fixture
def client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def stream() -> MagicMock:
    return MagicMock(external_id=integration.STREAM_EXTERNAL_ID, type="Mutable")


@pytest.fixture
def container() -> RecordContainerId:
    return RecordContainerId("test-space", "test-container")


def test_poll_delayed_visibility(clock: list[float]) -> None:
    def visible() -> None:
        assert clock[0] >= 5, "not visible yet"

    integration.assert_eventually(visible)
    assert 5 <= clock[0] < 60


def test_poll_timeout_preserves_context_and_last_failure(clock: list[float]) -> None:
    def replacement_is_visible() -> None:
        assert False, "expected value 99, got 0"

    with pytest.raises(AssertionError, match=r"replacement_is_visible.*expected value 99") as error:
        integration.assert_eventually(replacement_is_visible)
    assert clock[0] == 60
    assert isinstance(error.value.__cause__, AssertionError)


def test_poll_does_not_retry_api_errors(clock: list[float]) -> None:
    def forbidden() -> None:
        raise CogniteAPIError("Forbidden", 403)

    with pytest.raises(CogniteAPIError):
        integration.assert_eventually(forbidden)
    assert clock[0] == 0


def test_stream_creation_race(clock: list[float], client: MagicMock, stream: MagicMock) -> None:
    client.data_modeling.streams.retrieve.side_effect = [None, None, stream]
    client.data_modeling.streams.create.side_effect = CogniteAPIError("Already exists", 409)
    assert getattr(integration.mutable_stream, "__wrapped__")(client) is stream
    assert client.data_modeling.streams.create.call_count == 1


def test_stream_conflict_without_visible_stream_times_out(clock: list[float], client: MagicMock) -> None:
    client.data_modeling.streams.retrieve.return_value = None
    client.data_modeling.streams.create.side_effect = CogniteAPIError("Already exists", 409)
    with pytest.raises(AssertionError, match="concurrently_created_stream_is_visible"):
        getattr(integration.mutable_stream, "__wrapped__")(client)
    assert clock[0] == 60


@pytest.mark.parametrize("code", [400, 403, 500])
def test_stream_creation_propagates_other_errors(clock: list[float], client: MagicMock, code: int) -> None:
    client.data_modeling.streams.retrieve.return_value = None
    client.data_modeling.streams.create.side_effect = CogniteAPIError("failure", code)
    with pytest.raises(CogniteAPIError) as error:
        getattr(integration.mutable_stream, "__wrapped__")(client)
    assert error.value.code == code
    assert clock[0] == 0


def test_shared_stream_must_be_mutable(client: MagicMock, stream: MagicMock) -> None:
    stream.type = "Immutable"
    client.data_modeling.streams.retrieve.return_value = stream
    with pytest.raises(AssertionError, match="must be mutable"):
        getattr(integration.mutable_stream, "__wrapped__")(client)


@pytest.mark.parametrize("failure", ["ingest", "readiness", "test", None])
def test_batch_cleanup_on_failure_and_success(
    clock: list[float], client: MagicMock, stream: MagicMock, container: RecordContainerId, failure: str | None
) -> None:
    api = client.data_modeling.records
    events = []

    def seed(**kwargs: Any) -> Iterator[SimpleNamespace]:
        events.append("cursor")
        yield SimpleNamespace(cursor="before-ingestion")

    def ingest(records: list[RecordWrite], **kwargs: Any) -> None:
        events.append("ingest")
        api.filter.return_value = [] if failure == "readiness" else records
        if failure == "ingest":
            raise CogniteAPIError("partial ingestion failure", 400)

    api.sync.side_effect = seed
    api.ingest.side_effect = ingest
    fixture = getattr(integration.ingested_records, "__wrapped__")(client, stream, container)
    if failure in ("ingest", "readiness"):
        with pytest.raises((CogniteAPIError, AssertionError)):
            next(fixture)
    else:
        batch = next(fixture)
        assert batch.cursor == "before-ingestion"
        if failure == "test":
            with pytest.raises(RuntimeError, match="test failed"):
                fixture.throw(RuntimeError("test failed"))
        else:
            fixture.close()
    assert events == ["cursor", "ingest"]
    written = api.ingest.call_args.args[0]
    api.delete.assert_called_once_with([record.as_id() for record in written], stream_id=stream.external_id)


def test_upsert_ignores_other_batches_with_same_value(
    clock: list[float], client: MagicMock, stream: MagicMock, container: RecordContainerId
) -> None:
    target = RecordWrite(
        container.space, "own-0", [RecordSource(container, {"name": "own", "value": 0.0, "processed": True})]
    )
    tagged = filters.Equals([container.space, container.external_id, "name"], "own")
    batch = integration.RecordBatch([target], tagged, "cursor")
    # Evaluate the actual filter against both batches: the old value-only query returns both.
    rows = [
        Record(
            container.space,
            external_id,
            1,
            1,
            {
                container.space: {
                    container.external_id: {
                        "name": tag,
                        "value": 99.0,
                        "processed": True,
                    }
                }
            },
        )
        for external_id, tag in [("other-0", "other"), ("own-0", "own")]
    ]

    def matches(expression: dict[str, Any], properties: dict[str, Any]) -> bool:
        if "and" in expression:
            return all(matches(child, properties) for child in expression["and"])
        equals = expression["equals"]
        return properties[equals["property"][-1]] == equals["value"]

    def query(**kwargs: Any) -> list[Record]:
        return [
            row
            for row in rows
            if row.properties is not None
            and matches(kwargs["filter"].dump(), row.properties[container.space][container.external_id])
        ]

    client.data_modeling.records.filter.side_effect = query
    integration.TestRecordsIntegration().test_upsert_replaces_record(client, stream, container, [], batch)
    client.data_modeling.records.upsert.assert_called_once()


@pytest.mark.parametrize("sizes", [(0, 1, 2), (1, 0, 2), (1, 2)])
def test_sync_retries_from_fixed_cursor_with_arbitrary_pages(
    clock: list[float], client: MagicMock, stream: MagicMock, container: RecordContainerId, sizes: tuple[int, ...]
) -> None:
    records = [RecordWrite(container.space, f"own-{i}", []) for i in range(3)]
    batch = integration.RecordBatch(
        records, filters.Equals([container.space, container.external_id, "name"], "own"), "fixed"
    )
    calls: list[dict[str, Any]] = []

    class Page(list[RecordWrite]):
        cursor = "next"
        has_next = False

    def sync(**kwargs: Any) -> Iterator[Page]:
        calls.append(kwargs)
        if len(calls) == 1:
            yield Page()  # The feed can initially lag behind filter visibility.
            return
        offset = 0
        for index, size in enumerate(sizes):
            page = Page(records[offset : offset + size])
            page.has_next = index < len(sizes) - 1
            offset += size
            yield page

    client.data_modeling.records.sync.side_effect = sync
    integration.TestRecordsIntegration().test_sync_iterates_until_feed_exhausted(client, stream, [], batch, 2)
    assert len(calls) == 2
    assert all(call["cursor"] == "fixed" and "initialize_cursor" not in call for call in calls)


def test_sync_never_exhausting_feed_times_out(
    clock: list[float], client: MagicMock, stream: MagicMock, container: RecordContainerId
) -> None:
    batch = integration.RecordBatch(
        [], filters.Equals([container.space, container.external_id, "name"], "own"), "fixed"
    )

    class Page(list[RecordWrite]):
        cursor = "next"
        has_next = True

    def sync(**kwargs: Any) -> Iterator[Page]:
        while True:
            clock[0] += 1
            yield Page()

    client.data_modeling.records.sync.side_effect = sync
    with pytest.raises(AssertionError, match="sync did not exhaust the feed"):
        integration.TestRecordsIntegration().test_sync_iterates_until_feed_exhausted(client, stream, [], batch, 2)
    assert clock[0] == 60


@pytest.mark.parametrize("endpoint", ["filter", "aggregate"])
def test_each_read_endpoint_waits_for_its_own_visibility(
    clock: list[float],
    client: MagicMock,
    stream: MagicMock,
    container: RecordContainerId,
    endpoint: str,
) -> None:
    records = [RecordWrite(container.space, f"own-{i}", []) for i in range(3)]
    batch = integration.RecordBatch(
        records, filters.Equals([container.space, container.external_id, "name"], "own"), "fixed"
    )
    tests = integration.TestRecordsIntegration()
    if endpoint == "filter":
        client.data_modeling.records.filter.side_effect = [[], records]
        tests.test_filter_returns_ingested_records(client, stream, container, [], batch)
        assert client.data_modeling.records.filter.call_count == 2
    else:
        client.data_modeling.records.aggregate.side_effect = [
            {"total": MetricResult("count", 0), "avg_value": MetricResult("avg", None)},
            {"total": MetricResult("count", 3), "avg_value": MetricResult("avg", 1.0)},
        ]
        tests.test_aggregate_over_ingested_records(client, stream, container, batch)
        assert client.data_modeling.records.aggregate.call_count == 2
