"""
===============================================================================
2058e4bb661c1caf5ebdf1598c7ddc36
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.records import (
    AggregateResult,
    AggregateSpec,
    Record,
    RecordList,
    RecordSortSpec,
    RecordSourceSelector,
    RecordWrite,
    SyncRecord,
    TargetUnits,
    TimeRange,
)
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._async_helpers import SyncIterator, run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncRecordsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def ingest(self, stream_id: str, items: RecordWrite | Sequence[RecordWrite]) -> None:
        """
        `Ingest records into a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/ingestRecords>`_.

        Creates new records. For immutable streams, duplicate records (identical
        ``space``, ``externalId``, and all property values) are silently discarded.
        For mutable streams, duplicate ``space + externalId`` within a single batch
        returns a 422.

        Each request accepts up to 1 000 records; larger lists are chunked automatically.

        Args:
            stream_id (str): External ID of the stream to ingest into.
            items (RecordWrite | Sequence[RecordWrite]): One or more records to ingest.

        Examples:

            Ingest a single record:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.records import (
                ...     RecordWrite,
                ...     RecordSource,
                ...     RecordSourceReference,
                ... )
                >>> client = CogniteClient()
                >>> client.data_modeling.records.ingest(
                ...     stream_id="my-stream",
                ...     items=RecordWrite(
                ...         space="my-space",
                ...         external_id="rec-1",
                ...         sources=[
                ...             RecordSource(
                ...                 source=RecordSourceReference(
                ...                     space="my-space", external_id="my-container"
                ...                 ),
                ...                 properties={"temperature": 22.5},
                ...             )
                ...         ],
                ...     ),
                ... )
        """
        return run_sync(self.__async_client.data_modeling.records.ingest(stream_id=stream_id, items=items))

    def upsert(self, stream_id: str, items: RecordWrite | Sequence[RecordWrite]) -> None:
        """
        `Upsert records into a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/upsertRecords>`_.

        Creates or updates records. Only valid for mutable streams (returns 422 on
        immutable). Existing records matching ``space + externalId`` are updated at
        the property level.

        Each request accepts up to 1 000 records; larger lists are chunked automatically.

        Args:
            stream_id (str): External ID of the stream to upsert into.
            items (RecordWrite | Sequence[RecordWrite]): One or more records to upsert.

        Examples:

            Upsert a single record:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.records import (
                ...     RecordWrite,
                ...     RecordSource,
                ...     RecordSourceReference,
                ... )
                >>> client = CogniteClient()
                >>> client.data_modeling.records.upsert(
                ...     stream_id="my-stream",
                ...     items=RecordWrite(
                ...         space="my-space",
                ...         external_id="rec-1",
                ...         sources=[
                ...             RecordSource(
                ...                 source=RecordSourceReference(
                ...                     space="my-space", external_id="my-container"
                ...                 ),
                ...                 properties={"temperature": 23.0},
                ...             )
                ...         ],
                ...     ),
                ... )
        """
        return run_sync(self.__async_client.data_modeling.records.upsert(stream_id=stream_id, items=items))

    def delete(self, stream_id: str, items: Record | RecordWrite | Sequence[Record | RecordWrite]) -> None:
        """
        `Delete records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/deleteRecords>`_.

        Only valid for mutable streams (returns 422 on immutable). Unknown
        ``space + externalId`` pairs are silently ignored.

        Each request accepts up to 1 000 identifiers; larger lists are chunked automatically.

        Args:
            stream_id (str): External ID of the stream to delete from.
            items (Record | RecordWrite | Sequence[Record | RecordWrite]): Records to delete.
                Only ``space`` and ``external_id`` are used; other fields are ignored.

        Examples:

            Delete records by external ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.records import RecordWrite
                >>> client = CogniteClient()
                >>> client.data_modeling.records.delete(
                ...     stream_id="my-stream",
                ...     items=[
                ...         RecordWrite(space="my-space", external_id="rec-1", sources=[]),
                ...         RecordWrite(space="my-space", external_id="rec-2", sources=[]),
                ...     ],
                ... )
        """
        return run_sync(self.__async_client.data_modeling.records.delete(stream_id=stream_id, items=items))

    def list(
        self,
        stream_id: str,
        *,
        last_updated_time: TimeRange | None = None,
        filter: Filter | None = None,
        sources: Sequence[RecordSourceSelector] | None = None,
        sort: Sequence[RecordSortSpec] | None = None,
        limit: int | None = 25,
        target_units: TargetUnits | None = None,
        include_typing: bool = False,
    ) -> RecordList:
        """
        `List records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/filterRecords>`_.

        Returns records matching the given filters. For immutable streams, ``last_updated_time``
        with at least one lower bound is required. The endpoint has no cursor — to page over
        large time windows, issue multiple calls with partitioned ``last_updated_time`` ranges.

        Args:
            stream_id (str): External ID of the stream to query.
            last_updated_time (TimeRange | None): Filter records by last-updated time.
                **Required** for immutable streams (must include a lower bound).
            filter (Filter | None): Filter expression (see :mod:`cognite.client.data_classes.filters`).
            sources (Sequence[RecordSourceSelector] | None): Which container properties to return.
            sort (Sequence[RecordSortSpec] | None): Sort specifications (up to 5).
            limit (int | None): Maximum number of records to return (1-1000). Defaults to 25.
            target_units (TargetUnits | None): Unit conversion to apply to numeric properties.
            include_typing (bool): Include property type metadata in the response.

        Returns:
            RecordList: Matching records.

        Examples:

            List all records updated in the last hour:

                >>> import time
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.records import TimeRange
                >>> client = CogniteClient()
                >>> now_ms = int(time.time() * 1000)
                >>> res = client.data_modeling.records.list(
                ...     stream_id="my-stream",
                ...     last_updated_time=TimeRange(gte=now_ms - 3_600_000),
                ...     limit=100,
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.records.list(
                stream_id=stream_id,
                last_updated_time=last_updated_time,
                filter=filter,
                sources=sources,
                sort=sort,
                limit=limit,
                target_units=target_units,
                include_typing=include_typing,
            )
        )

    def sync(
        self,
        stream_id: str,
        *,
        cursor: str | None = None,
        initialize_cursor: str | None = None,
        filter: Filter | None = None,
        sources: Sequence[RecordSourceSelector] | None = None,
        limit: int | None = None,
        target_units: TargetUnits | None = None,
        include_typing: bool = False,
    ) -> Iterator[SyncRecord]:
        """
        `Sync records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/syncRecords>`_.

        Returns a change feed of new, updated, and deleted records. Provide either
        ``cursor`` (to resume a previous sync position) or ``initialize_cursor``
        (to start from a relative time, e.g. ``"7d-ago"``). The generator drains all
        available pages (following ``hasNext``) then stops — call ``sync()`` again with
        a new ``cursor`` to poll for subsequent changes.

        Args:
            stream_id (str): External ID of the stream to sync.
            cursor (str | None): Resume from a previous sync cursor.
            initialize_cursor (str | None): Starting point if no cursor (e.g. ``"7d-ago"``).
                Ignored when ``cursor`` is set.
            filter (Filter | None): Filter expression.
            sources (Sequence[RecordSourceSelector] | None): Which container properties to return.
            limit (int | None): Maximum records per API call (1-1000).
            target_units (TargetUnits | None): Unit conversion for numeric properties.
            include_typing (bool): Include property type metadata.

        Yields:
            SyncRecord: Records in change order. Deleted records have ``properties=None``.

        Examples:

            Sync all records changed in the last 7 days:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for record in client.data_modeling.records.sync(
                ...     stream_id="my-stream",
                ...     initialize_cursor="7d-ago",
                ... ):
                ...     pass  # process record
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.data_modeling.records.sync(
                stream_id=stream_id,
                cursor=cursor,
                initialize_cursor=initialize_cursor,
                filter=filter,
                sources=sources,
                limit=limit,
                target_units=target_units,
                include_typing=include_typing,
            )
        )

    def aggregate(
        self,
        stream_id: str,
        aggregates: dict[str, AggregateSpec],
        *,
        last_updated_time: TimeRange | None = None,
        filter: Filter | None = None,
        target_units: TargetUnits | None = None,
        include_typing: bool = False,
    ) -> dict[str, AggregateResult]:
        """
        `Aggregate records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/aggregateRecords>`_.

        Compute metrics (avg, count, min, max, sum) and bucket aggregations
        (unique values, time histogram, number histogram, filters) over records.
        Bucket aggregates support nested sub-aggregates.

        For immutable streams, ``last_updated_time`` with at least one lower bound is required.

        Args:
            stream_id (str): External ID of the stream to aggregate.
            aggregates (dict[str, AggregateSpec]): Mapping of user-defined aggregate IDs
                to aggregate specifications (1-5 top-level entries). IDs must not contain
                ``"."`` and cannot be ``"_count"`` or ``"_bucket_count"``.
            last_updated_time (TimeRange | None): Time range filter. **Required** for immutable streams.
            filter (Filter | None): Filter expression applied before aggregating.
            target_units (TargetUnits | None): Unit conversion for numeric properties.
            include_typing (bool): Include property type metadata in the response.

        Returns:
            dict[str, AggregateResult]: Mapping of aggregate IDs to their results.
                Metric aggregates return a ``{"avg": value}``-style dict; bucket aggregates
                return lists of bucket objects.

        Examples:

            Count records and compute average temperature:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.records import (
                ...     AvgAggregate,
                ...     CountAggregate,
                ...     TimeRange,
                ... )
                >>> client = CogniteClient()
                >>> res = client.data_modeling.records.aggregate(
                ...     stream_id="my-stream",
                ...     aggregates={
                ...         "total": CountAggregate(
                ...             property=["my-space", "my-container", "temperature"]
                ...         ),
                ...         "avg_temp": AvgAggregate(
                ...             property=["my-space", "my-container", "temperature"]
                ...         ),
                ...     },
                ...     last_updated_time=TimeRange(gte="2024-01-01T00:00:00Z"),
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.records.aggregate(
                stream_id=stream_id,
                aggregates=aggregates,
                last_updated_time=last_updated_time,
                filter=filter,
                target_units=target_units,
                include_typing=include_typing,
            )
        )
