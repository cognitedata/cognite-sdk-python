from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any, Literal

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.records import (
    AggregateResult,
    AggregateSpec,
    RecordId,
    RecordList,
    RecordSortSpec,
    RecordSourceSelector,
    RecordWrite,
    SyncRecord,
    TargetUnits,
    TimeRange,
)
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._auxiliary import split_into_chunks
from cognite.client.utils._concurrency import RecordsConcurrencyOperation
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._url import interpolate_and_url_encode

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class RecordsAPI(APIClient):
    """API for reading and writing records in a stream.

    Records are stored in a stream and their schema is defined by the containers
    referenced as sources.
    """

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Records"
        )

    def _get_semaphore(self, operation: RecordsConcurrencyOperation) -> asyncio.BoundedSemaphore:
        from cognite.client import global_config

        return global_config.concurrency_settings.records._semaphore_factory(
            operation, project=self._cognite_client.config.project
        )

    def _records_url(self, stream_id: str, suffix: str = "") -> str:
        return interpolate_and_url_encode("/streams/%s/records%s", stream_id, suffix)

    async def ingest(self, stream_id: str, items: RecordWrite | Sequence[RecordWrite]) -> None:
        """`Ingest records into a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/ingestRecords>`_.

        Creates new records. For immutable streams, duplicate records (identical
        ``space``, ``externalId``, and all property values) are silently discarded.
        For mutable streams, duplicate ``space + externalId`` within a single batch
        returns a 422.

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
        self._warning.warn()
        item_list: list[RecordWrite] = [items] if isinstance(items, RecordWrite) else list(items)
        semaphore = self._get_semaphore(RecordsConcurrencyOperation.WRITE)
        for chunk in split_into_chunks(item_list, self._CREATE_LIMIT):
            await self._post(
                url_path=self._records_url(stream_id),
                json={"items": [r.dump() for r in chunk]},
                semaphore=semaphore,
            )

    async def upsert(self, stream_id: str, items: RecordWrite | Sequence[RecordWrite]) -> None:
        """`Upsert records into a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/upsertRecords>`_.

        Creates or updates records. Only valid for mutable streams (returns 422 on
        immutable). Existing records matching ``space + externalId`` are updated at
        the property level.

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
        self._warning.warn()
        item_list: list[RecordWrite] = [items] if isinstance(items, RecordWrite) else list(items)
        semaphore = self._get_semaphore(RecordsConcurrencyOperation.WRITE)
        for chunk in split_into_chunks(item_list, self._CREATE_LIMIT):
            await self._post(
                url_path=self._records_url(stream_id, "/upsert"),
                json={"items": [r.dump() for r in chunk]},
                semaphore=semaphore,
            )

    async def delete(
        self,
        stream_id: str,
        items: RecordId | Sequence[RecordId],
        ignore_unknown_ids: Literal[True] = True,
    ) -> None:
        """`Delete records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/deleteRecords>`_.

        Only valid for mutable streams (returns 422 on immutable). Unknown
        ``space + externalId`` pairs are silently ignored.

        Args:
            stream_id (str): External ID of the stream to delete from.
            items (RecordId | Sequence[RecordId]): Records to delete.
            ignore_unknown_ids (Literal[True]): is always true

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
        self._warning.warn()
        item_list = [items] if isinstance(items, RecordId) else list(items)
        semaphore = self._get_semaphore(RecordsConcurrencyOperation.DELETE)
        for chunk in split_into_chunks(item_list, self._CREATE_LIMIT):
            await self._post(
                url_path=self._records_url(stream_id, "/delete"),
                json={"items": [{"space": r.space, "externalId": r.external_id} for r in chunk]},
                semaphore=semaphore,
            )

    async def list(
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
        """`List records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/filterRecords>`_.

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
        self._warning.warn()
        body: dict[str, Any] = {}
        if last_updated_time is not None:
            body["lastUpdatedTime"] = last_updated_time.dump()
        if filter is not None:
            body["filter"] = filter.dump()
        if sources is not None:
            body["sources"] = [s.dump() for s in sources]
        if sort is not None:
            body["sort"] = [s.dump() for s in sort]
        if limit is not None:
            body["limit"] = limit
        if target_units is not None:
            body["targetUnits"] = target_units.dump()
        if include_typing:
            body["includeTyping"] = True

        res = await self._post(
            url_path=self._records_url(stream_id, "/filter"),
            json=body,
            semaphore=self._get_semaphore(RecordsConcurrencyOperation.RETRIEVE),
        )
        return RecordList._load(res.json()["items"])

    async def sync(
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
    ) -> AsyncIterator[SyncRecord]:
        """`Sync records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/syncRecords>`_.

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
        """
        self._warning.warn()
        body: dict[str, Any] = {}
        if cursor is not None:
            body["cursor"] = cursor
        elif initialize_cursor is not None:
            body["initializeCursor"] = initialize_cursor
        if filter is not None:
            body["filter"] = filter.dump()
        if sources is not None:
            body["sources"] = [s.dump() for s in sources]
        if limit is not None:
            body["limit"] = limit
        if target_units is not None:
            body["targetUnits"] = target_units.dump()
        if include_typing:
            body["includeTyping"] = True

        semaphore = self._get_semaphore(RecordsConcurrencyOperation.SYNC)
        while True:
            res = await self._post(
                url_path=self._records_url(stream_id, "/sync"),
                json=body,
                semaphore=semaphore,
            )
            data = res.json()
            for item in data["items"]:
                yield SyncRecord._load(item)
            if not data["hasNext"]:
                break
            body["cursor"] = data["nextCursor"]

    async def aggregate(
        self,
        stream_id: str,
        aggregates: dict[str, AggregateSpec],
        *,
        last_updated_time: TimeRange | None = None,
        filter: Filter | None = None,
        target_units: TargetUnits | None = None,
        include_typing: bool = False,
    ) -> dict[str, AggregateResult]:
        """`Aggregate records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/aggregateRecords>`_.

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
        self._warning.warn()
        body: dict[str, Any] = {
            "aggregates": {k: v.dump() for k, v in aggregates.items()},
        }
        if last_updated_time is not None:
            body["lastUpdatedTime"] = last_updated_time.dump()
        if filter is not None:
            body["filter"] = filter.dump()
        if target_units is not None:
            body["targetUnits"] = target_units.dump()
        if include_typing:
            body["includeTyping"] = True

        res = await self._post(
            url_path=self._records_url(stream_id, "/aggregate"),
            json=body,
            semaphore=self._get_semaphore(RecordsConcurrencyOperation.AGGREGATE),
        )
        return res.json()["aggregates"]
