"""
===============================================================================
e50d855222acc9a875059316cd3d6fce
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.aggregates import Aggregate
from cognite.client.data_classes.data_modeling.instances import InstanceSort
from cognite.client.data_classes.data_modeling.records import (
    RecordId,
    RecordList,
    RecordsAggregation,
    RecordSourceSelector,
    RecordTargetUnit,
    RecordTargetUnits,
    RecordWrite,
    SyncRecordList,
    TimeRange,
)
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._async_helpers import SyncIterator, run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient

_FILTER_MAX_LIMIT = 1000


class SyncRecordsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def delete(
        self, items: RecordId | Sequence[RecordId], *, stream_id: str, ignore_unknown_ids: Literal[True] = True
    ) -> None:
        """
        `Delete records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/deleteRecords>`_.

        Only valid for mutable streams (returns 422 on immutable). Unknown
        ``space + externalId`` pairs are silently ignored.

        Args:
            items (RecordId | Sequence[RecordId]): Records to delete.
            stream_id (str): External ID of the stream to delete from.
            ignore_unknown_ids (Literal[True]): Currently only True is supported

        Examples:

            Delete records:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.records import RecordId
                >>> client = CogniteClient()
                >>> client.data_modeling.records.delete(
                ...     stream_id="my-stream",
                ...     items=[
                ...         RecordId(space="my-space", external_id="rec-1"),
                ...         RecordId(space="my-space", external_id="rec-2"),
                ...     ],
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.records.delete(
                items=items, stream_id=stream_id, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def ingest(self, items: RecordWrite | Sequence[RecordWrite], *, stream_id: str) -> None:
        """
        `Ingest records into a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/ingestRecords>`_.

        Creates new records. For immutable streams, duplicate records (identical
        ``space``, ``externalId``, and all property values) are silently discarded.
        For mutable streams, duplicate ``space + externalId`` within a single batch
        returns a 422.

        Args:
            items (RecordWrite | Sequence[RecordWrite]): One or more records to ingest.
            stream_id (str): External ID of the stream to ingest into.

        Examples:

            Ingest a single record:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.records import (
                ...     RecordWrite,
                ...     RecordContainerId,
                ...     RecordSource,
                ... )
                >>> client = CogniteClient()
                >>> client.data_modeling.records.ingest(
                ...     RecordWrite(
                ...         space="my-space",
                ...         external_id="rec-1",
                ...         sources=[
                ...             RecordSource(
                ...                 source=RecordContainerId(
                ...                     space="my-space", external_id="my-container"
                ...                 ),
                ...                 properties={"temperature": 22.5},
                ...             )
                ...         ],
                ...     ),
                ...     stream_id="my-stream",
                ... )
        """
        return run_sync(self.__async_client.data_modeling.records.ingest(items=items, stream_id=stream_id))

    def upsert(
        self, items: RecordWrite | Sequence[RecordWrite], *, stream_id: str, upsert_mode: Literal["replace"] = "replace"
    ) -> None:
        """
        `Upsert records into a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/upsertRecords>`_.

        Creates or fully updates records. Only valid for mutable streams (returns 422 on
        immutable). When a record with the same ``space + externalId`` already exists it is
        fully replaced (this endpoint does not do partial property updates); otherwise it is
        created.

        Args:
            items (RecordWrite | Sequence[RecordWrite]): One or more records to upsert.
            stream_id (str): External ID of the stream to upsert into.
            upsert_mode (Literal['replace']): How existing records are updated. Currently only ``"replace"`` is supported, which fully replaces the existing record. Defaults to ``"replace"``.

        Examples:

            Upsert a single record:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.records import (
                ...     RecordWrite,
                ...     RecordContainerId,
                ...     RecordSource,
                ... )
                >>> client = CogniteClient()
                >>> client.data_modeling.records.upsert(
                ...     RecordWrite(
                ...         space="my-space",
                ...         external_id="rec-1",
                ...         sources=[
                ...             RecordSource(
                ...                 source=RecordContainerId(
                ...                     space="my-space", external_id="my-container"
                ...                 ),
                ...                 properties={"temperature": 23.0},
                ...             )
                ...         ],
                ...     ),
                ...     stream_id="my-stream",
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.records.upsert(items=items, stream_id=stream_id, upsert_mode=upsert_mode)
        )

    def aggregate(
        self,
        aggregates: Mapping[str, Aggregate | dict[str, Any]],
        *,
        stream_id: str,
        last_updated_time: TimeRange | None = None,
        filter: Filter | dict[str, Any] | None = None,
        target_units: RecordTargetUnits | Sequence[RecordTargetUnit] | None = None,
        include_typing: bool = False,
    ) -> RecordsAggregation:
        """
        `Aggregate records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/aggregateRecords>`_.

        Args:
            aggregates (Mapping[str, Aggregate | dict[str, Any]]): Aggregate request tree keyed
                by client-defined aggregate IDs.
            stream_id (str): External ID of the stream to aggregate from.
            last_updated_time (TimeRange | None): Filter records by last-updated time.
                **Required** for immutable streams (must include a lower bound).
            filter (Filter | dict[str, Any] | None): Filter expression.
            target_units (RecordTargetUnits | Sequence[RecordTargetUnit] | None): Unit conversion specification.
            include_typing (bool): Include property type metadata in the response.

        Returns:
            RecordsAggregation: Aggregate results keyed by the requested aggregate IDs.

        Examples:

            The examples below aggregate over a stream of padel game statistics records; each
            example builds on the previous one.

            The property paths used below:

                >>> game_time = ["paddle", "game_statistics", "game_time"]
                >>> player_name = ["paddle", "game_statistics", "player_name"]
                >>> points_scored = ["paddle", "game_statistics", "points_scored"]


            Find the average points scored across all games, using a typed helper:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.aggregates import Average
                >>> client = CogniteClient()
                >>> res = client.data_modeling.records.aggregate(
                ...     stream_id="my-stream",
                ...     aggregates={"avg_points_scored": Average(points_scored)},
                ... )


            Count the total number of games, and how many of them have a recorded score, only
            considering games updated after a given time:

                >>> from cognite.client.data_classes.data_modeling.aggregates import Count
                >>> from cognite.client.data_classes.data_modeling.records import TimeRange
                >>> res = client.data_modeling.records.aggregate(
                ...     stream_id="my-stream",
                ...     aggregates={
                ...         "total_games": Count(),
                ...         "games_with_score": Count(points_scored),
                ...     },
                ...     last_updated_time=TimeRange(gt=1759276800000),
                ... )


            Group games by day, then by player, and for each player-day compute their total,
            highest, and average points scored, alongside the single highest score across all
            games:

                >>> from cognite.client.data_classes.data_modeling.aggregates import (
                ...     Average,
                ...     Max,
                ...     Sum,
                ...     TimeHistogram,
                ...     UniqueValues,
                ... )
                >>> res = client.data_modeling.records.aggregate(
                ...     stream_id="my-stream",
                ...     aggregates={
                ...         "my_groups_by_1d_range": TimeHistogram(
                ...             property=game_time,
                ...             calendar_interval="1d",
                ...             aggregates={
                ...                 "my_groups_by_player_name": UniqueValues(
                ...                     property=player_name,
                ...                     aggregates={
                ...                         "my_player_daily_scores_sum": Sum(points_scored),
                ...                         "my_player_daily_scores_maximum": Max(points_scored),
                ...                     },
                ...                 ),
                ...                 "my_daily_scores_average": Average(points_scored),
                ...             },
                ...         ),
                ...         "my_scores_maximum_across_all_games": Max(points_scored),
                ...     },
                ... )


            Bucket games by day and smooth the daily count with a 7-day moving average, using the
            ``MovingFunctions`` enum so the pipeline function name cannot be mistyped:

                >>> from cognite.client.data_classes.data_modeling.aggregates import (
                ...     Count,
                ...     MovingFunction,
                ...     MovingFunctions,
                ...     TimeHistogram,
                ... )
                >>> res = client.data_modeling.records.aggregate(
                ...     stream_id="my-stream",
                ...     aggregates={
                ...         "games_per_day": TimeHistogram(
                ...             property=game_time,
                ...             calendar_interval="1d",
                ...             aggregates={
                ...                 "games": Count(),
                ...                 "games_7d_avg": MovingFunction(
                ...                     buckets_path="games",
                ...                     window=7,
                ...                     function=MovingFunctions.UNWEIGHTED_AVG,
                ...                 ),
                ...             },
                ...         ),
                ...     },
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.records.aggregate(
                aggregates=aggregates,
                stream_id=stream_id,
                last_updated_time=last_updated_time,
                filter=filter,
                target_units=target_units,
                include_typing=include_typing,
            )
        )

    def filter(
        self,
        stream_id: str,
        *,
        last_updated_time: TimeRange | None = None,
        filter: Filter | None = None,
        sources: Sequence[RecordSourceSelector] | None = None,
        sort: Sequence[InstanceSort] | InstanceSort | None = None,
        limit: int = 10,
        include_typing: bool = False,
    ) -> RecordList:
        """
        `Filter records in a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/filterRecords>`_.

        Returns records matching the given filters, sorted by ``lastUpdatedTime`` unless a custom
        ``sort`` is given.

        Args:
            stream_id (str): External ID of the stream to query.
            last_updated_time (TimeRange | None): Filter by last-updated time. **Required for
                immutable streams** (must include a lower bound).
            filter (Filter | None): Filter expression (see :mod:`cognite.client.data_classes.filters`).
            sources (Sequence[RecordSourceSelector] | None): Which container properties to return.
            sort (Sequence[InstanceSort] | InstanceSort | None): Sort specification(s); up to 5.
            limit (int): Maximum number of records to return (1-1000). This endpoint returns a single
                page and does not paginate, so a larger limit is an error rather than a silent cap.
            include_typing (bool): If True, include property type information on the returned
                list's ``typing`` attribute.

        Returns:
            RecordList: The matching records.

        Examples:

            List records updated since a given timestamp:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.records import TimeRange
                >>> client = CogniteClient()
                >>> res = client.data_modeling.records.filter(
                ...     stream_id="my-stream",
                ...     last_updated_time=TimeRange(gt=1705341600000),
                ...     limit=100,
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.records.filter(
                stream_id=stream_id,
                last_updated_time=last_updated_time,
                filter=filter,
                sources=sources,
                sort=sort,
                limit=limit,
                include_typing=include_typing,
            )
        )

    @overload
    def sync(
        self,
        stream_id: str,
        *,
        initialize_cursor: str,
        filter: Filter | None = None,
        sources: Sequence[RecordSourceSelector] | None = None,
        target_units: RecordTargetUnits | Sequence[RecordTargetUnit] | None = None,
        chunk_size: int = 1000,
        include_typing: bool = False,
    ) -> Iterator[SyncRecordList]: ...

    @overload
    def sync(
        self,
        stream_id: str,
        *,
        cursor: str,
        filter: Filter | None = None,
        sources: Sequence[RecordSourceSelector] | None = None,
        target_units: RecordTargetUnits | Sequence[RecordTargetUnit] | None = None,
        chunk_size: int = 1000,
        include_typing: bool = False,
    ) -> Iterator[SyncRecordList]: ...

    def sync(
        self,
        stream_id: str,
        *,
        initialize_cursor: str | None = None,
        cursor: str | None = None,
        filter: Filter | None = None,
        sources: Sequence[RecordSourceSelector] | None = None,
        target_units: RecordTargetUnits | Sequence[RecordTargetUnit] | None = None,
        chunk_size: int = 1000,
        include_typing: bool = False,
    ) -> Iterator[SyncRecordList]:
        """
        `Sync records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/syncRecords>`_.

        Iterate over the change feed (new, updated and deleted records), yielding one chunk of
        records per request until the feed is exhausted (``has_next`` is False). Pass exactly one of
        ``initialize_cursor`` (to start from a relative time such as ``"7d-ago"``) or ``cursor``
        (to resume from where a previous sync left off). Each yielded :class:`SyncRecordList`
        carries the ``cursor`` to persist for resuming later; this is also why records are always
        yielded in chunks rather than one by one.

        Warning:
            Every chunk is fetched with a separate API request, so a small ``chunk_size`` increases
            the number of requests (i.e. comes at a high performance penalty). Keep the default of
            1000 (the API maximum) unless your per-chunk processing genuinely needs smaller batches.

        Args:
            stream_id (str): External ID of the stream to sync.
            initialize_cursor (str | None): Where to start, as a relative duration like ``"7d-ago"``.
                Mutually exclusive with ``cursor``.
            cursor (str | None): Resume from a cursor from a previously yielded chunk. Mutually
                exclusive with ``initialize_cursor``.
            filter (Filter | None): Filter expression (see :mod:`cognite.client.data_classes.filters`).
            sources (Sequence[RecordSourceSelector] | None): Which container properties to return.
            target_units (RecordTargetUnits | Sequence[RecordTargetUnit] | None): Properties to convert
                to another unit.
            chunk_size (int): Number of records per yielded chunk, between 1 and 1000. Defaults to 1000.
            include_typing (bool): If True, include property type information on each yielded
                list's ``typing`` attribute.

        Yields:
            SyncRecordList: One chunk of change records, with ``cursor`` and ``has_next`` set.

        Examples:

            Iterate over all changes from the last 7 days, persisting the cursor after
            each processed chunk:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for chunk in client.data_modeling.records.sync(
                ...     stream_id="my-stream", initialize_cursor="7d-ago"
                ... ):
                ...     for record in chunk:
                ...         pass  # process record; record.status is created/updated/deleted
                ...     last_cursor = chunk.cursor

            Later, sync only what changed since then by resuming from the stored cursor:

                >>> for chunk in client.data_modeling.records.sync(
                ...     stream_id="my-stream", cursor="previously-stored-cursor"
                ... ):
                ...     pass

            Fetch chunks with manual control, e.g. to poll at your own cadence. Store the
            iterator in a variable to keep pulling chunks from where you left off:

                >>> feed = client.data_modeling.records.sync(
                ...     stream_id="my-stream", cursor="previously-stored-cursor"
                ... )
                >>> first_chunk = next(feed)
                >>> second_chunk = next(feed)
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.data_modeling.records.sync(  # type: ignore [call-overload, misc]
                stream_id=stream_id,
                initialize_cursor=initialize_cursor,
                cursor=cursor,
                filter=filter,
                sources=sources,
                target_units=target_units,
                chunk_size=chunk_size,
                include_typing=include_typing,
            )
        )
