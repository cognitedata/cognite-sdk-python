"""
===============================================================================
a4b6626d64aa7bf3e6b9983e697182b0
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
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
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


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
        aggregates: Mapping[str, Any],
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
            aggregates (Mapping[str, Any]): Aggregate request tree keyed by client-defined aggregate IDs.
            stream_id (str): External ID of the stream to aggregate from.
            last_updated_time (TimeRange | None): Filter records by last-updated time.
                **Required** for immutable streams (must include a lower bound).
            filter (Filter | dict[str, Any] | None): Filter expression.
            target_units (RecordTargetUnits | Sequence[RecordTargetUnit] | None): Unit conversion specification.
            include_typing (bool): Include property type metadata in the response.

        Returns:
            RecordsAggregation: Aggregate results keyed by the requested aggregate IDs.

        Examples:

            The examples below aggregate over a stream of padel game statistics records, going
            from a simple metric to a filtered count to a nested per-player, per-day breakdown.

            Find the average points scored across all games, using a typed helper:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.records import Avg
                >>> client = CogniteClient()
                >>> res = client.data_modeling.records.aggregate(
                ...     stream_id="my-stream",
                ...     aggregates={
                ...         "avg_points_scored": Avg(
                ...             property=["paddle", "game_statistics", "points_scored"]
                ...         ),
                ...     },
                ... )

            Count the total number of games, and how many of them have a recorded score, only
            considering games updated after a given time:

                >>> from cognite.client.data_classes.data_modeling.records import Count, TimeRange
                >>> res = client.data_modeling.records.aggregate(
                ...     stream_id="my-stream",
                ...     aggregates={
                ...         "total_games": Count(),
                ...         "games_with_score": Count(
                ...             property=["paddle", "game_statistics", "points_scored"]
                ...         ),
                ...     },
                ...     last_updated_time=TimeRange(gt=1759276800000),
                ... )

            Group games by day, then by player, and for each player-day compute their total,
            highest, and average points scored, alongside the single highest score across all
            games:

                >>> from cognite.client.data_classes.data_modeling.records import (
                ...     Avg,
                ...     Max,
                ...     Sum,
                ...     TimeHistogram,
                ...     UniqueValues,
                ... )
                >>> res = client.data_modeling.records.aggregate(
                ...     stream_id="my-stream",
                ...     aggregates={
                ...         "my_groups_by_1d_range": TimeHistogram(
                ...             property=["paddle", "game_statistics", "game_time"],
                ...             calendar_interval="1d",
                ...             aggregates={
                ...                 "my_groups_by_player_name": UniqueValues(
                ...                     property=["paddle", "game_statistics", "player_name"],
                ...                     aggregates={
                ...                         "my_player_daily_scores_sum": Sum(
                ...                             property=["paddle", "game_statistics", "points_scored"]
                ...                         ),
                ...                         "my_player_daily_scores_maximum": Max(
                ...                             property=["paddle", "game_statistics", "points_scored"]
                ...                         ),
                ...                     },
                ...                 ),
                ...                 "my_daily_scores_average": Avg(
                ...                     property=["paddle", "game_statistics", "points_scored"]
                ...                 ),
                ...             },
                ...         ),
                ...         "my_scores_maximum_across_all_games": Max(
                ...             property=["paddle", "game_statistics", "points_scored"]
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
            limit (int): Maximum number of records to return (1-1000).
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

    def sync(
        self,
        stream_id: str,
        *,
        initialize_cursor: str,
        filter: Filter | None = None,
        sources: Sequence[RecordSourceSelector] | None = None,
        target_units: RecordTargetUnits | Sequence[RecordTargetUnit] | None = None,
        limit: int = 10,
        include_typing: bool = False,
    ) -> SyncRecordList:
        """
        `Sync records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/syncRecords>`_.

        Returns the first page of the change feed (new, updated and deleted records). Provide
        ``initialize_cursor`` to start from a relative time such as ``"7d-ago"``. Persist the returned
        :attr:`SyncRecordList.cursor` and pass it to :meth:`sync_resume` on the next call to continue;
        :attr:`SyncRecordList.has_next` indicates whether more changes are immediately available.

        Args:
            stream_id (str): External ID of the stream to sync.
            initialize_cursor (str): Where to start, as a relative duration like ``"7d-ago"``.
            filter (Filter | None): Filter expression (see :mod:`cognite.client.data_classes.filters`).
            sources (Sequence[RecordSourceSelector] | None): Which container properties to return.
            target_units (RecordTargetUnits | Sequence[RecordTargetUnit] | None): Properties to convert
                to another unit.
            limit (int): Maximum number of records to return in this page (1-1000). Defaults to 10.
            include_typing (bool): If True, include property type information on the returned
                list's ``typing`` attribute.

        Returns:
            SyncRecordList: One page of change records, with ``cursor`` and ``has_next`` set.

        Examples:

            Initialize a sync, process the page, then resume from the cursor later:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> page = client.data_modeling.records.sync(
                ...     stream_id="my-stream", initialize_cursor="7d-ago"
                ... )
                >>> for record in page:
                ...     pass  # process record; record.status is created/updated/deleted
                >>> next_page = client.data_modeling.records.sync_resume(
                ...     stream_id="my-stream", cursor=page.cursor
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.records.sync(
                stream_id=stream_id,
                initialize_cursor=initialize_cursor,
                filter=filter,
                sources=sources,
                target_units=target_units,
                limit=limit,
                include_typing=include_typing,
            )
        )

    def sync_resume(
        self,
        stream_id: str,
        *,
        cursor: str,
        filter: Filter | None = None,
        sources: Sequence[RecordSourceSelector] | None = None,
        target_units: RecordTargetUnits | Sequence[RecordTargetUnit] | None = None,
        limit: int = 10,
        include_typing: bool = False,
    ) -> SyncRecordList:
        """
        Resume syncing records from a stream using a cursor from :meth:`sync` or :meth:`sync_resume`.

        Args:
            stream_id (str): External ID of the stream to sync.
            cursor (str): Resume from a cursor returned by a previous sync call.
            filter (Filter | None): Filter expression (see :mod:`cognite.client.data_classes.filters`).
            sources (Sequence[RecordSourceSelector] | None): Which container properties to return.
            target_units (RecordTargetUnits | Sequence[RecordTargetUnit] | None): Properties to convert
                to another unit.
            limit (int): Maximum number of records to return in this page (1-1000). Defaults to 10.
            include_typing (bool): If True, include property type information on the returned
                list's ``typing`` attribute.

        Returns:
            SyncRecordList: One page of change records, with ``cursor`` and ``has_next`` set.
        """
        return run_sync(
            self.__async_client.data_modeling.records.sync_resume(
                stream_id=stream_id,
                cursor=cursor,
                filter=filter,
                sources=sources,
                target_units=target_units,
                limit=limit,
                include_typing=include_typing,
            )
        )
