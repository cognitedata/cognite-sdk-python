"""
===============================================================================
1b13cd8cb9a6d5758aee61658b3c8230
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.records import (
    RecordId,
    RecordSourceSelector,
    RecordTargetUnit,
    RecordTargetUnits,
    RecordWrite,
    SyncRecordList,
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

    def sync(
        self,
        stream_id: str,
        *,
        cursor: str | None = None,
        initialize_cursor: str | None = None,
        filter: Filter | None = None,
        sources: Sequence[RecordSourceSelector] | None = None,
        target_units: RecordTargetUnits | Sequence[RecordTargetUnit] | None = None,
        limit: int = 10,
        include_typing: bool = False,
    ) -> SyncRecordList:
        """
        `Sync records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/syncRecords>`_.

        Returns the next page of the change feed (new, updated and deleted records). Provide exactly
        one of ``cursor`` (to resume a previous position) or ``initialize_cursor`` (to start from a
        relative time such as ``"7d-ago"``). Persist the returned :attr:`SyncRecordList.cursor` and
        pass it as ``cursor`` on the next call to continue; :attr:`SyncRecordList.has_next` indicates
        whether more changes are immediately available.

        Args:
            stream_id (str): External ID of the stream to sync.
            cursor (str | None): Resume from a cursor returned by a previous sync call.
            initialize_cursor (str | None): Where to start when no ``cursor`` is given, as a
                relative duration like ``"7d-ago"``. Ignored when ``cursor`` is set.
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
                >>> next_page = client.data_modeling.records.sync(
                ...     stream_id="my-stream", cursor=page.cursor
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.records.sync(
                stream_id=stream_id,
                cursor=cursor,
                initialize_cursor=initialize_cursor,
                filter=filter,
                sources=sources,
                target_units=target_units,
                limit=limit,
                include_typing=include_typing,
            )
        )
