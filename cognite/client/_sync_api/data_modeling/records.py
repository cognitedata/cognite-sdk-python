"""
===============================================================================
419d6a4367d905ade2f6d51bbbbf817b
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.records import (
    RecordId,
    RecordsAggregation,
    RecordWrite,
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
        last_updated_time: Mapping[str, Any] | None = None,
        filter: Filter | dict[str, Any] | None = None,
        target_units: Mapping[str, Any] | None = None,
        include_typing: bool = False,
    ) -> RecordsAggregation:
        """
        `Aggregate records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/aggregateRecords>`_.

        Args:
            aggregates (Mapping[str, Any]): Aggregate request tree keyed by client-defined aggregate IDs.
            stream_id (str): External ID of the stream to aggregate from.
            last_updated_time (Mapping[str, Any] | None): Filter records by last-updated time.
                **Required** for immutable streams (must include a lower bound).
            filter (Filter | dict[str, Any] | None): Filter expression.
            target_units (Mapping[str, Any] | None): Unit conversion specification.
            include_typing (bool): Include property type metadata in the response.

        Returns:
            RecordsAggregation: Aggregate results keyed by the requested aggregate IDs.

        Examples:

            Aggregate average temperature:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.records.aggregate(
                ...     stream_id="my-stream",
                ...     aggregates={
                ...         "avg_temperature": {
                ...             "avg": {"property": ["my-space", "sensor", "temperature"]}
                ...         }
                ...     },
                ... )
                >>> res.aggregates["avg_temperature"]["avg"]
                22.5
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
