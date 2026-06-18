from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.records import RecordId, RecordIdSequence, RecordWrite
from cognite.client.utils._concurrency import HierarchicalBoundedSemaphore, RecordsConcurrencyOperation
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._url import interpolate_and_url_encode

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig

StreamType = Literal["mutable", "immutable"]


class RecordsAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Records"
        )

    def _get_semaphore(  # type: ignore[override]
        self,
        operation: Literal["write", "delete", "query", "retrieve", "aggregate"],
        stream_type: StreamType = "immutable",
    ) -> asyncio.BoundedSemaphore | HierarchicalBoundedSemaphore:
        from cognite.client import global_config

        write_op = RecordsConcurrencyOperation.WRITE
        query_op = (
            RecordsConcurrencyOperation.QUERY_MUTABLE
            if stream_type == "mutable"
            else RecordsConcurrencyOperation.QUERY_IMMUTABLE
        )

        factory = global_config.concurrency_settings.records._semaphore_factory
        project = self._cognite_client.config.project
        match operation:
            case "write" | "delete":
                return factory(write_op, project)
            case "query":
                return factory(query_op, project)
            case "retrieve":
                dedicated_op = (
                    RecordsConcurrencyOperation.RETRIEVE_MUTABLE
                    if stream_type == "mutable"
                    else RecordsConcurrencyOperation.RETRIEVE_IMMUTABLE
                )
                return HierarchicalBoundedSemaphore(factory(dedicated_op, project), factory(query_op, project))
            case "aggregate":
                dedicated_op = (
                    RecordsConcurrencyOperation.AGGREGATE_MUTABLE
                    if stream_type == "mutable"
                    else RecordsConcurrencyOperation.AGGREGATE_IMMUTABLE
                )
                return HierarchicalBoundedSemaphore(factory(dedicated_op, project), factory(query_op, project))

    def _records_url(self, stream_id: str, suffix: str = "") -> str:
        # Encode only stream_id; the suffix is a literal path segment (e.g. "/upsert"),
        # so it must not be percent-encoded.
        return interpolate_and_url_encode("/streams/{}/records", stream_id) + suffix

    async def delete(
        self,
        items: RecordId | Sequence[RecordId],
        *,
        stream_id: str,
        ignore_unknown_ids: Literal[True] = True,
    ) -> None:
        """`Delete records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/deleteRecords>`_.

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
        self._warning.warn()
        await self._delete_multiple(
            identifiers=RecordIdSequence.load(items),
            wrap_ids=True,
            resource_path=self._records_url(stream_id),
        )

    async def ingest(
        self,
        items: RecordWrite | Sequence[RecordWrite],
        *,
        stream_id: str,
    ) -> None:
        """`Ingest records into a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/ingestRecords>`_.

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
        self._warning.warn()
        item_list: list[RecordWrite] = [items] if isinstance(items, RecordWrite) else list(items)
        await self._create_multiple(
            items=item_list,
            resource_path=self._records_url(stream_id),
            no_response=True,
        )

    async def upsert(
        self,
        items: RecordWrite | Sequence[RecordWrite],
        *,
        stream_id: str,
        upsert_mode: Literal["replace"] = "replace",
    ) -> None:
        """`Upsert records into a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/upsertRecords>`_.

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
        self._warning.warn()
        item_list: list[RecordWrite] = [items] if isinstance(items, RecordWrite) else list(items)
        await self._create_multiple(
            items=item_list,
            resource_path=self._records_url(stream_id, "/upsert"),
            no_response=True,
        )
