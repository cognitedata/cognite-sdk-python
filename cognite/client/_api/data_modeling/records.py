from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.records import RecordId, RecordIdSequence, RecordWrite
from cognite.client.utils._auxiliary import split_into_chunks
from cognite.client.utils._concurrency import RecordsConcurrencyOperation
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._url import interpolate_and_url_encode

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class RecordsAPI(APIClient):
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
        return interpolate_and_url_encode("/streams/{}/records{}", stream_id, suffix)

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
            ignore_unknown_ids (Literal[True]): currently only True is supported

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
            override_semaphore=self._get_semaphore(RecordsConcurrencyOperation.INGEST),
        )

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
        semaphore = self._get_semaphore(RecordsConcurrencyOperation.INGEST)
        for chunk in split_into_chunks(item_list, self._CREATE_LIMIT):
            await self._post(
                url_path=self._records_url(stream_id),
                json={"items": [r.dump() for r in chunk]},
                semaphore=semaphore,
            )
