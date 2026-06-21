from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.records import (
    RecordId,
    RecordIdSequence,
    RecordSourceSelector,
    RecordTargetUnit,
    RecordTargetUnits,
    RecordWrite,
    SyncRecord,
    SyncRecordList,
)
from cognite.client.data_classes.filters import Filter
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

    _OPERATION_TO_RATE_LIMIT: ClassVar[dict[str, RecordsConcurrencyOperation]] = {
        "read": RecordsConcurrencyOperation.READ,
        "write": RecordsConcurrencyOperation.WRITE,
        "delete": RecordsConcurrencyOperation.WRITE,
    }

    def _get_semaphore(self, operation: Literal["read", "write", "delete"]) -> asyncio.BoundedSemaphore:
        from cognite.client import global_config

        return global_config.concurrency_settings.records._semaphore_factory(
            self._OPERATION_TO_RATE_LIMIT[operation], project=self._cognite_client.config.project
        )

    def _records_url(self, stream_id: str, suffix: str = "") -> str:
        # Encode only stream_id; the suffix is a literal path segment (e.g. "/upsert"),
        # so it must not be percent-encoded.
        return interpolate_and_url_encode("/streams/{}/records", stream_id) + suffix

    @staticmethod
    def _dump_target_units(target_units: RecordTargetUnits | Sequence[RecordTargetUnit]) -> dict[str, Any]:
        if isinstance(target_units, RecordTargetUnits):
            if (target_units.properties is None) == (target_units.unit_system_name is None):
                raise ValueError("Provide exactly one of 'properties' or 'unit_system_name'.")
            return target_units.dump()
        return RecordTargetUnits(properties=target_units).dump()

    async def _sync(
        self,
        stream_id: str,
        *,
        filter: Filter | None = None,
        sources: Sequence[RecordSourceSelector] | None = None,
        target_units: RecordTargetUnits | Sequence[RecordTargetUnit] | None = None,
        limit: int = 10,
        include_typing: bool = False,
        initialize_cursor: str | None = None,
        cursor: str | None = None,
    ) -> SyncRecordList:
        other_params: dict[str, Any] = {}
        if initialize_cursor is not None:
            other_params["initializeCursor"] = initialize_cursor
        if sources is not None:
            other_params["sources"] = [source.dump() for source in sources]
        if target_units is not None:
            other_params["targetUnits"] = self._dump_target_units(target_units)
        if include_typing:
            other_params["includeTyping"] = True

        return await self._list(
            list_cls=SyncRecordList,
            resource_cls=SyncRecord,
            method="POST",
            resource_path=self._records_url(stream_id),
            url_path=self._records_url(stream_id, "/sync"),
            limit=limit,
            filter=filter.dump(camel_case_property=False) if isinstance(filter, Filter) else filter,
            other_params=other_params,
            initial_cursor=cursor,
            settings_forcing_raw_response_loading=["records_sync_cursor"],
            override_semaphore=self._get_semaphore("read"),
        )

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

    async def sync(
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
        """`Sync records from a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/syncRecords>`_.

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
        self._warning.warn()
        return await self._sync(
            stream_id=stream_id,
            initialize_cursor=initialize_cursor,
            limit=limit,
            filter=filter,
            sources=sources,
            target_units=target_units,
            include_typing=include_typing,
        )

    async def sync_resume(
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
        """Resume syncing records from a stream using a cursor from :meth:`sync` or :meth:`sync_resume`.

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
        self._warning.warn()
        return await self._sync(
            stream_id=stream_id,
            cursor=cursor,
            limit=limit,
            filter=filter,
            sources=sources,
            target_units=target_units,
            include_typing=include_typing,
        )
