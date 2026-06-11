from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.instances import InstanceSort
from cognite.client.data_classes.data_modeling.records import (
    RecordId,
    RecordIdSequence,
    RecordList,
    RecordSourceSelector,
    RecordWrite,
    TimeRange,
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
        # Encode only stream_id; the suffix is a literal path segment (e.g. "/filter"),
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

    async def list(
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
        """`Filter records in a stream <https://api-docs.cognite.com/20230101/tag/Records/operation/filterRecords>`_.

        Returns records matching the given filters, sorted by ``lastUpdatedTime`` unless a custom
        ``sort`` is given. This endpoint is not cursor-paged: it returns at most ``limit`` records
        (max 1000). To page over a large time window, issue multiple calls with partitioned
        ``last_updated_time`` ranges.

        Args:
            stream_id (str): External ID of the stream to query.
            last_updated_time (TimeRange | None): Filter by last-updated time. **Required for
                immutable streams** (must include a lower bound).
            filter (Filter | None): Filter expression (see :mod:`cognite.client.data_classes.filters`).
            sources (Sequence[RecordSourceSelector] | None): Which container properties to return.
            sort (Sequence[InstanceSort] | InstanceSort | None): Sort specification(s); up to 5.
            limit (int): Maximum number of records to return (1-1000). Defaults to 10.
            include_typing (bool): If True, include property type information on the returned
                list's ``typing`` attribute.

        Returns:
            RecordList: The matching records.

        Examples:

            List records updated since a given timestamp:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.records import TimeRange
                >>> client = CogniteClient()
                >>> res = client.data_modeling.records.list(
                ...     stream_id="my-stream",
                ...     last_updated_time=TimeRange(gt=1705341600000),
                ...     limit=100,
                ... )
        """
        self._warning.warn()
        body: dict[str, Any] = {"limit": limit}
        if last_updated_time is not None:
            body["lastUpdatedTime"] = last_updated_time.dump()
        if filter is not None:
            body["filter"] = filter.dump()
        if sources is not None:
            body["sources"] = [source.dump() for source in sources]
        if sort is not None:
            sort_list = [sort] if isinstance(sort, InstanceSort) else list(sort)
            body["sort"] = [spec.dump() for spec in sort_list]
        if include_typing:
            body["includeTyping"] = True

        response = await self._post(
            url_path=self._records_url(stream_id, "/filter"),
            json=body,
            semaphore=self._get_semaphore("read"),
        )
        return RecordList._load_raw_api_response([response.json()])
