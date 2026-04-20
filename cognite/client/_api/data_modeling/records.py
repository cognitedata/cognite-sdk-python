from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.streams import (
    RecordsAggregateResponse,
    RecordsDeleteResponse,
    RecordsFilterResponse,
    RecordsIngestResponse,
    RecordsSyncResponse,
)
from cognite.client.utils._url import interpolate_and_url_encode

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class StreamsRecordsAPI(APIClient):
    """ILA record operations under ``/streams/{streamId}/records/...``."""

    _RESOURCE_PATH = "/streams"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)

    def _records_base(self, stream_external_id: str) -> str:
        return interpolate_and_url_encode("/streams/{}/records", stream_external_id)

    async def ingest(self, stream_external_id: str, body: dict[str, Any]) -> RecordsIngestResponse:
        """`Ingest records <https://api-docs.cognite.com/20230101/tag/Records/operation/ingestRecords>`_ into a stream."""
        res = await self._post(
            self._records_base(stream_external_id), json=body, semaphore=self._get_semaphore("write")
        )
        return RecordsIngestResponse._load(res.json())

    async def upsert(self, stream_external_id: str, body: dict[str, Any]) -> RecordsIngestResponse:
        """`Upsert records <https://api-docs.cognite.com/20230101/tag/Records/operation/upsertRecords>`_ in a mutable stream."""
        res = await self._post(
            self._records_base(stream_external_id) + "/upsert", json=body, semaphore=self._get_semaphore("write")
        )
        return RecordsIngestResponse._load(res.json())

    async def delete(self, stream_external_id: str, body: dict[str, Any]) -> RecordsDeleteResponse:
        """`Delete records <https://api-docs.cognite.com/20230101/tag/Records/operation/deleteRecords>`_ from a mutable stream."""
        res = await self._post(
            self._records_base(stream_external_id) + "/delete", json=body, semaphore=self._get_semaphore("write")
        )
        return RecordsDeleteResponse._load(res.json())

    async def filter(self, stream_external_id: str, body: dict[str, Any]) -> RecordsFilterResponse:
        """`Filter records <https://api-docs.cognite.com/20230101/tag/Records/operation/filterRecords>`_."""
        res = await self._post(
            self._records_base(stream_external_id) + "/filter", json=body, semaphore=self._get_semaphore("read")
        )
        return RecordsFilterResponse._load(res.json())

    async def aggregate(self, stream_external_id: str, body: dict[str, Any]) -> RecordsAggregateResponse:
        """`Aggregate over records <https://api-docs.cognite.com/20230101/tag/Records/operation/aggregateRecords>`_."""
        res = await self._post(
            self._records_base(stream_external_id) + "/aggregate", json=body, semaphore=self._get_semaphore("read")
        )
        return RecordsAggregateResponse._load(res.json())

    async def sync(self, stream_external_id: str, body: dict[str, Any]) -> RecordsSyncResponse:
        """`Sync records <https://api-docs.cognite.com/20230101/tag/Records/operation/syncRecords>`_ (cursor-based read)."""
        res = await self._post(
            self._records_base(stream_external_id) + "/sync", json=body, semaphore=self._get_semaphore("read")
        )
        return RecordsSyncResponse._load(res.json())

    async def ingest_items(
        self,
        stream_external_id: str,
        items: Sequence[Mapping[str, Any]],
    ) -> RecordsIngestResponse:
        """Ingest records using the ``items`` array shape (1-1000 records per request).

        Each element must match the API ``recordItems`` object (``space``, ``externalId``, ``sources``, ...).
        This is a thin wrapper around :meth:`ingest` that builds ``{"items": [...]}``.
        """
        if not items:
            raise ValueError("ingest_items requires at least one record (API allows 1-1000 items per call).")
        return await self.ingest(stream_external_id, {"items": [dict(i) for i in items]})

    async def upsert_items(
        self,
        stream_external_id: str,
        items: Sequence[Mapping[str, Any]],
    ) -> RecordsIngestResponse:
        """Upsert records using the ``items`` array (mutable streams). Same shape as :meth:`ingest_items`."""
        if not items:
            raise ValueError("upsert_items requires at least one record (API allows 1-1000 items per call).")
        return await self.upsert(stream_external_id, {"items": [dict(i) for i in items]})

    async def delete_items(
        self,
        stream_external_id: str,
        items: Sequence[Mapping[str, Any]],
    ) -> RecordsDeleteResponse:
        """Delete records by identifier (``space`` + ``externalId`` per item). Wrapper for :meth:`delete`."""
        if not items:
            raise ValueError("delete_items requires at least one record identifier.")
        return await self.delete(stream_external_id, {"items": [dict(i) for i in items]})
