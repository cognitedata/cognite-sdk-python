"""
===============================================================================
badc14c34fd9b89351938c7cc2d98caf
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.streams.stream_record import (
    RecordsAggregateResponse,
    RecordsDeleteResponse,
    RecordsFilterResponse,
    RecordsIngestResponse,
    RecordsSyncResponse,
)
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncStreamsRecordsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def ingest(self, stream_external_id: str, body: dict[str, Any]) -> RecordsIngestResponse:
        """
        `Ingest records <https://api-docs.cognite.com/20230101/tag/Records/operation/ingestRecords>`_ into a stream.
        """
        return run_sync(self.__async_client.streams.records.ingest(stream_external_id=stream_external_id, body=body))

    def upsert(self, stream_external_id: str, body: dict[str, Any]) -> RecordsIngestResponse:
        """
        `Upsert records <https://api-docs.cognite.com/20230101/tag/Records/operation/upsertRecords>`_ in a mutable stream.
        """
        return run_sync(self.__async_client.streams.records.upsert(stream_external_id=stream_external_id, body=body))

    def delete(self, stream_external_id: str, body: dict[str, Any]) -> RecordsDeleteResponse:
        """
        `Delete records <https://api-docs.cognite.com/20230101/tag/Records/operation/deleteRecords>`_ from a mutable stream.
        """
        return run_sync(self.__async_client.streams.records.delete(stream_external_id=stream_external_id, body=body))

    def filter(self, stream_external_id: str, body: dict[str, Any]) -> RecordsFilterResponse:
        """
        `Filter records <https://api-docs.cognite.com/20230101/tag/Records/operation/filterRecords>`_.
        """
        return run_sync(self.__async_client.streams.records.filter(stream_external_id=stream_external_id, body=body))

    def aggregate(self, stream_external_id: str, body: dict[str, Any]) -> RecordsAggregateResponse:
        """
        `Aggregate over records <https://api-docs.cognite.com/20230101/tag/Records/operation/aggregateRecords>`_.
        """
        return run_sync(self.__async_client.streams.records.aggregate(stream_external_id=stream_external_id, body=body))

    def sync(self, stream_external_id: str, body: dict[str, Any]) -> RecordsSyncResponse:
        """
        `Sync records <https://api-docs.cognite.com/20230101/tag/Records/operation/syncRecords>`_ (cursor-based read).
        """
        return run_sync(self.__async_client.streams.records.sync(stream_external_id=stream_external_id, body=body))
