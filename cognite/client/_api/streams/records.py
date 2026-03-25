from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cognite.client._api_client import APIClient
from cognite.client.data_classes.streams.stream_record import (
    RecordsAggregateResponse,
    RecordsDeleteResponse,
    RecordsFilterResponse,
    RecordsIngestResponse,
    RecordsSyncResponse,
)
from cognite.client.utils._auxiliary import interpolate_and_url_encode

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class StreamsRecordsAPI(APIClient):
    """ILA record operations under ``/streams/{streamId}/records/...``."""

    _RESOURCE_PATH = "/streams"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)

    def _records_base(self, stream_external_id: str) -> str:
        return interpolate_and_url_encode("/streams/{}/records", stream_external_id)

    def ingest(self, stream_external_id: str, body: dict[str, Any]) -> RecordsIngestResponse:
        """`Ingest records <https://api-docs.cognite.com/20230101/tag/Records/operation/ingestRecords>`_ into a stream."""
        res = self._post(self._records_base(stream_external_id), json=body)
        return RecordsIngestResponse._load(res.json(), cognite_client=self._cognite_client)

    def upsert(self, stream_external_id: str, body: dict[str, Any]) -> RecordsIngestResponse:
        """`Upsert records <https://api-docs.cognite.com/20230101/tag/Records/operation/upsertRecords>`_ in a mutable stream."""
        res = self._post(self._records_base(stream_external_id) + "/upsert", json=body)
        return RecordsIngestResponse._load(res.json(), cognite_client=self._cognite_client)

    def delete(self, stream_external_id: str, body: dict[str, Any]) -> RecordsDeleteResponse:
        """`Delete records <https://api-docs.cognite.com/20230101/tag/Records/operation/deleteRecords>`_ from a mutable stream."""
        res = self._post(self._records_base(stream_external_id) + "/delete", json=body)
        return RecordsDeleteResponse._load(res.json(), cognite_client=self._cognite_client)

    def filter(self, stream_external_id: str, body: dict[str, Any]) -> RecordsFilterResponse:
        """`Filter records <https://api-docs.cognite.com/20230101/tag/Records/operation/filterRecords>`_."""
        res = self._post(self._records_base(stream_external_id) + "/filter", json=body)
        return RecordsFilterResponse._load(res.json(), cognite_client=self._cognite_client)

    def aggregate(self, stream_external_id: str, body: dict[str, Any]) -> RecordsAggregateResponse:
        """`Aggregate over records <https://api-docs.cognite.com/20230101/tag/Records/operation/aggregateRecords>`_."""
        res = self._post(self._records_base(stream_external_id) + "/aggregate", json=body)
        return RecordsAggregateResponse._load(res.json(), cognite_client=self._cognite_client)

    def sync(self, stream_external_id: str, body: dict[str, Any]) -> RecordsSyncResponse:
        """`Sync records <https://api-docs.cognite.com/20230101/tag/Records/operation/syncRecords>`_ (cursor-based read)."""
        res = self._post(self._records_base(stream_external_id) + "/sync", json=body)
        return RecordsSyncResponse._load(res.json(), cognite_client=self._cognite_client)
