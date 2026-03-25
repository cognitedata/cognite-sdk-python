from __future__ import annotations

from collections.abc import MutableSequence
from typing import TYPE_CHECKING, Any

from cognite.client._api_client import APIClient
from cognite.client.utils._auxiliary import interpolate_and_url_encode

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class StreamsAPI(APIClient):
    """ILA Streams and Records APIs under ``/streams`` and ``/streams/{streamId}/records/...``."""

    _RESOURCE_PATH = "/streams"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)

    def _records_base(self, stream_external_id: str) -> str:
        return interpolate_and_url_encode("/streams/{}/records", stream_external_id)

    # --- Streams (operationIds: createStream, listStreams, getStream, deleteStream, deleteStreams) ---

    def create(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        """Create stream(s). Body matches ``{"items": [StreamRequestItem, ...]}``."""
        res = self._post(self._RESOURCE_PATH, json={"items": items})
        return res.json()

    def list(self) -> dict[str, Any]:
        """List streams in the project."""
        res = self._get(self._RESOURCE_PATH)
        return res.json()

    def retrieve(self, stream_external_id: str, include_statistics: bool | None = None) -> dict[str, Any]:
        """Retrieve one stream."""
        path = interpolate_and_url_encode(f"{self._RESOURCE_PATH}/{{}}", stream_external_id)
        params: dict[str, Any] | None = None
        if include_statistics is not None:
            # API expects lowercase "true"/"false" query strings, not Python bool serialization.
            params = {"includeStatistics": "true" if include_statistics else "false"}
        res = self._get(path, params=params)
        return res.json()

    def delete(self, items: MutableSequence[dict[str, Any]]) -> None:
        """Delete streams via ``POST /streams/delete``."""
        self._post(f"{self._RESOURCE_PATH}/delete", json={"items": items})

    def delete_deprecated(self, stream_external_id: str) -> dict[str, Any]:
        """Deprecated ``DELETE /streams/{streamId}`` — API responds with **410**; raises ``CogniteAPIError``."""
        path = interpolate_and_url_encode(f"{self._RESOURCE_PATH}/{{}}", stream_external_id)
        res = self._delete(path)
        return res.json()

    # --- Records (operationIds: ingestRecords, upsertRecords, deleteRecords, filterRecords, aggregateRecords, syncRecords) ---

    def ingest_records(self, stream_external_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """POST ``/streams/{id}/records``."""
        res = self._post(self._records_base(stream_external_id), json=body)
        return res.json()

    def upsert_records(self, stream_external_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """POST ``.../records/upsert``."""
        res = self._post(self._records_base(stream_external_id) + "/upsert", json=body)
        return res.json()

    def delete_records(self, stream_external_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """POST ``.../records/delete``."""
        res = self._post(self._records_base(stream_external_id) + "/delete", json=body)
        return res.json()

    def filter_records(self, stream_external_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """POST ``.../records/filter``."""
        res = self._post(self._records_base(stream_external_id) + "/filter", json=body)
        return res.json()

    def aggregate_records(self, stream_external_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """POST ``.../records/aggregate``."""
        res = self._post(self._records_base(stream_external_id) + "/aggregate", json=body)
        return res.json()

    def sync_records(self, stream_external_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """POST ``.../records/sync``."""
        res = self._post(self._records_base(stream_external_id) + "/sync", json=body)
        return res.json()
