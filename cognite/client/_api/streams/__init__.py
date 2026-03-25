from __future__ import annotations

from collections.abc import MutableSequence, Sequence
from typing import TYPE_CHECKING, Any

from cognite.client._api.streams.records import StreamsRecordsAPI
from cognite.client._api_client import APIClient
from cognite.client.data_classes.streams.stream import (
    Stream,
    StreamDeleteItem,
    StreamList,
    StreamWrite,
)
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


def _dump_write_item(obj: StreamWrite | dict[str, Any]) -> dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    return obj.dump()


def _dump_delete_item(obj: StreamDeleteItem | dict[str, Any]) -> dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    return obj.dump()


class StreamsAPI(APIClient):
    """ILA Streams API (``/streams``) and nested :class:`StreamsRecordsAPI` (``/streams/{id}/records``)."""

    _RESOURCE_PATH = "/streams"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.records = StreamsRecordsAPI(config, api_version, cognite_client)

    def create(self, items: Sequence[StreamWrite | dict[str, Any]]) -> StreamList:
        """`Create streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/createStream>`_."""
        res = self._post(self._RESOURCE_PATH, json={"items": [_dump_write_item(i) for i in items]})
        return StreamList._load(res.json()["items"], cognite_client=self._cognite_client)

    def list(self) -> StreamList:
        """`List streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/listStreams>`_ in the project."""
        res = self._get(self._RESOURCE_PATH)
        return StreamList._load(res.json()["items"], cognite_client=self._cognite_client)

    def retrieve(self, stream_external_id: str, include_statistics: bool | None = None) -> Stream:
        """`Retrieve a stream <https://api-docs.cognite.com/20230101/tag/Streams/operation/getStream>`_."""
        path = interpolate_and_url_encode(f"{self._RESOURCE_PATH}/{{}}", stream_external_id)
        params: dict[str, Any] | None = None
        if include_statistics is not None:
            params = {"includeStatistics": "true" if include_statistics else "false"}
        res = self._get(path, params=params)
        return Stream._load(res.json(), cognite_client=self._cognite_client)

    def delete(self, items: MutableSequence[StreamDeleteItem | dict[str, Any]]) -> None:
        """`Delete streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/deleteStreams>`_ (POST)."""
        self._post(f"{self._RESOURCE_PATH}/delete", json={"items": [_dump_delete_item(i) for i in items]})

    def delete_deprecated(self, stream_external_id: str) -> dict[str, Any]:
        """Deprecated ``DELETE`` stream — may yield **410**; see API docs."""
        path = interpolate_and_url_encode(f"{self._RESOURCE_PATH}/{{}}", stream_external_id)
        res = self._delete(path)
        return res.json()

    # --- Backwards-compatible names (delegate to :attr:`records`) ---

    def ingest_records(self, stream_external_id: str, body: dict[str, Any]) -> RecordsIngestResponse:
        return self.records.ingest(stream_external_id, body)

    def upsert_records(self, stream_external_id: str, body: dict[str, Any]) -> RecordsIngestResponse:
        return self.records.upsert(stream_external_id, body)

    def delete_records(self, stream_external_id: str, body: dict[str, Any]) -> RecordsDeleteResponse:
        return self.records.delete(stream_external_id, body)

    def filter_records(self, stream_external_id: str, body: dict[str, Any]) -> RecordsFilterResponse:
        return self.records.filter(stream_external_id, body)

    def aggregate_records(self, stream_external_id: str, body: dict[str, Any]) -> RecordsAggregateResponse:
        return self.records.aggregate(stream_external_id, body)

    def sync_records(self, stream_external_id: str, body: dict[str, Any]) -> RecordsSyncResponse:
        return self.records.sync(stream_external_id, body)
