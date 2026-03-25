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
from cognite.client.utils._url import interpolate_and_url_encode

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
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

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.records = StreamsRecordsAPI(config, api_version, cognite_client)

    async def create(self, items: Sequence[StreamWrite | dict[str, Any]]) -> StreamList:
        """`Create streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/createStream>`_.

        The API accepts **exactly one** stream per request. Pass a single-element sequence.
        Stream creation is rate-limited; avoid issuing many create calls in a tight loop.
        """
        if len(items) != 1:
            raise ValueError("ILA create stream accepts exactly one item; see API documentation.")
        res = await self._post(
            self._RESOURCE_PATH,
            json={"items": [_dump_write_item(i) for i in items]},
            semaphore=self._get_semaphore("write"),
        )
        return StreamList._load(res.json()["items"])

    async def list(self) -> StreamList:
        """`List streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/listStreams>`_ in the project.

        There is no paging limit parameter: the endpoint returns all streams in the project
        (projects are expected to have few streams).
        """
        res = await self._get(url_path=self._RESOURCE_PATH, semaphore=self._get_semaphore("read"))
        return StreamList._load(res.json()["items"])

    async def retrieve(self, stream_external_id: str, include_statistics: bool | None = None) -> Stream:
        """`Retrieve a stream <https://api-docs.cognite.com/20230101/tag/Streams/operation/getStream>`_.

        Args:
            stream_external_id (str): Stream external id.
            include_statistics (bool | None): When ``True``, the response may include **statistics**. Computing
                statistics can be expensive; the list endpoint does not offer this flag for that reason.

        Returns:
            Stream: The stream metadata (and optionally statistics).
        """
        path = interpolate_and_url_encode(f"{self._RESOURCE_PATH}/{{}}", stream_external_id)
        params: dict[str, Any] | None = None
        if include_statistics is not None:
            params = {"includeStatistics": "true" if include_statistics else "false"}
        res = await self._get(url_path=path, params=params, semaphore=self._get_semaphore("read"))
        return Stream._load(res.json())

    async def delete(self, items: MutableSequence[StreamDeleteItem | dict[str, Any]]) -> None:
        """`Delete streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/deleteStreams>`_ (POST).

        The API accepts **exactly one** stream per request. Deletion is soft-delete and retains
        capacity for an extended period; prefer deleting only when necessary.
        """
        if len(items) != 1:
            raise ValueError("ILA delete stream accepts exactly one item; see API documentation.")
        await self._post(
            f"{self._RESOURCE_PATH}/delete",
            json={"items": [_dump_delete_item(i) for i in items]},
            semaphore=self._get_semaphore("write"),
        )
