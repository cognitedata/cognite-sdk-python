from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.streams import (
    Stream,
    StreamList,
    StreamWrite,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._url import interpolate_and_url_encode
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class StreamsAPI(APIClient):
    """Streams API (``/streams``)."""

    _RESOURCE_PATH = "/streams"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 1
        self._DELETE_LIMIT = 1

    @overload
    async def create(self, items: StreamWrite) -> Stream: ...

    @overload
    async def create(self, items: Sequence[StreamWrite]) -> StreamList: ...

    async def create(self, items: StreamWrite | Sequence[StreamWrite]) -> Stream | StreamList:
        """`Create streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/createStream>`_.

        Args:
            items (StreamWrite | Sequence[StreamWrite]): One or more streams to create.

        Returns:
            Stream | StreamList: The created stream or streams.
        """
        return await self._create_multiple(
            items=items,
            list_cls=StreamList,
            resource_cls=Stream,
            input_resource_cls=StreamWrite,
        )

    async def list(self) -> StreamList:
        """`List streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/listStreams>`_ in the project.

        Note:
            There is no paging limit parameter: the endpoint returns all streams in the project
            (projects are expected to have few streams).

        Returns:
            StreamList: The streams in the project.
        """
        res = await self._get(url_path=self._RESOURCE_PATH, semaphore=self._get_semaphore("read"))
        return StreamList._load(res.json()["items"])

    async def retrieve(self, stream_external_id: str, include_statistics: bool | None = None) -> Stream:
        """`Retrieve a stream <https://api-docs.cognite.com/20230101/tag/Streams/operation/getStream>`_.

        Args:
            stream_external_id (str): Stream external id.
            include_statistics (bool | None): When ``True``, the response may include **statistics**. Computing
                statistics can be expensive.

        Returns:
            Stream: The stream metadata (and optionally statistics).
        """
        path = interpolate_and_url_encode(f"{self._RESOURCE_PATH}/{{}}", stream_external_id)
        params: dict[str, bool] | None = None
        if include_statistics is not None:
            params = {"includeStatistics": include_statistics}
        res = await self._get(url_path=path, params=params, semaphore=self._get_semaphore("read"))
        return Stream._load(res.json())

    async def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """`Delete streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/deleteStreams>`_.

        Deletion is a soft delete that retains capacity for an extended period; prefer deleting only
        when necessary.

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external IDs.
        """
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
        )
