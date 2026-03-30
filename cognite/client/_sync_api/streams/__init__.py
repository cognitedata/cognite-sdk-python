"""
===============================================================================
f780e4edf88535a6df4726a0958d12a7
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import MutableSequence, Sequence
from typing import Any

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.streams.records import SyncStreamsRecordsAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.streams.stream import Stream, StreamDeleteItem, StreamList, StreamWrite
from cognite.client.utils._async_helpers import run_sync


class SyncStreamsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.records = SyncStreamsRecordsAPI(async_client)

    def create(self, items: Sequence[StreamWrite | dict[str, Any]]) -> StreamList:
        """
        `Create streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/createStream>`_.

        The API accepts **exactly one** stream per request. Pass a single-element sequence.
        Stream creation is rate-limited; avoid issuing many create calls in a tight loop.
        """
        return run_sync(self.__async_client.streams.create(items=items))

    def list(self) -> StreamList:
        """
        `List streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/listStreams>`_ in the project.

        There is no paging limit parameter: the endpoint returns all streams in the project
        (projects are expected to have few streams).
        """
        return run_sync(self.__async_client.streams.list())

    def retrieve(self, stream_external_id: str, include_statistics: bool | None = None) -> Stream:
        """
        `Retrieve a stream <https://api-docs.cognite.com/20230101/tag/Streams/operation/getStream>`_.

        Args:
            stream_external_id (str): Stream external id.
            include_statistics (bool | None): When ``True``, the response may include **statistics**. Computing
                statistics can be expensive; the list endpoint does not offer this flag for that reason.

        Returns:
            Stream: The stream metadata (and optionally statistics).
        """
        return run_sync(
            self.__async_client.streams.retrieve(
                stream_external_id=stream_external_id, include_statistics=include_statistics
            )
        )

    def delete(self, items: MutableSequence[StreamDeleteItem | dict[str, Any]]) -> None:
        """
        `Delete streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/deleteStreams>`_ (POST).

        The API accepts **exactly one** stream per request. Deletion is soft-delete and retains
        capacity for an extended period; prefer deleting only when necessary.
        """
        return run_sync(self.__async_client.streams.delete(items=items))
