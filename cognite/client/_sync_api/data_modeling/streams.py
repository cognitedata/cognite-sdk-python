"""
===============================================================================
60ce0afb3ca022c6b4fc479885bc5668
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.data_modeling.records import SyncStreamsRecordsAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.streams import Stream, StreamList, StreamWrite
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncStreamsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.records = SyncStreamsRecordsAPI(async_client)

    @overload
    def create(self, items: StreamWrite) -> Stream: ...

    @overload
    def create(self, items: Sequence[StreamWrite]) -> StreamList: ...

    def create(self, items: StreamWrite | Sequence[StreamWrite]) -> Stream | StreamList:
        """
        `Create streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/createStream>`_.

        Args:
            items (StreamWrite | Sequence[StreamWrite]): One or more streams to create.

        Returns:
            Stream | StreamList: The created stream or streams.
        """
        return run_sync(self.__async_client.data_modeling.streams.create(items=items))

    def list(self) -> StreamList:
        """
        `List streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/listStreams>`_ in the project.

        Note:
            There is no paging limit parameter: the endpoint returns all streams in the project
            (projects are expected to have few streams).

        Returns:
            StreamList: The streams in the project.
        """
        return run_sync(self.__async_client.data_modeling.streams.list())

    def retrieve(self, stream_external_id: str, include_statistics: bool | None = None) -> Stream:
        """
        `Retrieve a stream <https://api-docs.cognite.com/20230101/tag/Streams/operation/getStream>`_.

        Args:
            stream_external_id (str): Stream external id.
            include_statistics (bool | None): When ``True``, the response may include **statistics**. Computing
                statistics can be expensive.

        Returns:
            Stream: The stream metadata (and optionally statistics).
        """
        return run_sync(
            self.__async_client.data_modeling.streams.retrieve(
                stream_external_id=stream_external_id, include_statistics=include_statistics
            )
        )

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """
        `Delete streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/deleteStreams>`_.

        Deletion is a soft delete that retains capacity for an extended period; prefer deleting only
        when necessary.

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external IDs.
        """
        return run_sync(self.__async_client.data_modeling.streams.delete(external_id=external_id))
