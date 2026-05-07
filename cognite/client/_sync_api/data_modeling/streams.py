"""
===============================================================================
e9f8107403fdebf35c0af3100888423b
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
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

        Examples:

            Create a single stream from a template:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.streams import (
                ...     StreamWrite,
                ...     StreamTemplate,
                ...     StreamTemplateWriteSettings,
                ... )
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.streams.create(
                ...     StreamWrite(
                ...         external_id="my-stream",
                ...         settings=StreamTemplateWriteSettings(
                ...             template=StreamTemplate(name="ImmutableTestStream"),
                ...         ),
                ...     )
                ... )
        """
        return run_sync(self.__async_client.data_modeling.streams.create(items=items))

    def list(self) -> StreamList:
        """
        `List streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/listStreams>`_.

        Note:
            There is no paging limit parameter: the endpoint returns all streams in the project
            (projects are expected to have few streams).

        Returns:
            StreamList: The streams in the project.

        Examples:

            List all streams in the project:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.streams.list()
        """
        return run_sync(self.__async_client.data_modeling.streams.list())

    def retrieve(self, external_id: str, include_statistics: bool | None = None) -> Stream | None:
        """
        `Retrieve a stream <https://api-docs.cognite.com/20230101/tag/Streams/operation/getStream>`_.

        Args:
            external_id (str): External ID of the stream to retrieve.
            include_statistics (bool | None): When ``True``, usage statistics will be returned together
                with stream settings. Computing statistics can be expensive.

        Returns:
            Stream | None: The stream metadata (and optionally statistics), or ``None`` if not found.

        Examples:

            Retrieve a stream by external ID:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.streams.retrieve("my-stream")

            Retrieve a stream with usage statistics:

                >>> res = client.data_modeling.streams.retrieve(
                ...     "my-stream",
                ...     include_statistics=True,
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.streams.retrieve(
                external_id=external_id, include_statistics=include_statistics
            )
        )

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """
        `Delete streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/deleteStreams>`_.

        Note:
            Deletion is a soft delete that retains capacity for an extended period;
            prefer deleting only when necessary.

        Args:
            external_id (str | SequenceNotStr[str]): External ID or list of external IDs of
                streams to delete.

        Examples:

            Delete a single stream:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.data_modeling.streams.delete("my-stream")

            Delete multiple streams:

                >>> client.data_modeling.streams.delete(["stream-a", "stream-b"])
        """
        return run_sync(self.__async_client.data_modeling.streams.delete(external_id=external_id))
