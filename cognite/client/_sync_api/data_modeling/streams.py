"""
===============================================================================
d5cc9b88dcf564328fbb78370c32cab7
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.streams import Stream, StreamList, StreamWrite
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncStreamsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def retrieve(self, external_id: str, include_statistics: bool = False) -> Stream | None:
        """
        `Retrieve a stream by external ID. <https://api-docs.cognite.com/20230101/tag/Streams/operation/getStream>`_

        Args:
            external_id (str): Stream external ID
            include_statistics (bool): If set to True, usage statistics will be returned together with stream settings. Defaults to False.

        Returns:
            Stream | None: Requested stream or None if it does not exist.

        Examples:

            Retrieve a single stream by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.streams.retrieve(external_id="my_stream")

            Retrieve a stream with statistics:

                >>> res = client.data_modeling.streams.retrieve(
                ...     external_id="my_stream", include_statistics=True
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.streams.retrieve(
                external_id=external_id, include_statistics=include_statistics
            )
        )

    def delete(self, external_id: str) -> None:
        """
        `Delete a stream <https://api-docs.cognite.com/20230101/tag/Streams/operation/deleteStreams>`_

        Args:
            external_id (str): External ID of stream.

        Examples:

            Delete a stream by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.data_modeling.streams.delete(external_id="my_stream")
        """
        return run_sync(self.__async_client.data_modeling.streams.delete(external_id=external_id))

    def list(self) -> StreamList:
        """
        `List streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/listStreams>`_

        Returns all streams available in the project.

        Returns:
            StreamList: List of all streams in the project

        Examples:

            List all streams:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> stream_list = client.data_modeling.streams.list()

            Iterate over the returned list:

                >>> for stream in stream_list:
                ...     stream  # do something with the stream
        """
        return run_sync(self.__async_client.data_modeling.streams.list())

    def create(self, stream: StreamWrite) -> Stream:
        """
        `Create a stream. <https://api-docs.cognite.com/20230101/tag/Streams/operation/createStream>`_

        Args:
            stream (StreamWrite): Stream to create.

        Returns:
            Stream: Created stream

        Examples:

            Create a new stream:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes.data_modeling import (
                ...     StreamWrite,
                ...     StreamWriteSettings,
                ... )
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> stream = StreamWrite(
                ...     external_id="my_stream",
                ...     settings=StreamWriteSettings(template_name="ImmutableTestStream"),
                ... )
                >>> res = client.data_modeling.streams.create(stream)
        """
        return run_sync(self.__async_client.data_modeling.streams.create(stream=stream))
