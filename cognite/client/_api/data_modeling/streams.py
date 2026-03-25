from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Literal

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.streams import Stream, StreamList, StreamWrite
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class StreamsAPI(APIClient):
    _RESOURCE_PATH = "/streams"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 1
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Streams API"
        )

    def _get_semaphore(self, operation: Literal["read", "write", "delete"]) -> asyncio.BoundedSemaphore:
        from cognite.client import global_config

        assert operation not in ("read_schema", "write_schema"), "Streams API should not use schema semaphores"
        return global_config.concurrency_settings.data_modeling._semaphore_factory(
            operation, project=self._cognite_client.config.project
        )

    async def retrieve(self, external_id: str, include_statistics: bool = False) -> Stream | None:
        """`Retrieve a stream by external ID. <https://api-docs.cognite.com/20230101/tag/Streams/operation/getStream>`_

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
        self._warning.warn()
        params = {"includeStatistics": "true"} if include_statistics else None
        try:
            result = await self._get(
                url_path=f"{self._RESOURCE_PATH}/{external_id}",
                params=params,
                semaphore=self._get_semaphore("read"),
            )
            return Stream._load(result.json())
        except CogniteAPIError as e:
            if e.code == 404:
                return None
            raise

    async def delete(self, external_id: str) -> None:
        """`Delete a stream <https://api-docs.cognite.com/20230101/tag/Streams/operation/deleteStreams>`_

        Args:
            external_id (str): External ID of stream.

        Examples:

            Delete a stream by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.data_modeling.streams.delete(external_id="my_stream")
        """
        self._warning.warn()
        await self._post(
            url_path=f"{self._RESOURCE_PATH}/delete",
            json={"items": [{"externalId": external_id}]},
            semaphore=self._get_semaphore("write"),
        )

    async def list(self) -> StreamList:
        """`List streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/listStreams>`_

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
        self._warning.warn()
        return await self._list(
            list_cls=StreamList,
            resource_cls=Stream,
            method="GET",
            limit=None,
            override_semaphore=self._get_semaphore("read"),
        )

    async def create(self, stream: StreamWrite) -> Stream:
        """`Create a stream. <https://api-docs.cognite.com/20230101/tag/Streams/operation/createStream>`_

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
        self._warning.warn()
        return await self._create_multiple(
            list_cls=StreamList,
            resource_cls=Stream,
            items=stream,
            input_resource_cls=StreamWrite,
            override_semaphore=self._get_semaphore("write"),
        )
