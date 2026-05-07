from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.streams import (
    Stream,
    StreamList,
    StreamWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class StreamsAPI(APIClient):
    _RESOURCE_PATH = "/streams"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 1
        self._DELETE_LIMIT = 1
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Streams"
        )

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
        self._warning.warn()
        return await self._create_multiple(
            items=items,
            list_cls=StreamList,
            resource_cls=Stream,
            input_resource_cls=StreamWrite,
        )

    async def list(self) -> StreamList:
        """`List streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/listStreams>`_.

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
        self._warning.warn()
        res = await self._get(url_path=self._RESOURCE_PATH, semaphore=self._get_semaphore("read"))
        return StreamList._load(res.json()["items"])

    async def retrieve(self, external_id: str, include_statistics: bool | None = None) -> Stream | None:
        """`Retrieve a stream <https://api-docs.cognite.com/20230101/tag/Streams/operation/getStream>`_.

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
        self._warning.warn()
        return await self._retrieve(
            cls=Stream,
            identifier=Identifier(external_id),
            params={"includeStatistics": include_statistics} if include_statistics is not None else None,
        )

    async def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """`Delete streams <https://api-docs.cognite.com/20230101/tag/Streams/operation/deleteStreams>`_.

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
        self._warning.warn()
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
        )
