"""
===============================================================================
5eacaa7290d67a35f580b40c4caf2cbe
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.hosted_extractors.sources import Source, SourceList, SourceUpdate, SourceWrite
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSourcesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[Source]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[SourceList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[Source] | Iterator[SourceList]:
        """
        Iterate over sources

        Fetches sources as they are iterated over, so you keep a limited number of sources in memory.

        Args:
            chunk_size: Number of sources to return in each chunk. Defaults to yielding one source a time.
            limit: Maximum number of sources to return. Defaults to returning all items.

        Yields:
            yields Source one by one if chunk_size is not specified, else SourceList objects.
        """  # noqa: DOC404
        yield from SyncIterator(self.__async_client.hosted_extractors.sources(chunk_size=chunk_size, limit=limit))  # type: ignore [misc]

    @overload
    def retrieve(self, external_ids: str, ignore_unknown_ids: bool = False) -> Source: ...

    @overload
    def retrieve(self, external_ids: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> SourceList: ...

    def retrieve(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Source | SourceList:
        """
        `Retrieve one or more sources. <https://developer.cognite.com/api#tag/Sources/operation/retrieve_sources>`_

        Args:
            external_ids: The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids: Ignore external IDs that are not found rather than throw an exception.

        Returns:
            Requested sources

        Examples:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.hosted_extractors.sources.retrieve('myMQTTSource')

            Get multiple sources by id:

                >>> res = client.hosted_extractors.sources.retrieve(["myMQTTSource", "MyEventHubSource"], ignore_unknown_ids=True)
        """
        return run_sync(
            self.__async_client.hosted_extractors.sources.retrieve(
                external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def delete(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False, force: bool = False
    ) -> None:
        """
        `Delete one or more sources  <https://developer.cognite.com/api#tag/Sources/operation/delete_sources>`_

        Args:
            external_ids: The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids: Ignore external IDs that are not found rather than throw an exception.
            force: Delete any jobs associated with each item.
        Examples:

            Delete sources by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.hosted_extractors.sources.delete(["myMQTTSource", "MyEventHubSource"])
        """
        return run_sync(
            self.__async_client.hosted_extractors.sources.delete(
                external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids, force=force
            )
        )

    @overload
    def create(self, items: SourceWrite) -> Source: ...

    @overload
    def create(self, items: Sequence[SourceWrite]) -> SourceList: ...

    def create(self, items: SourceWrite | Sequence[SourceWrite]) -> Source | SourceList:
        """
        `Create one or more sources. <https://developer.cognite.com/api#tag/Sources/operation/create_sources>`_

        Args:
            items: Source(s) to create.

        Returns:
            Created source(s)

        Examples:

            Create new source:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import EventHubSourceWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> source = EventHubSourceWrite('my_event_hub', 'http://myeventhub.com', "My EventHub", 'my_key', 'my_value')
                >>> res = client.hosted_extractors.sources.create(source)
        """
        return run_sync(self.__async_client.hosted_extractors.sources.create(items=items))

    @overload
    def update(
        self,
        items: SourceWrite | SourceUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Source: ...

    @overload
    def update(
        self,
        items: Sequence[SourceWrite | SourceUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> SourceList: ...

    def update(
        self,
        items: SourceWrite | SourceUpdate | Sequence[SourceWrite | SourceUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Source | SourceList:
        """
        `Update one or more sources. <https://developer.cognite.com/api#tag/Sources/operation/update_sources>`_

        Args:
            items: Source(s) to update.
            mode: How to update data when a non-update object is given (SourceWrite). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Updated source(s)

        Examples:

            Update source:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import EventHubSourceUpdate
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> source = EventHubSourceUpdate('my_event_hub').event_hub_name.set("My Updated EventHub")
                >>> res = client.hosted_extractors.sources.update(source)
        """
        return run_sync(self.__async_client.hosted_extractors.sources.update(items=items, mode=mode))

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> SourceList:
        """
        `List sources <https://developer.cognite.com/api#tag/Sources/operation/list_sources>`_

        Args:
            limit: Maximum number of sources to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            List of requested sources

        Examples:

            List sources:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> source_list = client.hosted_extractors.sources.list(limit=5)

            Iterate over sources, one-by-one:

                >>> for source in client.hosted_extractors.sources():
                ...     source  # do something with the source

            Iterate over chunks of sources to reduce memory load:

                >>> for source_list in client.hosted_extractors.sources(chunk_size=25):
                ...     source_list # do something with the sources
        """
        return run_sync(self.__async_client.hosted_extractors.sources.list(limit=limit))
