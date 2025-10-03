"""
===============================================================================
b9125b61edebab5a3c8a161728e60608
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Coroutine, Iterator, Sequence
from typing import TYPE_CHECKING, Any, TypeVar, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.hosted_extractors import Mapping, MappingList, MappingUpdate, MappingWrite
from cognite.client.utils._async_helpers import SyncIterator
from cognite.client.utils._concurrency import ConcurrencySettings
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient

_T = TypeVar("_T")


class SyncMappingsAPI:
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client

    def _run_sync(self, coro: Coroutine[Any, Any, _T]) -> _T:
        executor = ConcurrencySettings._get_event_loop_executor()
        return executor.run_coro(coro)

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> AsyncIterator[Mapping]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> AsyncIterator[Mapping]: ...

    def __call__(self, chunk_size: int | None = None, limit: int | None = None) -> Iterator[Mapping | MappingList]:
        """
        Iterate over mappings

        Fetches Mapping as they are iterated over, so you keep a limited number of mappings in memory.

        Args:
            chunk_size (int | None): Number of Mappings to return in each chunk. Defaults to yielding one mapping at a time.
            limit (int | None): Maximum number of mappings to return. Defaults to returning all items.

        Returns:
            Iterator[Mapping | MappingList]: No description.
        Yields:
            Mapping | MappingList: yields Mapping one by one if chunk_size is not specified, else MappingList objects.
        """
        return SyncIterator(self.__async_client.hosted_extractors.mappings(chunk_size=chunk_size, limit=limit))

    @overload
    def retrieve(self, external_ids: str, ignore_unknown_ids: bool = False) -> Mapping: ...

    @overload
    def retrieve(self, external_ids: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> MappingList: ...

    def retrieve(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Mapping | MappingList:
        """
        `Retrieve one or more mappings. <https://api-docs.cognite.com/20230101-beta/tag/Mappings/operation/retrieve_mappings>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids (bool): Ignore external IDs that are not found


        Returns:
            Mapping | MappingList: Requested mappings

        Examples:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.hosted_extractors.mappings.retrieve('myMapping')

            Get multiple mappings by id:

                >>> res = client.hosted_extractors.mappings.retrieve(["myMapping", "myMapping2"], ignore_unknown_ids=True)
        """
        return self._run_sync(
            self.__async_client.hosted_extractors.mappings.retrieve(
                external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def delete(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False, force: bool = False
    ) -> None:
        """
        `Delete one or more mappings  <https://api-docs.cognite.com/20230101-beta/tag/Mappings/operation/delete_mappings>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids (bool): Ignore external IDs that are not found
            force (bool): Delete any jobs associated with each item.

        Examples:

            Delete mappings by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.hosted_extractors.mappings.delete(["myMapping", "MyMapping2"])
        """
        return self._run_sync(
            self.__async_client.hosted_extractors.mappings.delete(
                external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids, force=force
            )
        )

    @overload
    def create(self, items: MappingWrite) -> Mapping: ...

    @overload
    def create(self, items: Sequence[MappingWrite]) -> MappingList: ...

    def create(self, items: MappingWrite | Sequence[MappingWrite]) -> Mapping | MappingList:
        """
        `Create one or more mappings. <https://api-docs.cognite.com/20230101-beta/tag/Mappings/operation/create_mappings>`_

        Args:
            items (MappingWrite | Sequence[MappingWrite]): Mapping(s) to create.

        Returns:
            Mapping | MappingList: Created mapping(s)

        Examples:

            Create new mapping:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import MappingWrite, CustomMapping
                >>> client = CogniteClient()
                >>> mapping = MappingWrite(external_id="my_mapping", mapping=CustomMapping("some expression"), published=True, input="json")
                >>> res = client.hosted_extractors.mappings.create(mapping)
        """
        return self._run_sync(self.__async_client.hosted_extractors.mappings.create(items=items))

    @overload
    def update(self, items: MappingWrite | MappingUpdate) -> Mapping: ...

    @overload
    def update(self, items: Sequence[MappingWrite | MappingUpdate]) -> MappingList: ...

    def update(
        self, items: MappingWrite | MappingUpdate | Sequence[MappingWrite | MappingUpdate]
    ) -> Mapping | MappingList:
        """
        `Update one or more mappings. <https://api-docs.cognite.com/20230101-beta/tag/Mappings/operation/update_mappings>`_

        Args:
            items (MappingWrite | MappingUpdate | Sequence[MappingWrite | MappingUpdate]): Mapping(s) to update.

        Returns:
            Mapping | MappingList: Updated mapping(s)

        Examples:

            Update mapping:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import MappingUpdate
                >>> client = CogniteClient()
                >>> mapping = MappingUpdate('my_mapping').published.set(False)
                >>> res = client.hosted_extractors.mappings.update(mapping)
        """
        return self._run_sync(self.__async_client.hosted_extractors.mappings.update(items=items))

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> MappingList:
        """
        `List mappings <https://api-docs.cognite.com/20230101-beta/tag/Mappings/operation/list_mappings>`_

        Args:
            limit (int | None): Maximum number of mappings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            MappingList: List of requested mappings

        Examples:

            List mappings:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> mapping_list = client.hosted_extractors.mappings.list(limit=5)

            Iterate over mappings, one-by-one:

                >>> for mapping in client.hosted_extractors.mappings():
                ...     mapping  # do something with the mapping

            Iterate over chunks of mappings to reduce memory load:

                >>> for mapping_list in client.hosted_extractors.mappings(chunk_size=25):
                ...     mapping_list # do something with the mappings
        """
        return self._run_sync(self.__async_client.hosted_extractors.mappings.list(limit=limit))
