"""
===============================================================================
682cc895226cf66f89083037d763db10
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.hosted_extractors import Mapping, MappingList, MappingUpdate, MappingWrite
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncMappingsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[Mapping]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[Mapping]: ...

    def __call__(self, chunk_size: int | None = None, limit: int | None = None) -> Iterator[Mapping | MappingList]:
        """
        Iterate over mappings

        Fetches Mapping as they are iterated over, so you keep a limited number of mappings in memory.

        Args:
            chunk_size (int | None): Number of Mappings to return in each chunk. Defaults to yielding one mapping at a time.
            limit (int | None): Maximum number of mappings to return. Defaults to returning all items.

        Yields:
            Mapping | MappingList: yields Mapping one by one if chunk_size is not specified, else MappingList objects.
        """
        yield from SyncIterator(self.__async_client.hosted_extractors.mappings(chunk_size=chunk_size, limit=limit))

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
        return run_sync(
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
        return run_sync(
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
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> mapping = MappingWrite(external_id="my_mapping", mapping=CustomMapping("some expression"), published=True, input="json")
                >>> res = client.hosted_extractors.mappings.create(mapping)
        """
        return run_sync(self.__async_client.hosted_extractors.mappings.create(items=items))

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
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> mapping = MappingUpdate('my_mapping').published.set(False)
                >>> res = client.hosted_extractors.mappings.update(mapping)
        """
        return run_sync(self.__async_client.hosted_extractors.mappings.update(items=items))

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
        return run_sync(self.__async_client.hosted_extractors.mappings.list(limit=limit))
