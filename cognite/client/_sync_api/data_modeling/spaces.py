"""
===============================================================================
9ed46ef4aec8046bc0b80e72b9445c0e
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.spaces import Space, SpaceApply, SpaceList
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSpacesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[Space]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[SpaceList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[Space] | Iterator[SpaceList]:
        """
        Iterate over spaces

        Fetches spaces as they are iterated over, so you keep a limited number of spaces in memory.

        Args:
            chunk_size (int | None): Number of spaces to return in each chunk. Defaults to yielding one space a time.
            limit (int | None): Maximum number of spaces to return. Defaults to returning all items.

        Yields:
            Space | SpaceList: yields Space one by one if chunk_size is not specified, else SpaceList objects.
        """  # noqa: DOC404
        yield from SyncIterator(self.__async_client.data_modeling.spaces(chunk_size=chunk_size, limit=limit))  # type: ignore [misc]

    @overload
    def retrieve(self, spaces: str) -> Space | None: ...

    @overload
    def retrieve(self, spaces: SequenceNotStr[str]) -> SpaceList: ...

    def retrieve(self, spaces: str | SequenceNotStr[str]) -> Space | SpaceList | None:
        """
        `Retrieve one or more spaces. <https://developer.cognite.com/api#tag/Spaces/operation/bySpaceIdsSpaces>`_

        Args:
            spaces (str | SequenceNotStr[str]): Space ID

        Returns:
            Space | SpaceList | None: Requested space or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.spaces.retrieve(spaces='mySpace')

            Get multiple spaces by id:

                >>> res = client.data_modeling.spaces.retrieve(spaces=["MySpace", "MyAwesomeSpace", "MyOtherSpace"])
        """
        return run_sync(self.__async_client.data_modeling.spaces.retrieve(spaces=spaces))

    def delete(self, spaces: str | SequenceNotStr[str]) -> list[str]:
        """
        `Delete one or more spaces <https://developer.cognite.com/api#tag/Spaces/operation/deleteSpacesV3>`_

        Args:
            spaces (str | SequenceNotStr[str]): ID or ID list ids of spaces.
        Returns:
            list[str]: The space(s) which has been deleted.
        Examples:

            Delete spaces by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.data_modeling.spaces.delete(spaces=["mySpace", "myOtherSpace"])
        """
        return run_sync(self.__async_client.data_modeling.spaces.delete(spaces=spaces))

    def list(self, limit: int | None = DEFAULT_LIMIT_READ, include_global: bool = False) -> SpaceList:
        """
        `List spaces <https://developer.cognite.com/api#tag/Spaces/operation/listSpacesV3>`_

        Args:
            limit (int | None): Maximum number of spaces to return. Defaults to 10. Set to -1, float("inf") or None to return all items.
            include_global (bool): Whether to include global spaces. Defaults to False.

        Returns:
            SpaceList: List of requested spaces

        Examples:

            List spaces and filter on max start time:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> space_list = client.data_modeling.spaces.list(limit=5)

            Iterate over spaces, one-by-one:

                >>> for space in client.data_modeling.spaces():
                ...     space  # do something with the space

            Iterate over chunks of spaces to reduce memory load:

                >>> for space_list in client.data_modeling.spaces(chunk_size=2500):
                ...     space_list # do something with the spaces
        """
        return run_sync(self.__async_client.data_modeling.spaces.list(limit=limit, include_global=include_global))

    @overload
    def apply(self, spaces: Sequence[SpaceApply]) -> SpaceList: ...

    @overload
    def apply(self, spaces: SpaceApply) -> Space: ...

    def apply(self, spaces: SpaceApply | Sequence[SpaceApply]) -> Space | SpaceList:
        """
        `Create or patch one or more spaces. <https://developer.cognite.com/api#tag/Spaces/operation/ApplySpaces>`_

        Args:
            spaces (SpaceApply | Sequence[SpaceApply]): Space | Sequence[Space]): Space or spaces of spacesda to create or update.

        Returns:
            Space | SpaceList: Created space(s)

        Examples:

            Create new spaces:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import SpaceApply
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> spaces = [SpaceApply(space="mySpace", description="My first space", name="My Space"),
                ... SpaceApply(space="myOtherSpace", description="My second space", name="My Other Space")]
                >>> res = client.data_modeling.spaces.apply(spaces)
        """
        return run_sync(self.__async_client.data_modeling.spaces.apply(spaces=spaces))
