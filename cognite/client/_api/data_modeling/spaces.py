from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Literal, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling.ids import _load_space_identifier
from cognite.client.data_classes.data_modeling.spaces import Space, SpaceApply, SpaceList
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class SpacesAPI(APIClient):
    _RESOURCE_PATH = "/models/spaces"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._DELETE_LIMIT = 100
        self._RETRIEVE_LIMIT = 100
        self._CREATE_LIMIT = 100

    def _get_semaphore(self, operation: Literal["read_schema", "write_schema"]) -> asyncio.BoundedSemaphore:
        from cognite.client import global_config

        assert operation in ("read_schema", "write_schema"), "Spaces API should use schema semaphores"
        return global_config.concurrency_settings.data_modeling._semaphore_factory(
            operation, project=self._cognite_client.config.project
        )

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> AsyncIterator[Space]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> AsyncIterator[SpaceList]: ...

    async def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> AsyncIterator[Space] | AsyncIterator[SpaceList]:
        """Iterate over spaces

        Fetches spaces as they are iterated over, so you keep a limited number of spaces in memory.

        Args:
            chunk_size (int | None): Number of spaces to return in each chunk. Defaults to yielding one space a time.
            limit (int | None): Maximum number of spaces to return. Defaults to returning all items.

        Yields:
            Space | SpaceList: yields Space one by one if chunk_size is not specified, else SpaceList objects.
        """  # noqa: DOC404
        async for item in self._list_generator(
            list_cls=SpaceList,
            resource_cls=Space,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            semaphore=self._get_semaphore("read_schema"),
        ):
            yield item

    @overload
    async def retrieve(self, spaces: str) -> Space | None: ...

    @overload
    async def retrieve(self, spaces: SequenceNotStr[str]) -> SpaceList: ...

    async def retrieve(self, spaces: str | SequenceNotStr[str]) -> Space | SpaceList | None:
        """`Retrieve one or more spaces. <https://api-docs.cognite.com/20230101/tag/Spaces/operation/bySpaceIdsSpaces>`_

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
        identifier = _load_space_identifier(spaces)
        return await self._retrieve_multiple(
            list_cls=SpaceList,
            resource_cls=Space,
            identifiers=identifier,
            override_semaphore=self._get_semaphore("read_schema"),
        )

    async def delete(self, spaces: str | SequenceNotStr[str]) -> list[str]:
        """`Delete one or more spaces <https://api-docs.cognite.com/20230101/tag/Spaces/operation/deleteSpacesV3>`_

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
        deleted_spaces = cast(
            list,
            await self._delete_multiple(
                identifiers=_load_space_identifier(spaces),
                wrap_ids=True,
                returns_items=True,
                override_semaphore=self._get_semaphore("write_schema"),
            ),
        )
        return [item["space"] for item in deleted_spaces]

    async def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
        include_global: bool = False,
    ) -> SpaceList:
        """`List spaces <https://api-docs.cognite.com/20230101/tag/Spaces/operation/listSpacesV3>`_

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
        return await self._list(
            list_cls=SpaceList,
            resource_cls=Space,
            method="GET",
            limit=limit,
            other_params={"includeGlobal": include_global},
            override_semaphore=self._get_semaphore("read_schema"),
        )

    @overload
    async def apply(self, spaces: Sequence[SpaceApply]) -> SpaceList: ...

    @overload
    async def apply(self, spaces: SpaceApply) -> Space: ...

    async def apply(self, spaces: SpaceApply | Sequence[SpaceApply]) -> Space | SpaceList:
        """`Create or patch one or more spaces. <https://api-docs.cognite.com/20230101/tag/Spaces/operation/ApplySpaces>`_

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
        return await self._create_multiple(
            list_cls=SpaceList,
            resource_cls=Space,
            items=spaces,
            input_resource_cls=SpaceApply,
            override_semaphore=self._get_semaphore("write_schema"),
        )
