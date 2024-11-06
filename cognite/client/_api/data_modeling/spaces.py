from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling.ids import _load_space_identifier
from cognite.client.data_classes.data_modeling.spaces import Space, SpaceApply, SpaceList
from cognite.client.utils._concurrency import ConcurrencySettings
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class SpacesAPI(APIClient):
    _RESOURCE_PATH = "/models/spaces"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._DELETE_LIMIT = 100
        self._RETRIEVE_LIMIT = 100
        self._CREATE_LIMIT = 100

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
    ) -> Iterator[Space]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
    ) -> Iterator[SpaceList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[Space] | Iterator[SpaceList]:
        """Iterate over spaces

        Fetches spaces as they are iterated over, so you keep a limited number of spaces in memory.

        Args:
            chunk_size (int | None): Number of spaces to return in each chunk. Defaults to yielding one space a time.
            limit (int | None): Maximum number of spaces to return. Defaults to returning all items.

        Returns:
            Iterator[Space] | Iterator[SpaceList]: yields Space one by one if chunk_size is not specified, else SpaceList objects.
        """
        return self._list_generator(
            list_cls=SpaceList,
            resource_cls=Space,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
        )

    def __iter__(self) -> Iterator[Space]:
        """Iterate over spaces

        Fetches spaces as they are iterated over, so you keep a limited number of spaces in memory.

        Returns:
            Iterator[Space]: yields Spaces one by one.
        """
        return self()

    @overload
    def retrieve(self, spaces: str) -> Space | None: ...

    @overload
    def retrieve(self, spaces: SequenceNotStr[str]) -> SpaceList: ...

    def retrieve(self, spaces: str | SequenceNotStr[str]) -> Space | SpaceList | None:
        """`Retrieve one or more spaces. <https://developer.cognite.com/api#tag/Spaces/operation/bySpaceIdsSpaces>`_

        Args:
            spaces (str | SequenceNotStr[str]): Space ID

        Returns:
            Space | SpaceList | None: Requested space or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.spaces.retrieve(spaces='mySpace')

            Get multiple spaces by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.spaces.retrieve(spaces=["MySpace", "MyAwesomeSpace", "MyOtherSpace"])

        """
        identifier = _load_space_identifier(spaces)
        return self._retrieve_multiple(
            list_cls=SpaceList,
            resource_cls=Space,
            identifiers=identifier,
            executor=ConcurrencySettings.get_data_modeling_executor(),
        )

    def delete(self, spaces: str | SequenceNotStr[str]) -> list[str]:
        """`Delete one or more spaces <https://developer.cognite.com/api#tag/Spaces/operation/deleteSpacesV3>`_

        Args:
            spaces (str | SequenceNotStr[str]): ID or ID list ids of spaces.
        Returns:
            list[str]: The space(s) which has been deleted.
        Examples:

            Delete spaces by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.data_modeling.spaces.delete(spaces=["mySpace", "myOtherSpace"])
        """
        deleted_spaces = cast(
            list,
            self._delete_multiple(
                identifiers=_load_space_identifier(spaces),
                wrap_ids=True,
                returns_items=True,
                executor=ConcurrencySettings.get_data_modeling_executor(),
            ),
        )
        return [item["space"] for item in deleted_spaces]

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
        include_global: bool = False,
    ) -> SpaceList:
        """`List spaces <https://developer.cognite.com/api#tag/Spaces/operation/listSpacesV3>`_

        Args:
            limit (int | None): Maximum number of spaces to return. Defaults to 10. Set to -1, float("inf") or None to return all items.
            include_global (bool): Whether to include global spaces. Defaults to False.

        Returns:
            SpaceList: List of requested spaces

        Examples:

            List spaces and filter on max start time::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> space_list = client.data_modeling.spaces.list(limit=5)

            Iterate over spaces::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for space in client.data_modeling.spaces:
                ...     space # do something with the space

            Iterate over chunks of spaces to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for space_list in client.data_modeling.spaces(chunk_size=2500):
                ...     space_list # do something with the spaces
        """
        return self._list(
            list_cls=SpaceList,
            resource_cls=Space,
            method="GET",
            limit=limit,
            other_params={"includeGlobal": include_global},
        )

    @overload
    def apply(self, spaces: Sequence[SpaceApply]) -> SpaceList: ...

    @overload
    def apply(self, spaces: SpaceApply) -> Space: ...

    def apply(self, spaces: SpaceApply | Sequence[SpaceApply]) -> Space | SpaceList:
        """`Create or patch one or more spaces. <https://developer.cognite.com/api#tag/Spaces/operation/ApplySpaces>`_

        Args:
            spaces (SpaceApply | Sequence[SpaceApply]): Space | Sequence[Space]): Space or spaces of spacesda to create or update.

        Returns:
            Space | SpaceList: Created space(s)

        Examples:

            Create new spaces:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import SpaceApply
                >>> client = CogniteClient()
                >>> spaces = [SpaceApply(space="mySpace", description="My first space", name="My Space"),
                ... SpaceApply(space="myOtherSpace", description="My second space", name="My Other Space")]
                >>> res = client.data_modeling.spaces.apply(spaces)
        """
        return self._create_multiple(
            list_cls=SpaceList,
            resource_cls=Space,
            items=spaces,
            input_resource_cls=SpaceApply,
            executor=ConcurrencySettings.get_data_modeling_executor(),
        )
