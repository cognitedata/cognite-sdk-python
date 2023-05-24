from __future__ import annotations

from typing import Iterator, Optional, Sequence, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import LIST_LIMIT_DEFAULT
from cognite.client.data_classes import Space, SpaceList
from cognite.client.utils._identifier import DataModelingIdentifierSequence


class SpacesAPI(APIClient):
    _RESOURCE_PATH = "/models/spaces"

    def __call__(
        self,
        chunk_size: int = None,
        limit: int = None,
        partitions: int = None,
    ) -> Iterator[Space] | Iterator[SpaceList]:
        """Iterate over spaces

        Fetches spaces as they are iterated over, so you keep a limited number of spaces in memory.

        Args:
            chunk_size (int, optional): Number of spaces to return in each chunk. Defaults to yielding one space a time.
            limit (int, optional): Maximum number of events to return. Default to return all items.
            partitions (int): Retrieve spaces in parallel using this number of workers. Also requires `limit=None` to be passed.

        Yields:
            Union[Space, SpaceList]: yields Space one by one if chunk_size is not specified, else SpaceList objects.
        """
        return self._list_generator(
            list_cls=SpaceList,
            resource_cls=Space,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            partitions=partitions,
        )

    def __iter__(self) -> Iterator[Space]:
        """Iterate over events

        Fetches events as they are iterated over, so you keep a limited number of events in memory.

        Yields:
            Space: yields Spaces one by one.
        """
        return cast(Iterator[Space], self())

    def retrieve(self, space: str) -> Optional[Space]:
        """`Retrieve a single space by id. <https://docs.cognite.com/api/v1/#tag/Spaces/operation/bySpaceIdsSpaces>`_

        Args:
            space (str): Space ID

        Returns:
            Optional[Space]: Requested space or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.models.spaces.retrieve(space='mySpace')

        """
        identifiers = DataModelingIdentifierSequence.load_spaces(spaces=space).as_singleton()
        return self._retrieve_multiple(list_cls=SpaceList, resource_cls=Space, identifiers=identifiers)

    def retrieve_multiple(
        self,
        spaces: Sequence[str],
    ) -> SpaceList:
        """`Retrieve multiple spaces by id. <https://docs.cognite.com/api/v1/#tag/Spaces/operation/bySpaceIdsSpaces>`_

        Args:
            spaces (Sequence[str]): Space IDs.

        Returns:
            SpaceList: The requested spaces.

        Examples:

            Get events by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.models.spaces.retrieve_multiple(spaces=["MySpace", "MyAwesomeSpace", "MyOtherSpace"])

        """
        identifiers = DataModelingIdentifierSequence.load_spaces(spaces=spaces)
        return self._retrieve_multiple(list_cls=SpaceList, resource_cls=Space, identifiers=identifiers)

    def list(
        self,
        partitions: int = None,
        limit: int = LIST_LIMIT_DEFAULT,
    ) -> SpaceList:
        """`List events <https://docs.cognite.com/api/v1/#tag/Spaces/operation/listSpacesV3>`_

        Args:
            partitions (int): Retrieve events in parallel using this number of workers. Also requires `limit=None` to be passed.
            limit (int, optional): Maximum number of events to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            SpaceList: List of requested events

        Examples:

            List events and filter on max start time::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> event_list = c.models.spaces.list(limit=5)

            Iterate over events::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for space in c.models.spaces:
                ...     space # do something with the space

            Iterate over chunks of events to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for space_list in c.events(chunk_size=2500):
                ...     space_list # do something with the spaces
        """
        return self._list(
            list_cls=SpaceList,
            resource_cls=Space,
            method="GET",
            limit=limit,
            partitions=partitions,
        )

    @overload
    def apply(self, space: Sequence[Space]) -> SpaceList:
        ...

    @overload
    def apply(self, space: Space) -> Space:
        ...

    def apply(self, space: Space | Sequence[Space]) -> Space | SpaceList:
        """`Create or update one or more spaces. <https://docs.cognite.com/api/v1/#tag/Spaces/operation/ApplySpaces>`_

        Args:
            space (space: Space | Sequence[Space]): Space or spaces of events to create or update.

        Returns:
            Space | SpaceList: Created space(s)

        Examples:

            Create new events::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Space
                >>> c = CogniteClient()
                >>> spaces = [Space(space="mySpace", description="My first space", name="My Space"),
                ... Space(space="myOtherSpace", description="My second space", name="My Other Space")]
                >>> res = c.models.spaces.create(spaces)
        """
        return self._create_multiple(list_cls=SpaceList, resource_cls=Space, items=space)

    def delete(
        self,
        space: str | Sequence[str],
    ) -> None:
        """`Delete one or more spaces <https://docs.cognite.com/api/v1/#tag/Spaces/operation/deleteSpacesV3>`_

        Args:
            space (str | Sequence[str]): ID or ID list ids of spaces.
        Returns:
            None
        Examples:

            Delete spaces by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.events.delete(space=["mySpace", "myOtherSpace"])
        """
        self._delete_multiple(
            identifiers=DataModelingIdentifierSequence.load_spaces(spaces=space),
            wrap_ids=True,
        )
