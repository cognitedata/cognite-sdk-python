from __future__ import annotations

from typing import Iterator, Optional, Sequence, cast, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import LIST_LIMIT_DEFAULT
from cognite.client.data_classes import Space, SpaceList
from cognite.client.utils._identifier import IdentifierSequence


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
            method="POST",
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

    def retrieve(self, id: str) -> Optional[Space]:
        """`Retrieve a single space by id. <https://docs.cognite.com/api/v1/#tag/Spaces/operation/bySpaceIdsSpaces>`_

        Args:
            id (int): ID

        Returns:
            Optional[Space]: Requested space or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.models.spaces.retrieve(id='mySpace')

        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=None).as_singleton()
        return self._retrieve_multiple(list_cls=SpaceList, resource_cls=Space, identifiers=identifiers)

    def retrieve_multiple(
        self,
        ids: Sequence[str],
    ) -> SpaceList:
        """`Retrieve multiple spaces by id. <https://docs.cognite.com/api/v1/#tag/Spaces/operation/bySpaceIdsSpaces>`_

        Args:
            ids (Sequence[int]): IDs

        Returns:
            SpaceList: The requested spaces.

        Examples:

            Get events by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.models.spaces.retrieve_multiple(ids=["MySpace", "MyAwesomeSpace", "MyOtherSpace"])

        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=None)
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
            method="POST",
            limit=limit,
            partitions=partitions,
        )

    @overload
    def apply(self, event: Sequence[Space]) -> SpaceList:
        ...

    @overload
    def apply(self, event: Space) -> Space:
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
                >>> spaces = [Space(id="mySpace", description="My first space", name="My Space"),
                ... Space(id="myOtherSpace", description="My second space", name="My Other Space")]
                >>> res = c.models.spaces.create(spaces)
        """
        return self._create_multiple(list_cls=SpaceList, resource_cls=Space, items=space)

    def delete(
        self,
        id: str | Sequence[str],
    ) -> None:
        """`Delete one or more spaces <https://docs.cognite.com/api/v1/#tag/Spaces/operation/deleteSpacesV3>`_

        Args:
            id (Union[int, Sequence[int]): Id or list of ids
        Returns:
            None
        Examples:

            Delete spaces by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.events.delete(id=["mySpace", "myOtherSpace"])
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=None),
            wrap_ids=True,
        )
