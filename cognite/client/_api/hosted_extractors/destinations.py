from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.hosted_extractors.destinations import (
    Destination,
    DestinationList,
    DestinationUpdate,
    DestinationWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class DestinationsAPI(APIClient):
    _RESOURCE_PATH = "/hostedextractors/destinations"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="alpha", feature_name="Hosted Extractors"
        )
        self._CREATE_LIMIT = 100
        self._LIST_LIMIT = 100
        self._RETRIEVE_LIMIT = 100
        self._DELETE_LIMIT = 100
        self._UPDATE_LIMIT = 100

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
    ) -> Iterator[Destination]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
    ) -> Iterator[Destination]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[Destination] | Iterator[DestinationList]:
        """Iterate over destinations

        Fetches Destination as they are iterated over, so you keep a limited number of destinations in memory.

        Args:
            chunk_size (int | None): Number of Destinations to return in each chunk. Defaults to yielding one Destination a time.
            limit (int | None): Maximum number of Destination to return. Defaults to returning all items.

        Returns:
            Iterator[Destination] | Iterator[DestinationList]: yields Destination one by one if chunk_size is not specified, else DestinationList objects.
        """
        self._warning.warn()

        return self._list_generator(
            list_cls=DestinationList,
            resource_cls=Destination,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            headers={"cdf-version": "beta"},
        )

    def __iter__(self) -> Iterator[Destination]:
        """Iterate over destinations

        Fetches destinations as they are iterated over, so you keep a limited number of destinations in memory.

        Returns:
            Iterator[Destination]: yields Destination one by one.
        """
        return self()

    @overload
    def retrieve(self, external_ids: str, ignore_unknown_ids: bool = False) -> Destination: ...

    @overload
    def retrieve(self, external_ids: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> DestinationList: ...

    def retrieve(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Destination | DestinationList:
        """`Retrieve one or more destinations. <https://api-docs.cognite.com/20230101-beta/tag/Destinations/operation/retrieve_destinations>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids (bool): Ignore external IDs that are not found


        Returns:
            Destination | DestinationList: Requested destinations

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.hosted_extractors.destinations.retrieve('myDestination')

            Get multiple destinations by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.hosted_extractors.destinations.retrieve(["myDestination", "myDestination2"], ignore_unknown_ids=True)

        """
        self._warning.warn()
        return self._retrieve_multiple(
            list_cls=DestinationList,
            resource_cls=Destination,
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            ignore_unknown_ids=ignore_unknown_ids,
            headers={"cdf-version": "beta"},
        )

    def delete(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False, force: bool = False
    ) -> None:
        """`Delete one or more destsinations <https://api-docs.cognite.com/20230101-beta/tag/Destinations/operation/delete_destinations>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids (bool): Ignore external IDs that are not found
            force (bool): Delete any jobs associated with each item.

        Examples:

            Delete destinations by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.hosted_extractors.destinations.delete(["myDest", "MyDest2"])
        """
        self._warning.warn()
        extra_body_fields: dict[str, Any] = {}
        if ignore_unknown_ids:
            extra_body_fields["ignoreUnknownIds"] = True
        if force:
            extra_body_fields["force"] = True

        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            wrap_ids=True,
            returns_items=False,
            headers={"cdf-version": "beta"},
            extra_body_fields=extra_body_fields or None,
        )

    @overload
    def create(self, items: DestinationWrite) -> Destination: ...

    @overload
    def create(self, items: Sequence[DestinationWrite]) -> DestinationList: ...

    def create(self, items: DestinationWrite | Sequence[DestinationWrite]) -> Destination | DestinationList:
        """`Create one or more destinations. <https://api-docs.cognite.com/20230101-beta/tag/Destinations/operation/create_destinations>`_

        Args:
            items (DestinationWrite | Sequence[DestinationWrite]): Destination(s) to create.

        Returns:
            Destination | DestinationList: Created destination(s)

        Examples:

            Create new destination:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import DestinationWrite, SessionWrite
                >>> client = CogniteClient()
                >>> destination = DestinationWrite(external_id='my_dest', credentials=SessionWrite("my_nonce"), target_data_set_id=123)
                >>> res = client.hosted_extractors.destinations.create(destination)
        """
        self._warning.warn()
        return self._create_multiple(
            list_cls=DestinationList,
            resource_cls=Destination,
            items=items,
            input_resource_cls=DestinationWrite,
            headers={"cdf-version": "beta"},
        )

    @overload
    def update(
        self,
        items: DestinationWrite | DestinationUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Destination: ...

    @overload
    def update(
        self,
        items: Sequence[DestinationWrite | DestinationUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> DestinationList: ...

    def update(
        self,
        items: DestinationWrite | DestinationUpdate | Sequence[DestinationWrite | DestinationUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Destination | DestinationList:
        """`Update one or more destinations. <https://api-docs.cognite.com/20230101-beta/tag/Destinations/operation/update_destinations>`_

        Args:
            items (DestinationWrite | DestinationUpdate | Sequence[DestinationWrite | DestinationUpdate]): Destination(s) to update.
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (DestinationWrite). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Destination | DestinationList: Updated destination(s)

        Examples:

            Update destination:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import DestinationUpdate
                >>> client = CogniteClient()
                >>> destination = DestinationUpdate('my_dest').target_data_set_id.set(123)
                >>> res = client.hosted_extractors.destinations.update(destination)
        """
        self._warning.warn()
        return self._update_multiple(
            items=items,
            list_cls=DestinationList,
            resource_cls=Destination,
            update_cls=DestinationUpdate,
            mode=mode,
            headers={"cdf-version": "beta"},
        )

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> DestinationList:
        """`List destinations <https://api-docs.cognite.com/20230101-beta/tag/Destinations/operation/list_destinations>`_

        Args:
            limit (int | None): Maximum number of destinations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            DestinationList: List of requested destinations

        Examples:

            List destinations:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> destination_list = client.hosted_extractors.destinations.list(limit=5)

            Iterate over destinations::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for destination in client.hosted_extractors.destinations:
                ...     destination # do something with the destination

            Iterate over chunks of destinations to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for destination_list in client.hosted_extractors.destinations(chunk_size=25):
                ...     destination_list # do something with the destinationss
        """
        self._warning.warn()
        return self._list(
            list_cls=DestinationList,
            resource_cls=Destination,
            method="GET",
            limit=limit,
            headers={"cdf-version": "beta"},
        )
