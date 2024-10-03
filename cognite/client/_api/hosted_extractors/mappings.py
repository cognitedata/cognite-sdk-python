from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.hosted_extractors import (
    Mapping,
    MappingList,
    MappingUpdate,
    MappingWrite,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class MappingsAPI(APIClient):
    _RESOURCE_PATH = "/hostedextractors/mappings"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="alpha", sdk_maturity="alpha", feature_name="Hosted Extractors"
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
    ) -> Iterator[Mapping]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
    ) -> Iterator[Mapping]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[Mapping] | Iterator[MappingList]:
        """Iterate over mappings

        Fetches Mapping as they are iterated over, so you keep a limited number of mappings in memory.

        Args:
            chunk_size (int | None): Number of Mappings to return in each chunk. Defaults to yielding one mapping at a time.
            limit (int | None): Maximum number of mappings to return. Defaults to returning all items.

        Returns:
            Iterator[Mapping] | Iterator[MappingList]: yields Mapping one by one if chunk_size is not specified, else MappingList objects.
        """
        self._warning.warn()

        return self._list_generator(
            list_cls=MappingList,
            resource_cls=Mapping,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            headers={"cdf-version": "beta"},
        )

    def __iter__(self) -> Iterator[Mapping]:
        """Iterate over mappings

        Fetches mappings as they are iterated over, so you keep a limited number of mappings in memory.

        Returns:
            Iterator[Mapping]: yields Mapping one by one.
        """
        return self()

    @overload
    def retrieve(self, external_ids: str, ignore_unknown_ids: bool = False) -> Mapping: ...

    @overload
    def retrieve(self, external_ids: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> MappingList: ...

    def retrieve(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Mapping | MappingList:
        """`Retrieve one or more mappings. <https://api-docs.cognite.com/20230101-beta/tag/Mappings/operation/retrieve_mappings>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids (bool): Ignore external IDs that are not found


        Returns:
            Mapping | MappingList: Requested mappings

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.hosted_extractors.mappings.retrieve('myMapping')

            Get multiple mappings by id:

                >>> res = client.hosted_extractors.mappings.retrieve(["myMapping", "myMapping2"], ignore_unknown_ids=True)

        """
        self._warning.warn()
        return self._retrieve_multiple(
            list_cls=MappingList,
            resource_cls=Mapping,
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            ignore_unknown_ids=ignore_unknown_ids,
            headers={"cdf-version": "beta"},
        )

    def delete(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False, force: bool = False
    ) -> None:
        """`Delete one or more mappings  <https://api-docs.cognite.com/20230101-beta/tag/Mappings/operation/delete_mappings>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids (bool): Ignore external IDs that are not found
            force (bool): Delete any jobs associated with each item.

        Examples:

            Delete mappings by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.hosted_extractors.mappings.delete(["myMapping", "MyMapping2"])
        """
        self._warning.warn()
        extra_body_fields: dict[str, Any] = {
            "ignoreUnknownIds": ignore_unknown_ids,
            "force": force,
        }

        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            wrap_ids=True,
            returns_items=False,
            headers={"cdf-version": "beta"},
            extra_body_fields=extra_body_fields,
        )

    @overload
    def create(self, items: MappingWrite) -> Mapping: ...

    @overload
    def create(self, items: Sequence[MappingWrite]) -> MappingList: ...

    def create(self, items: MappingWrite | Sequence[MappingWrite]) -> Mapping | MappingList:
        """`Create one or more mappings. <https://api-docs.cognite.com/20230101-beta/tag/Mappings/operation/create_mappings>`_

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
        self._warning.warn()
        return self._create_multiple(
            list_cls=MappingList,
            resource_cls=Mapping,
            items=items,
            input_resource_cls=MappingWrite,
            headers={"cdf-version": "beta"},
        )

    @overload
    def update(self, items: MappingWrite | MappingUpdate) -> Mapping: ...

    @overload
    def update(self, items: Sequence[MappingWrite | MappingUpdate]) -> MappingList: ...

    def update(
        self, items: MappingWrite | MappingUpdate | Sequence[MappingWrite | MappingUpdate]
    ) -> Mapping | MappingList:
        """`Update one or more mappings. <https://api-docs.cognite.com/20230101-beta/tag/Mappings/operation/update_mappings>`_

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
        self._warning.warn()
        return self._update_multiple(
            items=items,
            list_cls=MappingList,
            resource_cls=Mapping,
            update_cls=MappingUpdate,
            headers={"cdf-version": "beta"},
        )

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> MappingList:
        """`List mappings <https://api-docs.cognite.com/20230101-beta/tag/Mappings/operation/list_mappings>`_

        Args:
            limit (int | None): Maximum number of mappings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            MappingList: List of requested mappings

        Examples:

            List mappings:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> mapping_list = client.hosted_extractors.mappings.list(limit=5)

            Iterate over mappings::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for mapping in client.hosted_extractors.mappings:
                ...     mapping # do something with the mapping

            Iterate over chunks of mappings to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for mapping_list in client.hosted_extractors.mappings(chunk_size=25):
                ...     mapping_list # do something with the mappings
        """
        self._warning.warn()
        return self._list(
            list_cls=MappingList,
            resource_cls=Mapping,
            method="GET",
            limit=limit,
            headers={"cdf-version": "beta"},
        )
