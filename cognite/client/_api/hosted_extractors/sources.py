from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes._base import CogniteResource, PropertySpec
from cognite.client.data_classes.hosted_extractors.sources import Source, SourceList, SourceUpdate, SourceWrite
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SourcesAPI(APIClient):
    _RESOURCE_PATH = "/hostedextractors/sources"

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
    ) -> Iterator[Source]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
    ) -> Iterator[SourceList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[Source] | Iterator[SourceList]:
        """Iterate over sources

        Fetches sources as they are iterated over, so you keep a limited number of sources in memory.

        Args:
            chunk_size (int | None): Number of sources to return in each chunk. Defaults to yielding one source a time.
            limit (int | None): Maximum number of sources to return. Defaults to returning all items.

        Returns:
            Iterator[Source] | Iterator[SourceList]: yields Source one by one if chunk_size is not specified, else SourceList objects.
        """
        self._warning.warn()

        return self._list_generator(
            list_cls=SourceList,
            resource_cls=Source,  # type: ignore[type-abstract]
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            headers={"cdf-version": "beta"},
        )

    def __iter__(self) -> Iterator[Source]:
        """Iterate over sources

        Fetches sources as they are iterated over, so you keep a limited number of sources in memory.

        Returns:
            Iterator[Source]: yields Source one by one.
        """
        return self()

    @overload
    def retrieve(self, external_ids: str, ignore_unknown_ids: bool = False) -> Source: ...

    @overload
    def retrieve(self, external_ids: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> SourceList: ...

    def retrieve(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Source | SourceList:
        """`Retrieve one or more sources. <https://developer.cognite.com/api#tag/Sources/operation/retrieve_sources>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Returns:
            Source | SourceList: Requested sources

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.hosted_extractors.sources.retrieve('myMQTTSource')

            Get multiple sources by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.hosted_extractors.sources.retrieve(["myMQTTSource", "MyEventHubSource"], ignore_unknown_ids=True)

        """
        self._warning.warn()
        return self._retrieve_multiple(
            list_cls=SourceList,
            resource_cls=Source,  # type: ignore[type-abstract]
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            ignore_unknown_ids=ignore_unknown_ids,
            headers={"cdf-version": "beta"},
        )

    def delete(
        self, external_ids: str | SequenceNotStr[str], ignore_unknown_ids: bool = False, force: bool = False
    ) -> None:
        """`Delete one or more sources  <https://developer.cognite.com/api#tag/Sources/operation/delete_sources>`_

        Args:
            external_ids (str | SequenceNotStr[str]): The external ID provided by the client. Must be unique for the resource type.
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.
            force (bool): Delete any jobs associated with each item.
        Examples:

            Delete sources by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.hosted_extractors.sources.delete(["myMQTTSource", "MyEventHubSource"])
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
            headers={"cdf-version": "beta"},
            extra_body_fields=extra_body_fields or None,
        )

    @overload
    def create(self, items: SourceWrite) -> Source: ...

    @overload
    def create(self, items: Sequence[SourceWrite]) -> SourceList: ...

    def create(self, items: SourceWrite | Sequence[SourceWrite]) -> Source | SourceList:
        """`Create one or more sources. <https://developer.cognite.com/api#tag/Sources/operation/create_sources>`_

        Args:
            items (SourceWrite | Sequence[SourceWrite]): Source(s) to create.

        Returns:
            Source | SourceList: Created source(s)

        Examples:

            Create new source:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import EventHubSourceWrite
                >>> client = CogniteClient()
                >>> source = EventHubSourceWrite('my_event_hub', 'http://myeventhub.com', "My EventHub", 'my_key', 'my_value')
                >>> res = client.hosted_extractors.sources.create(source)
        """
        self._warning.warn()
        return self._create_multiple(
            list_cls=SourceList,
            resource_cls=Source,  # type: ignore[type-abstract]
            items=items,  # type: ignore[arg-type]
            input_resource_cls=SourceWrite,
            headers={"cdf-version": "beta"},
        )

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
        """`Update one or more sources. <https://developer.cognite.com/api#tag/Sources/operation/update_sources>`_

        Args:
            items (SourceWrite | SourceUpdate | Sequence[SourceWrite | SourceUpdate]): Source(s) to update.
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (SourceWrite). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Source | SourceList: Updated source(s)

        Examples:

            Update source:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.hosted_extractors import EventHubSourceUpdate
                >>> client = CogniteClient()
                >>> source = EventHubSourceUpdate('my_event_hub').event_hub_name.set("My Updated EventHub")
                >>> res = client.hosted_extractors.sources.update(source)
        """
        self._warning.warn()
        return self._update_multiple(
            items=items,  # type: ignore[arg-type]
            list_cls=SourceList,
            resource_cls=Source,  # type: ignore[type-abstract]
            update_cls=SourceUpdate,
            mode=mode,
            headers={"cdf-version": "beta"},
        )

    @classmethod
    def _convert_resource_to_patch_object(
        cls,
        resource: CogniteResource,
        update_attributes: list[PropertySpec],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> dict[str, dict[str, dict]]:
        output = super()._convert_resource_to_patch_object(resource, update_attributes, mode)
        if hasattr(resource, "_type"):
            output["type"] = resource._type
        return output

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> SourceList:
        """`List sources <https://developer.cognite.com/api#tag/Sources/operation/list_sources>`_

        Args:
            limit (int | None): Maximum number of sources to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            SourceList: List of requested sources

        Examples:

            List sources:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> source_list = client.hosted_extractors.sources.list(limit=5)

            Iterate over sources::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for source in client.hosted_extractors.sources:
                ...     source # do something with the source

            Iterate over chunks of sources to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for source_list in client.hosted_extractors.sources(chunk_size=25):
                ...     source_list # do something with the sources
        """
        self._warning.warn()
        return self._list(
            list_cls=SourceList,
            resource_cls=Source,  # type: ignore[type-abstract]
            method="GET",
            limit=limit,
            headers={"cdf-version": "beta"},
        )
