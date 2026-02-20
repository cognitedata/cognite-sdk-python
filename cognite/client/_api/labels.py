from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    LabelDefinition,
    LabelDefinitionFilter,
    LabelDefinitionList,
    LabelDefinitionWrite,
)
from cognite.client.data_classes.labels import LabelDefinitionCore
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import process_data_set_ids
from cognite.client.utils.useful_types import SequenceNotStr


class LabelsAPI(APIClient):
    _RESOURCE_PATH = "/labels"

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        name: str | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
    ) -> AsyncIterator[LabelDefinition]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        name: str | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
    ) -> AsyncIterator[LabelDefinitionList]: ...

    async def __call__(
        self,
        chunk_size: int | None = None,
        name: str | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
    ) -> AsyncIterator[LabelDefinition] | AsyncIterator[LabelDefinitionList]:
        """Iterate over Labels

        Args:
            chunk_size: Number of Labels to return in each chunk. Defaults to yielding one Label a time.
            name: returns the label definitions matching that name
            external_id_prefix: filter label definitions with external ids starting with the prefix specified
            limit: Maximum number of label definitions to return. Defaults return all labels.
            data_set_ids: return only labels in the data sets with this id / these ids.
            data_set_external_ids: return only labels in the data sets with this external id / these external ids.

        Yields:
            yields Labels one by one or in chunks.
        """  # noqa: DOC404
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        flt = LabelDefinitionFilter(
            name=name, external_id_prefix=external_id_prefix, data_set_ids=data_set_ids_processed
        ).dump(camel_case=True)

        async for item in self._list_generator(
            list_cls=LabelDefinitionList,
            resource_cls=LabelDefinition,
            method="POST",
            limit=limit,
            filter=flt,
            chunk_size=chunk_size,
        ):
            yield item

    @overload
    async def retrieve(self, external_id: str, ignore_unknown_ids: Literal[True]) -> LabelDefinition | None: ...

    @overload
    async def retrieve(self, external_id: str, ignore_unknown_ids: Literal[False] = False) -> LabelDefinition: ...

    @overload
    async def retrieve(
        self, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> LabelDefinitionList: ...

    async def retrieve(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> LabelDefinition | LabelDefinitionList | None:
        """`Retrieve one or more label definitions by external id. <https://developer.cognite.com/api#tag/Labels/operation/byIdsLabels>`_

        Args:
            external_id: External ID or list of external ids
            ignore_unknown_ids: If True, ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            The requested label definition(s)

        Examples:

            Get label by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.labels.retrieve(external_id="my_label", ignore_unknown_ids=True)

        """
        is_single = isinstance(external_id, str)
        external_ids = [external_id] if is_single else external_id
        identifiers = IdentifierSequence.load(external_ids=external_ids)  # type: ignore[arg-type]
        result = await self._retrieve_multiple(
            list_cls=LabelDefinitionList,
            resource_cls=LabelDefinition,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )
        if is_single:
            return result[0] if result else None
        return result

    async def list(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> LabelDefinitionList:
        """`List Labels <https://developer.cognite.com/api#tag/Labels/operation/listLabels>`_

        Args:
            name: returns the label definitions matching that name
            external_id_prefix: filter label definitions with external ids starting with the prefix specified
            data_set_ids: return only labels in the data sets with this id / these ids.
            data_set_external_ids: return only labels in the data sets with this external id / these external ids.
            limit: Maximum number of label definitions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            List of requested Labels

        Examples:

            List Labels and filter on name:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> label_list = client.labels.list(limit=5, name="Pump")

            Iterate over label definitions, one-by-one:

                >>> for label in client.labels():
                ...     label  # do something with the label definition

            Iterate over chunks of label definitions to reduce memory load:

                >>> for label_list in client.labels(chunk_size=2500):
                ...     label_list # do something with the type definitions
        """
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = LabelDefinitionFilter(
            name=name, external_id_prefix=external_id_prefix, data_set_ids=data_set_ids_processed
        ).dump(camel_case=True)
        return await self._list(
            list_cls=LabelDefinitionList, resource_cls=LabelDefinition, method="POST", limit=limit, filter=filter
        )

    @overload
    async def create(self, label: LabelDefinition | LabelDefinitionWrite) -> LabelDefinition: ...

    @overload
    async def create(self, label: Sequence[LabelDefinition | LabelDefinitionWrite]) -> LabelDefinitionList: ...

    async def create(
        self, label: LabelDefinition | LabelDefinitionWrite | Sequence[LabelDefinition | LabelDefinitionWrite]
    ) -> LabelDefinition | LabelDefinitionList:
        """`Create one or more label definitions. <https://developer.cognite.com/api#tag/Labels/operation/createLabelDefinitions>`_

        Args:
            label: The label definition(s) to create.

        Returns:
            Created label definition(s)

        Raises:
            TypeError: Function input 'label' is of the wrong type

        Examples:

            Create new label definitions:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import LabelDefinitionWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> labels = [LabelDefinitionWrite(external_id="ROTATING_EQUIPMENT", name="Rotating equipment"), LabelDefinitionWrite(external_id="PUMP", name="pump")]
                >>> res = client.labels.create(labels)
        """
        if isinstance(label, Sequence):
            if len(label) > 0 and not isinstance(label[0], LabelDefinitionCore):
                raise TypeError("'label' must be of type LabelDefinitionWrite or Sequence[LabelDefinitionWrite]")
        elif not isinstance(label, LabelDefinitionCore):
            raise TypeError("'label' must be of type LabelDefinitionWrite or Sequence[LabelDefinitionWrite]")

        return await self._create_multiple(list_cls=LabelDefinitionList, resource_cls=LabelDefinition, items=label)

    async def delete(self, external_id: str | SequenceNotStr[str] | None = None) -> None:
        """`Delete one or more label definitions <https://developer.cognite.com/api#tag/Labels/operation/deleteLabels>`_

        Args:
            external_id: One or more label external ids

        Examples:

            Delete label definitions by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.labels.delete(external_id=["big_pump", "small_pump"])
        """
        await self._delete_multiple(identifiers=IdentifierSequence.load(external_ids=external_id), wrap_ids=True)
