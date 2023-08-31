from __future__ import annotations

from typing import Iterator, Sequence, cast

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import LabelDefinition, LabelDefinitionFilter, LabelDefinitionList
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import process_data_set_ids


class LabelsAPI(APIClient):
    _RESOURCE_PATH = "/labels"

    def __iter__(self) -> Iterator[LabelDefinition]:
        """Iterate over Labels

        Fetches Labels as they are iterated over, so you keep a limited number of Labels in memory.

        Returns:
            Iterator[LabelDefinition]: yields Labels one by one.
        """
        return cast(Iterator[LabelDefinition], self())

    def __call__(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        limit: int | None = None,
        chunk_size: int | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
    ) -> Iterator[LabelDefinition] | Iterator[LabelDefinitionList]:
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = LabelDefinitionFilter(
            name=name, external_id_prefix=external_id_prefix, data_set_ids=data_set_ids_processed
        ).dump(camel_case=True)
        return self._list_generator(
            list_cls=LabelDefinitionList,
            resource_cls=LabelDefinition,
            method="POST",
            limit=limit,
            filter=filter,
            chunk_size=chunk_size,
        )

    def list(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> LabelDefinitionList:
        """`List Labels <https://developer.cognite.com/api#tag/Labels/operation/listLabels>`_

        Args:
            name (str | None): returns the label definitions matching that name
            external_id_prefix (str | None): filter label definitions with external ids starting with the prefix specified
            data_set_ids (int | Sequence[int] | None): return only labels in the data sets with this id / these ids.
            data_set_external_ids (str | Sequence[str] | None): return only labels in the data sets with this external id / these external ids.
            limit (int | None): Maximum number of label definitions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            LabelDefinitionList: List of requested Labels

        Examples:

            List Labels and filter on name::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> label_list = c.labels.list(limit=5, name="Pump")

            Iterate over label definitions::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for label in c.labels:
                ...     label # do something with the label definition

            Iterate over chunks of label definitions to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for label_list in c.labels(chunk_size=2500):
                ...     label_list # do something with the type definitions
        """
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = LabelDefinitionFilter(
            name=name, external_id_prefix=external_id_prefix, data_set_ids=data_set_ids_processed
        ).dump(camel_case=True)
        return self._list(
            list_cls=LabelDefinitionList, resource_cls=LabelDefinition, method="POST", limit=limit, filter=filter
        )

    def create(self, label: LabelDefinition | Sequence[LabelDefinition]) -> LabelDefinition | LabelDefinitionList:
        """`Create one or more label definitions. <https://developer.cognite.com/api#tag/Labels/operation/createLabelDefinitions>`_

        Args:
            label (LabelDefinition | Sequence[LabelDefinition]): No description.

        Returns:
            LabelDefinition | LabelDefinitionList: Created label definition(s)

        Raises:
            TypeError: Function input 'label' is of the wrong type

        Examples:

            Create new label definitions::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import LabelDefinition
                >>> c = CogniteClient()
                >>> labels = [LabelDefinition(external_id="ROTATING_EQUIPMENT", name="Rotating equipment"), LabelDefinition(external_id="PUMP", name="pump")]
                >>> res = c.labels.create(labels)
        """
        if isinstance(label, Sequence):
            if len(label) > 0 and not isinstance(label[0], LabelDefinition):
                raise TypeError("'label' must be of type LabelDefinition or Sequence[LabelDefinition]")
        elif not isinstance(label, LabelDefinition):
            raise TypeError("'label' must be of type LabelDefinition or Sequence[LabelDefinition]")
        return self._create_multiple(list_cls=LabelDefinitionList, resource_cls=LabelDefinition, items=label)

    def delete(self, external_id: str | Sequence[str] | None = None) -> None:
        """`Delete one or more label definitions <https://developer.cognite.com/api#tag/Labels/operation/deleteLabels>`_

        Args:
            external_id (str | Sequence[str] | None): One or more label external ids

        Examples:

            Delete label definitions by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.labels.delete(external_id=["big_pump", "small_pump"])
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(external_ids=external_id), wrap_ids=True)
