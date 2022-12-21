from typing import Iterator, Sequence, Union, cast

from cognite.client._api_client import APIClient
from cognite.client.data_classes import LabelDefinition, LabelDefinitionFilter, LabelDefinitionList
from cognite.client.utils._identifier import IdentifierSequence


class LabelsAPI(APIClient):
    _RESOURCE_PATH = "/labels"

    def __iter__(self) -> Iterator[LabelDefinition]:
        """Iterate over Labels

        Fetches Labels as they are iterated over, so you keep a limited number of Labels in memory.

        Yields:
            Type: yields Labels one by one.
        """
        return cast(Iterator[LabelDefinition], self())

    def __call__(
        self,
        name: str = None,
        external_id_prefix: str = None,
        limit: int = None,
        chunk_size: int = None,
        data_set_ids: Sequence[int] = None,
        data_set_external_ids: Sequence[str] = None,
    ) -> Union[Iterator[LabelDefinition], Iterator[LabelDefinitionList]]:
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
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
        name: str = None,
        external_id_prefix: str = None,
        data_set_ids: Sequence[int] = None,
        data_set_external_ids: Sequence[str] = None,
        limit: int = 25,
    ) -> LabelDefinitionList:
        """`List Labels <https://docs.cognite.com/api/v1/#operation/listLabels>`_

        Args:
            name (str): returns the label definitions matching that name
            data_set_ids (Sequence[int]): return only labels in the data sets with these ids.
            data_set_external_ids (Sequence[str]): return only labels in the data sets with these external ids.
            external_id_prefix (str): filter label definitions with external ids starting with the prefix specified
            limit (int, optional): Maximum number of label definitions to return.

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
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = LabelDefinitionFilter(
            name=name, external_id_prefix=external_id_prefix, data_set_ids=data_set_ids_processed
        ).dump(camel_case=True)
        return self._list(
            list_cls=LabelDefinitionList, resource_cls=LabelDefinition, method="POST", limit=limit, filter=filter
        )

    def create(
        self, label: Union[LabelDefinition, Sequence[LabelDefinition]]
    ) -> Union[LabelDefinition, LabelDefinitionList]:
        """`Create one or more label definitions. <https://docs.cognite.com/api/v1/#operation/createLabelDefinitions>`_

        Args:
            Label (Union[LabelDefinition, Sequence[LabelDefinition]]): label definition or a list of label definitions to create.

        Returns:
            Union[LabelDefinition, LabelDefinitionList]: Created label definition(s)

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

    def delete(self, external_id: Union[str, Sequence[str]] = None) -> None:
        """`Delete one or more label definitions <https://docs.cognite.com/api/v1/#operation/deleteLabels>`_

        Args:
            external_id (Union[str, Sequence[str]]): One or more label external ids

        Returns:
            None

        Examples:

            Delete label definitions by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.labels.delete(external_id=["big_pump", "small_pump"])
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(external_ids=external_id), wrap_ids=True)
