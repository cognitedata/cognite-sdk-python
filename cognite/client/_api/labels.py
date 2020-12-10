from typing import Generator, List, Union

from cognite.client._api_client import APIClient
from cognite.client.data_classes import LabelDefinition, LabelDefinitionFilter, LabelDefinitionList


class LabelsAPI(APIClient):
    _RESOURCE_PATH = "/labels"
    _LIST_CLASS = LabelDefinitionList

    def __iter__(self) -> Generator[LabelDefinition, None, None]:
        """Iterate over Labels

        Fetches Labels as they are iterated over, so you keep a limited number of Labels in memory.

        Yields:
            Type: yields Labels one by one.
        """
        return self.__call__()

    def __call__(self, name: str = None, external_id_prefix: str = None, limit: int = None, chunk_size: int = None):
        filter = LabelDefinitionFilter(name=name, external_id_prefix=external_id_prefix).dump(camel_case=True)
        return self._list_generator(method="POST", limit=limit, filter=filter, chunk_size=chunk_size)

    def list(self, name: str = None, external_id_prefix: str = None, limit: int = 25) -> LabelDefinitionList:
        """`List Labels <https://docs.cognite.com/api/v1/#operation/listLabels>`_

        Args:
            name (str): returns the label definitions matching that name
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
        filter = LabelDefinitionFilter(name=name, external_id_prefix=external_id_prefix).dump(camel_case=True)
        return self._list(method="POST", limit=limit, filter=filter)

    def create(
        self, label: Union[LabelDefinition, List[LabelDefinition]]
    ) -> Union[LabelDefinition, LabelDefinitionList]:
        """`Create one or more label definitions. <https://docs.cognite.com/api/v1/#operation/createLabelDefinitions>`_

        Args:
            Label (Union[LabelDefinition, List[LabelDefinition]]): label definition or a list of label definitions to create.

        Returns:
            Union[LabelDefinition, List[LabelDefinition]]: Created label definition(s)

        Examples:

            Create new label definitions::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import LabelDefinition
                >>> c = CogniteClient()
                >>> labels = [LabelDefinition(external_id="ROTATING_EQUIPMENT", name="Rotating equipment"), LabelDefinition(external_id="PUMP", name="pump")]
                >>> res = c.labels.create(labels)
        """
        if isinstance(label, list):
            if len(label) > 0 and not isinstance(label[0], LabelDefinition):
                raise TypeError("'label' must be of type List[LabelDefinition]")
        elif not isinstance(label, LabelDefinition):
            raise TypeError("'label' must be of type LabelDefinition")
        return self._create_multiple(items=label)

    def delete(self, external_id: Union[str, List[str]] = None) -> None:
        """`Delete one or more label definitions <https://docs.cognite.com/api/v1/#operation/deleteLabels>`_

        Args:
            external_id (Union[str, List[str]]): One or more label external ids

        Returns:
            None

        Examples:

            Delete label definitions by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.labels.delete(external_id=["big_pump", "small_pump"])
        """
        self._delete_multiple(external_ids=external_id, wrap_ids=True)
