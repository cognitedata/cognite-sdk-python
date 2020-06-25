from typing import Generator, List, Union

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Label, LabelFilter, LabelList


class LabelsAPI(APIClient):
    _RESOURCE_PATH = "/labels"
    _LIST_CLASS = LabelList

    def __iter__(self) -> Generator[Label, None, None]:
        """Iterate over Labels

        Fetches Labels as they are iterated over, so you keep a limited number of Labels in memory.

        Yields:
            Type: yields Labels one by one.
        """
        return self.__call__()

    def __call__(self, name: str = None, external_id_prefix: str = None, limit: int = None, chunk_size: int = None):
        filter = LabelFilter(name=name, external_id_prefix=external_id_prefix).dump(camel_case=True)
        return self._list_generator(method="POST", limit=limit, filter=filter, chunk_size=chunk_size)

    def list(self, name: str = None, external_id_prefix: str = None, limit: int = 25) -> LabelList:
        """`List Labels <https://docs.cognite.com/api/playground/#operation/listLabels>`_

        Args:
            name (str): returns the label definitions matching that name
            external_id_prefix (str): filter external ids starting with the prefix specified
            limit (int, optional): Maximum number of label definitions to return.

        Returns:
            LabelList: List of requested Labels

        Examples:

            List Labels and filter on name::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> label_list = c.labels.list(limit=5, name="name")

            Iterate over label definitions::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for label in c.labels:
                ...     label # do something with the label definition

            Iterate over chunks of label definitions to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for label_list in c.types(chunk_size=2500):
                ...     label_list # do something with the type definitions
        """
        filter = LabelFilter(name=name, external_id_prefix=external_id_prefix).dump(camel_case=True)
        return self._list(method="POST", limit=limit, filter=filter)

    def create(self, label: Union[Label, List[Label]]) -> Union[Label, LabelList]:
        """`Create one or more Labels. <https://docs.cognite.com/api/playground/#operation/createLabelDefinitions>`_

        Args:
            Label (Union[Label, List[Label]]): label definition or list of label definitions to create.

        Returns:
            Union[Label, LabelList]: Created label definition(s)

        Examples:

            Create new label definitions::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Label
                >>> c = CogniteClient()
                >>> labels = [Label(external_id="valve"), Label(external_id="pipe")]
                >>> res = c.labels.create(labels)
        """
        return self._create_multiple(items=label)

    def delete(self, external_id: Union[str, List[str]] = None) -> None:
        """`Delete one or more label definitions <https://docs.cognite.com/api/playground/#operation/deleteLabels>`_

        Args:
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            None

        Examples:

            Delete label definitions by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.labels.delete(external_id=["big_pump", "small_pump"])
        """
        self._delete_multiple(external_ids=external_id, wrap_ids=True)
