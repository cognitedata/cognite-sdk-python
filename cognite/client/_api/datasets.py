from typing import *

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Dataset, DatasetFilter, DatasetList, DatasetUpdate, TimestampRange
from cognite.client.utils._experimental_warning import experimental_api


@experimental_api(api_name="Datasets")
class DatasetsAPI(APIClient):
    _RESOURCE_PATH = "/datasets"
    _LIST_CLASS = DatasetList

    def __call__(
        self,
        chunk_size: int = None,
        metadata: Dict[str, str] = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        limit: int = None,
    ) -> Generator[Union[Dataset, DatasetList], None, None]:
        """Iterate over datasets

        Fetches datasets as they are iterated over, so you keep a limited number of datasets in memory.

        Args:
            chunk_size (int, optional): Number of datasets to return in each chunk. Defaults to yielding one dataset a time.
            metadata (Dict[str, str]): Customizable extra data about the dataset. String key -> String value.
            created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps
            last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps
            external_id_prefix (str): External Id provided by client. Should be unique within the project
            limit (int, optional): Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Yields:
            Union[Dataset, DatasetList]: yields dataset one by one if chunk is not specified, else DatasetList objects.
        """
        filter = DatasetFilter(
            metadata=metadata,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)
        return self._list_generator(method="POST", chunk_size=chunk_size, filter=filter, limit=limit)

    def __iter__(self) -> Generator[Dataset, None, None]:
        """Iterate over datasets

        Fetches datasets as they are iterated over, so you keep a limited number of datasets in memory.

        Yields:
            dataset: yields datasets one by one.
        """
        return self.__call__()

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[Dataset]:
        """`Retrieve a single dataset by id. <https://docs.cognite.com/api/playground/#operation/getDatasetByInternalId>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[Dataset]: Requested dataset or None if it does not exist.

        Examples:

            Get dataset by id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datasets.retrieve(id=1)

            Get dataset by external id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datasets.retrieve(external_id="1")
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def retrieve_multiple(
        self, ids: Optional[List[int]] = None, external_ids: Optional[List[str]] = None
    ) -> DatasetList:
        """`Retrieve multiple datasets by id. <https://docs.cognite.com/api/playground/#operation/byIdsDatasets>`_

        Args:
            ids (List[int], optional): IDs
            external_ids (List[str], optional): External IDs

        Returns:
            DatasetList: The requested datasets.

        Examples:

            Get datasets by id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datasets.retrieve_multiple(ids=[1, 2, 3])

            Get datasets by external id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.datasets.retrieve_multiple(external_ids=["abc", "def"])
        """
        utils._auxiliary.assert_type(ids, "id", [List], allow_none=True)
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=True)
        return self._retrieve_multiple(ids=ids, external_ids=external_ids, wrap_ids=True)

    def list(
        self,
        metadata: Dict[str, str] = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        limit: int = 25,
    ) -> DatasetList:
        """`List datasets <https://docs.cognite.com/api/playground/#operation/advancedListDatasets>`_

        Args:
            metadata (Dict[str, str]): Customizable extra data about the dataset. String key -> String value.
            created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
            last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
            external_id_prefix (str): External Id provided by client. Should be unique within the project.
            limit (int, optional): Maximum number of datasets to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            DatasetList: List of requested datasets

        Examples:

            List datasets and filter on max start time::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> file_list = c.datasets.list(limit=5, created_time={"max": 1500000000})

            Iterate over datasets::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> for dataset in c.datasets:
                ...     dataset # do something with the dataset

            Iterate over chunks of datasets to reduce memory load::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> for dataset_list in c.datasets(chunk_size=2500):
                ...     dataset_list # do something with the files
        """
        filter = DatasetFilter(
            metadata=metadata,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)
        return self._list(method="POST", limit=limit, filter=filter)

    def create(self, dataset: Union[Dataset, List[Dataset]]) -> Union[Dataset, DatasetList]:
        """`Create one or more datasets. <https://docs.cognite.com/api/playground/#operation/createDatasets>`_

        Args:
            dataset (Union[Dataset, List[Dataset]]): Dataset or list of datasets to create.

        Returns:
            Union[Dataset, DatasetList]: Created dataset(s)

        Examples:

            Create new datasets::

                >>> from cognite.client.experimental import CogniteClient
                >>> from cognite.client.data_classes import Dataset
                >>> c = CogniteClient()
                >>> datasets = [Dataset(external_id="a"), Dataset(external_id="b")]
                >>> res = c.datasets.create(datasets)
        """
        return self._create_multiple(items=dataset, resource_path=self._RESOURCE_PATH + "/create")

        #    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """`Delete one or more datasets <https://docs.cognite.com/api/playground/#operation/deleteDatasets>`_

        Args:
            id (Union[int, List[int]): Id or list of ids
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            None
        Examples:

            Delete datasets by id or external id::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> c.datasets.delete(id=[1,2,3], external_id="3")
        """

    #        self._delete_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def update(
        self, item: Union[Dataset, DatasetUpdate, List[Union[Dataset, DatasetUpdate]]]
    ) -> Union[Dataset, DatasetList]:
        """`Update one or more datasets <https://docs.cognite.com/api/playground/#operation/updateDatasets>`_

        Args:
            item (Union[Dataset, DatasetUpdate, List[Union[Dataset, DatasetUpdate]]]): Dataset(s) to update

        Returns:
            Union[Dataset, DatasetList]: Updated dataset(s)

        Examples:

            Update a dataset that you have fetched. This will perform a full update of the dataset::

                >>> from cognite.client.experimental import CogniteClient
                >>> c = CogniteClient()
                >>> dataset = c.datasets.retrieve(id=1)
                >>> datasett.description = "New description"
                >>> res = c.datasets.update(dataset)

            Perform a partial update on a dataset, updating the description and adding a new field to metadata::

                >>> from cognite.client.experimental import CogniteClient
                >>> from cognite.client.data_classes import DatasetUpdate
                >>> c = CogniteClient()
                >>> my_update = DatasetUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = c.datasets.update(my_update)
        """
        return self._update_multiple(items=item)
