from typing import *

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    DataSet,
    DataSetAggregate,
    DataSetFilter,
    DataSetList,
    DataSetUpdate,
    TimestampRange,
)


class DataSetsAPI(APIClient):
    _RESOURCE_PATH = "/datasets"
    _LIST_CLASS = DataSetList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._CREATE_LIMIT = 10

    def __call__(
        self,
        chunk_size: int = None,
        metadata: Dict[str, str] = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        write_protected: bool = None,
        limit: int = None,
    ) -> Generator[Union[DataSet, DataSetList], None, None]:
        """Iterate over data sets

        Fetches data sets as they are iterated over, so you keep a limited number of data sets in memory.

        Args:
            chunk_size (int, optional): Number of data sets to return in each chunk. Defaults to yielding one data set a time.
            metadata (Dict[str, str]): Custom, application-specific metadata. String key -> String value.
            created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
            last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
            external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
            write_protected (bool): Specify whether the filtered data sets are write-protected, or not. Set to True to only list write-protected data sets.
            limit (int, optional): Maximum number of data sets to return. Defaults to return all items.

        Yields:
            Union[DataSet, DataSetList]: yields DataSet one by one if chunk is not specified, else DataSetList objects.
        """
        filter = DataSetFilter(
            metadata=metadata,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            write_protected=write_protected,
        ).dump(camel_case=True)
        return self._list_generator(method="POST", chunk_size=chunk_size, filter=filter, limit=limit)

    def __iter__(self) -> Generator[DataSet, None, None]:
        """Iterate over data sets

        Fetches data sets as they are iterated over, so you keep a limited number of data sets in memory.

        Yields:
            Event: yields DataSet one by one.
        """
        return self.__call__()

    def create(self, data_set: Union[DataSet, List[DataSet]]) -> Union[DataSet, DataSetList]:
        """`Create one or more data sets. <https://docs.cognite.com/api/v1/#operation/createDataSets>`_

        Args:
            data_set: Union[DataSet, List[DataSet]]: Data set or list of data sets to create.

        Returns:
            Union[DataSet, DataSetList]: Created data set(s)

        Examples:

            Create new data sets::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataSet
                >>> c = CogniteClient()
                >>> data_sets = [DataSet(name="1st level"), DataSet(name="2nd level")]
                >>> res = c.data_sets.create(data_sets)
        """
        return self._create_multiple(items=data_set)

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[DataSet]:
        """`Retrieve a single data set by id. <https://docs.cognite.com/api/v1/#operation/getDataSets>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[DataSet]: Requested data set or None if it does not exist.

        Examples:

            Get data set by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_sets.retrieve(id=1)

            Get data set by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_sets.retrieve(external_id="1")
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def retrieve_multiple(
        self,
        ids: Optional[List[int]] = None,
        external_ids: Optional[List[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> DataSetList:
        """`Retrieve multiple data sets by id. <https://docs.cognite.com/api/v1/#operation/getDataSets>`_

        Args:
            ids (List[int], optional): IDs
            external_ids (List[str], optional): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            DataSetList: The requested data sets.

        Examples:

            Get data sets by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_sets.retrieve_multiple(ids=[1, 2, 3])

            Get data sets by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.data_sets.retrieve_multiple(external_ids=["abc", "def"], ignore_unknown_ids=True)
        """
        utils._auxiliary.assert_type(ids, "id", [List], allow_none=True)
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=True)
        return self._retrieve_multiple(
            ids=ids, external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids, wrap_ids=True
        )

    def list(
        self,
        metadata: Dict[str, str] = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        write_protected: bool = None,
        limit: int = 25,
    ) -> DataSetList:
        """`List data sets <https://docs.cognite.com/api/v1/#operation/listDataSets>`_

        Args:
            metadata (Dict[str, str]): Custom, application-specific metadata. String key -> String value.
            created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
            last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
            external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
            write_protected (bool): Specify whether the filtered data sets are write-protected, or not. Set to True to only list write-protected data sets.
            limit (int, optional): Maximum number of data sets to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            DataSetList: List of requested data sets

        Examples:

            List data sets and filter on write_protected::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> data_sets_list = c.data_sets.list(limit=5, write_protected=False)

            Iterate over data sets::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for data_set in c.data_sets:
                ...     data_set # do something with the data_set

            Iterate over chunks of data sets to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for data_set_list in c.data_sets(chunk_size=2500):
                ...     data_set_list # do something with the list
        """

        filter = DataSetFilter(
            metadata=metadata,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            write_protected=write_protected,
        ).dump(camel_case=True)
        return self._list(method="POST", limit=limit, filter=filter)

    def aggregate(self, filter: Union[DataSetFilter, Dict] = None) -> List[DataSetAggregate]:
        """`Aggregate data sets <https://docs.cognite.com/api/v1/#operation/aggregateDataSets>`_

        Args:
            filter (Union[DataSetFilter, Dict]): Filter on data set filter with exact match

        Returns:
            List[DataSetAggregate]: List of data set aggregates

        Examples:

            Aggregate data_sets:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> aggregate_protected = c.data_sets.aggregate(filter={"write_protected": True})
        """

        return self._aggregate(filter=filter, cls=DataSetAggregate)

    def update(
        self, item: Union[DataSet, DataSetUpdate, List[Union[DataSet, DataSetUpdate]]]
    ) -> Union[DataSet, DataSetList]:
        """`Update one or more data sets <https://docs.cognite.com/api/v1/#operation/updateDataSets>`_

        Args:
            item: Union[DataSet, DataSetUpdate, List[Union[DataSet, DataSetUpdate]]]: Data set(s) to update

        Returns:
            Union[DataSet, DataSetList]: Updated data set(s)

        Examples:

            Update a data set that you have fetched. This will perform a full update of the data set::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> data_set = c.data_sets.retrieve(id=1)
                >>> data_set.description = "New description"
                >>> res = c.data_sets.update(data_set)

            Perform a partial update on a data set, updating the description and removing a field from metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataSetUpdate
                >>> c = CogniteClient()
                >>> my_update = DataSetUpdate(id=1).description.set("New description").metadata.remove(["key"])
                >>> res = c.data_sets.update(my_update)
        """
        return self._update_multiple(items=item)
