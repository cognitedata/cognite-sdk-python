from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    CountAggregate,
    DataSet,
    DataSetFilter,
    DataSetList,
    DataSetUpdate,
    DataSetWrite,
    TimestampRange,
)
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class DataSetsAPI(APIClient):
    _RESOURCE_PATH = "/datasets"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 10

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        metadata: dict[str, str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
        limit: int | None = None,
    ) -> Iterator[DataSet]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        metadata: dict[str, str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
        limit: int | None = None,
    ) -> Iterator[DataSetList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        metadata: dict[str, str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
        limit: int | None = None,
    ) -> Iterator[DataSet] | Iterator[DataSetList]:
        """Iterate over data sets

        Fetches data sets as they are iterated over, so you keep a limited number of data sets in memory.

        Args:
            chunk_size (int | None): Number of data sets to return in each chunk. Defaults to yielding one data set a time.
            metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value.
            created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
            last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
            external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
            write_protected (bool | None): Specify whether the filtered data sets are write-protected, or not. Set to True to only list write-protected data sets.
            limit (int | None): Maximum number of data sets to return. Defaults to return all items.

        Returns:
            Iterator[DataSet] | Iterator[DataSetList]: yields DataSet one by one if chunk is not specified, else DataSetList objects.
        """
        filter = DataSetFilter(
            metadata=metadata,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            write_protected=write_protected,
        ).dump(camel_case=True)
        return self._list_generator(
            list_cls=DataSetList, resource_cls=DataSet, method="POST", chunk_size=chunk_size, filter=filter, limit=limit
        )

    def __iter__(self) -> Iterator[DataSet]:
        """Iterate over data sets

        Fetches data sets as they are iterated over, so you keep a limited number of data sets in memory.

        Returns:
            Iterator[DataSet]: yields DataSet one by one.
        """
        return self()

    @overload
    def create(self, data_set: Sequence[DataSet] | Sequence[DataSetWrite]) -> DataSetList: ...

    @overload
    def create(self, data_set: DataSet | DataSetWrite) -> DataSet: ...

    def create(
        self, data_set: DataSet | DataSetWrite | Sequence[DataSet] | Sequence[DataSetWrite]
    ) -> DataSet | DataSetList:
        """`Create one or more data sets. <https://developer.cognite.com/api#tag/Data-sets/operation/createDataSets>`_

        Args:
            data_set (DataSet | DataSetWrite | Sequence[DataSet] | Sequence[DataSetWrite]): Union[DataSet, Sequence[DataSet]]: Data set or list of data sets to create.

        Returns:
            DataSet | DataSetList: Created data set(s)

        Examples:

            Create new data sets::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataSetWrite
                >>> client = CogniteClient()
                >>> data_sets = [DataSetWrite(name="1st level"), DataSetWrite(name="2nd level")]
                >>> res = client.data_sets.create(data_sets)
        """
        return self._create_multiple(
            list_cls=DataSetList, resource_cls=DataSet, items=data_set, input_resource_cls=DataSetWrite
        )

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> DataSet | None:
        """`Retrieve a single data set by id. <https://developer.cognite.com/api#tag/Data-sets/operation/getDataSets>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            DataSet | None: Requested data set or None if it does not exist.

        Examples:

            Get data set by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_sets.retrieve(id=1)

            Get data set by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_sets.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=DataSetList, resource_cls=DataSet, identifiers=identifiers)

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> DataSetList:
        """`Retrieve multiple data sets by id. <https://developer.cognite.com/api#tag/Data-sets/operation/getDataSets>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            DataSetList: The requested data sets.

        Examples:

            Get data sets by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_sets.retrieve_multiple(ids=[1, 2, 3])

            Get data sets by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_sets.retrieve_multiple(external_ids=["abc", "def"], ignore_unknown_ids=True)
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=DataSetList, resource_cls=DataSet, identifiers=identifiers, ignore_unknown_ids=ignore_unknown_ids
        )

    def aggregate(self, filter: DataSetFilter | dict[str, Any] | None = None) -> list[CountAggregate]:
        """`Aggregate data sets <https://developer.cognite.com/api#tag/Data-sets/operation/aggregateDataSets>`_

        Args:
            filter (DataSetFilter | dict[str, Any] | None): Filter on data set filter with exact match

        Returns:
            list[CountAggregate]: List of data set aggregates

        Examples:

            Aggregate data_sets:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> aggregate_protected = client.data_sets.aggregate(filter={"write_protected": True})
        """

        return self._aggregate(filter=filter, cls=CountAggregate)

    @overload
    def update(
        self,
        item: DataSet | DataSetWrite | DataSetUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> DataSet: ...

    @overload
    def update(
        self,
        item: Sequence[DataSet | DataSetWrite | DataSetUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> DataSetList: ...

    def update(
        self,
        item: DataSet | DataSetWrite | DataSetUpdate | Sequence[DataSet | DataSetWrite | DataSetUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> DataSet | DataSetList:
        """`Update one or more data sets <https://developer.cognite.com/api#tag/Data-sets/operation/updateDataSets>`_

        Args:
            item (DataSet | DataSetWrite | DataSetUpdate | Sequence[DataSet | DataSetWrite | DataSetUpdate]): Data set(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (DataSet or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            DataSet | DataSetList: Updated data set(s)

        Examples:

            Update a data set that you have fetched. This will perform a full update of the data set::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> data_set = client.data_sets.retrieve(id=1)
                >>> data_set.description = "New description"
                >>> res = client.data_sets.update(data_set)

            Perform a partial update on a data set, updating the description and removing a field from metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataSetUpdate
                >>> client = CogniteClient()
                >>> my_update = DataSetUpdate(id=1).description.set("New description").metadata.remove(["key"])
                >>> res = client.data_sets.update(my_update)
        """
        return self._update_multiple(
            list_cls=DataSetList, resource_cls=DataSet, update_cls=DataSetUpdate, items=item, mode=mode
        )

    def list(
        self,
        metadata: dict[str, str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> DataSetList:
        """`List data sets <https://developer.cognite.com/api#tag/Data-sets/operation/listDataSets>`_

        Args:
            metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value.
            created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
            last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
            external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
            write_protected (bool | None): Specify whether the filtered data sets are write-protected, or not. Set to True to only list write-protected data sets.
            limit (int | None): Maximum number of data sets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            DataSetList: List of requested data sets

        Examples:

            List data sets and filter on write_protected::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> data_sets_list = client.data_sets.list(limit=5, write_protected=False)

            Iterate over data sets::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for data_set in client.data_sets:
                ...     data_set # do something with the data_set

            Iterate over chunks of data sets to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for data_set_list in client.data_sets(chunk_size=2500):
                ...     data_set_list # do something with the list
        """

        filter = DataSetFilter(
            metadata=metadata,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            write_protected=write_protected,
        ).dump(camel_case=True)
        return self._list(list_cls=DataSetList, resource_cls=DataSet, method="POST", limit=limit, filter=filter)
