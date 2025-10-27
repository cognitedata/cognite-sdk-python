"""
===============================================================================
98e4f0c8b9c49ed283cc7f11792b2999
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import DataSet, DataSetFilter, DataSetList, DataSetUpdate, DataSetWrite, TimestampRange
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncDataSetsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client

    @overload
    def __call__(self, chunk_size: None = None) -> Iterator[DataSet]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[DataSetList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        metadata: dict[str, str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
        limit: int | None = None,
    ) -> Iterator[DataSet | DataSetList]:
        """
        Iterate over data sets

        Fetches data sets as they are iterated over, so you keep a limited number of data sets in memory.

        Args:
            chunk_size (int | None): Number of data sets to return in each chunk. Defaults to yielding one data set a time.
            metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value.
            created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
            last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
            external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
            write_protected (bool | None): Specify whether the filtered data sets are write-protected, or not. Set to True to only list write-protected data sets.
            limit (int | None): Maximum number of data sets to return. Defaults to return all items.

        Yields:
            DataSet | DataSetList: yields DataSet one by one if chunk is not specified, else DataSetList objects.
        """
        yield from SyncIterator(
            self.__async_client.data_sets(
                chunk_size=chunk_size,
                metadata=metadata,
                created_time=created_time,
                last_updated_time=last_updated_time,
                external_id_prefix=external_id_prefix,
                write_protected=write_protected,
                limit=limit,
            )
        )

    @overload
    def create(self, data_set: Sequence[DataSet] | Sequence[DataSetWrite]) -> DataSetList: ...

    @overload
    def create(self, data_set: DataSet | DataSetWrite) -> DataSet: ...

    def create(
        self, data_set: DataSet | DataSetWrite | Sequence[DataSet] | Sequence[DataSetWrite]
    ) -> DataSet | DataSetList:
        """
        `Create one or more data sets. <https://developer.cognite.com/api#tag/Data-sets/operation/createDataSets>`_

        Args:
            data_set (DataSet | DataSetWrite | Sequence[DataSet] | Sequence[DataSetWrite]): Union[DataSet, Sequence[DataSet]]: Data set or list of data sets to create.

        Returns:
            DataSet | DataSetList: Created data set(s)

        Examples:

            Create new data sets:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import DataSetWrite
                >>> client = CogniteClient()
                >>> data_sets = [DataSetWrite(name="1st level"), DataSetWrite(name="2nd level")]
                >>> res = client.data_sets.create(data_sets)
        """
        return run_sync(self.__async_client.data_sets.create(data_set=data_set))

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> DataSet | None:
        """
        `Retrieve a single data set by id. <https://developer.cognite.com/api#tag/Data-sets/operation/getDataSets>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            DataSet | None: Requested data set or None if it does not exist.

        Examples:

            Get data set by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_sets.retrieve(id=1)

            Get data set by external id:

                >>> res = client.data_sets.retrieve(external_id="1")
        """
        return run_sync(self.__async_client.data_sets.retrieve(id=id, external_id=external_id))

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> DataSetList:
        """
        `Retrieve multiple data sets by id. <https://developer.cognite.com/api#tag/Data-sets/operation/getDataSets>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            DataSetList: The requested data sets.

        Examples:

            Get data sets by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_sets.retrieve_multiple(ids=[1, 2, 3])

            Get data sets by external id:

                >>> res = client.data_sets.retrieve_multiple(external_ids=["abc", "def"], ignore_unknown_ids=True)
        """
        return run_sync(
            self.__async_client.data_sets.retrieve_multiple(
                ids=ids, external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def aggregate_count(self, filter: DataSetFilter | dict[str, Any] | None = None) -> int:
        """
        `Aggregate data sets <https://developer.cognite.com/api#tag/Data-sets/operation/aggregateDataSets>`_

        Args:
            filter (DataSetFilter | dict[str, Any] | None): Filter on data set filter with exact match

        Returns:
            int: Count of data sets matching the filter.

        Examples:

            Get the number of write-protected data sets:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> aggregate_protected = client.data_sets.aggregate_count(
                ...     filter={"write_protected": True}
                ... )
        """
        return run_sync(self.__async_client.data_sets.aggregate_count(filter=filter))

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
        """
        `Update one or more data sets <https://developer.cognite.com/api#tag/Data-sets/operation/updateDataSets>`_

        Args:
            item (DataSet | DataSetWrite | DataSetUpdate | Sequence[DataSet | DataSetWrite | DataSetUpdate]): Data set(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (DataSet or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            DataSet | DataSetList: Updated data set(s)

        Examples:

            Update a data set that you have fetched. This will perform a full update of the data set:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> data_set = client.data_sets.retrieve(id=1)
                >>> data_set.description = "New description"
                >>> res = client.data_sets.update(data_set)

            Perform a partial update on a data set, updating the description and removing a field from metadata:

                >>> from cognite.client.data_classes import DataSetUpdate
                >>> my_update = DataSetUpdate(id=1).description.set("New description").metadata.remove(["key"])
                >>> res = client.data_sets.update(my_update)
        """
        return run_sync(self.__async_client.data_sets.update(item=item, mode=mode))

    def list(
        self,
        metadata: dict[str, str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        write_protected: bool | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> DataSetList:
        """
        `List data sets <https://developer.cognite.com/api#tag/Data-sets/operation/listDataSets>`_

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

            List data sets and filter on write_protected:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> data_sets_list = client.data_sets.list(limit=5, write_protected=False)

            Iterate over data sets, one-by-one:

                >>> for data_set in client.data_sets():
                ...     data_set  # do something with the data set

            Iterate over chunks of data sets to reduce memory load:

                >>> for data_set_list in client.data_sets(chunk_size=2500):
                ...     data_set_list # do something with the list
        """
        return run_sync(
            self.__async_client.data_sets.list(
                metadata=metadata,
                created_time=created_time,
                last_updated_time=last_updated_time,
                external_id_prefix=external_id_prefix,
                write_protected=write_protected,
                limit=limit,
            )
        )
