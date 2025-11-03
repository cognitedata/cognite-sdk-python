"""
===============================================================================
1413f275800a9fcd1815c2b7301ba7bc
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

import typing
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._api.sequences import SortSpec
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api.sequence_data import SyncSequencesDataAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import Sequence, SequenceFilter, SequenceList, SequenceUpdate
from cognite.client.data_classes.aggregations import AggregationFilter, UniqueResultList
from cognite.client.data_classes.filters import Filter
from cognite.client.data_classes.sequences import (
    SequenceProperty,
    SequenceWrite,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSequencesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.data = SyncSequencesDataAPI(async_client)

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        name: str | None = None,
        external_id_prefix: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: typing.Sequence[int] | None = None,
        asset_subtree_ids: int | typing.Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | typing.Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | None = None,
        limit: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> Iterator[Sequence]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        name: str | None = None,
        external_id_prefix: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: typing.Sequence[int] | None = None,
        asset_subtree_ids: int | typing.Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | typing.Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | None = None,
        limit: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> Iterator[SequenceList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        name: str | None = None,
        external_id_prefix: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: typing.Sequence[int] | None = None,
        asset_subtree_ids: int | typing.Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | typing.Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | None = None,
        last_updated_time: dict[str, Any] | None = None,
        limit: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> Iterator[Sequence | SequenceList]:
        """
        Iterate over sequences

        Fetches sequences as they are iterated over, so you keep a limited number of objects in memory.

        Args:
            chunk_size (int | None): Number of sequences to return in each chunk. Defaults to yielding one event a time.
            name (str | None): Filter out sequences that do not have this *exact* name.
            external_id_prefix (str | None): Filter out sequences that do not have this string as the start of the externalId
            metadata (dict[str, str] | None): Filter out sequences that do not match these metadata fields and values (case-sensitive). Format is {"key1":"value1","key2":"value2"}.
            asset_ids (typing.Sequence[int] | None): Filter out sequences that are not linked to any of these assets.
            asset_subtree_ids (int | typing.Sequence[int] | None): Only include sequences that have a related asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include sequences that have a related asset in a subtree rooted at any of these assetExternalIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            data_set_ids (int | typing.Sequence[int] | None): Return only sequences in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only sequences in the specified data set(s) with this external id / these external ids.
            created_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int | None): Max number of sequences to return. Defaults to return all items.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL (Domain Specific Language). It allows defining complex filtering expressions that combine simple operations, such as equals, prefix, exists, etc., using boolean operators and, or, and not.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Defaults to desc for `_score_` and asc for all other properties. Sort is not allowed if `partitions` is used.

        Yields:
            Sequence | SequenceList: yields Sequence one by one if chunk_size is not specified, else SequenceList objects.
        """
        yield from SyncIterator(
            self.__async_client.sequences(
                chunk_size=chunk_size,
                name=name,
                external_id_prefix=external_id_prefix,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                created_time=created_time,
                last_updated_time=last_updated_time,
                limit=limit,
                advanced_filter=advanced_filter,
                sort=sort,
            )
        )  # type: ignore [misc]

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> Sequence | None:
        """
        `Retrieve a single sequence by id. <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceById>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            Sequence | None: Requested sequence or None if it does not exist.

        Examples:

            Get sequence by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.sequences.retrieve(id=1)

            Get sequence by external id:

                >>> res = client.sequences.retrieve()
        """
        return run_sync(self.__async_client.sequences.retrieve(id=id, external_id=external_id))

    def retrieve_multiple(
        self,
        ids: typing.Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> SequenceList:
        """
        `Retrieve multiple sequences by id. <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceById>`_

        Args:
            ids (typing.Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            SequenceList: The requested sequences.

        Examples:

            Get sequences by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.sequences.retrieve_multiple(ids=[1, 2, 3])

            Get sequences by external id:

                >>> res = client.sequences.retrieve_multiple(external_ids=["abc", "def"])
        """
        return run_sync(
            self.__async_client.sequences.retrieve_multiple(
                ids=ids, external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def aggregate_count(
        self,
        advanced_filter: Filter | dict[str, Any] | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
    ) -> int:
        """
        `Count of sequences matching the specified filters and search. <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

        Args:
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the sequences to count.
            filter (SequenceFilter | dict[str, Any] | None): The filter to narrow down sequences to count requiring exact match.

        Returns:
            int: The number of sequences matching the specified filters and search.

        Examples:

            Count the number of time series in your CDF project:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> count = client.sequences.aggregate_count()

            Count the number of sequences with external id prefixed with "mapping:" in your CDF project:

                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> is_mapping = filters.Prefix(SequenceProperty.external_id, "mapping:")
                >>> count = client.sequences.aggregate_count(advanced_filter=is_mapping)
        """
        return run_sync(self.__async_client.sequences.aggregate_count(advanced_filter=advanced_filter, filter=filter))

    def aggregate_cardinality_values(
        self,
        property: SequenceProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
    ) -> int:
        """
        `Find approximate property count for sequences. <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

        Args:
            property (SequenceProperty | str | list[str]): The property to count the cardinality of.
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the sequences to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            filter (SequenceFilter | dict[str, Any] | None): The filter to narrow down the sequences  to count requiring exact match.

        Returns:
            int: The number of properties matching the specified filters and search.

        Examples:

            Count the number of different values for the metadata key "efficiency" used for sequences in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> client = CogniteClient()
                >>> count = client.sequences.aggregate_cardinality_values(SequenceProperty.metadata_key("efficiency"))

            Count the number of timezones (metadata key) for sequences with the word "critical" in the description
            in your CDF project, but exclude timezones from america:

                >>> from cognite.client.data_classes import filters, aggregations as aggs
                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> not_america = aggs.Not(aggs.Prefix("america"))
                >>> is_critical = filters.Search(SequenceProperty.description, "critical")
                >>> timezone_count = client.sequences.aggregate_cardinality_values(
                ...     SequenceProperty.metadata_key("timezone"),
                ...     advanced_filter=is_critical,
                ...     aggregate_filter=not_america)
        """
        return run_sync(
            self.__async_client.sequences.aggregate_cardinality_values(
                property=property, advanced_filter=advanced_filter, aggregate_filter=aggregate_filter, filter=filter
            )
        )

    def aggregate_cardinality_properties(
        self,
        path: SequenceProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
    ) -> int:
        """
        `Find approximate paths count for sequences.  <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

        Args:
            path (SequenceProperty | str | list[str]): The scope in every document to aggregate properties. The only value allowed now is ["metadata"]. It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the sequences to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            filter (SequenceFilter | dict[str, Any] | None): The filter to narrow down the sequences  to count requiring exact match.

        Returns:
            int: The number of properties matching the specified filters and search.

        Examples:

            Count the number of different metadata keys in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> client = CogniteClient()
                >>> count = client.sequences.aggregate_cardinality_values(SequenceProperty.metadata)
        """
        return run_sync(
            self.__async_client.sequences.aggregate_cardinality_properties(
                path=path, advanced_filter=advanced_filter, aggregate_filter=aggregate_filter, filter=filter
            )
        )

    def aggregate_unique_values(
        self,
        property: SequenceProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
    ) -> UniqueResultList:
        """
        `Get unique paths with counts for sequences. <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

        Args:
            property (SequenceProperty | str | list[str]): The property to group by.
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the sequences to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            filter (SequenceFilter | dict[str, Any] | None): The filter to narrow down the sequences to count requiring exact match.

        Returns:
            UniqueResultList: List of unique values of sequences matching the specified filters and search.

        Examples:

            Get the timezones (metadata key) with count for your sequences in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> client = CogniteClient()
                >>> result = client.sequences.aggregate_unique_values(SequenceProperty.metadata_key("timezone"))
                >>> print(result.unique)

            Get the different metadata keys with count used for sequences created after 2020-01-01 in your CDF project:

                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> from cognite.client.utils import timestamp_to_ms
                >>> from datetime import datetime
                >>> created_after_2020 = filters.Range(SequenceProperty.created_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
                >>> result = client.sequences.aggregate_unique_values(SequenceProperty.metadata, advanced_filter=created_after_2020)
                >>> print(result.unique)

            Get the different metadata keys with count for sequences updated after 2020-01-01 in your CDF project, but exclude all metadata keys that
            starts with "test":

                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> from cognite.client.data_classes import aggregations as aggs, filters
                >>> not_test = aggs.Not(aggs.Prefix("test"))
                >>> created_after_2020 = filters.Range(SequenceProperty.last_updated_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
                >>> result = client.sequences.aggregate_unique_values(SequenceProperty.metadata, advanced_filter=created_after_2020, aggregate_filter=not_test)
                >>> print(result.unique)
        """
        return run_sync(
            self.__async_client.sequences.aggregate_unique_values(
                property=property, advanced_filter=advanced_filter, aggregate_filter=aggregate_filter, filter=filter
            )
        )

    def aggregate_unique_properties(
        self,
        path: SequenceProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
    ) -> UniqueResultList:
        """
        `Find approximate unique sequence properties. <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

        Args:
            path (SequenceProperty | str | list[str]): The scope in every document to aggregate properties. The only value allowed now is ["metadata"]. It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the sequences to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            filter (SequenceFilter | dict[str, Any] | None): The filter to narrow down the sequences to count requiring exact match.

        Returns:
            UniqueResultList: List of unique values of sequences matching the specified filters and search.

        Examples:

            Get the metadata keys with count for your sequences in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> client = CogniteClient()
                >>> result = client.sequences.aggregate_unique_properties(SequenceProperty.metadata)
        """
        return run_sync(
            self.__async_client.sequences.aggregate_unique_properties(
                path=path, advanced_filter=advanced_filter, aggregate_filter=aggregate_filter, filter=filter
            )
        )

    @overload
    def create(self, sequence: Sequence | SequenceWrite) -> Sequence: ...

    @overload
    def create(self, sequence: typing.Sequence[Sequence] | typing.Sequence[SequenceWrite]) -> SequenceList: ...

    def create(
        self, sequence: Sequence | SequenceWrite | typing.Sequence[Sequence] | typing.Sequence[SequenceWrite]
    ) -> Sequence | SequenceList:
        """
        `Create one or more sequences. <https://developer.cognite.com/api#tag/Sequences/operation/createSequence>`_

        Args:
            sequence (Sequence | SequenceWrite | typing.Sequence[Sequence] | typing.Sequence[SequenceWrite]): Sequence or list of Sequence to create. The Sequence columns parameter is a list of objects with fields `externalId` (external id of the column, when omitted, they will be given ids of 'column0, column1, ...'), `valueType` (data type of the column, either STRING, LONG, or DOUBLE, with default DOUBLE), `name`, `description`, `metadata` (optional fields to describe and store information about the data in the column). Other fields will be removed automatically, so a columns definition from a different sequence object can be passed here.

        Returns:
            Sequence | SequenceList: The created sequence(s).

        Examples:

            Create a new sequence:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceWrite, SequenceColumnWrite
                >>> client = CogniteClient()
                >>> column_def = [
                ...     SequenceColumnWrite(value_type="STRING", external_id="user", description="some description"),
                ...     SequenceColumnWrite(value_type="DOUBLE", external_id="amount")
                ... ]
                >>> seq = client.sequences.create(SequenceWrite(external_id="my_sequence", columns=column_def))

            Create a new sequence with the same column specifications as an existing sequence:

                >>> seq2 = client.sequences.create(SequenceWrite(external_id="my_copied_sequence", columns=column_def))
        """
        return run_sync(self.__async_client.sequences.create(sequence=sequence))

    def delete(
        self,
        id: int | typing.Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """
        `Delete one or more sequences. <https://developer.cognite.com/api#tag/Sequences/operation/deleteSequences>`_

        Args:
            id (int | typing.Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete sequences by id or external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.sequences.delete(id=[1,2,3], external_id="3")
        """
        return run_sync(
            self.__async_client.sequences.delete(id=id, external_id=external_id, ignore_unknown_ids=ignore_unknown_ids)
        )

    @overload
    def update(
        self,
        item: Sequence | SequenceWrite | SequenceUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Sequence: ...

    @overload
    def update(
        self,
        item: typing.Sequence[Sequence | SequenceWrite | SequenceUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> SequenceList: ...

    def update(
        self,
        item: Sequence | SequenceWrite | SequenceUpdate | typing.Sequence[Sequence | SequenceWrite | SequenceUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Sequence | SequenceList:
        """
        `Update one or more sequences. <https://developer.cognite.com/api#tag/Sequences/operation/updateSequences>`_

        Args:
            item (Sequence | SequenceWrite | SequenceUpdate | typing.Sequence[Sequence | SequenceWrite | SequenceUpdate]): Sequences to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (Sequence or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Sequence | SequenceList: Updated sequences.

        Examples:

            Update a sequence that you have fetched. This will perform a full update of the sequences:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.sequences.retrieve(id=1)
                >>> res.description = "New description"
                >>> res = client.sequences.update(res)

            Perform a partial update on a sequence, updating the description and adding a new field to metadata:

                >>> from cognite.client.data_classes import SequenceUpdate
                >>> my_update = SequenceUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = client.sequences.update(my_update)

            **Updating column definitions**

            Currently, updating the column definitions of a sequence is only supported through partial update, using `add`, `remove` and `modify` methods on the `columns` property.

            Add a single new column:

                >>> from cognite.client.data_classes import SequenceUpdate, SequenceColumnWrite
                >>>
                >>> my_update = SequenceUpdate(id=1).columns.add(
                ...     SequenceColumnWrite(value_type ="STRING",external_id="user", description ="some description")
                ... )
                >>> res = client.sequences.update(my_update)

            Add multiple new columns:

                >>> from cognite.client.data_classes import SequenceUpdate, SequenceColumnWrite
                >>>
                >>> column_def = [
                ...     SequenceColumnWrite(value_type ="STRING",external_id="user", description ="some description"),
                ...     SequenceColumnWrite(value_type="DOUBLE", external_id="amount")
                ... ]
                >>> my_update = SequenceUpdate(id=1).columns.add(column_def)
                >>> res = client.sequences.update(my_update)

            Remove a single column:

                >>> from cognite.client.data_classes import SequenceUpdate
                >>>
                >>> my_update = SequenceUpdate(id=1).columns.remove("col_external_id1")
                >>> res = client.sequences.update(my_update)

            Remove multiple columns:

                >>> from cognite.client.data_classes import SequenceUpdate
                >>>
                >>> my_update = SequenceUpdate(id=1).columns.remove(["col_external_id1","col_external_id2"])
                >>> res = client.sequences.update(my_update)

            Update existing columns:

                >>> from cognite.client.data_classes import SequenceUpdate, SequenceColumnUpdate
                >>>
                >>> column_updates = [
                ...     SequenceColumnUpdate(external_id="col_external_id_1").external_id.set("new_col_external_id"),
                ...     SequenceColumnUpdate(external_id="col_external_id_2").description.set("my new description"),
                ... ]
                >>> my_update = SequenceUpdate(id=1).columns.modify(column_updates)
                >>> res = client.sequences.update(my_update)
        """
        return run_sync(self.__async_client.sequences.update(item=item, mode=mode))

    @overload
    def upsert(
        self, item: typing.Sequence[Sequence | SequenceWrite], mode: Literal["patch", "replace"] = "patch"
    ) -> SequenceList: ...

    @overload
    def upsert(self, item: Sequence | SequenceWrite, mode: Literal["patch", "replace"] = "patch") -> Sequence: ...

    def upsert(
        self,
        item: Sequence | SequenceWrite | typing.Sequence[Sequence | SequenceWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> Sequence | SequenceList:
        """
        Upsert sequences, i.e., update if it exists, and create if it does not exist.
            Note this is a convenience method that handles the upserting for you by first calling update on all items,
            and if any of them fail because they do not exist, it will create them instead.

            For more details, see :ref:`appendix-upsert`.

        Args:
            item (Sequence | SequenceWrite | typing.Sequence[Sequence | SequenceWrite]): Sequence or list of sequences to upsert.
            mode (Literal['patch', 'replace']): Whether to patch or replace in the case the sequences are existing. If you set 'patch', the call will only update fields with non-null values (default). Setting 'replace' will unset any fields that are not specified.

        Returns:
            Sequence | SequenceList: The upserted sequence(s).

        Examples:

            Upsert for sequences:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceWrite, SequenceColumnWrite
                >>> client = CogniteClient()
                >>> existing_sequence = client.sequences.retrieve(id=1)
                >>> existing_sequence.description = "New description"
                >>> new_sequence = SequenceWrite(
                ...     external_id="new_sequence",
                ...     description="New sequence",
                ...     columns=[SequenceColumnWrite(external_id="col1", value_type="STRING")]
                ... )
                >>> res = client.sequences.upsert([existing_sequence, new_sequence], mode="replace")
        """
        return run_sync(self.__async_client.sequences.upsert(item=item, mode=mode))

    def search(
        self,
        name: str | None = None,
        description: str | None = None,
        query: str | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> SequenceList:
        """
        `Search for sequences. <https://developer.cognite.com/api#tag/Sequences/operation/searchSequences>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            name (str | None): Prefix and fuzzy search on name.
            description (str | None): Prefix and fuzzy search on description.
            query (str | None): Search on name and description using wildcard search on each of the words (separated by spaces). Retrieves results where at least one word must match. Example: 'some other'
            filter (SequenceFilter | dict[str, Any] | None): Filter to apply. Performs exact match on these fields.
            limit (int): Max number of results to return.

        Returns:
            SequenceList: The search result as a SequenceList

        Examples:

            Search for a sequence:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.sequences.search(name="some name")
        """
        return run_sync(
            self.__async_client.sequences.search(
                name=name, description=description, query=query, filter=filter, limit=limit
            )
        )

    def list(
        self,
        name: str | None = None,
        external_id_prefix: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: typing.Sequence[int] | None = None,
        asset_subtree_ids: int | typing.Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | typing.Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> SequenceList:
        """
        `List sequences <https://developer.cognite.com/api#tag/Sequences/operation/advancedListSequences>`_

        Args:
            name (str | None): Filter out sequences that do not have this *exact* name.
            external_id_prefix (str | None): Filter out sequences that do not have this string as the start of the externalId
            metadata (dict[str, str] | None): Filter out sequences that do not match these metadata fields and values (case-sensitive). Format is {"key1":"value1","key2":"value2"}.
            asset_ids (typing.Sequence[int] | None): Filter out sequences that are not linked to any of these assets.
            asset_subtree_ids (int | typing.Sequence[int] | None): Only include sequences that have a related asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include sequences that have a related asset in a subtree rooted at any of these assetExternalIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            data_set_ids (int | typing.Sequence[int] | None): Return only sequences in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only sequences in the specified data set(s) with this external id / these external ids.
            created_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            limit (int | None): Max number of sequences to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            partitions (int | None): Retrieve resources in parallel using this number of workers (values up to 10 allowed), limit must be set to `None` (or `-1`).
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL (Domain Specific Language). It allows defining complex filtering expressions that combine simple operations, such as equals, prefix, exists, etc., using boolean operators and, or, and not. See examples below for usage.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Defaults to desc for `_score_` and asc for all other properties. Sort is not allowed if `partitions` is used.

        Returns:
            SequenceList: The requested sequences.

        .. note::
            When using `partitions`, there are few considerations to keep in mind:
                * `limit` has to be set to `None` (or `-1`).
                * API may reject requests if you specify more than 10 partitions. When Cognite enforces this behavior, the requests result in a 400 Bad Request status.
                * Partitions are done independently of sorting: there's no guarantee of the sort order between elements from different partitions. For this reason providing a `sort` parameter when using `partitions` is not allowed.


        Examples:

            List sequences:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.sequences.list(limit=5)

            Iterate over sequences, one-by-one:

                >>> for seq in client.sequences():
                ...     seq  # do something with the sequence

            Iterate over chunks of sequences to reduce memory load:

                >>> for seq_list in client.sequences(chunk_size=2500):
                ...     seq_list # do something with the sequences

            Using advanced filter, find all sequences that have a metadata key 'timezone' starting with 'Europe',
            and sort by external id ascending:

                >>> from cognite.client.data_classes import filters
                >>> in_timezone = filters.Prefix(["metadata", "timezone"], "Europe")
                >>> res = client.sequences.list(advanced_filter=in_timezone, sort=("external_id", "asc"))

            Note that you can check the API documentation above to see which properties you can filter on
            with which filters.

            To make it easier to avoid spelling mistakes and easier to look up available properties
            for filtering and sorting, you can also use the `SequenceProperty` and `SortableSequenceProperty` Enums.

                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.sequences import SequenceProperty, SortableSequenceProperty
                >>> in_timezone = filters.Prefix(SequenceProperty.metadata_key("timezone"), "Europe")
                >>> res = client.sequences.list(
                ...     advanced_filter=in_timezone,
                ...     sort=(SortableSequenceProperty.external_id, "asc"))

            Combine filter and advanced filter:

                >>> from cognite.client.data_classes import filters
                >>> not_instrument_lvl5 = filters.And(
                ...    filters.ContainsAny("labels", ["Level5"]),
                ...    filters.Not(filters.ContainsAny("labels", ["Instrument"]))
                ... )
                >>> res = client.sequences.list(asset_subtree_ids=[123456], advanced_filter=not_instrument_lvl5)
        """
        return run_sync(
            self.__async_client.sequences.list(
                name=name,
                external_id_prefix=external_id_prefix,
                metadata=metadata,
                asset_ids=asset_ids,
                asset_subtree_ids=asset_subtree_ids,
                asset_subtree_external_ids=asset_subtree_external_ids,
                data_set_ids=data_set_ids,
                data_set_external_ids=data_set_external_ids,
                created_time=created_time,
                last_updated_time=last_updated_time,
                limit=limit,
                partitions=partitions,
                advanced_filter=advanced_filter,
                sort=sort,
            )
        )
