from __future__ import annotations

import collections
import typing
import warnings
from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast, overload

from cognite.client._api.sequence_data import SequencesDataAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Sequence,
    SequenceFilter,
    SequenceList,
    SequenceUpdate,
    filters,
)
from cognite.client.data_classes._base import CogniteResource, PropertySpec
from cognite.client.data_classes.aggregations import AggregationFilter, CountAggregate, UniqueResultList
from cognite.client.data_classes.filters import _BASIC_FILTERS, Filter, _validate_filter
from cognite.client.data_classes.sequences import (
    SequenceColumnUpdate,
    SequenceColumnWriteList,
    SequenceCore,
    SequenceProperty,
    SequenceSort,
    SequenceWrite,
    SortableSequenceProperty,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import (
    assert_type,
    prepare_filter_sort,
    process_asset_subtree_ids,
    process_data_set_ids,
)
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig

SortSpec: TypeAlias = (
    SequenceSort
    | str
    | SortableSequenceProperty
    | tuple[str, Literal["asc", "desc"]]
    | tuple[str, Literal["asc", "desc"], Literal["auto", "first", "last"]]
)

_FILTERS_SUPPORTED: frozenset[type[Filter]] = _BASIC_FILTERS | {filters.Search}


class SequencesAPI(APIClient):
    _RESOURCE_PATH = "/sequences"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.data = SequencesDataAPI(config, api_version, cognite_client)

    @property
    def rows(self) -> SequencesDataAPI:
        warnings.warn(
            "The 'sequences.rows' property is deprecated and may be removed in the future. "
            "Use 'client.sequences.data' to future-proof your code.",
            DeprecationWarning,
        )
        return self.data

    @overload
    def __call__(self, chunk_size: None = None) -> AsyncIterator[Sequence]: ...

    @overload
    def __call__(self, chunk_size: int) -> AsyncIterator[SequenceList]: ...

    async def __call__(
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
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> AsyncIterator[Sequence | SequenceList]:
        """Iterate over sequences

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
            partitions (int | None): Retrieve resources in parallel using this number of workers (values up to 10 allowed), limit must be set to `None` (or `-1`).
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL (Domain Specific Language). It allows defining complex filtering expressions that combine simple operations, such as equals, prefix, exists, etc., using boolean operators and, or, and not.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Defaults to desc for `_score_` and asc for all other properties. Sort is not allowed if `partitions` is used.

        Yields:
            Sequence | SequenceList: yields Sequence one by one if chunk_size is not specified, else SequenceList objects.
        """
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = SequenceFilter(
            name=name,
            metadata=metadata,
            external_id_prefix=external_id_prefix,
            asset_ids=asset_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            created_time=created_time,
            last_updated_time=last_updated_time,
            data_set_ids=data_set_ids_processed,
        ).dump(camel_case=True)

        prep_sort = prepare_filter_sort(sort, SequenceSort)
        self._validate_filter(advanced_filter)

        async for item in self._list_generator(
            list_cls=SequenceList,
            resource_cls=Sequence,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            advanced_filter=advanced_filter,
            limit=limit,
            sort=prep_sort,
            partitions=partitions,
        ):
            yield item

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> Sequence | None:
        """`Retrieve a single sequence by id. <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceById>`_

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
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(list_cls=SequenceList, resource_cls=Sequence, identifiers=identifiers)

    async def retrieve_multiple(
        self,
        ids: typing.Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> SequenceList:
        """`Retrieve multiple sequences by id. <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceById>`_

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
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return await self._retrieve_multiple(
            list_cls=SequenceList, resource_cls=Sequence, identifiers=identifiers, ignore_unknown_ids=ignore_unknown_ids
        )

    async def aggregate(self, filter: SequenceFilter | dict[str, Any] | None = None) -> list[CountAggregate]:
        """`Aggregate sequences <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

        Args:
            filter (SequenceFilter | dict[str, Any] | None): Filter on sequence filter with exact match

        Returns:
            list[CountAggregate]: List of sequence aggregates

        Examples:

            Aggregate sequences:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.sequences.aggregate(filter={"external_id_prefix": "prefix"})
        """
        warnings.warn(
            "This method will be deprecated in the next major release. Use aggregate_count instead.", DeprecationWarning
        )
        return await self._aggregate(filter=filter, cls=CountAggregate)

    async def aggregate_count(
        self,
        advanced_filter: Filter | dict[str, Any] | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Count of sequences matching the specified filters and search. <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

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
        self._validate_filter(advanced_filter)
        return await self._advanced_aggregate(
            "count",
            filter=filter,
            advanced_filter=advanced_filter,
            api_subversion="beta",
        )

    async def aggregate_cardinality_values(
        self,
        property: SequenceProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate property count for sequences. <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

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
        self._validate_filter(advanced_filter)
        return await self._advanced_aggregate(
            "cardinalityValues",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            api_subversion="beta",
        )

    async def aggregate_cardinality_properties(
        self,
        path: SequenceProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate paths count for sequences.  <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

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
        self._validate_filter(advanced_filter)
        return await self._advanced_aggregate(
            "cardinalityProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            api_subversion="beta",
        )

    async def aggregate_unique_values(
        self,
        property: SequenceProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
    ) -> UniqueResultList:
        """`Get unique paths with counts for sequences. <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

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
        self._validate_filter(advanced_filter)
        if property == ["metadata"] or property is SequenceProperty.metadata:
            return await self._advanced_aggregate(
                aggregate="uniqueProperties",
                path=property,
                filter=filter,
                advanced_filter=advanced_filter,
                aggregate_filter=aggregate_filter,
                api_subversion="beta",
            )
        return await self._advanced_aggregate(
            aggregate="uniqueValues",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            api_subversion="beta",
        )

    async def aggregate_unique_properties(
        self,
        path: SequenceProperty | str | list[str],
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
    ) -> UniqueResultList:
        """`Find approximate unique sequence properties. <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

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
        self._validate_filter(advanced_filter)
        return await self._advanced_aggregate(
            aggregate="uniqueProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            api_subversion="beta",
        )

    @overload
    async def create(self, sequence: Sequence | SequenceWrite) -> Sequence: ...

    @overload
    async def create(self, sequence: typing.Sequence[Sequence] | typing.Sequence[SequenceWrite]) -> SequenceList: ...

    async def create(
        self, sequence: Sequence | SequenceWrite | typing.Sequence[Sequence] | typing.Sequence[SequenceWrite]
    ) -> Sequence | SequenceList:
        """`Create one or more sequences. <https://developer.cognite.com/api#tag/Sequences/operation/createSequence>`_

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
        assert_type(sequence, "sequences", [typing.Sequence, SequenceCore])

        return await self._create_multiple(
            list_cls=SequenceList, resource_cls=Sequence, items=sequence, input_resource_cls=SequenceWrite
        )

    async def delete(
        self,
        id: int | typing.Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more sequences. <https://developer.cognite.com/api#tag/Sequences/operation/deleteSequences>`_

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
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    async def update(
        self,
        item: Sequence | SequenceWrite | SequenceUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Sequence: ...

    @overload
    async def update(
        self,
        item: typing.Sequence[Sequence | SequenceWrite | SequenceUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> SequenceList: ...

    async def update(
        self,
        item: Sequence | SequenceWrite | SequenceUpdate | typing.Sequence[Sequence | SequenceWrite | SequenceUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Sequence | SequenceList:
        """`Update one or more sequences. <https://developer.cognite.com/api#tag/Sequences/operation/updateSequences>`_

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
        cdf_item_by_id = await self._get_cdf_item_by_id(item, "updating")
        return await self._update_multiple(
            list_cls=SequenceList,
            resource_cls=Sequence,
            update_cls=SequenceUpdate,
            items=item,
            mode=mode,
            cdf_item_by_id=cdf_item_by_id,
        )

    @overload
    async def upsert(
        self, item: typing.Sequence[Sequence | SequenceWrite], mode: Literal["patch", "replace"] = "patch"
    ) -> SequenceList: ...

    @overload
    async def upsert(self, item: Sequence | SequenceWrite, mode: Literal["patch", "replace"] = "patch") -> Sequence: ...

    async def upsert(
        self,
        item: Sequence | SequenceWrite | typing.Sequence[Sequence | SequenceWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> Sequence | SequenceList:
        """Upsert sequences, i.e., update if it exists, and create if it does not exist.
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

        cdf_item_by_id = await self._get_cdf_item_by_id(item, "upserting")
        return await self._upsert_multiple(
            item,
            list_cls=SequenceList,
            resource_cls=Sequence,
            update_cls=SequenceUpdate,
            input_resource_cls=Sequence,
            mode=mode,
            cdf_item_by_id=cdf_item_by_id,
        )

    async def _get_cdf_item_by_id(
        self,
        item: Sequence | SequenceWrite | SequenceUpdate | typing.Sequence[Sequence | SequenceWrite | SequenceUpdate],
        operation: Literal["updating", "upserting"],
    ) -> Mapping[str | int, Sequence]:
        if isinstance(item, SequenceWrite):
            if item.external_id is None:
                raise ValueError(f"External ID must be set when {operation} a SequenceWrite object.")
            cdf_item = await self.retrieve(external_id=item.external_id)
            if cdf_item and cdf_item.external_id:
                return {cdf_item.external_id: cdf_item}
        elif isinstance(item, Sequence):
            if item.external_id:
                cdf_item = await self.retrieve(external_id=item.external_id)
                if cdf_item and cdf_item.external_id:
                    return {cdf_item.external_id: cdf_item}
            else:
                cdf_item = await self.retrieve(id=item.id)
                if cdf_item and cdf_item.id:
                    return {cdf_item.id: cdf_item}
        elif isinstance(item, collections.abc.Sequence):
            external_ids = [i.external_id for i in item if isinstance(i, SequenceWrite)]
            if None in external_ids:
                raise ValueError(f"External ID must be set when {operation} using one or more SequenceWrite object(s).")
            external_ids.extend(i.external_id for i in item if isinstance(i, Sequence) and i.external_id)
            internal_ids = [i.id for i in item if isinstance(i, Sequence) and not i.external_id]
            cdf_items = await self.retrieve_multiple(
                ids=internal_ids, external_ids=typing.cast(list[str], external_ids), ignore_unknown_ids=True
            )
            return {item.external_id if item.external_id is not None else item.id: item for item in cdf_items}
        return {}

    @classmethod
    def _convert_resource_to_patch_object(
        cls,
        resource: CogniteResource,
        update_attributes: list[PropertySpec],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
        cdf_item_by_id: Mapping[Any, Sequence] | None = None,  # type: ignore [override]
    ) -> dict[str, dict[str, dict]]:
        update_obj = super()._convert_resource_to_patch_object(resource, update_attributes, mode)
        if not isinstance(resource, SequenceWrite | Sequence):
            return update_obj
        # Lookup columns to check what to add, remove and modify
        cdf_item: Sequence | None = None
        if cdf_item_by_id:
            if isinstance(resource, Sequence | SequenceWrite) and resource.external_id in cdf_item_by_id:
                cdf_item = cdf_item_by_id[cast(str, resource.external_id)]
            elif isinstance(resource, Sequence) and resource.id in cdf_item_by_id:
                cdf_item = cdf_item_by_id[resource.id]
        if isinstance(resource, Sequence):
            if resource.columns is None:
                resource_columns = SequenceColumnWriteList([])
            else:
                resource_columns = resource.columns.as_write()
        else:
            resource_columns = resource.columns

        update_obj["update"]["columns"] = {}
        if cdf_item is None:
            update_obj["update"]["columns"]["add"] = [column.dump() for column in resource_columns]
            return update_obj

        cdf_columns_by_external_id = {
            column.external_id: column for column in cdf_item.columns or [] if column.external_id
        }
        columns_by_external_id = {column.external_id: column for column in resource_columns if column.external_id}
        to_remove = set(cdf_columns_by_external_id.keys()) - set(columns_by_external_id.keys())
        if mode != "patch" and to_remove:
            # Replace or replace_ignore_null, remove all columns that are not in the new columns
            update_obj["update"]["columns"]["remove"] = list(to_remove)
        if to_add := (set(columns_by_external_id.keys()) - set(cdf_columns_by_external_id.keys())):
            update_obj["update"]["columns"]["add"] = [columns_by_external_id[ext_id].dump() for ext_id in to_add]
        if to_modify := (set(columns_by_external_id.keys()) & set(cdf_columns_by_external_id.keys())):
            modify_list: list[dict[str, dict[str, Any]]] = []
            for col_ext_id in to_modify:
                col_write_obj = columns_by_external_id[col_ext_id]
                column_update = super()._convert_resource_to_patch_object(
                    col_write_obj,
                    SequenceColumnUpdate._get_update_properties(col_write_obj),
                    mode,
                )
                modify_list.append(column_update)
            update_obj["update"]["columns"]["modify"] = modify_list
        return update_obj

    async def search(
        self,
        name: str | None = None,
        description: str | None = None,
        query: str | None = None,
        filter: SequenceFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> SequenceList:
        """`Search for sequences. <https://developer.cognite.com/api#tag/Sequences/operation/searchSequences>`_
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
        return await self._search(
            list_cls=SequenceList,
            search={"name": name, "description": description, "query": query},
            filter=filter or {},
            limit=limit,
        )

    async def filter(
        self,
        filter: Filter | dict,
        sort: SortSpec | list[SortSpec] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> SequenceList:
        """`Advanced filter sequences <https://developer.cognite.com/api#tag/Sequences/operation/advancedListSequences>`_

        Advanced filter lets you create complex filtering expressions that combine simple operations,
        such as equals, prefix, exists, etc., using boolean operators and, or, and not.
        It applies to basic fields as well as metadata.

        Args:
            filter (Filter | dict): Filter to apply.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Can be up to two properties to sort by default to ascending order.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            SequenceList: List of sequences that match the filter criteria.

        Examples:

            Find all sequences with asset id '123' and metadata key 'type' equals 'efficiency' and
            return them sorted by created time:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> client = CogniteClient()
                >>> asset_filter = filters.Equals("asset_id", 123)
                >>> is_efficiency = filters.Equals(["metadata", "type"], "efficiency")
                >>> res = client.sequences.filter(filter=filters.And(asset_filter, is_efficiency), sort="created_time")

            Note that you can check the API documentation above to see which properties you can filter on
            with which filters.

            To make it easier to avoid spelling mistakes and easier to look up available properties
            for filtering and sorting, you can also use the `SequenceProperty` and `SortableSequenceProperty` enums.

                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.sequences import SequenceProperty, SortableSequenceProperty
                >>> asset_filter = filters.Equals(SequenceProperty.asset_id, 123)
                >>> is_efficiency = filters.Equals(SequenceProperty.metadata_key("type"), "efficiency")
                >>> res = client.sequences.filter(
                ...     filter=filters.And(asset_filter, is_efficiency),
                ...     sort=SortableSequenceProperty.created_time)

        """
        warnings.warn(
            f"{self.__class__.__name__}.filter() method is deprecated and will be removed in the next major version of the SDK. Please use the {self.__class__.__name__}.list() method with advanced_filter parameter instead.",
            DeprecationWarning,
        )
        self._validate_filter(filter)

        return await self._list(
            list_cls=SequenceList,
            resource_cls=Sequence,
            method="POST",
            limit=limit,
            advanced_filter=filter.dump(camel_case_property=True) if isinstance(filter, Filter) else filter,
            sort=prepare_filter_sort(sort, SequenceSort),
            api_subversion="beta",
        )

    def _validate_filter(self, filter: Filter | dict[str, Any] | None) -> None:
        _validate_filter(filter, _FILTERS_SUPPORTED, type(self).__name__)

    async def list(
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
        """`List sequences <https://developer.cognite.com/api#tag/Sequences/operation/advancedListSequences>`_

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
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = SequenceFilter(
            name=name,
            metadata=metadata,
            external_id_prefix=external_id_prefix,
            asset_ids=asset_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            created_time=created_time,
            last_updated_time=last_updated_time,
            data_set_ids=data_set_ids_processed,
        ).dump(camel_case=True)

        prep_sort = prepare_filter_sort(sort, SequenceSort)
        self._validate_filter(advanced_filter)

        return await self._list(
            list_cls=SequenceList,
            resource_cls=Sequence,
            method="POST",
            filter=filter,
            advanced_filter=advanced_filter,
            sort=prep_sort,
            limit=limit,
            partitions=partitions,
        )
