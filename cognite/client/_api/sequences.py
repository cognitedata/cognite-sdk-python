from __future__ import annotations

import math
import typing
import warnings
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Sequence,
    SequenceFilter,
    SequenceList,
    SequenceRows,
    SequenceRowsList,
    SequenceUpdate,
    filters,
)
from cognite.client.data_classes.aggregations import AggregationFilter, CountAggregate, UniqueResultList
from cognite.client.data_classes.filters import _BASIC_FILTERS, Filter, _validate_filter
from cognite.client.data_classes.sequences import (
    SequenceCore,
    SequenceProperty,
    SequenceSort,
    SequenceWrite,
    SortableSequenceProperty,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils._auxiliary import handle_renamed_argument
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._identifier import Identifier, IdentifierSequence
from cognite.client.utils._validation import (
    assert_type,
    prepare_filter_sort,
    process_asset_subtree_ids,
    process_data_set_ids,
)
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient
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

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.rows = SequencesDataAPI(config, api_version, cognite_client)
        self.data = self.rows

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
        partitions: int | None = None,
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
        partitions: int | None = None,
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
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> Iterator[Sequence] | Iterator[SequenceList]:
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

        Returns:
            Iterator[Sequence] | Iterator[SequenceList]: yields Sequence one by one if chunk_size is not specified, else SequenceList objects.
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

        return self._list_generator(
            list_cls=SequenceList,
            resource_cls=Sequence,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            advanced_filter=advanced_filter,
            limit=limit,
            sort=prep_sort,
            partitions=partitions,
        )

    def __iter__(self) -> Iterator[Sequence]:
        """Iterate over sequences

        Fetches sequences as they are iterated over, so you keep a limited number of metadata objects in memory.

        Returns:
            Iterator[Sequence]: yields Sequence one by one.
        """
        return self()

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> Sequence | None:
        """`Retrieve a single sequence by id. <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceById>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            Sequence | None: Requested sequence or None if it does not exist.

        Examples:

            Get sequence by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.retrieve(id=1)

            Get sequence by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.retrieve()
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=SequenceList, resource_cls=Sequence, identifiers=identifiers)

    def retrieve_multiple(
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

            Get sequences by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.retrieve_multiple(ids=[1, 2, 3])

            Get sequences by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=SequenceList, resource_cls=Sequence, identifiers=identifiers, ignore_unknown_ids=ignore_unknown_ids
        )

    def aggregate(self, filter: SequenceFilter | dict[str, Any] | None = None) -> list[CountAggregate]:
        """`Aggregate sequences <https://developer.cognite.com/api#tag/Sequences/operation/aggregateSequences>`_

        Args:
            filter (SequenceFilter | dict[str, Any] | None): Filter on sequence filter with exact match

        Returns:
            list[CountAggregate]: List of sequence aggregates

        Examples:

            Aggregate sequences::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.aggregate(filter={"external_id_prefix": "prefix"})
        """
        warnings.warn(
            "This method will be deprecated in the next major release. Use aggregate_count instead.", DeprecationWarning
        )
        return self._aggregate(filter=filter, cls=CountAggregate)

    def aggregate_count(
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

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> count = client.sequences.aggregate_count()

            Count the number of sequences with external id prefixed with "mapping:" in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> client = CogniteClient()
                >>> is_mapping = filters.Prefix(SequenceProperty.external_id, "mapping:")
                >>> count = client.sequences.aggregate_count(advanced_filter=is_mapping)

        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "count",
            filter=filter,
            advanced_filter=advanced_filter,
            api_subversion="beta",
        )

    def aggregate_cardinality_values(
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

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters, aggregations as aggs
                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> client = CogniteClient()
                >>> not_america = aggs.Not(aggs.Prefix("america"))
                >>> is_critical = filters.Search(SequenceProperty.description, "critical")
                >>> timezone_count = client.sequences.aggregate_cardinality_values(
                ...     SequenceProperty.metadata_key("timezone"),
                ...     advanced_filter=is_critical,
                ...     aggregate_filter=not_america)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "cardinalityValues",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            api_subversion="beta",
        )

    def aggregate_cardinality_properties(
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
        return self._advanced_aggregate(
            "cardinalityProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            api_subversion="beta",
        )

    def aggregate_unique_values(
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

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> from cognite.client.utils import timestamp_to_ms
                >>> from datetime import datetime
                >>> client = CogniteClient()
                >>> created_after_2020 = filters.Range(SequenceProperty.created_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
                >>> result = client.sequences.aggregate_unique_values(SequenceProperty.metadata, advanced_filter=created_after_2020)
                >>> print(result.unique)

            Get the different metadata keys with count for sequences updated after 2020-01-01 in your CDF project, but exclude all metadata keys that
            starts with "test":

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.sequences import SequenceProperty
                >>> from cognite.client.data_classes import aggregations as aggs, filters
                >>> client = CogniteClient()
                >>> not_test = aggs.Not(aggs.Prefix("test"))
                >>> created_after_2020 = filters.Range(SequenceProperty.last_updated_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
                >>> result = client.sequences.aggregate_unique_values(SequenceProperty.metadata, advanced_filter=created_after_2020, aggregate_filter=not_test)
                >>> print(result.unique)
        """
        self._validate_filter(advanced_filter)
        if property == ["metadata"] or property is SequenceProperty.metadata:
            return self._advanced_aggregate(
                aggregate="uniqueProperties",
                path=property,
                filter=filter,
                advanced_filter=advanced_filter,
                aggregate_filter=aggregate_filter,
                api_subversion="beta",
            )
        return self._advanced_aggregate(
            aggregate="uniqueValues",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            api_subversion="beta",
        )

    def aggregate_unique_properties(
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
        return self._advanced_aggregate(
            aggregate="uniqueProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            api_subversion="beta",
        )

    @overload
    def create(self, sequence: Sequence | SequenceWrite) -> Sequence: ...

    @overload
    def create(self, sequence: typing.Sequence[Sequence] | typing.Sequence[SequenceWrite]) -> SequenceList: ...

    def create(
        self, sequence: Sequence | SequenceWrite | typing.Sequence[Sequence] | typing.Sequence[SequenceWrite]
    ) -> Sequence | SequenceList:
        """`Create one or more sequences. <https://developer.cognite.com/api#tag/Sequences/operation/createSequence>`_

        Args:
            sequence (Sequence | SequenceWrite | typing.Sequence[Sequence] | typing.Sequence[SequenceWrite]): Sequence or list of Sequence to create. The Sequence columns parameter is a list of objects with fields `externalId` (external id of the column, when omitted, they will be given ids of 'column0, column1, ...'), `valueType` (data type of the column, either STRING, LONG, or DOUBLE, with default DOUBLE), `name`, `description`, `metadata` (optional fields to describe and store information about the data in the column). Other fields will be removed automatically, so a columns definition from a different sequence object can be passed here.

        Returns:
            Sequence | SequenceList: The created sequence(s).

        Examples:

            Create a new sequence::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceWrite, SequenceColumnWrite
                >>> client = CogniteClient()
                >>> column_def = [
                ...     SequenceColumnWrite(value_type="String", external_id="user", description="some description"),
                ...     SequenceColumnWrite(value_type="Double", external_id="amount")
                ... ]
                >>> seq = client.sequences.create(SequenceWrite(external_id="my_sequence", columns=column_def))

            Create a new sequence with the same column specifications as an existing sequence::

                >>> seq2 = client.sequences.create(SequenceWrite(external_id="my_copied_sequence", columns=column_def))

        """
        assert_type(sequence, "sequences", [typing.Sequence, SequenceCore])

        return self._create_multiple(
            list_cls=SequenceList, resource_cls=Sequence, items=sequence, input_resource_cls=SequenceWrite
        )

    def delete(
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

            Delete sequences by id or external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.sequences.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
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
        """`Update one or more sequences. <https://developer.cognite.com/api#tag/Sequences/operation/updateSequences>`_

        Args:
            item (Sequence | SequenceWrite | SequenceUpdate | typing.Sequence[Sequence | SequenceWrite | SequenceUpdate]): Sequences to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (Sequence or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Sequence | SequenceList: Updated sequences.

        Examples:

            Update a sequence that you have fetched. This will perform a full update of the sequences::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.retrieve(id=1)
                >>> res.description = "New description"
                >>> res = client.sequences.update(res)

            Perform a partial update on a sequence, updating the description and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate
                >>> client = CogniteClient()
                >>> my_update = SequenceUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = client.sequences.update(my_update)

            **Updating column definitions**

            Currently, updating the column definitions of a sequence is only supported through partial update, using `add`, `remove` and `modify` methods on the `columns` property.

            Add a single new column::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate, SequenceColumn
                >>> client = CogniteClient()
                >>>
                >>> my_update = SequenceUpdate(id=1).columns.add(SequenceColumn(value_type ="String",external_id="user", description ="some description"))
                >>> res = client.sequences.update(my_update)

            Add multiple new columns::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate, SequenceColumn
                >>> client = CogniteClient()
                >>>
                >>> column_def = [
                ...     SequenceColumn(value_type ="String",external_id="user", description ="some description"),
                ...     SequenceColumn(value_type="Double", external_id="amount")]
                >>> my_update = SequenceUpdate(id=1).columns.add(column_def)
                >>> res = client.sequences.update(my_update)

            Remove a single column::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate
                >>> client = CogniteClient()
                >>>
                >>> my_update = SequenceUpdate(id=1).columns.remove("col_external_id1")
                >>> res = client.sequences.update(my_update)

            Remove multiple columns::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate
                >>> client = CogniteClient()
                >>>
                >>> my_update = SequenceUpdate(id=1).columns.remove(["col_external_id1","col_external_id2"])
                >>> res = client.sequences.update(my_update)

            Update existing columns::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SequenceUpdate, SequenceColumnUpdate
                >>> client = CogniteClient()
                >>>
                >>> column_updates = [
                ...     SequenceColumnUpdate(external_id="col_external_id_1").external_id.set("new_col_external_id"),
                ...     SequenceColumnUpdate(external_id="col_external_id_2").description.set("my new description"),
                ... ]
                >>> my_update = SequenceUpdate(id=1).columns.modify(column_updates)
                >>> res = client.sequences.update(my_update)
        """
        return self._update_multiple(
            list_cls=SequenceList, resource_cls=Sequence, update_cls=SequenceUpdate, items=item, mode=mode
        )

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
                >>> from cognite.client.data_classes import Sequence
                >>> client = CogniteClient()
                >>> existing_sequence = client.sequences.retrieve(id=1)
                >>> existing_sequence.description = "New description"
                >>> new_sequence = Sequence(external_id="new_sequence", description="New sequence")
                >>> res = client.sequences.upsert([existing_sequence, new_sequence], mode="replace")
        """
        return self._upsert_multiple(
            item,
            list_cls=SequenceList,
            resource_cls=Sequence,
            update_cls=SequenceUpdate,
            input_resource_cls=Sequence,
            mode=mode,
        )

    def search(
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

            Search for a sequence::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.search(name="some name")
        """
        return self._search(
            list_cls=SequenceList,
            search={"name": name, "description": description, "query": query},
            filter=filter or {},
            limit=limit,
        )

    def filter(
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

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.sequences import SequenceProperty, SortableSequenceProperty
                >>> client = CogniteClient()
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

        return self._list(
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

            List sequences::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.list(limit=5)

            Iterate over sequences::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for seq in client.sequences:
                ...     seq # do something with the sequence

            Iterate over chunks of sequences to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for seq_list in client.sequences(chunk_size=2500):
                ...     seq_list # do something with the sequences

            Using advanced filter, find all sequences that have a metadata key 'timezone' starting with 'Europe',
            and sort by external id ascending:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> client = CogniteClient()
                >>> in_timezone = filters.Prefix(["metadata", "timezone"], "Europe")
                >>> res = client.sequences.list(advanced_filter=in_timezone, sort=("external_id", "asc"))

            Note that you can check the API documentation above to see which properties you can filter on
            with which filters.

            To make it easier to avoid spelling mistakes and easier to look up available properties
            for filtering and sorting, you can also use the `SequenceProperty` and `SortableSequenceProperty` Enums.

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.sequences import SequenceProperty, SortableSequenceProperty
                >>> client = CogniteClient()
                >>> in_timezone = filters.Prefix(SequenceProperty.metadata_key("timezone"), "Europe")
                >>> res = client.sequences.list(
                ...     advanced_filter=in_timezone,
                ...     sort=(SortableSequenceProperty.external_id, "asc"))

            Combine filter and advanced filter:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> client = CogniteClient()
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

        return self._list(
            list_cls=SequenceList,
            resource_cls=Sequence,
            method="POST",
            filter=filter,
            advanced_filter=advanced_filter,
            sort=prep_sort,
            limit=limit,
            partitions=partitions,
        )


class SequencesDataAPI(APIClient):
    _DATA_PATH = "/sequences/data"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._SEQ_POST_LIMIT_ROWS = 10_000
        self._SEQ_POST_LIMIT_VALUES = 100_000
        self._SEQ_RETRIEVE_LIMIT = 10_000

    def insert(
        self,
        rows: SequenceRows
        | dict[int, typing.Sequence[int | float | str]]
        | typing.Sequence[tuple[int, typing.Sequence[int | float | str]]]
        | typing.Sequence[dict[str, Any]],
        columns: SequenceNotStr[str] | None = None,
        id: int | None = None,
        external_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        """`Insert rows into a sequence <https://developer.cognite.com/api#tag/Sequences/operation/postSequenceData>`_

        Args:
            rows (SequenceRows | dict[int, typing.Sequence[int | float | str]] | typing.Sequence[tuple[int, typing.Sequence[int | float | str]]] | typing.Sequence[dict[str, Any]]):  The rows you wish to insert. Can either be a list of tuples, a list of {"rowNumber":... ,"values": ...} objects, a dictionary of rowNumber: data, or a SequenceData object. See examples below.
            columns (SequenceNotStr[str] | None): List of external id for the columns of the sequence.
            id (int | None): Id of sequence to insert rows into.
            external_id (str | None): External id of sequence to insert rows into.
            **kwargs (Any): To support deprecated argument 'column_external_ids', will be removed in the next major version. Use 'columns' instead.

        Examples:
            Your rows of data can be a list of tuples where the first element is the rownumber and the second element is the data to be inserted::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Sequence, SequenceColumn
                >>> client = CogniteClient()
                >>> seq = client.sequences.create(Sequence(columns=[SequenceColumn(value_type="String", external_id="col_a"),
                ...     SequenceColumn(value_type="Double", external_id ="col_b")]))
                >>> data = [(1, ['pi',3.14]), (2, ['e',2.72]) ]
                >>> client.sequences.data.insert(columns=["col_a","col_b"], rows=data, id=1)

            They can also be provided as a list of API-style objects with a rowNumber and values field::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> data = [{"rowNumber": 123, "values": ['str',3]}, {"rowNumber": 456, "values": ["bar",42]} ]
                >>> client.sequences.data.insert(data, id=1, columns=["col_a","col_b"]) # implicit columns are retrieved from metadata

            Or they can be a given as a dictionary with row number as the key, and the value is the data to be inserted at that row::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> data = {123 : ['str',3], 456 : ['bar',42] }
                >>> client.sequences.data.insert(columns=['stringColumn','intColumn'], rows=data, id=1)

            Finally, they can be a SequenceData object retrieved from another request. In this case columns from this object are used as well.

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> data = client.sequences.data.retrieve(id=2,start=0,end=10)
                >>> client.sequences.data.insert(rows=data, id=1,columns=None)
        """
        columns = handle_renamed_argument(columns, "columns", "column_external_ids", "insert", kwargs, False)
        if isinstance(rows, SequenceRows):
            columns = rows.column_external_ids
            rows = [{"rowNumber": k, "values": v} for k, v in rows.items()]

        if isinstance(rows, dict):
            all_rows: dict | typing.Sequence = [{"rowNumber": k, "values": v} for k, v in rows.items()]
        elif isinstance(rows, typing.Sequence) and len(rows) > 0 and isinstance(rows[0], dict):
            all_rows = rows
        elif isinstance(rows, typing.Sequence) and (len(rows) == 0 or isinstance(rows[0], tuple)):
            all_rows = [{"rowNumber": k, "values": v} for k, v in rows]
        else:
            raise TypeError("Invalid format for 'rows', expected a list of tuples, list of dict or dict")

        base_obj = Identifier.of_either(id, external_id).as_dict()
        base_obj.update(self._wrap_columns(columns))

        if len(all_rows) > 0:
            rows_per_request = min(
                self._SEQ_POST_LIMIT_ROWS, int(self._SEQ_POST_LIMIT_VALUES / len(all_rows[0]["values"]))
            )
        else:
            rows_per_request = self._SEQ_POST_LIMIT_ROWS

        row_objs = [{"rows": all_rows[i : i + rows_per_request]} for i in range(0, len(all_rows), rows_per_request)]
        tasks = [({**base_obj, **rows},) for rows in row_objs]
        summary = execute_tasks(self._insert_data, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks()

    def insert_dataframe(
        self, dataframe: pandas.DataFrame, id: int | None = None, external_id: str | None = None, dropna: bool = True
    ) -> None:
        """`Insert a Pandas dataframe. <https://developer.cognite.com/api#tag/Sequences/operation/postSequenceData>`_

        The index of the dataframe must contain the row numbers. The names of the remaining columns specify the column external ids.
        The sequence and columns must already exist.

        Args:
            dataframe (pandas.DataFrame):  Pandas DataFrame object containing the sequence data.
            id (int | None): Id of sequence to insert rows into.
            external_id (str | None): External id of sequence to insert rows into.
            dropna (bool): Whether to drop rows where all values are missing. Default: True.

        Examples:
            Insert three rows into columns 'col_a' and 'col_b' of the sequence with id=123:

                >>> from cognite.client import CogniteClient
                >>> import pandas as pd
                >>> client = CogniteClient()
                >>> df = pd.DataFrame({'col_a': [1, 2, 3], 'col_b': [4, 5, 6]}, index=[1, 2, 3])
                >>> client.sequences.data.insert_dataframe(df, id=123)
        """
        if dropna:
            # These will be rejected by the API, hence we remove them by default:
            dataframe = dataframe.dropna(how="all")
        dataframe = dataframe.replace({math.nan: None})  # TODO: Optimization required (memory usage)
        data = [(row.Index, row[1:]) for row in dataframe.itertuples()]
        columns = list(dataframe.columns.astype(str))
        self.insert(rows=data, columns=columns, id=id, external_id=external_id)

    def _insert_data(self, task: dict[str, Any]) -> None:
        self._post(url_path=self._DATA_PATH, json={"items": [task]})

    def delete(self, rows: typing.Sequence[int], id: int | None = None, external_id: str | None = None) -> None:
        """`Delete rows from a sequence <https://developer.cognite.com/api#tag/Sequences/operation/deleteSequenceData>`_

        Args:
            rows (typing.Sequence[int]): List of row numbers.
            id (int | None): Id of sequence to delete rows from.
            external_id (str | None): External id of sequence to delete rows from.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.sequences.data.delete(id=1, rows=[1,2,42])
        """
        post_obj = Identifier.of_either(id, external_id).as_dict()
        post_obj["rows"] = rows

        self._post(url_path=self._DATA_PATH + "/delete", json={"items": [post_obj]})

    def delete_range(self, start: int, end: int | None, id: int | None = None, external_id: str | None = None) -> None:
        """`Delete a range of rows from a sequence. Note this operation is potentially slow, as retrieves each row before deleting. <https://developer.cognite.com/api#tag/Sequences/operation/deleteSequenceData>`_

        Args:
            start (int): Row number to start from (inclusive).
            end (int | None): Upper limit on the row number (exclusive). Set to None or -1 to delete all rows until end of sequence.
            id (int | None): Id of sequence to delete rows from.
            external_id (str | None): External id of sequence to delete rows from.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.sequences.data.delete_range(id=1, start=0, end=None)
        """
        sequence = self._cognite_client.sequences.retrieve(external_id=external_id, id=id)
        assert sequence is not None
        post_obj = Identifier.of_either(id, external_id).as_dict()
        post_obj.update(self._wrap_columns(column_external_ids=sequence.column_external_ids))
        post_obj.update({"start": start, "end": end})
        for resp in self._fetch_data(post_obj):
            if rows := resp["rows"]:
                self.delete(rows=[r["rowNumber"] for r in rows], external_id=external_id, id=id)

    @overload
    def retrieve(
        self,
        *,
        external_id: str,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRows: ...

    @overload
    def retrieve(
        self,
        *,
        external_id: SequenceNotStr[str],
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRowsList: ...

    @overload
    def retrieve(
        self,
        *,
        id: int,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRows: ...

    @overload
    def retrieve(
        self,
        *,
        id: typing.Sequence[int],
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
    ) -> SequenceRowsList: ...

    def retrieve(
        self,
        external_id: str | SequenceNotStr[str] | None = None,
        id: int | typing.Sequence[int] | None = None,
        start: int = 0,
        end: int | None = None,
        columns: SequenceNotStr[str] | None = None,
        limit: int | None = None,
        **kwargs: Any,
    ) -> SequenceRows | SequenceRowsList:
        """`Retrieve data from a sequence <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceData>`_

        Args:
            external_id (str | SequenceNotStr[str] | None): The external id of the sequence to retrieve from.
            id (int | typing.Sequence[int] | None): The internal if the sequence to retrieve from.
            start (int): Row number to start from (inclusive).
            end (int | None): Upper limit on the row number (exclusive). Set to None or -1 to get all rows until end of sequence.
            columns (SequenceNotStr[str] | None): List of external id for the columns of the sequence. If 'None' is passed, all columns will be retrieved.
            limit (int | None): Maximum number of rows to return per sequence. Pass None to fetch all (possibly limited by 'end').
            **kwargs (Any): To support deprecated argument 'column_external_ids', will be removed in the next major version. Use 'columns' instead.

        Returns:
            SequenceRows | SequenceRowsList: SequenceRows if a single identifier was given, else SequenceDataList

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.data.retrieve(id=1)
                >>> tuples = [(r,v) for r,v in res.items()] # You can use this iterator in for loops and list comprehensions,
                >>> single_value = res[23] # ... get the values at a single row number,
                >>> col = res.get_column(external_id='columnExtId') # ... get the array of values for a specific column,
                >>> df = res.to_pandas() # ... or convert the result to a dataframe
        """
        columns = handle_renamed_argument(columns, "columns", "column_external_ids", "insert", kwargs, False)

        ident_sequence = IdentifierSequence.load(id, external_id)
        identifiers = ident_sequence.as_dicts()

        def _fetch_sequence(post_obj: dict[str, Any]) -> SequenceRows:
            post_obj.update(self._wrap_columns(column_external_ids=columns))
            post_obj.update({"start": start, "end": end, "limit": limit})

            row_response_iterator = self._fetch_data(post_obj)
            # Get the External ID and ID from the first response
            sequence_rows = next(row_response_iterator)
            for row_response in row_response_iterator:
                sequence_rows["rows"].extend(row_response["rows"])

            return SequenceRows._load(sequence_rows)

        tasks_summary = execute_tasks(_fetch_sequence, list(zip(identifiers)), max_workers=self._config.max_workers)
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_list_element_unwrap_fn=ident_sequence.extract_identifiers
        )
        results = tasks_summary.joined_results()
        if ident_sequence.is_singleton():
            return results[0]
        else:
            return SequenceRowsList(results)

    def retrieve_last_row(
        self,
        id: int | None = None,
        external_id: str | None = None,
        columns: SequenceNotStr[str] | None = None,
        before: int | None = None,
        **kwargs: Any,
    ) -> SequenceRows:
        """`Retrieves the last row (i.e the row with the highest row number) in a sequence. <https://developer.cognite.com/api#tag/Sequences/operation/getLatestSequenceRow>`_

        Args:
            id (int | None): Id or list of ids.
            external_id (str | None): External id or list of external ids.
            columns (SequenceNotStr[str] | None): List of external id for the columns of the sequence. If 'None' is passed, all columns will be retrieved.
            before (int | None): (optional, int): Get latest datapoint before this row number.
            **kwargs (Any): To support deprecated argument 'column_external_ids', will be removed in the next major version. Use 'columns' instead.

        Returns:
            SequenceRows: A Datapoints object containing the requested data, or a list of such objects.

        Examples:

            Getting the latest row in a sequence before row number 1000:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.sequences.data.retrieve_last_row(id=1, before=1000)
        """
        columns = handle_renamed_argument(columns, "columns", "column_external_ids", "insert", kwargs, False)
        identifier = Identifier.of_either(id, external_id).as_dict()
        res = self._do_request(
            "POST", self._DATA_PATH + "/latest", json={**identifier, "before": before, "columns": columns}
        ).json()
        return SequenceRows._load(res)

    def retrieve_dataframe(
        self,
        start: int,
        end: int | None,
        column_external_ids: list[str] | None = None,
        external_id: str | None = None,
        column_names: str | None = None,
        id: int | None = None,
        limit: int | None = None,
    ) -> pandas.DataFrame:
        """`Retrieve data from a sequence as a pandas dataframe <https://developer.cognite.com/api#tag/Sequences/operation/getSequenceData>`_

        Args:
            start (int): (inclusive) row number to start from.
            end (int | None): (exclusive) upper limit on the row number. Set to None or -1 to get all rows until end of sequence.
            column_external_ids (list[str] | None): List of external id for the columns of the sequence.  If 'None' is passed, all columns will be retrieved.
            external_id (str | None): External id of sequence.
            column_names (str | None):  Which field(s) to use as column header. Can use "externalId", "id", "columnExternalId", "id|columnExternalId" or "externalId|columnExternalId". Default is "externalId|columnExternalId" for queries on more than one sequence, and "columnExternalId" for queries on a single sequence.
            id (int | None): Id of sequence
            limit (int | None): Maximum number of rows to return per sequence.

        Returns:
            pandas.DataFrame: The requested sequence data in a pandas DataFrame

        Examples:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> df = client.sequences.data.retrieve_dataframe(id=1, start=0, end=None)
        """
        warnings.warn("This method is deprecated. Use retrieve(...).to_pandas(..) instead.", DeprecationWarning)
        if isinstance(external_id, list) or isinstance(id, list) or (id is not None and external_id is not None):
            column_names_default = "externalId|columnExternalId"
        else:
            column_names_default = "columnExternalId"

        if external_id is not None and id is None:
            return self.retrieve(
                external_id=external_id, start=start, end=end, limit=limit, columns=column_external_ids
            ).to_pandas(
                column_names=column_names or column_names_default,  # type: ignore  [arg-type]
            )
        elif id is not None and external_id is None:
            return self.retrieve(id=id, start=start, end=end, limit=limit, columns=column_external_ids).to_pandas(
                column_names=column_names or column_names_default,  # type: ignore  [arg-type]
            )
        else:
            raise ValueError("Either external_id or id must be specified")

    def _fetch_data(self, task: dict[str, Any]) -> Iterator[dict[str, Any]]:
        remaining_limit = task.get("limit")
        cursor = None
        if task["end"] == -1:
            task["end"] = None
        while True:
            task["limit"] = min(self._SEQ_RETRIEVE_LIMIT, remaining_limit or self._SEQ_RETRIEVE_LIMIT)
            task["cursor"] = cursor
            resp = self._post(url_path=self._DATA_PATH + "/list", json=task).json()
            yield resp
            cursor = resp.get("nextCursor")
            if remaining_limit:
                remaining_limit -= len(resp["rows"])
            if not cursor or (remaining_limit is not None and remaining_limit <= 0):
                break

    def _wrap_columns(self, column_external_ids: SequenceNotStr[str] | None) -> dict[str, SequenceNotStr[str]]:
        if column_external_ids is None:
            return {}  # for defaults
        return {"columns": column_external_ids}
