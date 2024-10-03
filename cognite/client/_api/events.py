from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, Literal, TypeAlias, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    AggregateResult,
    EndTimeFilter,
    Event,
    EventFilter,
    EventList,
    EventUpdate,
    TimestampRange,
    filters,
)
from cognite.client.data_classes.aggregations import AggregationFilter, UniqueResultList
from cognite.client.data_classes.events import EventPropertyLike, EventSort, EventWrite, SortableEventProperty
from cognite.client.data_classes.filters import _BASIC_FILTERS, Filter, _validate_filter
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import prepare_filter_sort, process_asset_subtree_ids, process_data_set_ids
from cognite.client.utils.useful_types import SequenceNotStr

SortSpec: TypeAlias = (
    EventSort
    | str
    | SortableEventProperty
    | tuple[str, Literal["asc", "desc"]]
    | tuple[str, Literal["asc", "desc"], Literal["auto", "first", "last"]]
)

_FILTERS_SUPPORTED: frozenset[type[Filter]] = _BASIC_FILTERS | {filters.Search}


class EventsAPI(APIClient):
    _RESOURCE_PATH = "/events"

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        start_time: dict[str, Any] | TimestampRange | None = None,
        end_time: dict[str, Any] | EndTimeFilter | None = None,
        active_at_time: dict[str, Any] | TimestampRange | None = None,
        type: str | None = None,
        subtype: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
    ) -> Iterator[Event]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        start_time: dict[str, Any] | TimestampRange | None = None,
        end_time: dict[str, Any] | EndTimeFilter | None = None,
        active_at_time: dict[str, Any] | TimestampRange | None = None,
        type: str | None = None,
        subtype: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
    ) -> Iterator[EventList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        start_time: dict[str, Any] | TimestampRange | None = None,
        end_time: dict[str, Any] | EndTimeFilter | None = None,
        active_at_time: dict[str, Any] | TimestampRange | None = None,
        type: str | None = None,
        subtype: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
    ) -> Iterator[Event] | Iterator[EventList]:
        """Iterate over events

        Fetches events as they are iterated over, so you keep a limited number of events in memory.

        Args:
            chunk_size (int | None): Number of events to return in each chunk. Defaults to yielding one event a time.
            start_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
            end_time (dict[str, Any] | EndTimeFilter | None): Range between two timestamps
            active_at_time (dict[str, Any] | TimestampRange | None): Event is considered active from its startTime to endTime inclusive. If startTime is null, event is never active. If endTime is null, event is active from startTime onwards. activeAtTime filter will match all events that are active at some point from min to max, from min, or to max, depending on which of min and max parameters are specified.
            type (str | None): Type of the event, e.g 'failure'.
            subtype (str | None): Subtype of the event, e.g 'electrical'.
            metadata (dict[str, str] | None): Customizable extra data about the event. String key -> String value.
            asset_ids (Sequence[int] | None): Asset IDs of related equipments that this event relates to.
            asset_external_ids (SequenceNotStr[str] | None): Asset External IDs of related equipment that this event relates to.
            asset_subtree_ids (int | Sequence[int] | None): Only include events that have a related asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include events that have a related asset in a subtree rooted at any of these assetExternalIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            data_set_ids (int | Sequence[int] | None): Return only events in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only events in the specified data set(s) with this external id / these external ids.
            source (str | None): The source of this event.
            created_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            external_id_prefix (str | None): External Id provided by client. Should be unique within the project
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Defaults to desc for `_score_` and asc for all other properties. Sort is not allowed if `partitions` is used.
            limit (int | None): Maximum number of events to return. Defaults to return all items.
            partitions (int | None): Retrieve resources in parallel using this number of workers (values up to 10 allowed), limit must be set to `None` (or `-1`).
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL (Domain Specific Language). It allows defining complex filtering expressions that combine simple operations, such as equals, prefix, exists, etc., using boolean operators and, or, and not.

        Returns:
            Iterator[Event] | Iterator[EventList]: yields Event one by one if chunk_size is not specified, else EventList objects.
        """
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = EventFilter(
            start_time=start_time,
            end_time=end_time,
            active_at_time=active_at_time,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            data_set_ids=data_set_ids_processed,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            type=type,
            subtype=subtype,
        ).dump(camel_case=True)

        prep_sort = prepare_filter_sort(sort, EventSort)
        self._validate_filter(advanced_filter)

        return self._list_generator(
            list_cls=EventList,
            resource_cls=Event,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            advanced_filter=advanced_filter,
            limit=limit,
            sort=prep_sort,
            partitions=partitions,
        )

    def __iter__(self) -> Iterator[Event]:
        """Iterate over events

        Fetches events as they are iterated over, so you keep a limited number of events in memory.

        Returns:
            Iterator[Event]: yields Events one by one.
        """
        return self()

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> Event | None:
        """`Retrieve a single event by id. <https://developer.cognite.com/api#tag/Events/operation/getEventByInternalId>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            Event | None: Requested event or None if it does not exist.

        Examples:

            Get event by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.events.retrieve(id=1)

            Get event by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.events.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=EventList, resource_cls=Event, identifiers=identifiers)

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> EventList:
        """`Retrieve multiple events by id. <https://developer.cognite.com/api#tag/Events/operation/byIdsEvents>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            EventList: The requested events.

        Examples:

            Get events by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.events.retrieve_multiple(ids=[1, 2, 3])

            Get events by external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.events.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=EventList, resource_cls=Event, identifiers=identifiers, ignore_unknown_ids=ignore_unknown_ids
        )

    def aggregate(self, filter: EventFilter | dict[str, Any] | None = None) -> list[AggregateResult]:
        """`Aggregate events <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            filter (EventFilter | dict[str, Any] | None): Filter on events filter with exact match

        Returns:
            list[AggregateResult]: List of event aggregates

        Examples:

            Aggregate events:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> aggregate_type = client.events.aggregate(filter={"type": "failure"})
        """
        warnings.warn(
            "This method is deprecated. Use aggregate_count, aggregate_unique_values, aggregate_cardinality_values, aggregate_cardinality_properties, or aggregate_unique_properties instead.",
            DeprecationWarning,
        )
        return self._aggregate(filter=filter, cls=AggregateResult)

    def aggregate_unique_values(
        self,
        filter: EventFilter | dict[str, Any] | None = None,
        property: EventPropertyLike | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
    ) -> UniqueResultList:
        """`Get unique properties with counts for events. <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            filter (EventFilter | dict[str, Any] | None): The filter to narrow down the events to count requiring exact match.
            property (EventPropertyLike | None): The property name(s) to apply the aggregation on.
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the events to consider.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.

        Returns:
            UniqueResultList: List of unique values of events matching the specified filters and search.

        Examples:

        Get the unique types with count of events in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.events import EventProperty
            >>> client = CogniteClient()
            >>> result = client.events.aggregate_unique_values(property=EventProperty.type)
            >>> print(result.unique)

        Get the unique types of events after 2020-01-01 in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import filters
            >>> from cognite.client.data_classes.events import EventProperty
            >>> from cognite.client.utils import timestamp_to_ms
            >>> from datetime import datetime
            >>> client = CogniteClient()
            >>> is_after_2020 = filters.Range(EventProperty.start_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
            >>> result = client.events.aggregate_unique_values(EventProperty.type, advanced_filter=is_after_2020)
            >>> print(result.unique)

        Get the unique types of events after 2020-01-01 in your CDF project, but exclude all types that start with
        "planned":

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.events import EventProperty
            >>> from cognite.client.data_classes import aggregations
            >>> client = CogniteClient()
            >>> agg = aggregations
            >>> not_planned = agg.Not(agg.Prefix("planned"))
            >>> is_after_2020 = filters.Range(EventProperty.start_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
            >>> result = client.events.aggregate_unique_values(EventProperty.type, advanced_filter=is_after_2020, aggregate_filter=not_planned)
            >>> print(result.unique)

        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            aggregate="uniqueValues",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    def aggregate_count(
        self,
        property: EventPropertyLike | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        filter: EventFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Count of event matching the specified filters. <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            property (EventPropertyLike | None): If specified, Get an approximate number of Events with a specific property
                (property is not null) and matching the filters.
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the events to count.
            filter (EventFilter | dict[str, Any] | None): The filter to narrow down the events to count requiring exact match.

        Returns:
            int: The number of events matching the specified filters and search.

        Examples:

            Count the number of events in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> count = client.events.aggregate_count()

            Count the number of workorder events in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.events import EventProperty
                >>> client = CogniteClient()
                >>> is_workorder = filters.Equals(EventProperty.type, "workorder")
                >>> workorder_count = client.events.aggregate_count(advanced_filter=is_workorder)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "count",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
        )

    def aggregate_cardinality_values(
        self,
        property: EventPropertyLike,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: EventFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate property count for events. <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            property (EventPropertyLike): The property to count the cardinality of.
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the events to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            filter (EventFilter | dict[str, Any] | None): The filter to narrow down the events to count requiring exact match.
        Returns:
            int: The number of properties matching the specified filter.

        Examples:

        Count the number of types of events in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.events import EventProperty
            >>> client = CogniteClient()
            >>> type_count = client.events.aggregate_cardinality_values(EventProperty.type)

        Count the number of types of events linked to asset 123 in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import filters
            >>> from cognite.client.data_classes.events import EventProperty
            >>> client = CogniteClient()
            >>> is_asset = filters.ContainsAny(EventProperty.asset_ids, 123)
            >>> plain_text_author_count = client.events.aggregate_cardinality_values(EventProperty.type, advanced_filter=is_asset)

        """
        self._validate_filter(advanced_filter)

        return self._advanced_aggregate(
            "cardinalityValues",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    def aggregate_cardinality_properties(
        self,
        path: EventPropertyLike,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: EventFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate paths count for events. <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            path (EventPropertyLike): The scope in every document to aggregate properties. The only value allowed now is ["metadata"].
                It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the events to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            filter (EventFilter | dict[str, Any] | None): The filter to narrow down the events to count requiring exact match.
        Returns:
            int: The number of properties matching the specified filters and search.

        Examples:

            Count the number of metadata keys for events in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.events import EventProperty
                >>> client = CogniteClient()
                >>> type_count = client.events.aggregate_cardinality_properties(EventProperty.metadata)

        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "cardinalityProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    def aggregate_unique_properties(
        self,
        path: EventPropertyLike,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        filter: EventFilter | dict[str, Any] | None = None,
    ) -> UniqueResultList:
        """`Get unique paths with counts for events. <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            path (EventPropertyLike): The scope in every document to aggregate properties. The only value allowed now is ["metadata"].
                It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict[str, Any] | None): The filter to narrow down the events to count cardinality.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): The filter to apply to the resulting buckets.
            filter (EventFilter | dict[str, Any] | None): The filter to narrow down the events to count requiring exact match.

        Returns:
            UniqueResultList: List of unique values of events matching the specified filters and search.

        Examples:

            Get the unique metadata keys with count of events in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.events import EventProperty
                >>> client = CogniteClient()
                >>> result = client.events.aggregate_unique_properties(EventProperty.metadata)
                >>> print(result.unique)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            aggregate="uniqueProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    @overload
    def create(self, event: Sequence[Event] | Sequence[EventWrite]) -> EventList: ...

    @overload
    def create(self, event: Event | EventWrite) -> Event: ...

    def create(self, event: Event | EventWrite | Sequence[Event] | Sequence[EventWrite]) -> Event | EventList:
        """`Create one or more events. <https://developer.cognite.com/api#tag/Events/operation/createEvents>`_

        Args:
            event (Event | EventWrite | Sequence[Event] | Sequence[EventWrite]): Event or list of events to create.

        Returns:
            Event | EventList: Created event(s)

        Examples:

            Create new events::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import EventWrite
                >>> client = CogniteClient()
                >>> events = [EventWrite(start_time=0, end_time=1), EventWrite(start_time=2, end_time=3)]
                >>> res = client.events.create(events)
        """
        return self._create_multiple(list_cls=EventList, resource_cls=Event, items=event, input_resource_cls=EventWrite)

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more events <https://developer.cognite.com/api#tag/Events/operation/deleteEvents>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete events by id or external id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.events.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    def update(
        self,
        item: Sequence[Event | EventWrite | EventUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> EventList: ...

    @overload
    def update(
        self,
        item: Event | EventWrite | EventUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Event: ...

    def update(
        self,
        item: Event | EventWrite | EventUpdate | Sequence[Event | EventWrite | EventUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> Event | EventList:
        """`Update one or more events <https://developer.cognite.com/api#tag/Events/operation/updateEvents>`_

        Args:
            item (Event | EventWrite | EventUpdate | Sequence[Event | EventWrite | EventUpdate]): Event(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (Event or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            Event | EventList: Updated event(s)

        Examples:

            Update an event that you have fetched. This will perform a full update of the event::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> event = client.events.retrieve(id=1)
                >>> event.description = "New description"
                >>> res = client.events.update(event)

            Perform a partial update on a event, updating the description and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import EventUpdate
                >>> client = CogniteClient()
                >>> my_update = EventUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = client.events.update(my_update)
        """
        return self._update_multiple(
            list_cls=EventList, resource_cls=Event, update_cls=EventUpdate, items=item, mode=mode
        )

    def search(
        self,
        description: str | None = None,
        filter: EventFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> EventList:
        """`Search for events <https://developer.cognite.com/api#tag/Events/operation/searchEvents>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            description (str | None): Fuzzy match on description.
            filter (EventFilter | dict[str, Any] | None): Filter to apply. Performs exact match on these fields.
            limit (int): Maximum number of results to return.

        Returns:
            EventList: List of requested events

        Examples:

            Search for events::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.events.search(description="some description")
        """
        return self._search(list_cls=EventList, search={"description": description}, filter=filter or {}, limit=limit)

    @overload
    def upsert(self, item: Sequence[Event | EventWrite], mode: Literal["patch", "replace"] = "patch") -> EventList: ...

    @overload
    def upsert(self, item: Event | EventWrite, mode: Literal["patch", "replace"] = "patch") -> Event: ...

    def upsert(
        self, item: Event | EventWrite | Sequence[Event | EventWrite], mode: Literal["patch", "replace"] = "patch"
    ) -> Event | EventList:
        """Upsert events, i.e., update if it exists, and create if it does not exist.
            Note this is a convenience method that handles the upserting for you by first calling update on all items,
            and if any of them fail because they do not exist, it will create them instead.

            For more details, see :ref:`appendix-upsert`.

        Args:
            item (Event | EventWrite | Sequence[Event | EventWrite]): Event or list of events to upsert.
            mode (Literal['patch', 'replace']): Whether to patch or replace in the case the events are existing. If you set 'patch', the call will only update fields with non-null values (default). Setting 'replace' will unset any fields that are not specified.

        Returns:
            Event | EventList: The upserted event(s).

        Examples:

            Upsert for events:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Event
                >>> client = CogniteClient()
                >>> existing_event = client.events.retrieve(id=1)
                >>> existing_event.description = "New description"
                >>> new_event = Event(external_id="new_event", description="New event")
                >>> res = client.events.upsert([existing_event, new_event], mode="replace")
        """
        return self._upsert_multiple(
            item,
            list_cls=EventList,
            resource_cls=Event,
            update_cls=EventUpdate,
            input_resource_cls=Event,
            mode=mode,
        )

    def filter(
        self,
        filter: Filter | dict,
        sort: SortSpec | list[SortSpec] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> EventList:
        """`Advanced filter events <https://developer.cognite.com/api#tag/Events/operation/advancedListEvents>`_

        Advanced filter lets you create complex filtering expressions that combine simple operations,
        such as equals, prefix, exists, etc., using boolean operators and, or, and not.
        It applies to basic fields as well as metadata.

        Args:
            filter (Filter | dict): Filter to apply.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Can be up to two properties to sort by default to ascending order.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            EventList: List of events that match the filter criteria.

        Examples:

            Find all events that has external id with prefix "workorder" and the word 'failure' in the description,
            and sort by start time descending:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> client = CogniteClient()
                >>> is_workorder = filters.Prefix("external_id", "workorder")
                >>> has_failure = filters.Search("description", "failure")
                >>> res = client.events.filter(
                ...     filter=filters.And(is_workorder, has_failure), sort=("start_time", "desc"))

            Note that you can check the API documentation above to see which properties you can filter on
            with which filters.

            To make it easier to avoid spelling mistakes and easier to look up available properties
            for filtering and sorting, you can also use the `EventProperty` and `SortableEventProperty` enums.

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.events import EventProperty, SortableEventProperty
                >>> client = CogniteClient()
                >>> is_workorder = filters.Prefix(EventProperty.external_id, "workorder")
                >>> has_failure = filters.Search(EventProperty.description, "failure")
                >>> res = client.events.filter(
                ...     filter=filters.And(is_workorder, has_failure),
                ...     sort=(SortableEventProperty.start_time, "desc"))
        """
        warnings.warn(
            f"{self.__class__.__name__}.filter() method is deprecated and will be removed in the next major version of the SDK. Please use the {self.__class__.__name__}.list() method with advanced_filter parameter instead.",
            DeprecationWarning,
        )
        self._validate_filter(filter)

        return self._list(
            list_cls=EventList,
            resource_cls=Event,
            method="POST",
            limit=limit,
            advanced_filter=filter.dump(camel_case_property=True) if isinstance(filter, Filter) else filter,
            sort=prepare_filter_sort(sort, EventSort),
        )

    def _validate_filter(self, filter: Filter | dict[str, Any] | None) -> None:
        _validate_filter(filter, _FILTERS_SUPPORTED, type(self).__name__)

    def list(
        self,
        start_time: dict[str, Any] | TimestampRange | None = None,
        end_time: dict[str, Any] | EndTimeFilter | None = None,
        active_at_time: dict[str, Any] | TimestampRange | None = None,
        type: str | None = None,
        subtype: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
        partitions: int | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        advanced_filter: Filter | dict[str, Any] | None = None,
    ) -> EventList:
        """`List events <https://developer.cognite.com/api#tag/Events/operation/advancedListEvents>`_

        Args:
            start_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
            end_time (dict[str, Any] | EndTimeFilter | None): Range between two timestamps.
            active_at_time (dict[str, Any] | TimestampRange | None): Event is considered active from its startTime to endTime inclusive. If startTime is null, event is never active. If endTime is null, event is active from startTime onwards. activeAtTime filter will match all events that are active at some point from min to max, from min, or to max, depending on which of min and max parameters are specified.
            type (str | None): Type of the event, e.g 'failure'.
            subtype (str | None): Subtype of the event, e.g 'electrical'.
            metadata (dict[str, str] | None): Customizable extra data about the event. String key -> String value.
            asset_ids (Sequence[int] | None): Asset IDs of related equipments that this event relates to.
            asset_external_ids (SequenceNotStr[str] | None): Asset External IDs of related equipment that this event relates to.
            asset_subtree_ids (int | Sequence[int] | None): Only include events that have a related asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include events that have a related asset in a subtree rooted at any of these assetExternalIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
            data_set_ids (int | Sequence[int] | None): Return only events in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only events in the specified data set(s) with this external id / these external ids.
            source (str | None): The source of this event.
            created_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            external_id_prefix (str | None): External Id provided by client. Should be unique within the project.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Defaults to desc for `_score_` and asc for all other properties. Sort is not allowed if `partitions` is used.
            partitions (int | None): Retrieve resources in parallel using this number of workers (values up to 10 allowed), limit must be set to `None` (or `-1`).
            limit (int | None): Maximum number of events to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL (Domain Specific Language). It allows defining complex filtering expressions that combine simple operations, such as equals, prefix, exists, etc., using boolean operators and, or, and not. See examples below for usage.

        Returns:
            EventList: List of requested events

        .. note::
            When using `partitions`, there are few considerations to keep in mind:
                * `limit` has to be set to `None` (or `-1`).
                * API may reject requests if you specify more than 10 partitions. When Cognite enforces this behavior, the requests result in a 400 Bad Request status.
                * Partitions are done independently of sorting: there's no guarantee of the sort order between elements from different partitions. For this reason providing a `sort` parameter when using `partitions` is not allowed.


        Examples:

            List events and filter on max start time::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> event_list = client.events.list(limit=5, start_time={"max": 1500000000})

            Iterate over events::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for event in client.events:
                ...     event # do something with the event

            Iterate over chunks of events to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for event_list in client.events(chunk_size=2500):
                ...     event_list # do something with the events

            Using advanced filter, find all events that have a metadata key 'timezone' starting with 'Europe',
            and sort by external id ascending:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> client = CogniteClient()
                >>> in_timezone = filters.Prefix(["metadata", "timezone"], "Europe")
                >>> res = client.events.list(advanced_filter=in_timezone, sort=("external_id", "asc"))

            Note that you can check the API documentation above to see which properties you can filter on
            with which filters.

            To make it easier to avoid spelling mistakes and easier to look up available properties
            for filtering and sorting, you can also use the `EventProperty` and `SortableEventProperty` Enums.

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.events import EventProperty, SortableEventProperty
                >>> client = CogniteClient()
                >>> in_timezone = filters.Prefix(EventProperty.metadata_key("timezone"), "Europe")
                >>> res = client.events.list(
                ...     advanced_filter=in_timezone,
                ...     sort=(SortableEventProperty.external_id, "asc"))

            Combine filter and advanced filter:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> client = CogniteClient()
                >>> not_instrument_lvl5 = filters.And(
                ...    filters.ContainsAny("labels", ["Level5"]),
                ...    filters.Not(filters.ContainsAny("labels", ["Instrument"]))
                ... )
                >>> res = client.events.list(asset_subtree_ids=[123456], advanced_filter=not_instrument_lvl5)

        """
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = EventFilter(
            start_time=start_time,
            end_time=end_time,
            active_at_time=active_at_time,
            metadata=metadata,
            asset_ids=asset_ids,
            asset_external_ids=asset_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            source=source,
            data_set_ids=data_set_ids_processed,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            type=type,
            subtype=subtype,
        ).dump(camel_case=True)

        prep_sort = prepare_filter_sort(sort, EventSort)
        self._validate_filter(advanced_filter)

        return self._list(
            list_cls=EventList,
            resource_cls=Event,
            method="POST",
            limit=limit,
            filter=filter,
            advanced_filter=advanced_filter,
            partitions=partitions,
            sort=prep_sort,
        )
