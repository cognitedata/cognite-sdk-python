from __future__ import annotations

import warnings
from typing import Any, Iterator, Literal, Sequence, Tuple, Union, cast, overload

from typing_extensions import TypeAlias

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    AggregateResult,
    AggregateUniqueValuesResult,
    EndTimeFilter,
    Event,
    EventFilter,
    EventList,
    EventUpdate,
    TimestampRange,
    filters,
)
from cognite.client.data_classes.aggregations import AggregationFilter, UniqueResultList
from cognite.client.data_classes.events import EventPropertyLike, EventSort, SortableEventProperty
from cognite.client.data_classes.filters import Filter, _validate_filter
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._validation import prepare_filter_sort, process_asset_subtree_ids, process_data_set_ids

SortSpec: TypeAlias = Union[
    EventSort,
    str,
    SortableEventProperty,
    Tuple[str, Literal["asc", "desc"]],
    Tuple[str, Literal["asc", "desc"], Literal["auto", "first", "last"]],
]

_FILTERS_SUPPORTED: frozenset[type[Filter]] = frozenset(
    {
        filters.And,
        filters.Or,
        filters.Not,
        filters.In,
        filters.Equals,
        filters.Exists,
        filters.Range,
        filters.Prefix,
        filters.ContainsAny,
        filters.ContainsAll,
        filters.Search,
    }
)


class EventsAPI(APIClient):
    _RESOURCE_PATH = "/events"

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
        asset_external_ids: Sequence[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        sort: Sequence[str] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
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
            asset_external_ids (Sequence[str] | None): Asset External IDs of related equipment that this event relates to.
            asset_subtree_ids (int | Sequence[int] | None): Asset subtree id or list of asset subtree ids to filter on.
            asset_subtree_external_ids (str | Sequence[str] | None): Asset subtree external id or list of asset subtree external ids to filter on.
            data_set_ids (int | Sequence[int] | None): Return only events in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | Sequence[str] | None): Return only events in the specified data set(s) with this external id / these external ids.
            source (str | None): The source of this event.
            created_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            external_id_prefix (str | None): External Id provided by client. Should be unique within the project
            sort (Sequence[str] | None): Sort by array of selected fields. Ex: ["startTime:desc']. Default sort order is asc when omitted. Filter accepts following field names: startTime, endTime, createdTime, lastUpdatedTime. We only support 1 field for now.
            limit (int | None): Maximum number of events to return. Defaults to return all items.
            partitions (int | None): Retrieve assets in parallel using this number of workers. Also requires `limit=None` to be passed. To prevent unexpected problems and maximize read throughput, API documentation recommends at most use 10 partitions. When using more than 10 partitions, actual throughout decreases. In future releases of the APIs, CDF may reject requests with more than 10 partitions.

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
        return self._list_generator(
            list_cls=EventList,
            resource_cls=Event,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            limit=limit,
            sort=sort,
            partitions=partitions,
        )

    def __iter__(self) -> Iterator[Event]:
        """Iterate over events

        Fetches events as they are iterated over, so you keep a limited number of events in memory.

        Returns:
            Iterator[Event]: yields Events one by one.
        """
        return cast(Iterator[Event], self())

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
                >>> c = CogniteClient()
                >>> res = c.events.retrieve(id=1)

            Get event by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.events.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=EventList, resource_cls=Event, identifiers=identifiers)

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: Sequence[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> EventList:
        """`Retrieve multiple events by id. <https://developer.cognite.com/api#tag/Events/operation/byIdsEvents>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (Sequence[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            EventList: The requested events.

        Examples:

            Get events by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.events.retrieve_multiple(ids=[1, 2, 3])

            Get events by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.events.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=EventList, resource_cls=Event, identifiers=identifiers, ignore_unknown_ids=ignore_unknown_ids
        )

    def aggregate(self, filter: EventFilter | dict | None = None) -> list[AggregateResult]:
        """`Aggregate events <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            filter (EventFilter | dict | None): Filter on events filter with exact match

        Returns:
            list[AggregateResult]: List of event aggregates

        Examples:

            Aggregate events:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> aggregate_type = c.events.aggregate(filter={"type": "failure"})
        """
        return self._aggregate(filter=filter, cls=AggregateResult)

    @overload
    def aggregate_unique_values(
        self,
        fields: Sequence[str],
        filter: EventFilter | dict | None = None,
        property: EventPropertyLike | None = None,
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
    ) -> list[AggregateUniqueValuesResult]:
        ...

    @overload
    def aggregate_unique_values(
        self,
        fields: Literal[None] = None,
        filter: EventFilter | dict | None = None,
        property: EventPropertyLike | None = None,
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
    ) -> UniqueResultList:
        ...

    def aggregate_unique_values(
        self,
        fields: Sequence[str] | None = None,
        filter: EventFilter | dict | None = None,
        property: EventPropertyLike | None = None,
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
    ) -> list[AggregateUniqueValuesResult] | UniqueResultList:
        """`Get unique properties with counts for events. <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            fields (Sequence[str] | None): The fields to return. Defaults to ["count"].
            filter (EventFilter | dict | None): The filter to narrow down the events to count requiring exact match.
            property (EventPropertyLike | None): The property name(s) to apply the aggregation on.
            advanced_filter (Filter | dict | None): The filter to narrow down the events to consider.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.

        Returns:
            list[AggregateUniqueValuesResult] | UniqueResultList: List of unique values of events matching the specified filters and search.

        Examples:

        Get the unique types with count of events in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.events import EventProperty
            >>> c = CogniteClient()
            >>> result = c.events.aggregate_unique_values(EventProperty.type)
            >>> print(result.unique)

        Get the unique types of events after 2020-01-01 in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import filters
            >>> from cognite.client.data_classes.events import EventProperty
            >>> from cognite.client.utils import timestamp_to_ms
            >>> from datetime import datetime
            >>> c = CogniteClient()
            >>> is_after_2020 = filters.Range(EventProperty.start_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
            >>> result = c.events.aggregate_unique_values(EventProperty.type, advanced_filter=is_after_2020)
            >>> print(result.unique)

        Get the unique types of events after 2020-01-01 in your CDF project, but exclude all types that start with
        "planned":

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.events import EventProperty
            >>> from cognite.client.data_classes import aggregations
            >>> c = CogniteClient()
            >>> agg = aggregations
            >>> not_planned = agg.Not(agg.Prefix("planned"))
            >>> is_after_2020 = filters.Range(EventProperty.start_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
            >>> result = c.events.aggregate_unique_values(EventProperty.type, advanced_filter=is_after_2020, aggregate_filter=not_planned)
            >>> print(result.unique)

        """
        if fields is not None:
            warnings.warn(
                "Using of the parameter 'fields' is deprecated and will be removed in future versions of the SDK.",
                DeprecationWarning,
            )
            return self._aggregate(
                filter=filter, fields=fields, aggregate="uniqueValues", cls=AggregateUniqueValuesResult
            )
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
        advanced_filter: Filter | dict | None = None,
        filter: EventFilter | dict | None = None,
    ) -> int:
        """`Count of event matching the specified filters. <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            property (EventPropertyLike | None): If specified, Get an approximate number of Events with a specific property
                (property is not null) and matching the filters.
            advanced_filter (Filter | dict | None): The filter to narrow down the events to count.
            filter (EventFilter | dict | None): The filter to narrow down the events to count requiring exact match.

        Returns:
            int: The number of events matching the specified filters and search.

        Examples:

            Count the number of events in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> count = c.events.aggregate_count()

            Count the number of workorder events in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.events import EventProperty
                >>> c = CogniteClient()
                >>> is_workorder = filters.Equals(EventProperty.type, "workorder")
                >>> workorder_count = c.events.aggregate_count(advanced_filter=is_workorder)
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
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        filter: EventFilter | dict | None = None,
    ) -> int:
        """`Find approximate property count for events. <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            property (EventPropertyLike): The property to count the cardinality of.
            advanced_filter (Filter | dict | None): The filter to narrow down the events to count cardinality.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.
            filter (EventFilter | dict | None): The filter to narrow down the events to count requiring exact match.
        Returns:
            int: The number of properties matching the specified filter.

        Examples:

        Count the number of types of events in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.events import EventProperty
            >>> c = CogniteClient()
            >>> type_count = c.events.aggregate_cardinality_values(EventProperty.type)

        Count the number of types of events linked to asset 123 in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import filters
            >>> from cognite.client.data_classes.events import EventProperty
            >>> c = CogniteClient()
            >>> is_asset = filters.ContainsAny(EventProperty.asset_ids, 123)
            >>> plain_text_author_count = c.events.aggregate_cardinality_values(EventProperty.type, advanced_filter=is_asset)

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
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        filter: EventFilter | dict | None = None,
    ) -> int:
        """`Find approximate paths count for events. <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            path (EventPropertyLike): The scope in every document to aggregate properties. The only value allowed now is ["metadata"].
                It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict | None): The filter to narrow down the events to count cardinality.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.
            filter (EventFilter | dict | None): The filter to narrow down the events to count requiring exact match.
        Returns:
            int: The number of properties matching the specified filters and search.

        Examples:

            Count the number of metadata keys for events in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.events import EventProperty
                >>> c = CogniteClient()
                >>> type_count = c.events.aggregate_cardinality_properties(EventProperty.metadata)

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
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        filter: EventFilter | dict | None = None,
    ) -> UniqueResultList:
        """`Get unique paths with counts for events. <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            path (EventPropertyLike): The scope in every document to aggregate properties. The only value allowed now is ["metadata"].
                It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict | None): The filter to narrow down the events to count cardinality.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.
            filter (EventFilter | dict | None): The filter to narrow down the events to count requiring exact match.

        Returns:
            UniqueResultList: List of unique values of events matching the specified filters and search.

        Examples:

            Get the unique metadata keys with count of events in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.events import EventProperty
                >>> c = CogniteClient()
                >>> result = c.events.aggregate_unique_properties(EventProperty.metadata)
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
    def create(self, event: Sequence[Event]) -> EventList:
        ...

    @overload
    def create(self, event: Event) -> Event:
        ...

    def create(self, event: Event | Sequence[Event]) -> Event | EventList:
        """`Create one or more events. <https://developer.cognite.com/api#tag/Events/operation/createEvents>`_

        Args:
            event (Event | Sequence[Event]): Event or list of events to create.

        Returns:
            Event | EventList: Created event(s)

        Examples:

            Create new events::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Event
                >>> c = CogniteClient()
                >>> events = [Event(start_time=0, end_time=1), Event(start_time=2, end_time=3)]
                >>> res = c.events.create(events)
        """
        return self._create_multiple(list_cls=EventList, resource_cls=Event, items=event)

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more events <https://developer.cognite.com/api#tag/Events/operation/deleteEvents>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | Sequence[str] | None): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete events by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.events.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    def update(self, item: Sequence[Event | EventUpdate]) -> EventList:
        ...

    @overload
    def update(self, item: Event | EventUpdate) -> Event:
        ...

    def update(self, item: Event | EventUpdate | Sequence[Event | EventUpdate]) -> Event | EventList:
        """`Update one or more events <https://developer.cognite.com/api#tag/Events/operation/updateEvents>`_

        Args:
            item (Event | EventUpdate | Sequence[Event | EventUpdate]): Event(s) to update

        Returns:
            Event | EventList: Updated event(s)

        Examples:

            Update an event that you have fetched. This will perform a full update of the event::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> event = c.events.retrieve(id=1)
                >>> event.description = "New description"
                >>> res = c.events.update(event)

            Perform a partial update on a event, updating the description and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import EventUpdate
                >>> c = CogniteClient()
                >>> my_update = EventUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = c.events.update(my_update)
        """
        return self._update_multiple(list_cls=EventList, resource_cls=Event, update_cls=EventUpdate, items=item)

    def search(
        self,
        description: str | None = None,
        filter: EventFilter | dict | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> EventList:
        """`Search for events <https://developer.cognite.com/api#tag/Events/operation/searchEvents>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            description (str | None): Fuzzy match on description.
            filter (EventFilter | dict | None): Filter to apply. Performs exact match on these fields.
            limit (int): Maximum number of results to return.

        Returns:
            EventList: List of requested events

        Examples:

            Search for events::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.events.search(description="some description")
        """
        return self._search(list_cls=EventList, search={"description": description}, filter=filter or {}, limit=limit)

    @overload
    def upsert(self, item: Sequence[Event], mode: Literal["patch", "replace"] = "patch") -> EventList:
        ...

    @overload
    def upsert(self, item: Event, mode: Literal["patch", "replace"] = "patch") -> Event:
        ...

    def upsert(self, item: Event | Sequence[Event], mode: Literal["patch", "replace"] = "patch") -> Event | EventList:
        """Upsert events, i.e., update if it exists, and create if it does not exist.
            Note this is a convenience method that handles the upserting for you by first calling update on all items,
            and if any of them fail because they do not exist, it will create them instead.

            For more details, see :ref:`appendix-upsert`.

        Args:
            item (Event | Sequence[Event]): Event or list of events to upsert.
            mode (Literal["patch", "replace"]): Whether to patch or replace in the case the events are existing. If you set 'patch', the call will only update fields with non-null values (default). Setting 'replace' will unset any fields that are not specified.

        Returns:
            Event | EventList: The upserted event(s).

        Examples:

            Upsert for events:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Event
                >>> c = CogniteClient()
                >>> existing_event = c.events.retrieve(id=1)
                >>> existing_event.description = "New description"
                >>> new_event = Event(external_id="new_event", description="New event")
                >>> res = c.events.upsert([existing_event, new_event], mode="replace")
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
                >>> c = CogniteClient()
                >>> f = filters
                >>> is_workorder = f.Prefix("external_id", "workorder")
                >>> has_failure = f.Search("description", "failure")
                >>> res = c.events.filter(filter=f.And(is_workorder, has_failure),
                ...                       sort=("start_time", "desc"))

            Note that you can check the API documentation above to see which properties you can filter on
            with which filters.

            To make it easier to avoid spelling mistakes and easier to look up available properties
            for filtering and sorting, you can also use the `EventProperty` and `SortableEventProperty` enums.

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.events import EventProperty, SortableEventProperty
                >>> c = CogniteClient()
                >>> f = filters
                >>> is_workorder = f.Prefix(EventProperty.external_id, "workorder")
                >>> has_failure = f.Search(EventProperty.description, "failure")
                >>> res = c.events.filter(filter=f.And(is_workorder, has_failure),
                ...                       sort=(SortableEventProperty.start_time, "desc"))
        """
        self._validate_filter(filter)

        return self._list(
            list_cls=EventList,
            resource_cls=Event,
            method="POST",
            limit=limit,
            advanced_filter=filter.dump(camel_case=True) if isinstance(filter, Filter) else filter,
            sort=prepare_filter_sort(sort, EventSort),
        )

    def _validate_filter(self, filter: Filter | dict | None) -> None:
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
        asset_external_ids: Sequence[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
        sort: Sequence[str] | None = None,
        partitions: int | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
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
            asset_external_ids (Sequence[str] | None): Asset External IDs of related equipment that this event relates to.
            asset_subtree_ids (int | Sequence[int] | None): Asset subtree id or list of asset subtree ids to filter on.
            asset_subtree_external_ids (str | Sequence[str] | None): Asset subtree external id or list of asset subtree external ids to filter on.
            data_set_ids (int | Sequence[int] | None): Return only events in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | Sequence[str] | None): Return only events in the specified data set(s) with this external id / these external ids.
            source (str | None): The source of this event.
            created_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            external_id_prefix (str | None): External Id provided by client. Should be unique within the project.
            sort (Sequence[str] | None): Sort by array of selected fields. Ex: ["startTime:desc']. Default sort order is asc when omitted. Filter accepts following field names: startTime, endTime, createdTime, lastUpdatedTime. We only support 1 field for now.
            partitions (int | None): Retrieve events in parallel using this number of workers. Also requires `limit=None` to be passed. To prevent unexpected problems and maximize read throughput, API documentation recommends at most use 10 partitions. When using more than 10 partitions, actual throughout decreases. In future releases of the APIs, CDF may reject requests with more than 10 partitions.
            limit (int | None): Maximum number of events to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            EventList: List of requested events

        Examples:

            List events and filter on max start time::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> event_list = c.events.list(limit=5, start_time={"max": 1500000000})

            Iterate over events::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for event in c.events:
                ...     event # do something with the event

            Iterate over chunks of events to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for event_list in c.events(chunk_size=2500):
                ...     event_list # do something with the events
        """
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        if end_time and ("max" in end_time or "min" in end_time) and "isNull" in end_time:
            raise ValueError("isNull cannot be used with min or max values")

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
        return self._list(
            list_cls=EventList,
            resource_cls=Event,
            method="POST",
            limit=limit,
            filter=filter,
            partitions=partitions,
            sort=sort,
        )
