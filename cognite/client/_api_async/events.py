from __future__ import annotations

import warnings
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any, Literal, TypeAlias, overload

from cognite.client._async_api_client import AsyncAPIClient
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


class AsyncEventsAPI(AsyncAPIClient):
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
    ) -> AsyncIterator[Event]: ...

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
    ) -> AsyncIterator[EventList]: ...

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
    ) -> AsyncIterator[Event] | AsyncIterator[EventList]:
        """Async iterator over events"""
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
            type=type,
            subtype=subtype,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)

        prep_sort = prepare_filter_sort(sort, EventSort)
        self._validate_filter(advanced_filter)

        return self._list_generator(
            list_cls=EventList,
            resource_cls=Event,
            method="POST",
            chunk_size=chunk_size,
            limit=limit,
            filter=filter,
            advanced_filter=advanced_filter,
            sort=prep_sort,
            partitions=partitions,
        )

    def __aiter__(self) -> AsyncIterator[Event]:
        """Async iterate over all events."""
        return self.__call__()

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> Event | None:
        """`Retrieve a single event by id. <https://developer.cognite.com/api#tag/Events/operation/byIdsEvents>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            Event | None: Requested event or None if it does not exist.

        Examples:

            Get event by id::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> res = await client.events.retrieve(id=1)

            Get event by external id::

                >>> res = await client.events.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=EventList,
            resource_cls=Event,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
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
            EventList: The retrieved events.

        Examples:

            Get events by id::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> res = await client.events.retrieve_multiple(ids=[1, 2, 3])

            Get events by external id::

                >>> res = await client.events.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids, external_ids)
        return await self._retrieve_multiple(
            list_cls=EventList,
            resource_cls=Event,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    async def aggregate(self, filter: EventFilter | dict[str, Any] | None = None) -> list[AggregateResult]:
        """`Aggregate events <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            filter (EventFilter | dict[str, Any] | None): Filter on events with exact match

        Returns:
            list[AggregateResult]: List of event aggregates

        Examples:

            Aggregate events::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> aggregate_type = await client.events.aggregate(filter={"type": "failure"})
        """

        return await self._aggregate(
            cls=AggregateResult,
            resource_path=self._RESOURCE_PATH,
            filter=filter,
        )

    async def aggregate_unique_values(
        self,
        property: EventPropertyLike,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> UniqueResultList:
        """`Get unique properties with counts for events <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            property (EventPropertyLike): The property to get unique values for.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): Aggregated filter applied to the result.
            limit (int | None): Maximum number of unique values to return.

        Returns:
            UniqueResultList: List of unique values with counts.
        """
        return await self._advanced_aggregate(
            aggregate="uniqueValues",
            properties=property,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            limit=limit,
        )

    async def aggregate_count(
        self,
        advanced_filter: Filter | dict[str, Any] | None = None,
    ) -> int:
        """`Count of events matching the specified filters. <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL.

        Returns:
            int: Count of events matching the specified filters.
        """
        return await self._advanced_aggregate(
            aggregate="count",
            advanced_filter=advanced_filter,
        )

    async def aggregate_cardinality_values(
        self,
        property: EventPropertyLike,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate property cardinality for events <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            property (EventPropertyLike): The property to count the cardinality of.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): Aggregated filter applied to the result.

        Returns:
            int: Approximate cardinality of property.
        """
        return await self._advanced_aggregate(
            aggregate="cardinalityValues",
            properties=property,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    async def aggregate_cardinality_properties(
        self,
        path: EventPropertyLike | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate paths cardinality for events <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            path (EventPropertyLike | None): The path to find the cardinality of.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): Aggregated filter applied to the result.

        Returns:
            int: Approximate cardinality of path.
        """
        return await self._advanced_aggregate(
            aggregate="cardinalityProperties",
            path=path,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    async def aggregate_unique_properties(
        self,
        path: EventPropertyLike | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> UniqueResultList:
        """`Get unique paths with counts for events <https://developer.cognite.com/api#tag/Events/operation/aggregateEvents>`_

        Args:
            path (EventPropertyLike | None): The path to get unique values for.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): Aggregated filter applied to the result.
            limit (int | None): Maximum number of unique values to return.

        Returns:
            UniqueResultList: List of unique paths with counts.
        """
        return await self._advanced_aggregate(
            aggregate="uniqueProperties",
            path=path,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            limit=limit,
        )

    @overload
    async def create(self, event: Sequence[Event] | Sequence[EventWrite]) -> EventList: ...

    @overload
    async def create(self, event: Event | EventWrite) -> Event: ...

    async def create(self, event: Event | EventWrite | Sequence[Event] | Sequence[EventWrite]) -> Event | EventList:
        """`Create one or more events. <https://developer.cognite.com/api#tag/Events/operation/createEvents>`_

        Args:
            event (Event | EventWrite | Sequence[Event] | Sequence[EventWrite]): Event or list of events to create.

        Returns:
            Event | EventList: Created event(s)

        Examples:

            Create new event::

                >>> from cognite.client import AsyncCogniteClient
                >>> from cognite.client.data_classes import Event
                >>> client = AsyncCogniteClient()
                >>> events = [Event(external_id="event1"), Event(external_id="event2")]
                >>> res = await client.events.create(events)
        """
        return await self._create_multiple(
            list_cls=EventList,
            resource_cls=Event,
            items=event,
        )

    async def delete(
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

        Returns:
            None

        Examples:

            Delete events by id or external id::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> await client.events.delete(id=[1,2,3], external_id="3")
        """
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(id, external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    async def update(self, item: Sequence[Event | EventUpdate]) -> EventList: ...

    @overload
    async def update(self, item: Event | EventUpdate) -> Event: ...

    async def update(self, item: Event | EventUpdate | Sequence[Event | EventUpdate]) -> Event | EventList:
        """`Update one or more events <https://developer.cognite.com/api#tag/Events/operation/updateEvents>`_

        Args:
            item (Event | EventUpdate | Sequence[Event | EventUpdate]): Event(s) to update

        Returns:
            Event | EventList: Updated event(s)

        Examples:

            Update an event that you have fetched. This will perform a full update of the event::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> event = await client.events.retrieve(id=1)
                >>> event.description = "New description"
                >>> res = await client.events.update(event)

            Perform a partial update on an event, updating the description and adding a new field to metadata::

                >>> from cognite.client.data_classes import EventUpdate
                >>> my_update = EventUpdate(id=1).description.set("New description").metadata.set({"key": "value"})
                >>> res = await client.events.update(my_update)
        """
        return await self._update_multiple(
            list_cls=EventList,
            resource_cls=Event,
            update_cls=EventUpdate,
            items=item,
        )

    @overload
    async def upsert(self, item: Sequence[Event | EventWrite], mode: Literal["patch", "replace"] = "patch") -> EventList: ...

    @overload 
    async def upsert(self, item: Event | EventWrite, mode: Literal["patch", "replace"] = "patch") -> Event: ...

    async def upsert(
        self,
        item: Event | EventWrite | Sequence[Event | EventWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> Event | EventList:
        """`Upsert events <https://developer.cognite.com/api#tag/Events/operation/createEvents>`_

        Args:
            item (Event | EventWrite | Sequence[Event | EventWrite]): Event or list of events to upsert.
            mode (Literal["patch", "replace"]): Whether to patch or replace in the case the events are existing.

        Returns:
            Event | EventList: The upserted event(s).

        Examples:

            Upsert for events::

                >>> from cognite.client import AsyncCogniteClient
                >>> from cognite.client.data_classes import Event
                >>> client = AsyncCogniteClient()
                >>> existing_event = await client.events.retrieve(id=1)
                >>> existing_event.description = "New description"
                >>> new_event = Event(external_id="new_event")
                >>> res = await client.events.upsert([existing_event, new_event], mode="replace")
        """
        return await self._upsert_multiple(
            items=item,
            list_cls=EventList,
            resource_cls=Event,
            update_cls=EventUpdate,
            mode=mode,
        )

    async def list(
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
            active_at_time (dict[str, Any] | TimestampRange | None): Event active time filter.
            type (str | None): Type of the event.
            subtype (str | None): Subtype of the event.
            metadata (dict[str, str] | None): Customizable extra data about the event.
            asset_ids (Sequence[int] | None): Asset IDs of related equipments.
            asset_external_ids (SequenceNotStr[str] | None): Asset External IDs of related equipment.
            asset_subtree_ids (int | Sequence[int] | None): Only include events that have a related asset in a subtree.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include events that have a related asset in a subtree.
            data_set_ids (int | Sequence[int] | None): Return only events in the specified data sets.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only events in the specified data sets.
            source (str | None): The source of this event.
            created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
            last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
            external_id_prefix (str | None): External Id provided by client.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by.
            partitions (int | None): Retrieve resources in parallel using this number of workers.
            limit (int | None): Maximum number of events to return.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL.

        Returns:
            EventList: List of requested events

        Examples:

            List events::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> event_list = await client.events.list(limit=5)

            Filter events by type::

                >>> event_list = await client.events.list(type="failure")
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
            type=type,
            subtype=subtype,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)

        prep_sort = prepare_filter_sort(sort, EventSort)
        self._validate_filter(advanced_filter)

        return await self._list(
            list_cls=EventList,
            resource_cls=Event,
            method="POST",
            limit=limit,
            filter=filter,
            advanced_filter=advanced_filter,
            sort=prep_sort,
            partitions=partitions,
        )

    async def search(
        self,
        description: str | None = None,
        query: str | None = None,
        filter: EventFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> EventList:
        """`Search for events <https://developer.cognite.com/api#tag/Events/operation/searchEvents>`_

        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and
        ordering may change over time. Use the `list` method for stable and performant iteration over all events.

        Args:
            description (str | None): Fuzzy match on description.
            query (str | None): Whitespace-separated terms to search for in events.
            filter (EventFilter | dict[str, Any] | None): Filter to apply.
            limit (int): Maximum number of results to return.

        Returns:
            EventList: Search results

        Examples:

            Search for events::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> res = await client.events.search(description="some description")
        """
        return await self._search(
            list_cls=EventList,
            search={
                "description": description,
                "query": query,
            },
            filter=filter or {},
            limit=limit,
        )

    async def filter(
        self,
        filter: Filter | dict,
        sort: SortSpec | list[SortSpec] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> EventList:
        """`Advanced filter events <https://developer.cognite.com/api#tag/Events/operation/advancedListEvents>`_

        Advanced filter lets you create complex filtering expressions that combine simple operations,
        such as equals, prefix, exists, etc., using boolean operators and, or, and not.

        Args:
            filter (Filter | dict): Filter to apply.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by.
            limit (int | None): Maximum number of results to return.

        Returns:
            EventList: List of events that match the filter criteria.
        """
        warnings.warn(
            f"{self.__class__.__name__}.filter() method is deprecated and will be removed in the next major version of the SDK. Please use the {self.__class__.__name__}.list() method with advanced_filter parameter instead.",
            DeprecationWarning,
        )
        self._validate_filter(filter)
        return await self._list(
            list_cls=EventList,
            resource_cls=Event,
            method="POST",
            limit=limit,
            advanced_filter=filter.dump(camel_case_property=True) if isinstance(filter, Filter) else filter,
            sort=prepare_filter_sort(sort, EventSort),
        )

    def _validate_filter(self, filter: Filter | dict[str, Any] | None) -> None:
        _validate_filter(filter, _FILTERS_SUPPORTED, type(self).__name__)