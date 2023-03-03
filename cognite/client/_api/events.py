from typing import Any, Dict, Iterator, List, Optional, Sequence, Union, cast, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    AggregateResult,
    AggregateUniqueValuesResult,
    EndTimeFilter,
    Event,
    EventFilter,
    EventList,
    EventUpdate,
    TimestampRange,
)
from cognite.client.utils._identifier import IdentifierSequence


class EventsAPI(APIClient):
    _RESOURCE_PATH = "/events"

    def __call__(
        self,
        chunk_size: int = None,
        start_time: Union[Dict[str, Any], TimestampRange] = None,
        end_time: Union[Dict[str, Any], EndTimeFilter] = None,
        active_at_time: Union[Dict[str, Any], TimestampRange] = None,
        type: str = None,
        subtype: str = None,
        metadata: Dict[str, str] = None,
        asset_ids: Sequence[int] = None,
        asset_external_ids: Sequence[str] = None,
        asset_subtree_ids: Sequence[int] = None,
        asset_subtree_external_ids: Sequence[str] = None,
        data_set_ids: Sequence[int] = None,
        data_set_external_ids: Sequence[str] = None,
        source: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        sort: Sequence[str] = None,
        limit: int = None,
        partitions: int = None,
    ) -> Union[Iterator[Event], Iterator[EventList]]:
        """Iterate over events

        Fetches events as they are iterated over, so you keep a limited number of events in memory.

        Args:
            chunk_size (int, optional): Number of events to return in each chunk. Defaults to yielding one event a time.
            start_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps
            end_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps
            active_at_time (Union[Dict[str, Any], TimestampRange]): Event is considered active from its startTime to endTime inclusive. If startTime is null, event is never active. If endTime is null, event is active from startTime onwards. activeAtTime filter will match all events that are active at some point from min to max, from min, or to max, depending on which of min and max parameters are specified.
            type (str): Type of the event, e.g 'failure'.
            subtype (str): Subtype of the event, e.g 'electrical'.
            metadata (Dict[str, str]): Customizable extra data about the event. String key -> String value.
            asset_ids (Sequence[int]): Asset IDs of related equipments that this event relates to.
            asset_external_ids (Sequence[str]): Asset External IDs of related equipment that this event relates to.
            asset_subtree_ids (Sequence[int]): List of asset subtrees ids to filter on.
            asset_subtree_external_ids (Sequence[str]): List of asset subtrees external ids to filter on.
            data_set_ids (Sequence[int]): Return only events in the specified data sets with these ids.
            data_set_external_ids (Sequence[str]): Return only events in the specified data sets with these external ids.
            source (str): The source of this event.
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            external_id_prefix (str): External Id provided by client. Should be unique within the project
            sort (Sequence[str]): Sort by array of selected fields. Ex: ["startTime:desc']. Default sort order is asc when ommitted. Filter accepts following field names: startTime, endTime, createdTime, lastUpdatedTime. We only support 1 field for now.
            limit (int, optional): Maximum number of events to return. Defaults to return all items.
            partitions (int): Retrieve assets in parallel using this number of workers. Also requires `limit=None` to be passed.

        Yields:
            Union[Event, EventList]: yields Event one by one if chunk is not specified, else EventList objects.
        """
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()

        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()

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

        Yields:
            Event: yields Events one by one.
        """
        return cast(Iterator[Event], self())

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[Event]:
        """`Retrieve a single event by id. <https://docs.cognite.com/api/v1/#operation/getEventByInternalId>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[Event]: Requested event or None if it does not exist.

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
        ids: Optional[Sequence[int]] = None,
        external_ids: Optional[Sequence[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> EventList:
        """`Retrieve multiple events by id. <https://docs.cognite.com/api/v1/#operation/byIdsEvents>`_

        Args:
            ids (Sequence[int], optional): IDs
            external_ids (Sequence[str], optional): External IDs
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

    def list(
        self,
        start_time: Union[Dict[str, Any], TimestampRange] = None,
        end_time: Union[Dict[str, Any], EndTimeFilter] = None,
        active_at_time: Union[Dict[str, Any], TimestampRange] = None,
        type: str = None,
        subtype: str = None,
        metadata: Dict[str, str] = None,
        asset_ids: Sequence[int] = None,
        asset_external_ids: Sequence[str] = None,
        asset_subtree_ids: Sequence[int] = None,
        asset_subtree_external_ids: Sequence[str] = None,
        data_set_ids: Sequence[int] = None,
        data_set_external_ids: Sequence[str] = None,
        source: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        sort: Sequence[str] = None,
        partitions: int = None,
        limit: int = 25,
    ) -> EventList:
        """`List events <https://docs.cognite.com/api/v1/#operation/advancedListEvents>`_

        Args:
            start_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
            end_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
            active_at_time (Union[Dict[str, Any], TimestampRange]): Event is considered active from its startTime to endTime inclusive. If startTime is null, event is never active. If endTime is null, event is active from startTime onwards. activeAtTime filter will match all events that are active at some point from min to max, from min, or to max, depending on which of min and max parameters are specified.
            type (str): Type of the event, e.g 'failure'.
            subtype (str): Subtype of the event, e.g 'electrical'.
            metadata (Dict[str, str]): Customizable extra data about the event. String key -> String value.
            asset_ids (Sequence[int]): Asset IDs of related equipments that this event relates to.
            asset_external_ids (Sequence[str]): Asset External IDs of related equipment that this event relates to.
            asset_subtree_ids (Sequence[int]): List of asset subtrees ids to filter on.
            asset_subtree_external_ids (Sequence[str]): List of asset subtrees external ids to filter on.
            data_set_ids (Sequence[int]): Return only events in the specified data sets with these ids.
            data_set_external_ids (Sequence[str]): Return only events in the specified data sets with these external ids.
            source (str): The source of this event.
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            external_id_prefix (str): External Id provided by client. Should be unique within the project.
            sort (Sequence[str]): Sort by array of selected fields. Ex: ["startTime:desc']. Default sort order is asc when ommitted. Filter accepts following field names: startTime, endTime, createdTime, lastUpdatedTime. We only support 1 field for now.
            partitions (int): Retrieve events in parallel using this number of workers. Also requires `limit=None` to be passed.
            limit (int, optional): Maximum number of events to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

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
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()

        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()

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

    def aggregate(self, filter: Union[EventFilter, Dict] = None) -> List[AggregateResult]:
        """`Aggregate events <https://docs.cognite.com/api/v1/#operation/aggregateEvents>`_

        Args:
            filter (Union[EventFilter, Dict]): Filter on events filter with exact match

        Returns:
            List[AggregateResult]: List of event aggregates

        Examples:

            Aggregate events:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> aggregate_type = c.events.aggregate(filter={"type": "failure"})
        """

        return self._aggregate(filter=filter, cls=AggregateResult)

    def aggregate_unique_values(
        self, filter: Union[EventFilter, Dict] = None, fields: Sequence[str] = None
    ) -> List[AggregateUniqueValuesResult]:
        """`Aggregate unique values for events <https://docs.cognite.com/api/v1/#operation/aggregateEvents>`_

        Args:
            filter (Union[EventFilter, Dict]): Filter on events filter with exact match
            fields (Sequence[str]): The field name(s) to apply the aggregation on. Currently limited to one field.

        Returns:
            List[AggregateUniqueValuesResult]: List of event aggregates

        Examples:

            Aggregate events:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> aggregate_subtype = c.events.aggregate_unique_values(filter={"type": "failure"}, fields=["subtype"])
        """

        return self._aggregate(filter=filter, fields=fields, aggregate="uniqueValues", cls=AggregateUniqueValuesResult)

    @overload
    def create(self, event: Sequence[Event]) -> EventList:
        ...

    @overload
    def create(self, event: Event) -> Event:
        ...

    def create(self, event: Union[Event, Sequence[Event]]) -> Union[Event, EventList]:
        """`Create one or more events. <https://docs.cognite.com/api/v1/#operation/createEvents>`_

        Args:
            event (Union[Event, Sequence[Event]]): Event or list of events to create.

        Returns:
            Union[Event, EventList]: Created event(s)

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
        id: Union[int, Sequence[int]] = None,
        external_id: Union[str, Sequence[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more events <https://docs.cognite.com/api/v1/#operation/deleteEvents>`_

        Args:
            id (Union[int, Sequence[int]): Id or list of ids
            external_id (Union[str, Sequence[str]]): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            None
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
    def update(self, item: Sequence[Union[Event, EventUpdate]]) -> EventList:
        ...

    @overload
    def update(self, item: Union[Event, EventUpdate]) -> Event:
        ...

    def update(self, item: Union[Event, EventUpdate, Sequence[Union[Event, EventUpdate]]]) -> Union[Event, EventList]:
        """`Update one or more events <https://docs.cognite.com/api/v1/#operation/updateEvents>`_

        Args:
            item (Union[Event, EventUpdate, Sequence[Union[Event, EventUpdate]]]): Event(s) to update

        Returns:
            Union[Event, EventList]: Updated event(s)

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

    def search(self, description: str = None, filter: Union[EventFilter, Dict] = None, limit: int = 100) -> EventList:
        """`Search for events <https://docs.cognite.com/api/v1/#operation/searchEvents>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            description (str): Fuzzy match on description.
            filter (Union[EventFilter, Dict]): Filter to apply. Performs exact match on these fields.
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
