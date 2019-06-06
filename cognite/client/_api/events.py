from typing import *

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Event, EventFilter, EventList, EventUpdate


class EventsAPI(APIClient):
    _RESOURCE_PATH = "/events"
    _LIST_CLASS = EventList

    def __call__(
        self,
        chunk_size: int = None,
        start_time: Dict[str, Any] = None,
        end_time: Dict[str, Any] = None,
        type: str = None,
        subtype: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
        limit: int = None,
    ) -> Generator[Union[Event, EventList], None, None]:
        """Iterate over events

        Fetches events as they are iterated over, so you keep a limited number of events in memory.

        Args:
            chunk_size (int, optional): Number of events to return in each chunk. Defaults to yielding one event a time.
            start_time (Dict[str, Any]): Range between two timestamps
            end_time (Dict[str, Any]): Range between two timestamps
            type (str): Type of the event, e.g 'failure'.
            subtype (str): Subtype of the event, e.g 'electrical'.
            metadata (Dict[str, Any]): Customizable extra data about the event. String key -> String value.
            asset_ids (List[int]): Asset IDs of related equipments that this event relates to.
            source (str): The source of this event.
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            external_id_prefix (str): External Id provided by client. Should be unique within the project
            limit (int, optional): Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Yields:
            Union[Event, EventList]: yields Event one by one if chunk is not specified, else EventList objects.
        """
        filter = EventFilter(
            start_time=start_time,
            end_time=end_time,
            metadata=metadata,
            asset_ids=asset_ids,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            type=type,
            subtype=subtype,
        ).dump(camel_case=True)
        return self._list_generator(method="POST", chunk_size=chunk_size, filter=filter, limit=limit)

    def __iter__(self) -> Generator[Event, None, None]:
        """Iterate over events

        Fetches events as they are iterated over, so you keep a limited number of events in memory.

        Yields:
            Event: yields Events one by one.
        """
        return self.__call__()

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[Event]:
        """Retrieve a single event by id.

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
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def retrieve_multiple(self, ids: Optional[List[int]] = None, external_ids: Optional[List[str]] = None) -> EventList:
        """Retrieve multiple events by id.

        Args:
            ids (List[int], optional): IDs
            external_ids (List[str], optional): External IDs

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
        utils._auxiliary.assert_type(ids, "id", [List], allow_none=True)
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=True)
        return self._retrieve_multiple(ids=ids, external_ids=external_ids, wrap_ids=True)

    def list(
        self,
        start_time: Dict[str, Any] = None,
        end_time: Dict[str, Any] = None,
        type: str = None,
        subtype: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
        limit: int = 25,
    ) -> EventList:
        """List events

        Args:
            start_time (Dict[str, Any]): Range between two timestamps
            end_time (Dict[str, Any]): Range between two timestamps
            type (str): Type of the event, e.g 'failure'.
            subtype (str): Subtype of the event, e.g 'electrical'.
            metadata (Dict[str, Any]): Customizable extra data about the event. String key -> String value.
            asset_ids (List[int]): Asset IDs of related equipments that this event relates to.
            source (str): The source of this event.
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            external_id_prefix (str): External Id provided by client. Should be unique within the project
            limit (int, optional): Maximum number of events to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            EventList: List of requested events

        Examples:

            List events and filter on max start time::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> file_list = c.events.list(limit=5, start_time={"max": 1500000000})

            Iterate over events::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for event in c.events:
                ...     event # do something with the event

            Iterate over chunks of events to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for event_list in c.events(chunk_size=2500):
                ...     event_list # do something with the files
        """
        filter = EventFilter(
            start_time=start_time,
            end_time=end_time,
            metadata=metadata,
            asset_ids=asset_ids,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            external_id_prefix=external_id_prefix,
            type=type,
            subtype=subtype,
        ).dump(camel_case=True)
        return self._list(method="POST", limit=limit, filter=filter)

    def create(self, event: Union[Event, List[Event]]) -> Union[Event, EventList]:
        """Create one or more events.

        Args:
            event (Union[Event, List[Event]]): Event or list of events to create.

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
        return self._create_multiple(items=event)

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """Delete one or more events

        Args:
            id (Union[int, List[int]): Id or list of ids
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            None
        Examples:

            Delete events by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.events.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def update(self, item: Union[Event, EventUpdate, List[Union[Event, EventUpdate]]]) -> Union[Event, EventList]:
        """Update one or more events

        Args:
            item (Union[Event, EventUpdate, List[Union[Event, EventUpdate]]]): Event(s) to update

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
        return self._update_multiple(items=item)

    def search(self, description: str = None, filter: Union[EventFilter, Dict] = None, limit: int = None) -> EventList:
        """Search for events

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
        return self._search(search={"description": description}, filter=filter, limit=limit)
