from typing import *

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Event, EventFilter, EventList, EventUpdate
from cognite.client.utils import _utils as utils


class EventsAPI(APIClient):
    _RESOURCE_PATH = "/events"
    _LIST_CLASS = EventList

    def __call__(
        self,
        chunk_size: int = None,
        start_time: Dict[str, Any] = None,
        end_time: Dict[str, Any] = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        asset_subtrees: List[int] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
    ) -> Generator[Union[Event, EventList], None, None]:
        """Iterate over events

        Fetches events as they are iterated over, so you keep a limited number of events in memory.

        Args:
            chunk_size (int, optional): Number of events to return in each chunk. Defaults to yielding one event a time.
            start_time (Dict[str, Any]): Range between two timestamps
            end_time (Dict[str, Any]): Range between two timestamps
            metadata (Dict[str, Any]): Customizable extra data about the event. String key -> String value.
            asset_ids (List[int]): Asset IDs of related equipments that this event relates to.
            asset_subtrees (List[int]): Filter out events that are not linked to assets in the subtree rooted at these assets.
            source (str): The source of this event.
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            external_id_prefix (str): External Id provided by client. Should be unique within the project

        Yields:
            Union[Event, EventList]: yields Event one by one if chunk is not specified, else EventList objects.
        """
        filter = EventFilter(
            start_time,
            end_time,
            metadata,
            asset_ids,
            asset_subtrees,
            source,
            created_time,
            last_updated_time,
            external_id_prefix,
        ).dump(camel_case=True)
        return self._list_generator(method="POST", chunk_size=chunk_size, filter=filter)

    def __iter__(self) -> Generator[Event, None, None]:
        """Iterate over events

        Fetches events as they are iterated over, so you keep a limited number of events in memory.

        Yields:
            Event: yields Events one by one.
        """
        return self.__call__()

    def retrieve(
        self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None
    ) -> Union[Event, EventList]:
        """Get events by id

        Args:
            id (Union[int, List[int], optional): Id or list of ids
            external_id (Union[str, List[str]], optional): External ID or list of external ids

        Returns:
            Union[Event, EventList]: Requested event(s)

        Examples:

            Get events by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.events.retrieve(id=[1,2,3])

            Get an event by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.events.retrieve(external_id="abc")
        """
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def list(
        self,
        start_time: Dict[str, Any] = None,
        end_time: Dict[str, Any] = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        asset_subtrees: List[int] = None,
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
            metadata (Dict[str, Any]): Customizable extra data about the event. String key -> String value.
            asset_ids (List[int]): Asset IDs of related equipments that this event relates to.
            asset_subtrees (List[int]): Filter out events that are not linked to assets in the subtree rooted at these assets.
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
            start_time,
            end_time,
            metadata,
            asset_ids,
            asset_subtrees,
            source,
            created_time,
            last_updated_time,
            external_id_prefix,
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
