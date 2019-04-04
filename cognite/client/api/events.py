from typing import *
from typing import List

from cognite.client._utils.api_client import APIClient
from cognite.client._utils.base import CogniteFilter, CogniteResource, CogniteResourceList, CogniteUpdate


# GenClass: Event
class Event(CogniteResource):
    """An event represents something that happened at a given interval in time, e.g a failure, a work order etc.

    Args:
        external_id (str): External Id provided by client. Should be unique within the project.
        start_time (int): It is the number of seconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time (int): It is the number of seconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        description (str): Textual description of the event.
        metadata (Dict[str, Any]): Customizable extra data about the event. String key -> String value.
        asset_ids (List[int]): Asset IDs of related equipments that this event relates to.
        source (str): The source of this event.
        id (int): Javascript friendly internal ID given to the object.
        last_updated_time (int): It is the number of seconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
    """

    def __init__(
        self,
        external_id: str = None,
        start_time: int = None,
        end_time: int = None,
        description: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        source: str = None,
        id: int = None,
        last_updated_time: int = None,
    ):
        self.external_id = external_id
        self.start_time = start_time
        self.end_time = end_time
        self.description = description
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.source = source
        self.id = id
        self.last_updated_time = last_updated_time

    # GenStop


class EventList(CogniteResourceList):
    _RESOURCE = Event


# GenClass: EventFilter.filter
class EventFilter(CogniteFilter):
    """No description.

    Args:
        start_time (Dict[str, Any]): Range between two timestamps
        end_time (Dict[str, Any]): Range between two timestamps
        metadata (Dict[str, Any]): Customizable extra data about the event. String key -> String value.
        asset_ids (List[int]): Asset IDs of related equipments that this event relates to.
        asset_subtrees (List[int]): Filter out events that are not linked to assets in the subtree rooted at these assets.
        source (str): The source of this event.
        created_time (Dict[str, Any]): Range between two timestamps
        last_updated_time (Dict[str, Any]): Range between two timestamps
        external_id_prefix (str): External Id provided by client. Should be unique within the project.
    """

    def __init__(
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
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.asset_subtrees = asset_subtrees
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.external_id_prefix = external_id_prefix

    # GenStop


# GenUpdateClass: EventChange
class EventUpdate(CogniteUpdate):
    """Changes will be applied to event.

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project.
    """

    def __init__(self, id: int = None, external_id: str = None):
        self.id = id
        self.external_id = external_id
        self._update_object = {}

    def external_id_set(self, value: Union[str, None]):
        if value is None:
            self._update_object["externalId"] = {"setNull": True}
            return self
        self._update_object["externalId"] = {"set": value}
        return self

    def start_time_set(self, value: Union[int, None]):
        if value is None:
            self._update_object["startTime"] = {"setNull": True}
            return self
        self._update_object["startTime"] = {"set": value}
        return self

    def end_time_set(self, value: Union[int, None]):
        if value is None:
            self._update_object["endTime"] = {"setNull": True}
            return self
        self._update_object["endTime"] = {"set": value}
        return self

    def description_set(self, value: Union[str, None]):
        if value is None:
            self._update_object["description"] = {"setNull": True}
            return self
        self._update_object["description"] = {"set": value}
        return self

    def metadata_add(self, value: Dict[str, Any]):
        self._update_object["metadata"] = {"add": value}
        return self

    def metadata_remove(self, value: List):
        self._update_object["metadata"] = {"remove": value}
        return self

    def metadata_set(self, value: Union[Dict[str, Any], None]):
        if value is None:
            self._update_object["metadata"] = {"setNull": True}
            return self
        self._update_object["metadata"] = {"set": value}
        return self

    def asset_ids_add(self, value: List):
        self._update_object["assetIds"] = {"add": value}
        return self

    def asset_ids_remove(self, value: List):
        self._update_object["assetIds"] = {"remove": value}
        return self

    def asset_ids_set(self, value: Union[List, None]):
        if value is None:
            self._update_object["assetIds"] = {"setNull": True}
            return self
        self._update_object["assetIds"] = {"set": value}
        return self

    def source_set(self, value: Union[str, None]):
        if value is None:
            self._update_object["source"] = {"setNull": True}
            return self
        self._update_object["source"] = {"set": value}
        return self

    # GenStop


class EventsAPI(APIClient):
    RESOURCE_PATH = "/events"

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
        return self._list_generator(
            EventList, resource_path=self.RESOURCE_PATH, method="POST", chunk_size=chunk_size, filter=filter
        )

    def __iter__(self) -> Generator[Event, None, None]:
        """Iterate over events

        Fetches events as they are iterated over, so you keep a limited number of events in memory.

        Yields:
            Event: yields Events one by one.
        """
        return self.__call__()

    def get(
        self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None
    ) -> Union[Event, EventList]:
        """Get events by id

        Args:
            id (Union[int, List[int], optional): Id or list of ids
            external_id (Union[str, List[str]], optional): External ID or list of external ids

        Returns:
            Union[Event, EventList]: Requested event(s)
        """
        return self._retrieve_multiple(EventList, self.RESOURCE_PATH, ids=id, external_ids=external_id, wrap_ids=True)

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
        limit: int = None,
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
            limit (int, optional): Maximum number of events to return. If not specified, all events will be returned.

        Returns:
            EventList: List of requested events
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
        return self._list(EventList, resource_path=self.RESOURCE_PATH, method="POST", limit=limit, filter=filter)

    def create(self, event: Union[Event, List[Event]]) -> Union[Event, EventList]:
        """Create one or more events.

        Args:
            event (Union[Event, List[Event]]): Event or list of events to create.

        Returns:
            Union[Event, EventList]: Created event(s)
        """
        return self._create_multiple(EventList, resource_path=self.RESOURCE_PATH, items=event)

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """Delete one or more events

        Args:
            id (Union[int, List[int]): Id or list of ids
            external_id (Union[str, List[str]]): External ID or list of external ids

        Returns:
            None
        """
        self._delete_multiple(resource_path=self.RESOURCE_PATH, ids=id, external_ids=external_id, wrap_ids=True)

    def update(self, item: Union[Event, EventUpdate, List[Union[Event, EventUpdate]]]) -> Union[Event, EventList]:
        """Update one or more events

        Args:
            item (Union[Event, EventUpdate, List[Union[Event, EventUpdate]]]): Event(s) to update

        Returns:
            Union[Event, EventList]: Updated event(s)
        """
        return self._update_multiple(cls=EventList, resource_path=self.RESOURCE_PATH, items=item)

    def search(self, description: str = None, filter: EventFilter = None, limit: int = None) -> EventList:
        """Search for events

        Args:
            description (str): Fuzzy match on description.
            filter (EventFilter): Filter to apply. Performs exact match on these fields.
            limit (int): Maximum number of results to return.

        Returns:
            EventList: List of requested events
        """
        filter = filter.dump(camel_case=True) if filter else None
        return self._search(
            cls=EventList,
            resource_path=self.RESOURCE_PATH,
            json={"search": {"description": description}, "filter": filter, "limit": limit},
        )
