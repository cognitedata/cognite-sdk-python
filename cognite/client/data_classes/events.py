from typing import *

from cognite.client.data_classes._base import *


# GenClass: Event
class Event(CogniteResource):
    """An event represents something that happened at a given interval in time, e.g a failure, a work order etc.

    Args:
        external_id (str): External Id provided by client. Should be unique within the project
        start_time (int): It is the number of milliseconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time (int): It is the number of milliseconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        type (str): Type of the event, e.g 'failure'.
        subtype (str): Subtype of the event, e.g 'electrical'.
        description (str): Textual description of the event.
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        asset_ids (List[int]): Asset IDs of related equipment that this event relates to.
        source (str): The source of this event.
        id (int): Javascript friendly internal ID given to the object.
        last_updated_time (int): It is the number of milliseconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): It is the number of milliseconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        start_time: int = None,
        end_time: int = None,
        type: str = None,
        subtype: str = None,
        description: str = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        source: str = None,
        id: int = None,
        last_updated_time: int = None,
        created_time: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.start_time = start_time
        self.end_time = end_time
        self.type = type
        self.subtype = subtype
        self.description = description
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.source = source
        self.id = id
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self._cognite_client = cognite_client

    # GenStop


# GenClass: EventFilter
class EventFilter(CogniteFilter):
    """Filter on events filter with exact match

    Args:
        start_time (Dict[str, Any]): Range between two timestamps
        end_time (Dict[str, Any]): Range between two timestamps
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        asset_ids (List[int]): Asset IDs of related equipment that this event relates to.
        source (str): The source of this event.
        type (str): The event type
        subtype (str): The event subtype
        created_time (Dict[str, Any]): Range between two timestamps
        last_updated_time (Dict[str, Any]): Range between two timestamps
        external_id_prefix (str): External Id provided by client. Should be unique within the project
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        start_time: Dict[str, Any] = None,
        end_time: Dict[str, Any] = None,
        metadata: Dict[str, Any] = None,
        asset_ids: List[int] = None,
        source: str = None,
        type: str = None,
        subtype: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        external_id_prefix: str = None,
        cognite_client=None,
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.source = source
        self.type = type
        self.subtype = subtype
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.external_id_prefix = external_id_prefix
        self._cognite_client = cognite_client

    # GenStop


# GenUpdateClass: EventChange
class EventUpdate(CogniteUpdate):
    """Changes will be applied to event.

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project
    """

    @property
    def external_id(self):
        return _PrimitiveEventUpdate(self, "externalId")

    @property
    def start_time(self):
        return _PrimitiveEventUpdate(self, "startTime")

    @property
    def end_time(self):
        return _PrimitiveEventUpdate(self, "endTime")

    @property
    def description(self):
        return _PrimitiveEventUpdate(self, "description")

    @property
    def metadata(self):
        return _ObjectEventUpdate(self, "metadata")

    @property
    def asset_ids(self):
        return _ListEventUpdate(self, "assetIds")

    @property
    def source(self):
        return _PrimitiveEventUpdate(self, "source")

    @property
    def type(self):
        return _PrimitiveEventUpdate(self, "type")

    @property
    def subtype(self):
        return _PrimitiveEventUpdate(self, "subtype")


class _PrimitiveEventUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> EventUpdate:
        return self._set(value)


class _ObjectEventUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> EventUpdate:
        return self._set(value)

    def add(self, value: Dict) -> EventUpdate:
        return self._add(value)

    def remove(self, value: List) -> EventUpdate:
        return self._remove(value)


class _ListEventUpdate(CogniteListUpdate):
    def set(self, value: List) -> EventUpdate:
        return self._set(value)

    def add(self, value: List) -> EventUpdate:
        return self._add(value)

    def remove(self, value: List) -> EventUpdate:
        return self._remove(value)

    # GenStop


class EventList(CogniteResourceList):
    _RESOURCE = Event
    _UPDATE = EventUpdate
