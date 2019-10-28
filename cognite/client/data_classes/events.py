from typing import *

from cognite.client.data_classes._base import *
from cognite.client.data_classes.shared import TimestampRange


# GenClass: Event
class Event(CogniteResource):
    """An event represents something that happened at a given interval in time, e.g a failure, a work order etc.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        start_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        type (str): Type of the event, e.g 'failure'.
        subtype (str): Subtype of the event, e.g 'electrical'.
        description (str): Textual description of the event.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        asset_ids (List[int]): Asset IDs of related equipment that this event relates to.
        source (str): The source of this event.
        id (int): A server-generated ID for the object.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
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
        metadata: Dict[str, str] = None,
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
        start_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        end_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        asset_ids (List[int]): Asset IDs of related equipment that this event relates to.
        root_asset_ids (List[Dict[str, Any]]): The IDs of the root assets that the related assets should be children of.
        source (str): The source of this event.
        type (str): The event type
        subtype (str): The event subtype
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        start_time: Union[Dict[str, Any], TimestampRange] = None,
        end_time: Union[Dict[str, Any], TimestampRange] = None,
        metadata: Dict[str, str] = None,
        asset_ids: List[int] = None,
        root_asset_ids: List[Dict[str, Any]] = None,
        source: str = None,
        type: str = None,
        subtype: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        cognite_client=None,
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.root_asset_ids = root_asset_ids
        self.source = source
        self.type = type
        self.subtype = subtype
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.external_id_prefix = external_id_prefix
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(EventFilter, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if instance.start_time is not None:
                instance.start_time = TimestampRange(**instance.start_time)
            if instance.end_time is not None:
                instance.end_time = TimestampRange(**instance.end_time)
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance

    # GenStop


# GenUpdateClass: EventChange
class EventUpdate(CogniteUpdate):
    """Changes will be applied to event.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
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
