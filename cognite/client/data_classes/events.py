from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Sequence, Union, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)
from cognite.client.data_classes.shared import TimestampRange

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class EndTimeFilter(dict):
    """Either range between two timestamps or isNull filter condition.

    Args:
        max (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        min (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        is_null (bool): Set to true if you want to search for data with field value not set, false to search for cases where some value is present.
    """

    def __init__(self, max: int = None, min: int = None, is_null: bool = None, **kwargs: Any) -> None:
        self.max = max
        self.min = min
        self.is_null = is_null
        self.update(kwargs)

    max = CognitePropertyClassUtil.declare_property("max")
    min = CognitePropertyClassUtil.declare_property("min")
    is_null = CognitePropertyClassUtil.declare_property("isNull")


class Event(CogniteResource):
    """An event represents something that happened at a given interval in time, e.g a failure, a work order etc.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        data_set_id (int): The id of the dataset this event belongs to.
        start_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        type (str): Type of the event, e.g 'failure'.
        subtype (str): SubType of the event, e.g 'electrical'.
        description (str): Textual description of the event.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
        asset_ids (Sequence[int]): Asset IDs of equipment that this event relates to.
        source (str): The source of this event.
        id (int): A server-generated ID for the object.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        data_set_id: int = None,
        start_time: int = None,
        end_time: int = None,
        type: str = None,
        subtype: str = None,
        description: str = None,
        metadata: Dict[str, str] = None,
        asset_ids: Sequence[int] = None,
        source: str = None,
        id: int = None,
        last_updated_time: int = None,
        created_time: int = None,
        cognite_client: CogniteClient = None,
    ):
        self.external_id = external_id
        self.data_set_id = data_set_id
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
        self._cognite_client = cast("CogniteClient", cognite_client)


class EventFilter(CogniteFilter):
    """Filter on events filter with exact match

    Args:
        start_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        end_time (Union[Dict[str, Any], EndTimeFilter]): Either range between two timestamps or isNull filter condition.
        active_at_time (Union[Dict[str, Any], TimestampRange]): Event is considered active from its startTime to endTime inclusive. If startTime is null, event is never active. If endTime is null, event is active from startTime onwards. activeAtTime filter will match all events that are active at some point from min to max, from min, or to max, depending on which of min and max parameters are specified.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
        asset_ids (Sequence[int]): Asset IDs of equipment that this event relates to.
        asset_external_ids (Sequence[str]): Asset External IDs of equipment that this event relates to.
        asset_subtree_ids (Sequence[Dict[str, Any]]): Only include events that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        data_set_ids (Sequence[Dict[str, Any]]): Only include events that belong to these datasets.
        source (str): The source of this event.
        type (str): Type of the event, e.g 'failure'.
        subtype (str): SubType of the event, e.g 'electrical'.
        created_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        last_updated_time (Union[Dict[str, Any], TimestampRange]): Range between two timestamps.
        external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        start_time: Union[Dict[str, Any], TimestampRange] = None,
        end_time: Union[Dict[str, Any], EndTimeFilter] = None,
        active_at_time: Union[Dict[str, Any], TimestampRange] = None,
        metadata: Dict[str, str] = None,
        asset_ids: Sequence[int] = None,
        asset_external_ids: Sequence[str] = None,
        asset_subtree_ids: Sequence[Dict[str, Any]] = None,
        data_set_ids: Sequence[Dict[str, Any]] = None,
        source: str = None,
        type: str = None,
        subtype: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        external_id_prefix: str = None,
        cognite_client: CogniteClient = None,
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.active_at_time = active_at_time
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.asset_external_ids = asset_external_ids
        self.asset_subtree_ids = asset_subtree_ids
        self.data_set_ids = data_set_ids
        self.source = source
        self.type = type
        self.subtype = subtype
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.external_id_prefix = external_id_prefix
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: Union[Dict, str]) -> EventFilter:
        instance = super()._load(resource)
        if isinstance(resource, Dict):
            if instance.start_time is not None:
                instance.start_time = TimestampRange(**instance.start_time)
            if instance.end_time is not None:
                instance.end_time = EndTimeFilter(**instance.end_time)
            if instance.active_at_time is not None:
                instance.active_at_time = TimestampRange(**instance.active_at_time)
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance


class EventUpdate(CogniteUpdate):
    """Changes will be applied to event.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

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

    class _LabelEventUpdate(CogniteLabelUpdate):
        def add(self, value: List) -> EventUpdate:
            return self._add(value)

        def remove(self, value: List) -> EventUpdate:
            return self._remove(value)

    @property
    def external_id(self) -> _PrimitiveEventUpdate:
        return EventUpdate._PrimitiveEventUpdate(self, "externalId")

    @property
    def data_set_id(self) -> _PrimitiveEventUpdate:
        return EventUpdate._PrimitiveEventUpdate(self, "dataSetId")

    @property
    def start_time(self) -> _PrimitiveEventUpdate:
        return EventUpdate._PrimitiveEventUpdate(self, "startTime")

    @property
    def end_time(self) -> _PrimitiveEventUpdate:
        return EventUpdate._PrimitiveEventUpdate(self, "endTime")

    @property
    def description(self) -> _PrimitiveEventUpdate:
        return EventUpdate._PrimitiveEventUpdate(self, "description")

    @property
    def metadata(self) -> _ObjectEventUpdate:
        return EventUpdate._ObjectEventUpdate(self, "metadata")

    @property
    def asset_ids(self) -> _ListEventUpdate:
        return EventUpdate._ListEventUpdate(self, "assetIds")

    @property
    def source(self) -> _PrimitiveEventUpdate:
        return EventUpdate._PrimitiveEventUpdate(self, "source")

    @property
    def type(self) -> _PrimitiveEventUpdate:
        return EventUpdate._PrimitiveEventUpdate(self, "type")

    @property
    def subtype(self) -> _PrimitiveEventUpdate:
        return EventUpdate._PrimitiveEventUpdate(self, "subtype")


class EventList(CogniteResourceList):
    _RESOURCE = Event
