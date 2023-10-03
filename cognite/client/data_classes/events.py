from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Literal, Sequence, Union, cast

from typing_extensions import TypeAlias

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
    CogniteSort,
    CogniteUpdate,
    EnumProperty,
    IdTransformerMixin,
    PropertySpec,
)
from cognite.client.data_classes.shared import TimestampRange

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class EndTimeFilter(dict):
    """Either range between two timestamps or isNull filter condition.

    Args:
        max (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        min (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        is_null (bool | None): Set to true if you want to search for data with field value not set, false to search for cases where some value is present.
        **kwargs (Any): No description.
    """

    def __init__(
        self, max: int | None = None, min: int | None = None, is_null: bool | None = None, **kwargs: Any
    ) -> None:
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
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        data_set_id (int | None): The id of the dataset this event belongs to.
        start_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        type (str | None): Type of the event, e.g 'failure'.
        subtype (str | None): SubType of the event, e.g 'electrical'.
        description (str | None): Textual description of the event.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
        asset_ids (Sequence[int] | None): Asset IDs of equipment that this event relates to.
        source (str | None): The source of this event.
        id (int | None): A server-generated ID for the object.
        last_updated_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str | None = None,
        data_set_id: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        type: str | None = None,
        subtype: str | None = None,
        description: str | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        source: str | None = None,
        id: int | None = None,
        last_updated_time: int | None = None,
        created_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
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
        start_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        end_time (dict[str, Any] | EndTimeFilter | None): Either range between two timestamps or isNull filter condition.
        active_at_time (dict[str, Any] | TimestampRange | None): Event is considered active from its startTime to endTime inclusive. If startTime is null, event is never active. If endTime is null, event is active from startTime onwards. activeAtTime filter will match all events that are active at some point from min to max, from min, or to max, depending on which of min and max parameters are specified.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
        asset_ids (Sequence[int] | None): Asset IDs of equipment that this event relates to.
        asset_external_ids (Sequence[str] | None): Asset External IDs of equipment that this event relates to.
        asset_subtree_ids (Sequence[dict[str, Any]] | None): Only include events that have a related asset in a subtree rooted at any of these assetIds (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        data_set_ids (Sequence[dict[str, Any]] | None): Only include events that belong to these datasets.
        source (str | None): The source of this event.
        type (str | None): Type of the event, e.g 'failure'.
        subtype (str | None): SubType of the event, e.g 'electrical'.
        created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
    """

    def __init__(
        self,
        start_time: dict[str, Any] | TimestampRange | None = None,
        end_time: dict[str, Any] | EndTimeFilter | None = None,
        active_at_time: dict[str, Any] | TimestampRange | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: Sequence[str] | None = None,
        asset_subtree_ids: Sequence[dict[str, Any]] | None = None,
        data_set_ids: Sequence[dict[str, Any]] | None = None,
        source: str | None = None,
        type: str | None = None,
        subtype: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        external_id_prefix: str | None = None,
    ) -> None:
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
        def set(self, value: dict) -> EventUpdate:
            return self._set(value)

        def add(self, value: dict) -> EventUpdate:
            return self._add(value)

        def remove(self, value: list) -> EventUpdate:
            return self._remove(value)

    class _ListEventUpdate(CogniteListUpdate):
        def set(self, value: list) -> EventUpdate:
            return self._set(value)

        def add(self, value: list) -> EventUpdate:
            return self._add(value)

        def remove(self, value: list) -> EventUpdate:
            return self._remove(value)

    class _LabelEventUpdate(CogniteLabelUpdate):
        def add(self, value: list) -> EventUpdate:
            return self._add(value)

        def remove(self, value: list) -> EventUpdate:
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

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            # External ID is nullable, but is used in the upsert logic and thus cannot be nulled out.
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("data_set_id"),
            PropertySpec("start_time"),
            PropertySpec("end_time"),
            PropertySpec("description"),
            PropertySpec("metadata", is_container=True),
            PropertySpec("asset_ids", is_container=True),
            PropertySpec("source"),
            PropertySpec("type"),
            PropertySpec("subtype"),
        ]


class EventList(CogniteResourceList[Event], IdTransformerMixin):
    _RESOURCE = Event


class EventProperty(EnumProperty):
    asset_ids = "assetIds"
    created_time = "createdTime"
    data_set_id = "dataSetId"
    end_time = "endTime"
    id = "id"
    last_updated_time = "lastUpdatedTime"
    start_time = "startTime"
    description = "description"
    external_id = "externalId"
    metadata = "metadata"
    source = "source"
    subtype = "subtype"
    type = "type"

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return ["metadata", key]


EventPropertyLike: TypeAlias = Union[EventProperty, str, List[str]]


class SortableEventProperty(EnumProperty):
    created_time = "createdTime"
    data_set_id = "dataSetId"
    description = "description"
    end_time = "endTime"
    external_id = "externalId"
    last_updated_time = "lastUpdatedTime"
    source = "source"
    start_time = "startTime"
    subtype = "subtype"
    type = "type"
    score = "_score_"

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return ["metadata", key]


SortableEventPropertyLike: TypeAlias = Union[SortableEventProperty, str, List[str]]


class EventSort(CogniteSort):
    def __init__(
        self,
        property: SortableEventPropertyLike,
        order: Literal["asc", "desc"] = "asc",
        nulls: Literal["auto", "first", "last"] = "auto",
    ):
        super().__init__(property, order, nulls)
