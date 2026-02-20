from __future__ import annotations

from collections.abc import Sequence
from enum import auto
from typing import Any, Literal, TypeAlias

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteSort,
    CogniteUpdate,
    EnumProperty,
    ExternalIDTransformerMixin,
    IdTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
    WriteableCogniteResourceWithClientRef,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils.useful_types import SequenceNotStr


class EndTimeFilter(CogniteResource):
    """Either range between two timestamps or isNull filter condition.

    Args:
        max: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        min: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        is_null: Set to true if you want to search for data with field value not set, false to search for cases where some value is present.
    """

    def __init__(self, max: int | None = None, min: int | None = None, is_null: bool | None = None) -> None:
        if is_null is not None and (max is not None or min is not None):
            raise ValueError("is_null cannot be used with min or max values")

        self.max = max
        self.min = min
        self.is_null = is_null

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            max=resource.get("max"),
            min=resource.get("min"),
            is_null=resource.get("isNull"),
        )


class Event(WriteableCogniteResourceWithClientRef["EventWrite"]):
    """An event represents something that happened at a given interval in time, e.g a failure, a work order etc.
    This is the read version of the Event class. It is used when retrieving existing events.

    Args:
        id: A server-generated ID for the object.
        last_updated_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        external_id: The external ID provided by the client. Must be unique for the resource type.
        data_set_id: The id of the dataset this event belongs to.
        start_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        type: Type of the event, e.g. 'failure'.
        subtype: SubType of the event, e.g. 'electrical'.
        description: Textual description of the event.
        metadata: Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
        asset_ids: Asset IDs of equipment that this event relates to.
        source: The source of this event.
    """

    def __init__(
        self,
        id: int,
        last_updated_time: int,
        created_time: int,
        external_id: str | None,
        data_set_id: int | None,
        start_time: int | None,
        end_time: int | None,
        type: str | None,
        subtype: str | None,
        description: str | None,
        metadata: dict[str, str] | None,
        asset_ids: Sequence[int] | None,
        source: str | None,
    ) -> None:
        self.external_id = external_id
        self.id = id
        self.last_updated_time = last_updated_time
        self.created_time = created_time
        self.data_set_id = data_set_id
        self.start_time = start_time
        self.end_time = end_time
        self.type = type
        self.subtype = subtype
        self.description = description
        self.metadata = metadata
        self.asset_ids = asset_ids
        self.source = source

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            id=resource["id"],
            last_updated_time=resource["lastUpdatedTime"],
            created_time=resource["createdTime"],
            external_id=resource.get("externalId"),
            data_set_id=resource.get("dataSetId"),
            start_time=resource.get("startTime"),
            end_time=resource.get("endTime"),
            type=resource.get("type"),
            subtype=resource.get("subtype"),
            description=resource.get("description"),
            metadata=resource.get("metadata"),
            asset_ids=resource.get("assetIds"),
            source=resource.get("source"),
        )

    def as_write(self) -> EventWrite:
        """Returns this Event in its write version."""
        return EventWrite(
            external_id=self.external_id,
            data_set_id=self.data_set_id,
            start_time=self.start_time,
            end_time=self.end_time,
            type=self.type,
            subtype=self.subtype,
            description=self.description,
            metadata=self.metadata,
            asset_ids=self.asset_ids,
            source=self.source,
        )


class EventWrite(WriteableCogniteResource["EventWrite"]):
    """An event represents something that happened at a given interval in time, e.g a failure, a work order etc.
    This is the write version of the Event class. It is used when creating new events.

    Args:
        external_id: The external ID provided by the client. Must be unique for the resource type.
        data_set_id: The id of the dataset this event belongs to.
        start_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        type: Type of the event, e.g. 'failure'.
        subtype: SubType of the event, e.g. 'electrical'.
        description: Textual description of the event.
        metadata: Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
        asset_ids: Asset IDs of equipment that this event relates to.
        source: The source of this event.
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

    def as_write(self) -> EventWrite:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource.get("externalId"),
            data_set_id=resource.get("dataSetId"),
            start_time=resource.get("startTime"),
            end_time=resource.get("endTime"),
            type=resource.get("type"),
            subtype=resource.get("subtype"),
            description=resource.get("description"),
            metadata=resource.get("metadata"),
            asset_ids=resource.get("assetIds"),
            source=resource.get("source"),
        )


class EventFilter(CogniteFilter):
    """Filter on events filter with exact match

    Args:
        start_time: Range between two timestamps.
        end_time: Either range between two timestamps or isNull filter condition.
        active_at_time: Event is considered active from its startTime to endTime inclusive. If startTime is null, event is never active. If endTime is null, event is active from startTime onwards. activeAtTime filter will match all events that are active at some point from min to max, from min, or to max, depending on which of min and max parameters are specified.
        metadata: Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
        asset_ids: Asset IDs of equipment that this event relates to.
        asset_external_ids: Asset External IDs of equipment that this event relates to.
        asset_subtree_ids: Only include events that have a related asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        data_set_ids: Only include events that belong to these datasets.
        source: The source of this event.
        type: Type of the event, e.g 'failure'.
        subtype: SubType of the event, e.g 'electrical'.
        created_time: Range between two timestamps.
        last_updated_time: Range between two timestamps.
        external_id_prefix: Filter by this (case-sensitive) prefix for the external ID.
    """

    def __init__(
        self,
        start_time: dict[str, Any] | TimestampRange | None = None,
        end_time: dict[str, Any] | EndTimeFilter | None = None,
        active_at_time: dict[str, Any] | TimestampRange | None = None,
        metadata: dict[str, str] | None = None,
        asset_ids: Sequence[int] | None = None,
        asset_external_ids: SequenceNotStr[str] | None = None,
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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case)
        if self.end_time and isinstance(self.end_time, EndTimeFilter):
            dumped["endTime" if camel_case else "end_time"] = self.end_time.dump(camel_case)
        keys = (
            ["startTime", "activeAtTime", "createdTime", "lastUpdatedTime"]
            if camel_case
            else ["start_time", "active_at_time", "created_time", "last_updated_time"]
        )
        for key in keys:
            if key in dumped and isinstance(dumped[key], TimestampRange):
                dumped[key] = dumped[key].dump(camel_case)
        return dumped


class EventUpdate(CogniteUpdate):
    """Changes will be applied to event.

    Args:
        id: A server-generated ID for the object.
        external_id: The external ID provided by the client. Must be unique for the resource type.
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
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            # External ID is nullable, but is used in the upsert logic and thus cannot be nulled out.
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("data_set_id"),
            PropertySpec("start_time"),
            PropertySpec("end_time"),
            PropertySpec("description"),
            PropertySpec("metadata", is_object=True),
            PropertySpec("asset_ids", is_list=True),
            PropertySpec("source"),
            PropertySpec("type"),
            PropertySpec("subtype"),
        ]


class EventWriteList(CogniteResourceList[EventWrite], ExternalIDTransformerMixin):
    _RESOURCE = EventWrite


class EventList(WriteableCogniteResourceList[EventWrite, Event], IdTransformerMixin):
    _RESOURCE = Event

    def as_write(self) -> EventWriteList:
        return EventWriteList([event.as_write() for event in self.data])


class EventProperty(EnumProperty):
    asset_ids = auto()
    created_time = auto()
    data_set_id = auto()
    end_time = auto()
    id = auto()
    last_updated_time = auto()
    start_time = auto()
    description = auto()
    external_id = auto()
    metadata = auto()
    source = auto()
    subtype = auto()
    type = auto()

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return ["metadata", key]


EventPropertyLike: TypeAlias = EventProperty | str | list[str]


class SortableEventProperty(EnumProperty):
    created_time = auto()
    data_set_id = auto()
    description = auto()
    end_time = auto()
    external_id = auto()
    last_updated_time = auto()
    source = auto()
    start_time = auto()
    subtype = auto()
    type = auto()
    score = "_score_"

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return ["metadata", key]


SortableEventPropertyLike: TypeAlias = SortableEventProperty | str | list[str]


class EventSort(CogniteSort):
    def __init__(
        self,
        property: SortableEventPropertyLike,
        order: Literal["asc", "desc"] = "asc",
        nulls: Literal["auto", "first", "last"] = "auto",
    ):
        super().__init__(property, order, nulls)
