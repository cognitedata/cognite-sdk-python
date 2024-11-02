from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from enum import auto
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObject,
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
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class EndTimeFilter(CogniteObject):
    """Either range between two timestamps or isNull filter condition.

    Args:
        max (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        min (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        is_null (bool | None): Set to true if you want to search for data with field value not set, false to search for cases where some value is present.
        **_ (Any): No description.
    """

    def __init__(self, max: int | None = None, min: int | None = None, is_null: bool | None = None, **_: Any) -> None:
        if is_null is not None and (max is not None or min is not None):
            raise ValueError("is_null cannot be used with min or max values")

        self.max = max
        self.min = min
        self.is_null = is_null


class EventCore(WriteableCogniteResource["EventWrite"], ABC):
    """An event represents something that happened at a given interval in time, e.g a failure, a work order etc.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        data_set_id (int | None): The id of the dataset this event belongs to.
        start_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        type (str | None): Type of the event, e.g. 'failure'.
        subtype (str | None): SubType of the event, e.g. 'electrical'.
        description (str | None): Textual description of the event.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
        asset_ids (Sequence[int] | None): Asset IDs of equipment that this event relates to.
        source (str | None): The source of this event.
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


class Event(EventCore):
    """An event represents something that happened at a given interval in time, e.g a failure, a work order etc.
    This is the reading version of the Event class. It is used when retrieving existing events.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        data_set_id (int | None): The id of the dataset this event belongs to.
        start_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        type (str | None): Type of the event, e.g. 'failure'.
        subtype (str | None): SubType of the event, e.g. 'electrical'.
        description (str | None): Textual description of the event.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
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
        super().__init__(
            external_id=external_id,
            data_set_id=data_set_id,
            start_time=start_time,
            end_time=end_time,
            type=type,
            subtype=subtype,
            description=description,
            metadata=metadata,
            asset_ids=asset_ids,
            source=source,
        )
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> EventWrite:
        """Returns this Event in its writing version."""
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


class EventWrite(EventCore):
    """An event represents something that happened at a given interval in time, e.g a failure, a work order etc.
    This is the writing version of the Event class. It is used when creating new events.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        data_set_id (int | None): The id of the dataset this event belongs to.
        start_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        end_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        type (str | None): Type of the event, e.g. 'failure'.
        subtype (str | None): SubType of the event, e.g. 'electrical'.
        description (str | None): Textual description of the event.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
        asset_ids (Sequence[int] | None): Asset IDs of equipment that this event relates to.
        source (str | None): The source of this event.
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
        super().__init__(
            external_id=external_id,
            data_set_id=data_set_id,
            start_time=start_time,
            end_time=end_time,
            type=type,
            subtype=subtype,
            description=description,
            metadata=metadata,
            asset_ids=asset_ids,
            source=source,
        )

    def as_write(self) -> EventWrite:
        """Returns self."""
        return self


class EventFilter(CogniteFilter):
    """Filter on events filter with exact match

    Args:
        start_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        end_time (dict[str, Any] | EndTimeFilter | None): Either range between two timestamps or isNull filter condition.
        active_at_time (dict[str, Any] | TimestampRange | None): Event is considered active from its startTime to endTime inclusive. If startTime is null, event is never active. If endTime is null, event is active from startTime onwards. activeAtTime filter will match all events that are active at some point from min to max, from min, or to max, depending on which of min and max parameters are specified.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 128000 bytes, up to 256 key-value pairs, of total size at most 200000.
        asset_ids (Sequence[int] | None): Asset IDs of equipment that this event relates to.
        asset_external_ids (SequenceNotStr[str] | None): Asset External IDs of equipment that this event relates to.
        asset_subtree_ids (Sequence[dict[str, Any]] | None): Only include events that have a related asset in a subtree rooted at any of these assetIds. If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
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
        return EventWriteList([event.as_write() for event in self.data], cognite_client=self._get_cognite_client())


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
