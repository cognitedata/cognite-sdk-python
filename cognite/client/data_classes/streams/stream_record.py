from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Record(CogniteResource):
    """A record returned from filter (ILA ``Record``)."""

    def __init__(
        self,
        space: str,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        properties: dict[str, Any],
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.properties = properties
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            properties=resource.get("properties", {}),
            cognite_client=cognite_client,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out = {
            "space": self.space,
            "external_id": self.external_id,
            "created_time": self.created_time,
            "last_updated_time": self.last_updated_time,
            "properties": self.properties,
        }
        return convert_all_keys_to_camel_case(out) if camel_case else out


class RecordList(CogniteResourceList[Record], ExternalIDTransformerMixin):
    _RESOURCE = Record


class SyncRecord(CogniteResource):
    """Record entry from sync (ILA ``SyncRecord``)."""

    def __init__(
        self,
        space: str,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        status: str,
        properties: dict[str, Any] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.space = space
        self.external_id = external_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.status = status
        self.properties = properties
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            space=resource["space"],
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            status=resource["status"],
            properties=resource.get("properties"),
            cognite_client=cognite_client,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {
            "space": self.space,
            "external_id": self.external_id,
            "created_time": self.created_time,
            "last_updated_time": self.last_updated_time,
            "status": self.status,
        }
        if self.properties is not None:
            out["properties"] = self.properties
        return convert_all_keys_to_camel_case(out) if camel_case else out


class SyncRecordList(CogniteResourceList[SyncRecord], ExternalIDTransformerMixin):
    _RESOURCE = SyncRecord


class RecordsFilterResponse(CogniteObject):
    """``POST .../records/filter`` response."""

    def __init__(self, items: RecordList, typing: dict[str, Any] | None = None) -> None:
        self.items = items
        self.typing = typing

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        items = RecordList._load(resource.get("items", []), cognite_client=cognite_client)
        return cls(items=items, typing=resource.get("typing"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {"items": self.items.dump(camel_case=camel_case)}
        if self.typing is not None:
            out["typing"] = self.typing
        return convert_all_keys_to_camel_case(out) if camel_case else out


class RecordsSyncResponse(CogniteObject):
    """``POST .../records/sync`` response."""

    def __init__(
        self,
        items: SyncRecordList,
        next_cursor: str,
        has_next: bool,
        typing: dict[str, Any] | None = None,
    ) -> None:
        self.items = items
        self.next_cursor = next_cursor
        self.has_next = has_next
        self.typing = typing

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        items = SyncRecordList._load(resource.get("items", []), cognite_client=cognite_client)
        return cls(
            items=items,
            next_cursor=resource["nextCursor"],
            has_next=resource["hasNext"],
            typing=resource.get("typing"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {
            "items": self.items.dump(camel_case=camel_case),
            "nextCursor": self.next_cursor,
            "hasNext": self.has_next,
        }
        if self.typing is not None:
            out["typing"] = self.typing
        return convert_all_keys_to_camel_case(out) if camel_case else out


class RecordsAggregateResponse(CogniteObject):
    """``POST .../records/aggregate`` response."""

    def __init__(self, aggregates: dict[str, Any], typing: dict[str, Any] | None = None) -> None:
        self.aggregates = aggregates
        self.typing = typing

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(aggregates=resource.get("aggregates", {}), typing=resource.get("typing"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {"aggregates": self.aggregates}
        if self.typing is not None:
            out["typing"] = self.typing
        return convert_all_keys_to_camel_case(out) if camel_case else out


class RecordsDeleteResponse(CogniteObject):
    """``POST .../records/delete`` — empty object means full success."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(data=resource)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return convert_all_keys_to_camel_case(self._data) if camel_case else self._data


class RecordsIngestResponse(CogniteObject):
    """``POST .../records`` (ingest/upsert) JSON body — often ``{}`` on success."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(data=resource)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return convert_all_keys_to_camel_case(self._data) if camel_case else self._data

    @property
    def is_empty_success(self) -> bool:
        return self._data == {}
