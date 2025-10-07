from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteObject, CogniteResource, CogniteResourceList
from cognite.client.data_classes.data_modeling.instances import Properties, SourceData
from cognite.client.utils import datetime_to_ms
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass(frozen=True)
class RecordId:
    space: str
    external_id: str

    def dump(self, camel_case: bool = True) -> dict[str, str]:
        return {"space": self.space, "externalId" if camel_case else "external_id": self.external_id}

    @classmethod
    def load(cls, data: dict[str, str] | tuple[str, str] | Self) -> Self:
        if isinstance(data, cls):
            return data
        elif isinstance(data, tuple) and len(data) == 2:
            return cls(*data)
        elif isinstance(data, dict):
            if "externalId" in data:
                return cls(space=data["space"], external_id=data["externalId"])
            if "external_id" in data:
                return cls(space=data["space"], external_id=data["external_id"])
        raise KeyError(f"Cannot load {data} into {cls}, missing 'externalId' or 'external_id' key")

    def as_tuple(self) -> tuple[str, str]:
        return self.space, self.external_id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(space={self.space!r}, external_id={self.external_id!r})"


@dataclass
class RecordIngest(CogniteObject):
    id: RecordId
    sources: list[SourceData]

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "space": self.id.space,
            "externalId" if camel_case else "external_id": self.id.external_id,
            "sources": [s.dump(camel_case) for s in self.sources],
        }

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        id = RecordId(space=resource["space"], external_id=resource["externalId"])
        sources = [SourceData._load(source, cognite_client) for source in resource["sources"]]
        return cls(id=id, sources=sources)


@dataclass
class Record(CogniteResource):
    id: RecordId
    created_time: int
    last_updated_time: int
    properties: Properties

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {
            "space": self.id.space,
            "external_id": self.id.external_id,
            "created_time": self.created_time,
            "last_updated_time": self.last_updated_time,
            "properties": self.properties.dump(),
        }
        if camel_case:
            return convert_all_keys_to_camel_case(out)
        return out

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        id = RecordId.load(resource)
        properties = Properties.load(resource.get("properties", {}))
        return cls(
            id=id,
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            properties=properties,
        )


class RecordList(CogniteResourceList[Record]):
    _RESOURCE = Record


class RecordListWithCursor(RecordList):
    def __init__(self, resources: list[Record], cursor: str | None, has_next: bool | None = None) -> None:
        super().__init__(resources)
        self.cursor = cursor
        self.has_next = has_next


@dataclass
class LastUpdatedRange(CogniteObject):
    gt: int | datetime | None = None
    gte: int | datetime | None = None
    lt: int | datetime | None = None
    lte: int | datetime | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.gt is not None:
            out["gt"] = datetime_to_ms(self.gt) if isinstance(self.gt, datetime) else self.gt
        if self.gte is not None:
            out["gte"] = datetime_to_ms(self.gte) if isinstance(self.gte, datetime) else self.gte
        if self.lt is not None:
            out["lt"] = datetime_to_ms(self.lt) if isinstance(self.lt, datetime) else self.lt
        if self.lte is not None:
            out["lte"] = datetime_to_ms(self.lte) if isinstance(self.lte, datetime) else self.lte
        return out
